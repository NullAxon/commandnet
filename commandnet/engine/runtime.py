import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Type, Iterable, Dict, Any, Union
from pydantic import BaseModel

from ..core.models import Event
from ..core.node import Node, Parallel, Schedule, Wait, Target, ParallelTask
from ..core.graph import GraphAnalyzer
from ..interfaces.persistence import Persistence
from ..interfaces.event_bus import EventBus
from ..interfaces.observer import Observer


class Engine:
    def __init__(
        self,
        persistence: Persistence,
        event_bus: EventBus,
        nodes: Iterable[Type[Node]],
        observer: Optional[Observer] = None,
    ):
        self.db = persistence
        self.bus = event_bus
        self.observer = observer or Observer()
        self.logger = logging.getLogger("CommandNet")
        self._scheduler_task: Optional[asyncio.Task] = None

        self._registry: Dict[str, Type[Node]] = {n.get_node_name(): n for n in nodes}

    # --- HELPER UTILITIES ---

    def _dump_ctx(self, ctx: Any) -> dict:
        """Helper to safely dump context whether it's a model or a dict."""
        if hasattr(ctx, "model_dump"):
            return ctx.model_dump()
        return ctx

    def _get_path(self, obj: Any, path: str) -> Any:
        """Helper to get a value from an object or a dict."""
        if isinstance(obj, dict):
            return obj.get(path)
        return getattr(obj, path)

    # --- CORE RECURSIVE RESOLVER ---

    async def _apply_target(
        self,
        agent_id: str,
        context: Any,
        target: Target,
        duration: float = 0.0,
        payload: Any = None,
    ):
        # 1. Terminal State
        if target is None:
            await self.observer.on_transition(agent_id, "RUN", "TERMINAL", duration)
            ctx_dict = self._dump_ctx(context)
            if "#" in agent_id:
                parent_id = agent_id.split("#")[0]
                await self.db.save_sub_state(
                    agent_id, parent_id, "TERMINAL", ctx_dict, None
                )
                join_node_name = await self.db.register_sub_task_completion(agent_id)
                if join_node_name:
                    await self._trigger_recompose(parent_id, join_node_name)
            else:
                await self.db.save_state(agent_id, "TERMINAL", ctx_dict, None)
            return

        # 2. Node Class Transition
        if isinstance(target, type) and issubclass(target, Node):
            node_name = target.get_node_name()
            await self.observer.on_transition(agent_id, "RUN", node_name, duration)

            p_load = payload.model_dump() if hasattr(payload, "model_dump") else payload
            evt = Event(agent_id=agent_id, node_name=node_name, payload=p_load)
            ctx_dict = self._dump_ctx(context)

            if "#" in agent_id:
                await self.db.save_sub_state(
                    agent_id, agent_id.split("#")[0], node_name, ctx_dict, evt
                )
            else:
                await self.db.save_state(agent_id, node_name, ctx_dict, evt)

            await self.bus.publish(evt)
            return

        # 3. Wait Directive
        if isinstance(target, Wait):
            actual_id = agent_id
            actual_ctx = context
            if target.sub_context_path and "#" not in agent_id:
                actual_id = f"{agent_id}#{target.sub_context_path}"
                actual_ctx = self._get_path(context, target.sub_context_path)

            await self.observer.on_transition(
                actual_id, "RUN", f"WAIT:{target.signal_id}", duration
            )
            await self.db.park_agent(
                actual_id,
                target.signal_id,
                target.resume_action,
                self._dump_ctx(actual_ctx),
            )
            return

        # 4. Parallel Directive
        if isinstance(target, Parallel):
            join_name = target.join_node.get_node_name() if target.join_node else "FORK"
            await self.observer.on_transition(
                agent_id, "RUN", f"PARALLEL:{join_name}", duration
            )

            if target.join_node:
                await self.db.create_task_group(
                    agent_id, join_name, len(target.branches)
                )

            for branch in target.branches:
                task = (
                    branch
                    if isinstance(branch, ParallelTask)
                    else ParallelTask(
                        action=branch,
                        sub_context_path=branch.sub_context_path,  # type: ignore
                    )
                )
                sub_ctx = self._get_path(context, task.sub_context_path)
                await self._apply_target(
                    f"{agent_id}#{task.sub_context_path}",
                    sub_ctx,
                    task.action,
                    duration=0,
                    payload=task.payload,
                )

            if target.join_node:
                await self.db.save_state(
                    agent_id, "WAITING_FOR_JOIN", self._dump_ctx(context), None
                )
            else:
                await self._apply_target(agent_id, context, None)
            return

        # 5. Schedule Directive
        if isinstance(target, Schedule):
            if isinstance(target.action, type) and issubclass(target.action, Node):
                target_node_name = target.action.get_node_name()
            else:
                raise TypeError("Schedule.action must be a Node class.")

            await self.observer.on_transition(
                agent_id, "RUN", f"SCHEDULED:{target_node_name}", duration
            )
            run_at_dt = datetime.now(timezone.utc) + timedelta(
                seconds=target.delay_seconds
            )
            p_load = (
                target.payload.model_dump()
                if hasattr(target.payload, "model_dump")
                else target.payload
            )

            scheduled_evt = Event(
                agent_id=agent_id,
                node_name=target_node_name,
                payload=p_load,
                run_at=run_at_dt.isoformat(),
                idempotency_key=target.idempotency_key,
            )

            await self.db.schedule_event(scheduled_evt)
            await self.db.save_state(
                agent_id, target_node_name, self._dump_ctx(context), None
            )
            return

    # --- WORKER & EXTERNAL TRIGGERS ---

    async def process_event(self, event: Event):
        start_time = asyncio.get_event_loop().time()
        agent_id = event.agent_id
        current_node_name, ctx_dict = await self.db.load_and_lock_agent(agent_id)

        if not current_node_name:
            return

        try:
            if current_node_name != event.node_name:
                return

            node_cls = self._registry.get(current_node_name)
            if not node_cls:
                raise RuntimeError(f"Node '{current_node_name}' not found.")

            ctx_type = GraphAnalyzer.get_context_type(node_cls)
            payload_type = GraphAnalyzer.get_payload_type(node_cls)

            ctx = (
                ctx_type.model_validate(ctx_dict)
                if issubclass(ctx_type, BaseModel)
                else ctx_dict
            )
            payload = (
                payload_type.model_validate(event.payload)
                if (event.payload and issubclass(payload_type, BaseModel))
                else event.payload
            )

            result = await node_cls().run(ctx, payload)
            await self._apply_target(
                agent_id,
                ctx,
                result,
                (asyncio.get_event_loop().time() - start_time) * 1000,
            )

        except Exception as e:
            await self.observer.on_error(agent_id, current_node_name, e)
            raise
        finally:
            await self.db.unlock_agent(agent_id)

    async def release_signal(self, signal_id: str, payload: Any = None):
        waiters = await self.db.get_and_clear_waiters(signal_id)
        for waiter in waiters:
            agent_id = waiter["agent_id"]
            await self.db.load_and_lock_agent(agent_id)
            try:
                await self._apply_target(
                    agent_id=agent_id,
                    context=waiter["context"],
                    target=waiter["next_target"],
                    payload=payload,
                )
            finally:
                await self.db.unlock_agent(agent_id)

    # --- LIFECYCLE METHODS ---

    async def start_worker(self, poll_interval: float = 1.0):
        await self.bus.subscribe(self.process_event)
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(poll_interval))
        self.logger.info("Worker started.")

    async def _scheduler_loop(self, poll_interval: float):
        while True:
            try:
                due = await self.db.pop_due_events()
                for evt in due:
                    await self.bus.publish(evt)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Scheduler Error: {e}")
            await asyncio.sleep(poll_interval)

    async def stop(self):
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

    async def trigger_agent(
        self,
        agent_id: str,
        start_node: Type[Node],
        initial_context: BaseModel,
        payload: Optional[BaseModel] = None,
    ):
        node_name = start_node.get_node_name()
        evt = Event(
            agent_id=agent_id,
            node_name=node_name,
            payload=payload.model_dump() if hasattr(payload, "model_dump") else payload,
        )
        await self.db.save_state(agent_id, node_name, initial_context.model_dump(), evt)
        await self.bus.publish(evt)

    async def _trigger_recompose(self, parent_id: str, join_node_name: str):
        await self.db.load_and_lock_agent(parent_id)
        merged_ctx_dict = await self.db.recompose_parent(parent_id)
        await self._apply_target(
            parent_id, merged_ctx_dict, self._registry[join_node_name]
        )

    def validate_graph(self, start_node: Type[Node]):
        return GraphAnalyzer.validate(start_node, self._registry)
