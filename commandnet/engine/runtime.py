import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Type, Iterable, Dict, Any, Union, List

from ..core.models import Event
from ..core.node import (
    Node,
    Parallel,
    Schedule,
    Wait,
    Target,
    ParallelTask,
    Call,
    Interrupt,
)
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
        self._active_tasks: Dict[str, asyncio.Task] = {}
        self._registry: Dict[str, Type[Node]] = {n.get_node_name(): n for n in nodes}

    def _dump_ctx(self, ctx: Any) -> dict:
        return ctx.model_dump() if hasattr(ctx, "model_dump") else ctx

    def _get_path(self, obj: Any, path: str) -> Any:
        if isinstance(obj, dict):
            return obj.get(path)
        return getattr(obj, path)

    # --- CORE WORKER LOGIC ---

    async def process_event(self, event: Event):
        # 1. Internal Control (Cross-worker Hard Cancel)
        if event.node_name == "__CONTROL__":
            action = (event.payload or {}).get("action")
            if action == "HARD_CANCEL" and event.subject_id in self._active_tasks:
                self._active_tasks[event.subject_id].cancel()
            return

        # 2. Check cancellation status
        if await self.db.is_cancelled(event.subject_id):
            return

        # 3. Task Tracking for Local Hard Cancel
        task = asyncio.create_task(self._run_node_logic(event))
        self._active_tasks[event.subject_id] = task
        try:
            await task
        except asyncio.CancelledError:
            self.logger.warning(f"Task {event.subject_id} hard-cancelled.")
        finally:
            self._active_tasks.pop(event.subject_id, None)

    async def _run_node_logic(self, event: Event):
        subject_id = event.subject_id
        # We allow "AWAITING_CALL" because an subject wakes up from that state when a 'Call' resolves
        node_name, ctx_dict = await self.db.lock_and_load(subject_id)
        if not node_name or (
            node_name != event.node_name and node_name != "AWAITING_CALL"
        ):
            if node_name:
                await self.db.unlock_subject(subject_id)
            return

        try:
            node_cls = self._registry.get(event.node_name)
            if not node_cls:
                raise RuntimeError(f"Node '{event.node_name}' not found.")

            ctx_type = GraphAnalyzer.get_context_type(node_cls)
            payload_type = GraphAnalyzer.get_payload_type(node_cls)

            ctx = (
                ctx_type.model_validate(ctx_dict)
                if hasattr(ctx_type, "model_validate")
                else ctx_dict
            )
            payload = (
                payload_type.model_validate(event.payload)
                if (event.payload and hasattr(payload_type, "model_validate"))
                else event.payload
            )

            # Support Soft-Cancel checking inside Node.run
            if hasattr(ctx, "is_cancelled"):
                ctx.is_cancelled = await self.db.is_cancelled(subject_id)

            start_t = asyncio.get_event_loop().time()
            result = await node_cls().run(ctx, payload)

            await self._apply_target(
                subject_id,
                ctx,
                result,
                (asyncio.get_event_loop().time() - start_t) * 1000,
            )
        except Exception as e:
            await self.observer.on_error(subject_id, event.node_name, e)
            raise
        finally:
            await self.db.unlock_subject(subject_id)

    # --- RECURSIVE TARGET RESOLVER ---

    async def _apply_target(
        self,
        subject_id: str,
        context: Any,
        target: Target,
        duration: float = 0.0,
        payload: Any = None,
    ):
        # 1. Interrupt (Cancellation)
        if isinstance(target, Interrupt):
            await self.cancel_subject(target.subject_id, target.hard)
            return

        # 2. Call (Idempotent Await)
        if isinstance(target, Call):
            is_leader = await self.db.add_call_waiter(
                target.idempotency_key,
                subject_id,
                target.resume_action,
                self._dump_ctx(context),
            )
            await self.observer.on_transition(
                subject_id, "RUN", f"CALLING:{target.node_cls.get_node_name()}", duration
            )
            await self.db.save_state(
                subject_id, "AWAITING_CALL", self._dump_ctx(context), None
            )
            if is_leader:
                await self.trigger_subject(
                    f"call#{target.idempotency_key}",
                    target.node_cls,
                    context,
                    target.payload,
                )
            return

        # 3. Terminal State
        if target is None:
            await self.observer.on_transition(subject_id, "RUN", "TERMINAL", duration)
            ctx_dict = self._dump_ctx(context)

            # Resolve Callers if this was a shared Virtual subject
            if subject_id.startswith("call#"):
                key = subject_id.split("#")[1]
                waiters = await self.db.resolve_call_group(key)
                for w in waiters:
                    # Resume waiters using the final context of the shared node as payload
                    await self._apply_target(
                        w["subject_id"],
                        w["context"],
                        w["resume_target"],
                        payload=ctx_dict,
                    )

            # Parallel Sub-task Completion Logic
            if "#" in subject_id and not subject_id.startswith("call#"):
                parent_id = subject_id.split("#")[0]
                await self.db.save_sub_state(
                    subject_id, parent_id, "TERMINAL", ctx_dict, None
                )
                join_node_name = await self.db.register_sub_task_completion(subject_id)
                if join_node_name:
                    await self._trigger_recompose(parent_id, join_node_name)
            else:
                await self.db.save_state(subject_id, "TERMINAL", ctx_dict, None)
            return

        # 4. Standard Node Transition
        if isinstance(target, type) and issubclass(target, Node):
            node_name = target.get_node_name()
            await self.observer.on_transition(subject_id, "RUN", node_name, duration)

            p_load = payload.model_dump() if hasattr(payload, "model_dump") else payload
            evt = Event(subject_id=subject_id, node_name=node_name, payload=p_load)
            ctx_dict = self._dump_ctx(context)

            if "#" in subject_id:
                await self.db.save_sub_state(
                    subject_id, subject_id.split("#")[0], node_name, ctx_dict, evt
                )
            else:
                await self.db.save_state(subject_id, node_name, ctx_dict, evt)

            await self.bus.publish(evt)
            return

        # 5. Wait / Signal Parking
        if isinstance(target, Wait):
            actual_id = subject_id
            actual_ctx = context
            if target.sub_context_path and "#" not in subject_id:
                actual_id = f"{subject_id}#{target.sub_context_path}"
                actual_ctx = self._get_path(context, target.sub_context_path)

            await self.observer.on_transition(
                actual_id, "RUN", f"WAIT:{target.signal_id}", duration
            )
            await self.db.park_subject(
                actual_id,
                target.signal_id,
                target.resume_action,
                self._dump_ctx(actual_ctx),
            )
            return

        # 6. Parallel Fan-out
        if isinstance(target, Parallel):
            join_name = target.join_node.get_node_name() if target.join_node else "FORK"
            await self.observer.on_transition(
                subject_id, "RUN", f"PARALLEL:{join_name}", duration
            )

            if target.join_node:
                await self.db.create_task_group(
                    subject_id, join_name, len(target.branches)
                )

            for branch in target.branches:
                # Normalize branch to ParallelTask without stripping Wait wrappers
                if isinstance(branch, ParallelTask):
                    task = branch
                else:
                    # If it's a Wait or a Node class, wrap it but keep the action intact
                    path = (
                        branch.sub_context_path
                        if isinstance(branch, Wait) and branch.sub_context_path
                        else "default"
                    )
                    task = ParallelTask(action=branch, sub_context_path=path)

                sub_ctx = self._get_path(context, task.sub_context_path)
                await self._apply_target(
                    f"{subject_id}#{task.sub_context_path}",
                    sub_ctx,
                    task.action,
                    payload=task.payload,
                )

            if target.join_node:
                await self.db.save_state(
                    subject_id, "WAITING_FOR_JOIN", self._dump_ctx(context), None
                )
            else:
                await self._apply_target(subject_id, context, None)
            return

        # 7. Schedule (Delayed Execution)
        if isinstance(target, Schedule):
            if not (
                isinstance(target.action, type) and issubclass(target.action, Node)
            ):
                raise TypeError("Schedule.action must be a Node class.")

            target_node_name = target.action.get_node_name()
            await self.observer.on_transition(
                subject_id, "RUN", f"SCHEDULED:{target_node_name}", duration
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
                subject_id=subject_id,
                node_name=target_node_name,
                payload=p_load,
                run_at=run_at_dt.isoformat(),
                idempotency_key=target.idempotency_key,
            )

            await self.db.schedule_event(scheduled_evt)
            await self.db.save_state(
                subject_id, target_node_name, self._dump_ctx(context), None
            )
            return

    # --- EXTERNAL CONTROL & SIGNALS ---

    async def signal_node(self, subject_id: str, signal_id: str, payload: Any = None):
        """Resumes subject and triggers its specific on_signal instance method."""
        node_name, ctx_dict = await self.db.lock_and_load(subject_id)
        if not node_name:
            return
        try:
            node_cls = self._registry.get(node_name)
            node_inst = node_cls()
            ctx_type = GraphAnalyzer.get_context_type(node_cls)
            ctx = (
                ctx_type.model_validate(ctx_dict)
                if hasattr(ctx_type, "model_validate")
                else ctx_dict
            )

            result = await node_inst.on_signal(ctx, signal_id, payload)
            await self._apply_target(subject_id, ctx, result)
        finally:
            await self.db.unlock_subject(subject_id)

    async def release_signal(self, signal_id: str, payload: Any = None):
        """Standard mass-resume for parked subjects."""
        waiters = await self.db.get_and_clear_waiters(signal_id)
        for waiter in waiters:
            subject_id = waiter["subject_id"]
            await self.db.lock_and_load(subject_id)
            try:
                await self._apply_target(
                    subject_id=subject_id,
                    context=waiter["context"],
                    target=waiter["next_target"],
                    payload=payload,
                )
            finally:
                await self.db.unlock_subject(subject_id)

    async def cancel_subject(self, subject_id: str, hard: bool = True):
        await self.db.set_cancel_flag(subject_id, hard)
        if hard:
            if subject_id in self._active_tasks:
                self._active_tasks[subject_id].cancel()
            await self.bus.publish(
                Event(
                    subject_id=subject_id,
                    node_name="__CONTROL__",
                    payload={"action": "HARD_CANCEL"},
                )
            )

    # --- LIFECYCLE & HELPERS ---

    async def _trigger_recompose(self, parent_id: str, join_node_name: str):
        await self.db.lock_and_load(parent_id)
        merged_ctx_dict = await self.db.recompose_parent(parent_id)
        await self._apply_target(
            parent_id, merged_ctx_dict, self._registry[join_node_name]
        )

    async def start_worker(self, poll_interval: float = 1.0):
        await self.bus.subscribe(self.process_event)
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(poll_interval))

    async def _scheduler_loop(self, poll_interval: float):
        while True:
            try:
                due = await self.db.pop_due_events()
                for evt in due:
                    await self.bus.publish(evt)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Scheduler: {e}")
            await asyncio.sleep(poll_interval)

    async def trigger_subject(
        self,
        subject_id: str,
        start_node: Type[Node],
        initial_context: Any,
        payload: Any = None,
    ):
        node_name = start_node.get_node_name()
        p_load = payload.model_dump() if hasattr(payload, "model_dump") else payload
        evt = Event(subject_id=subject_id, node_name=node_name, payload=p_load)
        await self.db.save_state(
            subject_id, node_name, self._dump_ctx(initial_context), evt
        )
        await self.bus.publish(evt)

    async def stop(self):
        if self._scheduler_task:
            self._scheduler_task.cancel()
        for task in self._active_tasks.values():
            task.cancel()

    def validate_graph(self, start_node: Type[Node]):
        return GraphAnalyzer.validate(start_node, self._registry)

