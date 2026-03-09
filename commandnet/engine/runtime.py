import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Type
from pydantic import BaseModel

from ..core.models import Event
from ..core.node import Node, NODE_REGISTRY, Parallel, Schedule
from ..core.graph import GraphAnalyzer
from ..interfaces.persistence import Persistence
from ..interfaces.event_bus import EventBus
from ..interfaces.observer import Observer

class Engine:
    def __init__(self, persistence: Persistence, event_bus: EventBus, observer: Optional[Observer] = None):
        self.db = persistence
        self.bus = event_bus
        self.observer = observer or Observer()
        self.logger = logging.getLogger("CommandNet")
        self._scheduler_task: Optional[asyncio.Task] = None

    async def start_worker(self, poll_interval: float = 1.0):
        await self.bus.subscribe(self.process_event)
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(poll_interval))
        self.logger.info("Worker and Scheduler started.")

    async def _scheduler_loop(self, poll_interval: float):
        while True:
            try:
                due_events = await self.db.pop_due_events()
                for evt in due_events:
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

    async def trigger_agent(self, agent_id: str, start_node: Type[Node], initial_context: BaseModel, payload: Optional[BaseModel] = None):
        node_name = start_node.get_node_name()
        start_event = Event(
            agent_id=agent_id, 
            node_name=node_name,
            payload=payload.model_dump() if hasattr(payload, "model_dump") else payload
        )
        await self.db.save_state(agent_id, node_name, initial_context.model_dump(), start_event)
        await self.bus.publish(start_event)

    async def process_event(self, event: Event):
        start_time = asyncio.get_event_loop().time()
        
        current_node_name, ctx_dict = await self.db.load_and_lock_agent(event.agent_id)
        if not current_node_name:
            return

        locked = True
        try:
            if current_node_name != event.node_name:
                if "#" in event.agent_id:
                    parent_id = event.agent_id.split("#")[0]
                    await self.db.save_sub_state(event.agent_id, parent_id, current_node_name, ctx_dict, None)
                else:
                    await self.db.save_state(event.agent_id, current_node_name, ctx_dict, None)
                locked = False
                return

            node_cls = NODE_REGISTRY.get(current_node_name)
            if not node_cls:
                raise RuntimeError(f"Node '{current_node_name}' missing from registry.")
            
            ctx_type = GraphAnalyzer.get_context_type(node_cls)
            payload_type = GraphAnalyzer.get_payload_type(node_cls)
            
            ctx = ctx_type.model_validate(ctx_dict) if issubclass(ctx_type, BaseModel) else ctx_dict
            
            payload = None
            if event.payload is not None:
                if isinstance(payload_type, type) and issubclass(payload_type, BaseModel):
                    payload = payload_type.model_validate(event.payload)
                else:
                    payload = event.payload

            node_instance = node_cls()
            result = await node_instance.run(ctx, payload)
            duration = (asyncio.get_event_loop().time() - start_time) * 1000
            
            if isinstance(result, Parallel):
                await self._handle_parallel_start(event.agent_id, ctx, result, duration)
            elif isinstance(result, Schedule):
                await self._handle_schedule(event.agent_id, current_node_name, ctx, result, duration)
            elif result:
                await self._handle_transition(event.agent_id, current_node_name, result, ctx, duration)
            else:
                await self._handle_terminal(event.agent_id, current_node_name, ctx, duration)
                
            locked = False # Success! The handle_* methods performed a save_state, which released the lock.
            
        except Exception as e:
            await self.observer.on_error(event.agent_id, current_node_name, e)
            raise
        finally:
            if locked:
                # Deadlock prevention: Exception occurred before state was saved
                await self.db.unlock_agent(event.agent_id)

    async def _handle_transition(self, agent_id: str, from_node: str, next_node_cls: Type[Node], ctx: BaseModel, duration: float):
        next_name = next_node_cls.get_node_name()
        await self.observer.on_transition(agent_id, from_node, next_name, duration)
        
        next_event = Event(agent_id=agent_id, node_name=next_name)
        await self.db.save_state(agent_id, next_name, ctx.model_dump(), next_event)
        await self.bus.publish(next_event)

    async def _handle_parallel_start(self, parent_id: str, parent_ctx: BaseModel, parallel: Parallel, duration: float):
        join_name = parallel.join_node.get_node_name()
        await self.observer.on_transition(parent_id, "ParallelStart", join_name, duration)
        
        await self.db.create_task_group(parent_id=parent_id, join_node_name=join_name, task_count=len(parallel.branches))
        
        for task in parallel.branches:
            if not hasattr(parent_ctx, task.sub_context_path):
                raise RuntimeError(f"Context missing path: '{task.sub_context_path}'. Cannot fan out.")
                
            sub_ctx = getattr(parent_ctx, task.sub_context_path)
            sub_id = f"{parent_id}#{task.sub_context_path}"
            
            evt = Event(
                agent_id=sub_id, 
                node_name=task.node_cls.get_node_name(),
                payload=task.payload.model_dump() if hasattr(task.payload, "model_dump") else task.payload
            )
            await self.db.save_sub_state(sub_id, parent_id, task.node_cls.get_node_name(), sub_ctx.model_dump(), evt)
            await self.bus.publish(evt)
            
        await self.db.save_state(parent_id, "WAITING_FOR_JOIN", parent_ctx.model_dump(), None)

    async def _handle_terminal(self, agent_id: str, from_node: str, ctx: BaseModel, duration: float):
        await self.observer.on_transition(agent_id, from_node, "TERMINAL", duration)
        
        if "#" in agent_id:
            parent_id = agent_id.split("#")[0]
            await self.db.save_sub_state(agent_id, parent_id, "TERMINAL", ctx.model_dump(), None)
            
            join_node_name = await self.db.register_sub_task_completion(agent_id)
            if join_node_name:
                await self._trigger_recompose(parent_id, join_node_name)
        else:
            await self.db.save_state(agent_id, "TERMINAL", ctx.model_dump(), None)

    async def _handle_schedule(self, agent_id: str, from_node: str, ctx: BaseModel, schedule: Schedule, duration: float):
        target_name = schedule.node_cls.get_node_name()
        await self.observer.on_transition(agent_id, from_node, f"SCHEDULED:{target_name}", duration)
        
        run_at_dt = datetime.now(timezone.utc) + timedelta(seconds=schedule.delay_seconds)
        
        evt = Event(
            agent_id=agent_id, 
            node_name=target_name,
            payload=schedule.payload.model_dump() if hasattr(schedule.payload, "model_dump") else schedule.payload,
            run_at=run_at_dt.isoformat(),
            idempotency_key=schedule.idempotency_key
        )
        
        scheduled = await self.db.schedule_event(evt)
        next_node = target_name if scheduled else "TERMINAL"
        
        if "#" in agent_id:
            parent_id = agent_id.split("#")[0]
            await self.db.save_sub_state(agent_id, parent_id, next_node, ctx.model_dump(), None)
            
            if not scheduled:
                join_node_name = await self.db.register_sub_task_completion(agent_id)
                if join_node_name:
                    await self._trigger_recompose(parent_id, join_node_name)
        else:
            await self.db.save_state(agent_id, next_node, ctx.model_dump(), None)

    async def _trigger_recompose(self, parent_id: str, join_node_name: str):
        await self.db.load_and_lock_agent(parent_id)
        merged_ctx_dict = await self.db.recompose_parent(parent_id)
        join_event = Event(agent_id=parent_id, node_name=join_node_name)
        await self.db.save_state(parent_id, join_node_name, merged_ctx_dict, join_event)
        await self.bus.publish(join_event)
