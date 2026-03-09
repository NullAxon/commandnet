import asyncio
import logging
from typing import Optional, Type
from pydantic import BaseModel

from ..core.models import Event
from ..core.node import Node, NODE_REGISTRY, Parallel
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

    async def start_worker(self):
        await self.bus.subscribe(self.process_event)

    async def trigger_agent(self, agent_id: str, start_node: Type[Node], initial_context: BaseModel, payload: Optional[BaseModel] = None):
        start_event = Event(
            agent_id=agent_id, 
            node_name=start_node.__name__,
            payload=payload.model_dump() if payload else None
        )
        await self.db.save_state(agent_id, start_node.__name__, initial_context.model_dump(), start_event)
        await self.bus.publish(start_event)

    async def process_event(self, event: Event):
        start_time = asyncio.get_event_loop().time()
        
        current_node_name, ctx_dict = await self.db.load_and_lock_agent(event.agent_id)
        if not current_node_name or current_node_name != event.node_name:
            return

        node_cls = NODE_REGISTRY.get(current_node_name)
        if not node_cls:
            raise RuntimeError(f"Node '{current_node_name}' missing from registry.")
        
        ctx_type = GraphAnalyzer.get_context_type(node_cls)
        payload_type = GraphAnalyzer.get_payload_type(node_cls)
        
        ctx = ctx_type.model_validate(ctx_dict) if issubclass(ctx_type, BaseModel) else ctx_dict
        payload = None
        if event.payload and payload_type and issubclass(payload_type, BaseModel):
            payload = payload_type.model_validate(event.payload)

        node_instance = node_cls()
        try:
            result = await node_instance.run(ctx, payload)
            duration = (asyncio.get_event_loop().time() - start_time) * 1000
            
            if isinstance(result, Parallel):
                await self._handle_parallel_start(event.agent_id, ctx, result, duration)
            elif result:
                await self._handle_transition(event.agent_id, current_node_name, result, ctx, duration)
            else:
                await self._handle_terminal(event.agent_id, current_node_name, ctx, duration)
                
        except Exception as e:
            await self.observer.on_error(event.agent_id, current_node_name, e)
            raise

    async def _handle_transition(self, agent_id: str, from_node: str, next_node_cls: Type[Node], ctx: BaseModel, duration: float):
        next_name = next_node_cls.__name__
        await self.observer.on_transition(agent_id, from_node, next_name, duration)
        
        next_event = Event(agent_id=agent_id, node_name=next_name)
        await self.db.save_state(agent_id, next_name, ctx.model_dump(), next_event)
        await self.bus.publish(next_event)

    async def _handle_parallel_start(self, parent_id: str, parent_ctx: BaseModel, parallel: Parallel, duration: float):
        await self.observer.on_transition(parent_id, "ParallelStart", parallel.join_node.__name__, duration)
        
        await self.db.create_task_group(
            parent_id=parent_id, 
            join_node_name=parallel.join_node.__name__, 
            task_count=len(parallel.branches)
        )
        
        for task in parallel.branches:
            sub_ctx = getattr(parent_ctx, task.sub_context_path)
            sub_id = f"{parent_id}#{task.sub_context_path}"
            
            evt = Event(
                agent_id=sub_id, 
                node_name=task.node_cls.__name__,
                payload=task.payload.model_dump() if task.payload else None
            )
            # Create sub-state
            await self.db.save_sub_state(sub_id, parent_id, task.node_cls.__name__, sub_ctx.model_dump(), evt)
            await self.bus.publish(evt)
            
        # VERY IMPORTANT: Mark the parent as waiting and free the lock
        await self.db.save_state(parent_id, "WAITING_FOR_JOIN", parent_ctx.model_dump(), None)

    async def _handle_terminal(self, agent_id: str, from_node: str, ctx: BaseModel, duration: float):
        await self.observer.on_transition(agent_id, from_node, "TERMINAL", duration)
        
        if "#" in agent_id:
            # Sub-context is terminal, save to sub-table and release lock
            parent_id = agent_id.split("#")[0]
            await self.db.save_sub_state(agent_id, parent_id, "TERMINAL", ctx.model_dump(), None)
            
            join_node_name = await self.db.register_sub_task_completion(agent_id)
            if join_node_name:
                await self._trigger_recompose(parent_id, join_node_name)
        else:
            await self.db.save_state(agent_id, "TERMINAL", ctx.model_dump(), None)

    async def _trigger_recompose(self, parent_id: str, join_node_name: str):
        # 1. Lock the parent to prevent race condition during merge
        await self.db.load_and_lock_agent(parent_id)
        
        # 2. Database structural merge
        merged_ctx_dict = await self.db.recompose_parent(parent_id)
        
        # 3. Transition parent to the Join Node
        join_event = Event(agent_id=parent_id, node_name=join_node_name)
        await self.db.save_state(parent_id, join_node_name, merged_ctx_dict, join_event)
        await self.bus.publish(join_event)
