import asyncio
import logging
import uuid
from typing import Any, Optional, Type
from pydantic import BaseModel

from ..core.models import Event
from ..core.node import Node, NODE_REGISTRY
from ..core.graph import GraphAnalyzer
from ..interfaces.persistence import Persistence
from ..interfaces.event_bus import EventBus
from ..interfaces.observer import Observer

class Engine:
    """Manages event-driven transitions, state rehydration, and execution."""
    
    def __init__(
        self, 
        persistence: Persistence, 
        event_bus: EventBus,
        observer: Optional[Observer] = None
    ):
        self.db = persistence
        self.bus = event_bus
        self.observer = observer or Observer()
        self.logger = logging.getLogger("TypedEngine")

    async def start_worker(self):
        """Subscribes the engine to the event bus."""
        await self.bus.subscribe(self.process_event)
        self.logger.info("Worker started, listening for events.")

    async def trigger_agent(self, agent_id: str, start_node: Type[Node], initial_context: Any):
        """Initializes a new agent and pushes the first event."""
        ctx_dict = initial_context.model_dump() if isinstance(initial_context, BaseModel) else initial_context
        
        start_event = Event(agent_id=agent_id, node_name=start_node.__name__)
        await self.db.save_state(agent_id, start_node.__name__, ctx_dict, start_event)
        await self.bus.publish(start_event)

    async def process_event(self, event: Event):
        """Core state machine worker loop."""
        start_time = asyncio.get_event_loop().time()
        
        # 1. Concurrency safe load
        current_node_name, ctx_dict = await self.db.load_and_lock_agent(event.agent_id)
        if not current_node_name:
            return

        # 2. Idempotency Check
        if current_node_name != event.node_name:
            self.logger.info(f"Stale event {event.event_id} for {event.agent_id}. Ignoring.")
            return

        node_cls = NODE_REGISTRY.get(current_node_name)
        if not node_cls:
            raise RuntimeError(f"Node '{current_node_name}' missing from registry.")

        # 3. Pydantic Automatic Rehydration
        ctx_type = GraphAnalyzer.get_context_type(node_cls)
        try:
            ctx = ctx_type.model_validate(ctx_dict) if issubclass(ctx_type, BaseModel) else ctx_dict
        except Exception as e:
            self.logger.error(f"Context validation failed for {event.agent_id}: {e}")
            raise

        # 4. Execute
        node_instance = node_cls()
        try:
            next_node_cls = await node_instance.run(ctx)
        except Exception as e:
            await self.observer.on_error(event.agent_id, current_node_name, e)
            raise  # Handled by external broker retry policy
            
        # 5. Determine transition & serialize context
        duration = (asyncio.get_event_loop().time() - start_time) * 1000
        new_ctx_dict = ctx.model_dump() if isinstance(ctx, BaseModel) else ctx
        
        if next_node_cls:
            next_name = next_node_cls.__name__
            await self.observer.on_transition(event.agent_id, current_node_name, next_name, duration)
            
            next_event = Event(agent_id=event.agent_id, node_name=next_name)
            await self.db.save_state(event.agent_id, next_name, new_ctx_dict, next_event)
            await self.bus.publish(next_event)
        else:
            await self.observer.on_transition(event.agent_id, current_node_name, "TERMINAL", duration)
