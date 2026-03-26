from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple, List, Any
from ..core.models import Event

class Persistence(ABC):
    @abstractmethod
    async def load_and_lock_agent(self, agent_id: str) -> Tuple[Optional[str], Optional[Dict]]:
        pass
        
    @abstractmethod
    async def unlock_agent(self, agent_id: str):
        """Releases the row lock. Called automatically by the engine if an exception occurs."""
        pass

    @abstractmethod
    async def save_state(self, agent_id: str, node_name: str, context: Dict, event: Optional[Event]):
        pass

    @abstractmethod
    async def save_sub_state(self, sub_id: str, parent_id: str, node_name: str, ctx: dict, evt: Optional[Event]):
        pass

    @abstractmethod
    async def create_task_group(self, parent_id: str, join_node_name: str, task_count: int):
        pass

    @abstractmethod
    async def register_sub_task_completion(self, sub_id: str) -> Optional[str]:
        pass

    @abstractmethod
    async def recompose_parent(self, parent_id: str) -> dict:
        pass

    @abstractmethod
    async def schedule_event(self, event: Event) -> bool:
        pass

    @abstractmethod
    async def pop_due_events(self) -> List[Event]:
        pass

    @abstractmethod
    async def park_agent(self, agent_id: str, signal_id: str, next_target: Any, context: Dict):
        pass

    @abstractmethod
    async def get_and_clear_waiters(self, signal_id: str) -> List[Dict]:
        pass
