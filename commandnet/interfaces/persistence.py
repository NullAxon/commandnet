from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple
from ..core.models import Event

class Persistence(ABC):
    @abstractmethod
    async def load_and_lock_agent(self, agent_id: str) -> Tuple[Optional[str], Optional[Dict]]:
        pass

    @abstractmethod
    async def save_state(self, agent_id: str, node_name: str, context: Dict, event: Optional[Event]):
        pass

    @abstractmethod
    async def save_sub_state(self, sub_id: str, parent_id: str, node_name: str, ctx: dict, evt: Optional[Event]):
        """Saves a sub-context slice."""
        pass

    @abstractmethod
    async def create_task_group(self, parent_id: str, join_node_name: str, task_count: int):
        """Registers a group of sub-tasks and the node to trigger upon completion."""
        pass

    @abstractmethod
    async def register_sub_task_completion(self, sub_id: str) -> Optional[str]:
        """
        Marks a sub-task terminal. 
        Returns the join_node_name IF this was the last pending task, else None.
        """
        pass

    @abstractmethod
    async def recompose_parent(self, parent_id: str) -> dict:
        """Merges all sub-context rows back into the main parent dictionary."""
        pass
