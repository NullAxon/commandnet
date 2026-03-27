from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple, List, Any
from ..core.models import Event

class Persistence(ABC):
    @abstractmethod
    async def lock_and_load(self, subject_id: str) -> Tuple[Optional[str], Optional[Dict]]:
        pass
        
    @abstractmethod
    async def unlock_subject(self, subject_id: str):
        pass

    @abstractmethod
    async def save_state(self, subject_id: str, node_name: str, context: Dict, event: Optional[Event]):
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
    async def park_subject(self, subject_id: str, signal_id: str, next_target: Any, context: Dict):
        pass

    @abstractmethod
    async def get_and_clear_waiters(self, signal_id: str) -> List[Dict]:
        pass

    @abstractmethod
    async def set_cancel_flag(self, subject_id: str, hard: bool):
        pass

    @abstractmethod
    async def is_cancelled(self, subject_id: str) -> bool:
        pass

    @abstractmethod
    async def add_call_waiter(self, key: str, subject_id: str, resume_target: Any, context: dict) -> bool:
        """Returns True if this is the first waiter for this key (the 'leader')."""
        pass

    @abstractmethod
    async def resolve_call_group(self, key: str) -> List[Dict[str, Any]]:
        pass

