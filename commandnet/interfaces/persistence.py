from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple
from ..core.models import Event

class Persistence(ABC):
    @abstractmethod
    async def load_and_lock_agent(self, agent_id: str) -> Tuple[Optional[str], Optional[Dict]]:
        """Loads state and locks the DB row. Returns (node_name, context_dict)."""
        pass

    @abstractmethod
    async def save_state(self, agent_id: str, node_name: str, context: Dict, event: Event):
        """Persists the updated context and records the transition event."""
        pass
