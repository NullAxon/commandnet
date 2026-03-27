from abc import ABC

class Observer(ABC):
    async def on_transition(self, subject_id: str, from_node: str, to_node: str, duration_ms: float): pass
    async def on_error(self, subject_id: str, node: str, error: Exception): pass

