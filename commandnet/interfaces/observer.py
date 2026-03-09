from abc import ABC

class Observer(ABC):
    async def on_transition(self, agent_id: str, from_node: str, to_node: str, duration_ms: float): pass
    async def on_error(self, agent_id: str, node: str, error: Exception): pass
