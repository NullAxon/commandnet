import inspect
from typing import Generic, TypeVar, Type, Optional, Union, List, Dict, Any
from pydantic import BaseModel, ConfigDict

C = TypeVar('C', bound=BaseModel) # Context
P = TypeVar('P', bound=BaseModel) # Payload (Optional)

NODE_REGISTRY: Dict[str, Type['Node']] = {}

class ParallelTask(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    node_cls: Type['Node']
    payload: Optional[Any] = None
    sub_context_path: str 

class Parallel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    branches: List[ParallelTask]
    join_node: Type['Node']

class Schedule(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    node_cls: Type['Node']
    delay_seconds: int
    payload: Optional[Any] = None
    idempotency_key: Optional[str] = None

# Minor typing improvement for readability
TransitionResult = Union[Type['Node'], Parallel, Schedule, None]

class Node(Generic[C, P]):
    @classmethod
    def get_node_name(cls) -> str:
        return cls.__name__

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not inspect.isabstract(cls):
            name = cls.get_node_name()
            # Prevent registry collisions across modules
            if name in NODE_REGISTRY and NODE_REGISTRY[name] is not cls:
                raise RuntimeError(
                    f"NODE_REGISTRY Collision: '{name}' is already registered by {NODE_REGISTRY[name].__module__}. "
                    f"Override `get_node_name()` on {cls.__module__}.{name} to ensure uniqueness."
                )
            NODE_REGISTRY[name] = cls

    async def run(self, ctx: C, payload: Optional[P] = None) -> TransitionResult:
        """Executes node logic. Returns the Next Node, a Parallel request, a Schedule request, or None."""
        pass
