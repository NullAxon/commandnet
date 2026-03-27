from typing import Generic, TypeVar, Type, Optional, Union, List, Any
from pydantic import BaseModel, ConfigDict

C = TypeVar('C', bound=BaseModel) # Context
P = TypeVar('P', bound=BaseModel) # Payload

# The Recursive Type Definition
Target = Union[Type['Node'], 'Parallel', 'Schedule', 'Wait', 'Call', 'Interrupt', None]

class ParallelTask(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    action: Target 
    sub_context_path: str 
    payload: Optional[Any] = None

class Parallel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    branches: List[Union[ParallelTask, 'Wait']] 
    join_node: Optional[Type['Node']] = None 

class Schedule(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    action: Target
    delay_seconds: int
    payload: Optional[Any] = None
    idempotency_key: Optional[str] = None

class Wait(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    signal_id: str
    resume_action: Target
    sub_context_path: Optional[str] = None 

class Call(BaseModel):
    """The 'Await' type: Deduplicates execution based on an idempotency key."""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    node_cls: Type['Node']
    idempotency_key: str
    payload: Optional[Any] = None
    # After the called node finishes, the caller resumes at this node
    resume_action: Target = None 

class Interrupt(BaseModel):
    """Signals the engine to stop execution."""
    subject_id: str
    hard: bool = True # True = kill task, False = flag for soft exit

TransitionResult = Target

class Node(Generic[C, P]):
    @classmethod
    def get_node_name(cls) -> str:
        return cls.__name__

    async def run(self, ctx: C, payload: Optional[P] = None) -> TransitionResult:
        """Executes node logic. Returns a Target (Node class, Parallel, Schedule, Wait, or None)."""
        pass

    async def on_signal(self, ctx: C, signal_id: str, payload: Any) -> Target:
        """
        CALLBACK: Called when a 'Wait' state is resolved or an external signal 
        is sent to this node specifically.
        """
        return None

