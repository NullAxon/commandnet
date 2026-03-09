import inspect
from typing import Generic, TypeVar, Type, Optional, Union, List, Dict
from pydantic import BaseModel, ConfigDict

C = TypeVar('C', bound=BaseModel) # Context
P = TypeVar('P', bound=BaseModel) # Payload (Optional)

NODE_REGISTRY: Dict[str, Type['Node']] = {}

class ParallelTask(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    node_cls: Type['Node']
    payload: Optional[BaseModel] = None
    sub_context_path: str 

class Parallel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    branches: List[ParallelTask]
    join_node: Type['Node']

class Node(Generic[C, P]):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not inspect.isabstract(cls):
            NODE_REGISTRY[cls.__name__] = cls

    async def run(self, ctx: C, payload: Optional[P] = None) -> Union[Type['Node'], Parallel, None]:
        """Executes node logic. Returns the Next Node, a Parallel execution request, or None."""
        pass
