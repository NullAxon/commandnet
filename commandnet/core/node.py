import inspect
from abc import ABC, abstractmethod
from typing import Dict, Generic, Optional, Type, TypeVar

C = TypeVar('C')  # Generic Context Type
NODE_REGISTRY: Dict[str, Type['Node']] = {}

class Node(Generic[C], ABC):
    """
    Stateless unit of execution.
    Subclasses must implement `run` and type-hint the return with next Node classes.
    """
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Automatically register non-abstract nodes
        if not inspect.isabstract(cls):
            NODE_REGISTRY[cls.__name__] = cls

    @abstractmethod
    async def run(self, ctx: C) -> Optional[Type['Node']]:
        """Executes node logic. Returns the next Node class, or None if terminal."""
        pass
