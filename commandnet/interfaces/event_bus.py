from abc import ABC, abstractmethod
from typing import Callable, Coroutine
from ..core.models import Event

class EventBus(ABC):
    @abstractmethod
    async def publish(self, event: Event):
        pass

    @abstractmethod
    async def subscribe(self, handler: Callable[[Event], Coroutine]):
        pass
