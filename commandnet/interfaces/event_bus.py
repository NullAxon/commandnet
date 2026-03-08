from abc import ABC, abstractmethod
from typing import Callable, Coroutine
from ..core.models import Event

class EventBus(ABC):
    @abstractmethod
    async def publish(self, event: Event):
        """Publishes an event to the queue/broker."""
        pass

    @abstractmethod
    async def subscribe(self, handler: Callable[[Event], Coroutine]):
        """Registers a worker callback to process incoming events."""
        pass
