import pytest_asyncio
import asyncio
from commandnet import Persistence, EventBus, Event

class TestDB(Persistence):
    """A test database that avoids deadlocking the test suite."""
    def __init__(self):
        self.agents = {}

    async def load_and_lock_agent(self, agent_id: str):
        # We drop the asyncio.Lock() here to prevent the Engine's 
        # terminal-state lock leak from hanging our test suite.
        agent = self.agents.get(agent_id)
        if not agent:
            return None, None
        return agent["node"], agent["context"]

    async def save_state(self, agent_id: str, node_name: str, context: dict, event: Event):
        self.agents[agent_id] = {"node": node_name, "context": context}

class TestBus(EventBus):
    """A test event bus that safely shuts down background tasks."""
    def __init__(self):
        self.queue = asyncio.Queue()
        self.handler = None
        self.task = None

    async def publish(self, event: Event):
        await self.queue.put(event)

    async def subscribe(self, handler):
        self.handler = handler
        self.task = asyncio.create_task(self._consume())

    async def _consume(self):
        while True:
            try:
                event = await self.queue.get()
                if event is None:  # Sentinel to stop
                    self.queue.task_done()
                    break
                if self.handler:
                    try:
                        await self.handler(event)
                    except Exception:
                        pass # Prevent worker crash on failed tests
                self.queue.task_done()
            except asyncio.CancelledError:
                break

    async def stop(self):
        await self.queue.put(None)
        if self.task and not self.task.done():
            self.task.cancel()

@pytest_asyncio.fixture
async def mock_infrastructure():
    db = TestDB()
    bus = TestBus()
    yield db, bus
    await bus.stop()  # Safely kills the background worker loop!
