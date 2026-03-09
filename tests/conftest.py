import pytest_asyncio
import asyncio
from commandnet import Persistence, EventBus, Event

class TestDB(Persistence):
    def __init__(self):
        self.agents = {}
        self.sub_agents = {}
        self.task_groups = {}

    async def load_and_lock_agent(self, agent_id: str):
        if "#" in agent_id:
            agent = self.sub_agents.get(agent_id)
        else:
            agent = self.agents.get(agent_id)
        if not agent: return None, None
        return agent.get("node"), agent.get("context")

    async def save_state(self, agent_id: str, node_name: str, context: dict, event: Event):
        self.agents[agent_id] = {"node": node_name, "context": context}

    async def save_sub_state(self, sub_id: str, parent_id: str, node_name: str, ctx: dict, evt: Event):
        path = sub_id.split("#")[1]
        self.sub_agents[sub_id] = {"parent_id": parent_id, "path": path, "node": node_name, "context": ctx}

    async def create_task_group(self, parent_id: str, join_node_name: str, task_count: int):
        self.task_groups[parent_id] = {"join_node": join_node_name, "pending": task_count}

    async def register_sub_task_completion(self, sub_id: str):
        parent_id = sub_id.split("#")[0]
        group = self.task_groups.get(parent_id)
        if group:
            group["pending"] -= 1
            if group["pending"] <= 0:
                join_node = group["join_node"]
                del self.task_groups[parent_id]
                return join_node
        return None

    async def recompose_parent(self, parent_id: str) -> dict:
        parent_ctx = self.agents.get(parent_id, {}).get("context", {})
        for sub_id, data in list(self.sub_agents.items()):
            if data["parent_id"] == parent_id:
                parent_ctx[data["path"]] = data["context"]
                del self.sub_agents[sub_id]
        self.agents[parent_id] = {"node": "Recomposing", "context": parent_ctx}
        return parent_ctx

    # --- Added implementations for new Scheduling ABC methods ---
    async def schedule_event(self, event: Event) -> bool:
        return True

    async def pop_due_events(self) -> list:
        return []


class TestBus(EventBus):
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
                if event is None:
                    self.queue.task_done()
                    break
                if self.handler:
                    try:
                        await self.handler(event)
                    except Exception:
                        pass 
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
    await bus.stop()
