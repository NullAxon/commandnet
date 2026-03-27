import pytest_asyncio
import asyncio
from commandnet import Persistence, EventBus, Event
from typing import Any, Dict, List, Any

class TestDB(Persistence):
    def __init__(self):
        self.subjects = {}
        self.sub_subjects = {}
        self.task_groups = {}
        self.locks = {}
        self.waiting_room = {}
        self.cancel_flags = {}
        self.call_groups = {}

        
    async def _get_lock(self, subject_id: str):
        if subject_id not in self.locks:
            self.locks[subject_id] = asyncio.Lock()
        return self.locks[subject_id]

    async def lock_and_load(self, subject_id: str):
        lock = await self._get_lock(subject_id)
        await lock.acquire()
        
        # FIX: Virtual shared subjects (call#) live in the main subjects table
        is_sub = "#" in subject_id and not subject_id.startswith("call#")
        subject = self.sub_subjects.get(subject_id) if is_sub else self.subjects.get(subject_id)
        
        if not subject: 
            lock.release()
            return None, None
        return subject.get("node"), subject.get("context")
        
    async def unlock_subject(self, subject_id: str):
        lock = await self._get_lock(subject_id)
        if lock.locked(): lock.release()

    async def save_state(self, subject_id: str, node_name: str, context: dict, event: Event):
        self.subjects[subject_id] = {"node": node_name, "context": context}
        await self.unlock_subject(subject_id)

    async def save_sub_state(self, sub_id: str, parent_id: str, node_name: str, ctx: dict, evt: Event):
        path = sub_id.split("#")[1]
        self.sub_subjects[sub_id] = {"parent_id": parent_id, "path": path, "node": node_name, "context": ctx}
        await self.unlock_subject(sub_id)

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
        parent_ctx = self.subjects.get(parent_id, {}).get("context", {})
        for sub_id, data in list(self.sub_subjects.items()):
            if data["parent_id"] == parent_id:
                parent_ctx[data["path"]] = data["context"]
                del self.sub_subjects[sub_id]
        self.subjects[parent_id] = {"node": "Recomposing", "context": parent_ctx}
        return parent_ctx

    async def schedule_event(self, event: Event) -> bool:
        return True

    async def pop_due_events(self) -> list:
        return []

    async def park_subject(self, subject_id: str, signal_id: str, next_target: Any, context: Dict):
        if signal_id not in self.waiting_room: self.waiting_room[signal_id] = []
        self.waiting_room[signal_id].append({
            "subject_id": subject_id,
            "next_target": next_target,
            "context": context
        })
        await self.unlock_subject(subject_id)

    async def get_and_clear_waiters(self, signal_id: str) -> List[Dict]:
        return self.waiting_room.pop(signal_id, [])

    async def set_cancel_flag(self, subject_id: str, hard: bool):
        self.cancel_flags[subject_id] = hard

    async def is_cancelled(self, subject_id: str):
        return subject_id in self.cancel_flags

    async def add_call_waiter(self, key, subject_id, resume_target, context):
        is_new = key not in self.call_groups
        if is_new: self.call_groups[key] = []
        self.call_groups[key].append({
            "subject_id": subject_id, 
            "resume_target": resume_target, 
            "context": context
        })
        return is_new

    async def resolve_call_group(self, key):
        return self.call_groups.pop(key, [])

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

