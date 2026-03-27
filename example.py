import asyncio
from typing import Dict, Optional, Tuple, Callable, Coroutine, List, Any
from datetime import datetime, timezone
from pydantic import BaseModel

from commandnet import Node, Engine, Event, Persistence, EventBus, Schedule, GraphAnalyzer

# ============================================================================
# 1. Infrastructure Mock (With dead-lock prevention and backpressure support)
# ============================================================================

class InMemoryDB(Persistence):
    def __init__(self):
        self.subjects: Dict[str, Dict] = {}       
        self.sub_subjects: Dict[str, Dict] = {}   
        self.task_groups: Dict[str, Dict] = {}  
        self.locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()
        self.waiting_room: Dict[str, List[Dict]] = {}
        
        self.scheduled_queue: List[Event] = []
        self.idempotency_store = set()
        self.cancel_flags = {} # subject_id -> bool (hard)
        self.call_groups = {}  # key -> list of waiter dicts
        self.waiting_room = {}
        
    async def _get_lock(self, subject_id: str) -> asyncio.Lock:
        async with self._global_lock:
            if subject_id not in self.locks:
                self.locks[subject_id] = asyncio.Lock()
            return self.locks[subject_id]
        
    async def lock_and_load(self, subject_id: str) -> Tuple[Optional[str], Optional[Dict]]:
        lock = await self._get_lock(subject_id)
        await lock.acquire()
        
        # FIX: 'call#' subjects are global, not sub-subjects
        is_sub = "#" in subject_id and not subject_id.startswith("call#")
        subject = self.sub_subjects.get(subject_id) if is_sub else self.subjects.get(subject_id)
        
        if not subject:
            lock.release()
            return None, None
        return subject.get("node"), subject.get("context")

    async def unlock_subject(self, subject_id: str):
        lock = await self._get_lock(subject_id)
        if lock.locked(): lock.release()

    async def save_state(self, subject_id: str, node_name: str, context: Dict, event: Optional[Event]):
        self.subjects[subject_id] = {"node": node_name, "context": context}
        await self.unlock_subject(subject_id)

    async def save_sub_state(self, sub_id: str, parent_id: str, node_name: str, ctx: dict, evt: Optional[Event]):
        path = sub_id.split("#")[1]
        self.sub_subjects[sub_id] = {"parent_id": parent_id, "path": path, "node": node_name, "context": ctx}
        await self.unlock_subject(sub_id)

    async def create_task_group(self, parent_id: str, join_node_name: str, task_count: int):
        self.task_groups[parent_id] = {"join_node": join_node_name, "pending": task_count}

    async def register_sub_task_completion(self, sub_id: str) -> Optional[str]:
        parent_id = sub_id.split("#")[0]
        group = self.task_groups.get(parent_id)
        if not group: return None
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
        self.subjects[parent_id]["context"] = parent_ctx
        return parent_ctx

    async def schedule_event(self, event: Event) -> bool:
        if event.idempotency_key:
            lookup = f"{event.subject_id}:{event.idempotency_key}"
            if lookup in self.idempotency_store:
                print(f"   [DB] 🛑 Blocked duplicate schedule: {lookup}")
                return False
            self.idempotency_store.add(lookup)
            
        print(f"   [DB] 📅 Scheduled {event.node_name} for {event.run_at}")
        self.scheduled_queue.append(event)
        return True

    async def pop_due_events(self) -> List[Event]:
        # Using fromisoformat correctly prevents string sort bugs on differing ISO precision
        now = datetime.now(timezone.utc)
        due = [evt for evt in self.scheduled_queue if datetime.fromisoformat(evt.run_at) <= now]
        self.scheduled_queue = [evt for evt in self.scheduled_queue if datetime.fromisoformat(evt.run_at) > now]
        return due

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

    async def is_cancelled(self, subject_id: str) -> bool:
        return subject_id in self.cancel_flags

    async def add_call_waiter(self, key: str, subject_id: str, resume_target: Any, context: dict) -> bool:
        is_new = key not in self.call_groups
        if is_new: self.call_groups[key] = []
        self.call_groups[key].append({
            "subject_id": subject_id, 
            "resume_target": resume_target, 
            "context": context
        })
        return is_new

    async def resolve_call_group(self, key: str) -> List[Dict]:
        return self.call_groups.pop(key, [])

class InMemoryBus(EventBus):
    def __init__(self):
        # Adding backpressure to protect the event loop memory
        self.queue = asyncio.Queue(maxsize=10000)
        self.handler = None
        self.running = True

    async def publish(self, event: Event): await self.queue.put(event)
    async def subscribe(self, handler):
        self.handler = handler
        asyncio.create_task(self._consume())
    async def stop(self):
        self.running = False
        await self.queue.put(None)

    async def _consume(self):
        while self.running:
            event = await self.queue.get()
            if event is None: break
            if self.handler:
                try:
                    await self.handler(event)
                    self.queue.task_done()
                except Exception as e:
                    print(f"Queue Error: {e}")

# ============================================================================
# 2. Define State & Nodes
# ============================================================================

class PingCtx(BaseModel):
    ping_count: int = 0
    pong_count: int = 0

class PongMessage(BaseModel):
    text: str

class PongNode(Node[PingCtx, PongMessage]):
    async def run(self, ctx: PingCtx, payload: PongMessage) -> None:
        ctx.pong_count += 1
        print(f"🎾 [PONG] Received delayed message: '{payload.text}' (Pong Count: {ctx.pong_count})")
        return None

class DuplicateTestNode(Node[PingCtx, None]):
    async def run(self, ctx: PingCtx, payload=None) -> Schedule:
        print("🏓 [DUPE TEST] Attempting to schedule duplicate pong...")
        return Schedule(
            action=PongNode,
            delay_seconds=1,
            payload=PongMessage(text="I should be blocked!"),
            idempotency_key="pong_round_1"
        )

class PingNode(Node[PingCtx, None]):
    async def run(self, ctx: PingCtx, payload=None) -> Schedule:
        ctx.ping_count += 1
        print(f"🏓 [PING] Scheduling a Pong for 2 seconds from now...")
        return Schedule(
            action=DuplicateTestNode, 
            delay_seconds=2,
            idempotency_key="pong_round_1"
        )


async def main():
    db = InMemoryDB()
    bus = InMemoryBus()

    my_nodes = [PingNode, PongNode, DuplicateTestNode]
    engine = Engine(persistence=db, event_bus=bus, nodes=my_nodes)

    engine.validate_graph(PingNode)
    
    await engine.start_worker(poll_interval=0.5)
    
    print("Triggering subject...")
    await engine.trigger_subject("subject-schedule", PingNode, PingCtx())
    
    print("Waiting 3 seconds to watch scheduler...")
    await asyncio.sleep(3.0) 
    
    await engine.stop()
    await bus.stop()

if __name__ == "__main__":
    asyncio.run(main())

