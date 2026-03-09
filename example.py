import asyncio
from typing import Dict, Optional, Tuple, Callable, Coroutine
from pydantic import BaseModel, Field

from commandnet import Node, Engine, Event, Persistence, EventBus, Parallel, ParallelTask

# ============================================================================
# 1. Infrastructure Mocks (In-Memory Database & Message Queue)
# ============================================================================

class InMemoryDB(Persistence):
    """A thread-safe, in-memory mock of a database with Sub-Context support."""
    def __init__(self):
        self.agents: Dict[str, Dict] = {}       
        self.sub_agents: Dict[str, Dict] = {}   
        self.task_groups: Dict[str, Dict] = {}  
        self.locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()
        
    async def _get_lock(self, agent_id: str) -> asyncio.Lock:
        async with self._global_lock:
            if agent_id not in self.locks:
                self.locks[agent_id] = asyncio.Lock()
            return self.locks[agent_id]
        
    async def load_and_lock_agent(self, agent_id: str) -> Tuple[Optional[str], Optional[Dict]]:
        lock = await self._get_lock(agent_id)
        await lock.acquire()
        
        if "#" in agent_id:
            agent = self.sub_agents.get(agent_id)
        else:
            agent = self.agents.get(agent_id)
            
        if not agent:
            lock.release()
            return None, None
        return agent.get("node"), agent.get("context")

    async def save_state(self, agent_id: str, node_name: str, context: Dict, event: Optional[Event]):
        self.agents[agent_id] = {"node": node_name, "context": context}
        lock = await self._get_lock(agent_id)
        if lock.locked():
            lock.release()

    async def save_sub_state(self, sub_id: str, parent_id: str, node_name: str, ctx: dict, evt: Optional[Event]):
        path = sub_id.split("#")[1]
        self.sub_agents[sub_id] = {"parent_id": parent_id, "path": path, "node": node_name, "context": ctx}
        lock = await self._get_lock(sub_id)
        if lock.locked():
            lock.release()

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
        parent_ctx = self.agents.get(parent_id, {}).get("context", {})
        for sub_id, data in list(self.sub_agents.items()):
            if data["parent_id"] == parent_id:
                path = data["path"]
                parent_ctx[path] = data["context"]
                del self.sub_agents[sub_id]
                
        self.agents[parent_id]["context"] = parent_ctx
        return parent_ctx

class InMemoryBus(EventBus):
    def __init__(self):
        self.queue = asyncio.Queue()
        self.handler: Optional[Callable[[Event], Coroutine]] = None
        self.running = True

    async def publish(self, event: Event):
        await self.queue.put(event)

    async def subscribe(self, handler: Callable[[Event], Coroutine]):
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
                except Exception as e:
                    print(f"Queue Handler Error: {e}")
            self.queue.task_done()

# ============================================================================
# 2. Define State (Context) & Payloads
# ============================================================================

class DocumentSection(BaseModel):
    text: str = ""
    summary: str = ""
    processed: bool = False

class DocContext(BaseModel):
    intro: DocumentSection = Field(default_factory=DocumentSection)
    body: DocumentSection = Field(default_factory=DocumentSection)
    final_report: str = ""

class ProcessingPayload(BaseModel):
    focus_keyword: str

# ============================================================================
# 3. Define Nodes (Parallel Execution)
# ============================================================================

class SummarizeSection(Node[DocumentSection, ProcessingPayload]):
    async def run(self, ctx: DocumentSection, payload: Optional[ProcessingPayload] = None) -> None:
        focus = payload.focus_keyword if payload else "general"
        print(f"   [Worker] Summarizing '{ctx.text[:10]}...' with focus: {focus}")
        # Artificial delay to simulate processing overhead
        await asyncio.sleep(0.1) 
        ctx.summary = f"Summary of '{ctx.text}' (Focus: {focus})"
        ctx.processed = True
        return None # Terminal for this sub-context

class FinalJoin(Node[DocContext, None]):
    async def run(self, ctx: DocContext, payload=None) -> None:
        print("\n--- Final Join Node ---")
        ctx.final_report = f"INTRO: [{ctx.intro.summary}] | BODY: [{ctx.body.summary}]"
        print(f"Report Generated: {ctx.final_report}")
        return None

class StartNode(Node[DocContext, None]):
    async def run(self, ctx: DocContext, payload=None) -> Parallel:
        print("--- Dispatching Parallel Tasks ---")
        return Parallel(
            branches=[
                ParallelTask(
                    node_cls=SummarizeSection, 
                    sub_context_path="intro", 
                    payload=ProcessingPayload(focus_keyword="hook")
                ),
                ParallelTask(
                    node_cls=SummarizeSection, 
                    sub_context_path="body",
                    payload=ProcessingPayload(focus_keyword="details")
                ),
            ],
            join_node=FinalJoin
        )

# ============================================================================
# 4. Setup and Run
# ============================================================================

async def main():
    db = InMemoryDB()
    bus = InMemoryBus()
    engine = Engine(persistence=db, event_bus=bus)
    
    await engine.start_worker()
    
    initial_ctx = DocContext(
        intro=DocumentSection(text="Once upon a time..."),
        body=DocumentSection(text="The hero fought the dragon...")
    )
    
    print("Triggering Document Analysis Agent...")
    await engine.trigger_agent(
        agent_id="doc-agent-001", 
        start_node=StartNode, 
        initial_context=initial_ctx
    )
    
    # Wait longer to ensure all workers finish their tasks
    await asyncio.sleep(1.0) 
    await bus.stop()

if __name__ == "__main__":
    asyncio.run(main())
