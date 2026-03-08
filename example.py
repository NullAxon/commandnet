import asyncio
from typing import Union, Type, Dict, Optional, Tuple, Callable, Coroutine
from pydantic import BaseModel, Field

# Import components from our CommandNet library
from commandnet import Node, Engine, GraphAnalyzer, Event, Persistence, EventBus

# ============================================================================
# 1. Infrastructure Mocks (In-Memory Database & Message Queue)
# ============================================================================

class InMemoryDB(Persistence):
    """A thread-safe, in-memory mock of a database with row-level locking."""
    def __init__(self):
        self.agents: Dict[str, Dict] = {}  # agent_id -> {"node": str, "context": dict}
        self.lock = asyncio.Lock()
        
    async def load_and_lock_agent(self, agent_id: str) -> Tuple[Optional[str], Optional[Dict]]:
        # Mocking a SELECT ... FOR UPDATE database lock
        await self.lock.acquire()
        agent = self.agents.get(agent_id)
        if not agent:
            self.lock.release()
            return None, None
        return agent["node"], agent["context"]

    async def save_state(self, agent_id: str, node_name: str, context: Dict, event: Event):
        try:
            self.agents[agent_id] = {"node": node_name, "context": context}
        finally:
            if self.lock.locked():
                self.lock.release()
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
        await self.queue.put(None) # Signal to break the loop

    async def _consume(self):
        while self.running:
            event = await self.queue.get()
            if event is None: # Correct sentinel check
                break
            if self.handler:
                try:
                    await self.handler(event)
                except Exception as e:
                    print(f"Error: {e}")
            self.queue.task_done()

# ============================================================================
# 2. Define State (Context)
# ============================================================================

class AgentContext(BaseModel):
    user_query: str
    is_authenticated: bool = False
    attempts: int = Field(default=0, ge=0)


# ============================================================================
# 3. Define Nodes
# ============================================================================

class Denied(Node[AgentContext]):
    async def run(self, ctx: AgentContext) -> None: # Returning None means Terminal
        print(f"[{ctx.user_query}] -> Access Denied (Attempts: {ctx.attempts}).")
        return None

class Executing(Node[AgentContext]):
    async def run(self, ctx: AgentContext) -> None: # Returning None means Terminal
        print(f"[{ctx.user_query}] -> Running task successfully!")
        return None

class AuthCheck(Node[AgentContext]):
    # Strong typing explicitly derives the execution DAG!
    async def run(self, ctx: AgentContext) -> Union[Executing, Denied]:
        print(f"[{ctx.user_query}] -> Checking Auth...")
        ctx.attempts += 1
        
        if ctx.user_query == "hack_system":
            ctx.is_authenticated = False
            return Denied
            
        ctx.is_authenticated = True
        return Executing


# ============================================================================
# 4. Setup and Run
# ============================================================================

async def main():
    # 1. Show off Graph Introspection
    # This proves the graph is derived entirely from the return type hints!
    dag = GraphAnalyzer.build_graph(AuthCheck)
    print("--- Derived Graph DAG ---")
    for node, edges in dag.items():
        print(f"{node} -> {edges}")
    print("-------------------------\n")
    
    # 2. Initialize Engine with our Mock Infrastructure
    db = InMemoryDB()
    bus = InMemoryBus()
    engine = Engine(persistence=db, event_bus=bus)
    
    # 3. Start the worker loop (subscribes to the EventBus)
    await engine.start_worker()
    
    # 4. Fire off an event with our strictly-typed Pydantic model
    print("Triggering Agent 1 (Valid Query):")
    await engine.trigger_agent(
        agent_id="agent-001", 
        start_node=AuthCheck, 
        initial_context=AgentContext(user_query="clean_logs")
    )
    
    # Wait for the async worker queue to process the first workflow
    await asyncio.sleep(0.1) 
    print()
    
    print("Triggering Agent 2 (Malicious Query):")
    await engine.trigger_agent(
        agent_id="agent-002", 
        start_node=AuthCheck, 
        initial_context=AgentContext(user_query="hack_system")
    )
    
    # Wait for the async worker queue to process the second workflow
    await asyncio.sleep(0.1)


if __name__ == "__main__":
    # Setup basic logging to see engine logs
    import logging
    logging.basicConfig(level=logging.INFO)
    
    asyncio.run(main())
