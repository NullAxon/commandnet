# CommandNet

**CommandNet** is a lightweight, distributed, event-driven state machine and typed node graph runtime for Python 3.11+.

It allows you to build durable, asynchronous workflow graphs using strictly typed Python classes and Pydantic models. **CommandNet is not an orchestrator** (no built-in crons, external scheduling, or magic workflow DSLs). Instead, it provides a minimal, dependency-free (except Pydantic) core for executing graph-based logic across distributed workers using any database and message broker you choose.

## Features

- **Strictly Typed Transitions**: Execution graphs are inferred directly from Python type hints (`-> NextNode`). No string-based identifiers.
- **First-Class Pydantic Support**: Context state is automatically serialized to your database and strictly rehydrated into Pydantic models before node execution.
- **Distributed-Worker Ready**: Safely runs across multiple horizontally scaled consumers via row-level locking patterns and idempotency checks.
- **Bring Your Own Infrastructure**: Clean abstract interfaces for `Persistence` (Postgres, SQLite) and `EventBus` (RabbitMQ, NATS, Redis).
- **Zero Magic**: Deterministic execution, highly observable, and easy to test.

---

## Installation

```bash
pip install commandnet
```
*Or with Poetry:*
```bash
poetry add commandnet
```

---

## Quick Start

### 1. Define your State (Context)
Use Pydantic to define the mutable state that will be passed through your graph. CommandNet will automatically validate and rehydrate this data from your database.

```python
from pydantic import BaseModel, Field

class AgentContext(BaseModel):
    user_query: str
    is_authenticated: bool = False
    attempts: int = Field(default=0, ge=0)
```

### 2. Define your Nodes
Nodes subclass `Node` and must implement an `async def run(self, ctx)`. The **return type hint** dictates the execution graph!

```python
from typing import Union, Type
from commandnet import Node

class Denied(Node[AgentContext]):
    async def run(self, ctx: AgentContext) -> None: # Returning None means Terminal state
        print(f"[{ctx.user_query}] -> Access Denied.")
        return None

class Executing(Node[AgentContext]):
    async def run(self, ctx: AgentContext) -> None: 
        print(f"[{ctx.user_query}] -> Running task successfully!")
        return None

class AuthCheck(Node[AgentContext]):
    # The return type explicitly defines the DAG edges:
    async def run(self, ctx: AgentContext) -> Union[Type[Executing], Type[Denied]]:
        print(f"[{ctx.user_query}] -> Checking Auth...")
        ctx.attempts += 1
        
        if ctx.user_query == "hack_system":
            return Denied
            
        ctx.is_authenticated = True
        return Executing
```

### 3. Run the Engine
Implement the `Persistence` and `EventBus` interfaces for your infrastructure (or use in-memory mocks for testing), and trigger the agent.

```python
import asyncio
from commandnet import Engine, GraphAnalyzer

# Note: You must implement Persistence and EventBus interfaces
# See the `interfaces/` directory for expected methods.
from my_app.adapters import PostgresPersistence, RabbitMQBus 

async def main():
    # 1. (Optional) Introspect your graph to visualize or validate it
    dag = GraphAnalyzer.build_graph(AuthCheck)
    print("Graph Structure:", dag) 
    # Output: {'AuthCheck': ['Executing', 'Denied'], 'Executing': [], 'Denied': []}

    # 2. Initialize Engine
    db = PostgresPersistence()
    bus = RabbitMQBus()
    engine = Engine(persistence=db, event_bus=bus)
    
    # 3. Start listening to the event queue
    await engine.start_worker()
    
    # 4. Trigger an execution
    initial_context = AgentContext(user_query="clean_logs")
    await engine.trigger_agent(
        agent_id="agent-001", 
        start_node=AuthCheck, 
        initial_context=initial_context
    )

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Pluggable Architecture

CommandNet forces you to own your infrastructure. You connect it to your stack by implementing three simple interfaces:

### `Persistence`
Handles locking, saving, and loading the agent's context.
```python
class Persistence(ABC):
    async def load_and_lock_agent(self, agent_id: str) -> Tuple[Optional[str], Optional[Dict]]: ...
    async def save_state(self, agent_id: str, node_name: str, context: Dict, event: Event): ...
```

### `EventBus`
Handles emitting transitions and consuming events in your worker loop.
```python
class EventBus(ABC):
    async def publish(self, event: Event): ...
    async def subscribe(self, handler: Callable[[Event], Coroutine]): ...
```

### `Observer` (Optional)
Hooks for integrating OpenTelemetry, Prometheus, or custom logging.
```python
class Observer(ABC):
    async def on_transition(self, agent_id: str, from_node: str, to_node: str, duration_ms: float): ...
    async def on_error(self, agent_id: str, node: str, error: Exception): ...
```

---

## Design Principles

1. **Minimalism**: CommandNet aims to be under 1,000 lines of core code. It does one thing perfectly: reliably transitioning state machines via queue events.
2. **Stateless Nodes**: Node classes are instantiated fresh on every execution. All mutable state lives exclusively in the Pydantic `Context`.
3. **No String Magic**: You shouldn't need a massive JSON file or string literals to define your graph. Python's `typing` module is powerful enough. If your IDE can autocomple it, CommandNet can route it.

## License

MIT
