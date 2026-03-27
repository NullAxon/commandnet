# 🕸️ CommandNet

**CommandNet** is a lightweight, distributed, event-driven state machine and typed node graph runtime for Python 3.11+.

It enables you to build durable, asynchronous workflows using strictly typed Python classes and Pydantic models. Unlike heavy orchestrators, CommandNet provides a minimal core that executes graph-based logic across distributed workers using your choice of database and message broker.

---

## 🚀 Installation

Install CommandNet via pip:

```bash
pip install commandnet
```

---

## ✨ Key Features

- **Type-Safe Transitions**: The execution graph is inferred directly from Python type hints (`-> Union[Type[NodeA], Type[NodeB]]`). No external JSON/YAML definitions.
- **Pydantic State Management**: Context is automatically serialized and rehydrated into Pydantic models with full validation.
- **Distributed by Design**: Built-in row-level locking and idempotency support for safe execution across horizontally scaled workers.
- **Fan-out / Fan-in (Parallel)**: Native support for triggering multiple concurrent sub-tasks and merging results back into the parent state.
- **Native Scheduling**: Schedule nodes to run after a specific delay with built-in idempotency keys to prevent duplicate execution.
- **Static Validation**: Validate your entire workflow graph (types and connectivity) before a single event is processed.

---

## 🛠️ Quick Start

### 1. Define Your Context
The "Context" is the persistent state of your subject, defined using Pydantic.

```python
from pydantic import BaseModel

class WorkflowCtx(BaseModel):
    user_id: str
    status: str = "pending"
    attempts: int = 0
```

### 2. Define Your Nodes
Nodes are the building blocks of your graph. The return type hint of the `run` method defines the edges of your DAG.

```python
from typing import Union, Type, Optional
from commandnet import Node

class ProcessPayment(Node[WorkflowCtx, None]):
    async def run(self, ctx: WorkflowCtx, payload: None) -> None:
        print(f"Processing for {ctx.user_id}...")
        ctx.status = "complete"
        return None # Terminal state

class CheckRisk(Node[WorkflowCtx, None]):
    # The return type explicitly defines the possible next nodes
    async def run(self, ctx: WorkflowCtx, payload: None) -> Union[Type[ProcessPayment], None]:
        ctx.attempts += 1
        if ctx.attempts > 3:
            return None # Failure/Stop
        return ProcessPayment
```

### 3. Advanced Routing (Parallel & Scheduled)
CommandNet supports complex workflow patterns beyond simple linear transitions.

#### Parallel Fan-out
```python
from commandnet import Parallel, ParallelTask

class StartAnalysis(Node[WorkflowCtx, None]):
    async def run(self, ctx: WorkflowCtx, payload: None) -> Parallel:
        return Parallel(
            branches=[
                ParallelTask(node_cls=SubTaskNode, sub_context_path="sub_data_1"),
                ParallelTask(node_cls=SubTaskNode, sub_context_path="sub_data_2")
            ],
            join_node=FinalMergeNode
        )
```

#### Delayed Scheduling
```python
from commandnet import Schedule

class RetryNode(Node[WorkflowCtx, None]):
    async def run(self, ctx: WorkflowCtx, payload: None) -> Schedule:
        return Schedule(
            node_cls=CheckRisk,
            delay_seconds=300,
            idempotency_key=f"retry-{ctx.attempts}"
        )
```

---

## 🏗️ Infrastructure Integration

CommandNet is unopinionated about your stack. You simply implement two abstract interfaces:

1.  **`Persistence`**: Handles locking state in your DB (Postgres, Redis, DynamoDB).
2.  **`EventBus`**: Handles moving events between workers (RabbitMQ, NATS, SQS).

```python
from commandnet import Engine

# Implement these interfaces for your specific stack
db = MyPostgresAdapter()
bus = MyRabbitMQAdapter()

engine = Engine(persistence=db, event_bus=bus)

# Start the worker loop
await engine.start_worker()

# Trigger an execution
await engine.trigger_subject("subject-123", CheckRisk, WorkflowCtx(user_id="user_abc"))
```

---

## 🔍 Static Analysis & Safety

Prevent runtime failures by validating your graph during CI/CD or at startup. The `GraphAnalyzer` checks for disconnected nodes and ensures that if `NodeA` transitions to `NodeB`, they share compatible `Context` types.

```python
from commandnet import GraphAnalyzer

# This will raise a TypeError if types don't match or a ValueError if edges are broken
GraphAnalyzer.validate(CheckRisk)

# Generate a dictionary representation of your DAG
dag = GraphAnalyzer.build_graph(CheckRisk)
print(dag) # {'CheckRisk': ['ProcessPayment'], 'ProcessPayment': []}
```

---

## ⚖️ Design Philosophy

1.  **Code as Truth**: If your IDE can navigate it, CommandNet can run it. No "magic strings."
2.  **Stateless Execution**: Workers don't keep local state. Every node execution starts with a fresh database fetch and lock.
3.  **Zero Magic**: No hidden background threads or global singletons. You control the `Engine` lifecycle.
4.  **Ownership**: CommandNet provides the orchestration logic; you provide the infrastructure.

## 📄 License

MIT

