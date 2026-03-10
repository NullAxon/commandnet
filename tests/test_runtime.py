import pytest
import asyncio
from pydantic import BaseModel
from commandnet import Engine, Node, Event

class SimpleCtx(BaseModel):
    val: int = 0

class TestSimpleNextNode(Node[SimpleCtx, None]):
    async def run(self, ctx, payload=None):
        ctx.val = 2
        return None

class TestSimpleStartNode(Node[SimpleCtx, None]):
    async def run(self, ctx, payload=None):
        ctx.val = 1
        return TestSimpleNextNode

@pytest.mark.asyncio
async def test_engine_execution_flow(mock_infrastructure):
    db, bus = mock_infrastructure
    engine = Engine(persistence=db, event_bus=bus, nodes=[TestSimpleStartNode, TestSimpleNextNode])
    await engine.start_worker()

    ctx = SimpleCtx()
    await engine.trigger_agent("agent-test-1", TestSimpleStartNode, ctx)
    
    await asyncio.sleep(0.1)
    
    agent_data = db.agents.get("agent-test-1")
    assert agent_data is not None
    assert agent_data["node"] == "TERMINAL" 
    assert agent_data["context"]["val"] == 2
