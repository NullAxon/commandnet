import pytest
import asyncio
from pydantic import BaseModel
from commandnet import Engine, Node, Call, Interrupt, Wait

class AdvCtx(BaseModel):
    val: str = ""
    count: int = 0
    is_cancelled: bool = False

class SharedNode(Node[AdvCtx, None]):
    async def run(self, ctx: AdvCtx, payload=None):
        ctx.count += 1
        await asyncio.sleep(0.2)
        return None

class CallerNode(Node[AdvCtx, None]):
    async def run(self, ctx: AdvCtx, payload=None):
        return Call(
            node_cls=SharedNode,
            idempotency_key="unique_key_1",
            resume_action=None
        )

class SignalNode(Node[AdvCtx, None]):
    def __init__(self):
        self.secret = "internal_state"

    async def run(self, ctx: AdvCtx, payload=None):
        return Wait(signal_id="ping", resume_action=None)

    async def on_signal(self, ctx: AdvCtx, signal_id: str, payload: str):
        # Accessing 'self' and payload
        ctx.val = f"{self.secret}_{payload}"
        return None

@pytest.mark.asyncio
async def test_call_deduplication(mock_infrastructure):
    db, bus = mock_infrastructure
    engine = Engine(persistence=db, event_bus=bus, nodes=[CallerNode, SharedNode])
    await engine.start_worker()

    await engine.trigger_subject("a1", CallerNode, AdvCtx())
    await engine.trigger_subject("a2", CallerNode, AdvCtx())

    # Give enough time for the shared task and the resolution loop
    await asyncio.sleep(0.6)

    assert db.subjects["a1"]["node"] == "TERMINAL"
    assert db.subjects["a2"]["node"] == "TERMINAL"
    
    # Verify the virtual subject that did the work exists in the main subjects table
    assert db.subjects["call#unique_key_1"]["context"]["count"] == 1

@pytest.mark.asyncio
async def test_on_signal_with_self(mock_infrastructure):
    db, bus = mock_infrastructure
    engine = Engine(persistence=db, event_bus=bus, nodes=[SignalNode])
    await engine.start_worker()

    await engine.trigger_subject("s1", SignalNode, AdvCtx())
    await asyncio.sleep(0.1)
    
    await engine.signal_node("s1", "ping", payload="hello")
    await asyncio.sleep(0.1)
    
    assert db.subjects["s1"]["context"]["val"] == "internal_state_hello"

@pytest.mark.asyncio
async def test_hard_interrupt(mock_infrastructure):
    db, bus = mock_infrastructure
    
    class LongNode(Node[AdvCtx, None]):
        async def run(self, ctx: AdvCtx, payload=None):
            await asyncio.sleep(10) # Should be killed
            ctx.val = "finished"
            return None

    engine = Engine(persistence=db, event_bus=bus, nodes=[LongNode])
    await engine.start_worker()
    
    await engine.trigger_subject("kill_me", LongNode, AdvCtx())
    await asyncio.sleep(0.1)
    
    await engine.cancel_subject("kill_me", hard=True)
    await asyncio.sleep(0.1)
    
    # Context should NOT be "finished"
    assert db.subjects["kill_me"]["context"]["val"] == ""

