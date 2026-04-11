import pytest
import asyncio
from pydantic import BaseModel
from commandnet import Engine, Node, Wait, Parallel

# -------------------------
# Context + Payload Models
# -------------------------

class SignalCtx(BaseModel):
    step: int = 0

class SimpleCtx(BaseModel):
    val: int = 0

# -------------------------
# Test 1: chained signals
# -------------------------

class SuccessNode(Node[SignalCtx, None]):
    async def run(self, ctx, payload=None):
        ctx.step = 99
        return None

class WaitChainNode(Node[SignalCtx, None]):
    async def run(self, ctx, payload=None):
        return Wait(
            signal_id="sig-1",
            resume_action=Wait(
                signal_id="sig-2",
                resume_action=SuccessNode
            )
        )

@pytest.mark.asyncio
async def test_chained_signals(mock_infrastructure):
    db, bus = mock_infrastructure
    engine = Engine(persistence=db, event_bus=bus, nodes=[WaitChainNode, SuccessNode])

    await engine.start_worker()
    await engine.trigger_subject("a1", WaitChainNode, SignalCtx())

    await asyncio.sleep(0.1)
    assert "sig-1" in db.waiting_room
    assert len(db.waiting_room["sig-1"]) == 1

    await engine.release_signal("sig-1")
    await asyncio.sleep(0.1)

    assert "sig-2" in db.waiting_room

    await engine.release_signal("sig-2")
    await asyncio.sleep(0.1)

    assert db.subjects["a1"]["context"]["step"] == 99

    await engine.stop()

# -------------------------
# Test 2: parallel wait
# -------------------------

class MultiCtx(BaseModel):
    sub: SignalCtx = SignalCtx()

class ParallelWaitNode(Node[MultiCtx, None]):
    async def run(self, ctx, payload=None):
        return Parallel(
            branches=[
                Wait(
                    signal_id="p-sig",
                    resume_action=SuccessNode,
                    sub_context_path="sub",
                )
            ],
            join_node=SuccessNode
        )

@pytest.mark.asyncio
async def test_parallel_wait_shorthand(mock_infrastructure):
    db, bus = mock_infrastructure
    engine = Engine(persistence=db, event_bus=bus, nodes=[ParallelWaitNode, SuccessNode])

    await engine.start_worker()
    await engine.trigger_subject("p1", ParallelWaitNode, MultiCtx())

    await asyncio.sleep(0.1)
    assert "p-sig" in db.waiting_room

    await engine.release_signal("p-sig")
    await asyncio.sleep(0.1)

    assert db.subjects["p1"]["node"] == "TERMINAL"

    await engine.stop()

# -------------------------
# Test 3: signal payload propagation (FIXED)
# -------------------------

class InputData(BaseModel):
    secret_code: int

class FinalNode(Node[SimpleCtx, InputData]):
    async def run(self, ctx: SimpleCtx, payload: InputData):
        ctx.val = payload.secret_code
        return None

class StartNode(Node[SimpleCtx, None]):
    async def run(self, ctx: SimpleCtx, payload=None):
        return Wait(signal_id="gate", resume_action=FinalNode)

@pytest.mark.asyncio
async def test_signal_payload_propagation(mock_infrastructure):
    db, bus = mock_infrastructure
    engine = Engine(persistence=db, event_bus=bus, nodes=[StartNode, FinalNode])

    await engine.start_worker()

    await engine.trigger_subject("sub-1", StartNode, SimpleCtx())
    await asyncio.sleep(0.1)

    # Release signal with payload
    await engine.release_signal(
        "gate",
        payload=InputData(secret_code=12345)
    )

    await asyncio.sleep(0.1)

    assert db.subjects["sub-1"]["context"]["val"] == 12345

    await engine.stop()
