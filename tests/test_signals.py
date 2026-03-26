import pytest
import asyncio
from pydantic import BaseModel
from commandnet import Engine, Node, Wait, Parallel, ParallelTask

class SignalCtx(BaseModel):
    step: int = 0

class SuccessNode(Node[SignalCtx, None]):
    async def run(self, ctx, payload=None):
        ctx.step = 99
        return None

class WaitChainNode(Node[SignalCtx, None]):
    async def run(self, ctx, payload=None):
        # Recursion: Wait 1 -> Wait 2 -> Success
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
    await engine.trigger_agent("a1", WaitChainNode, SignalCtx())
    await asyncio.sleep(0.1) 
    assert "sig-1" in db.waiting_room
    assert len(db.waiting_room["sig-1"]) == 1
    await engine.release_signal("sig-1")
    await asyncio.sleep(0.1)
    assert "sig-2" in db.waiting_room
    await engine.release_signal("sig-2")
    await asyncio.sleep(0.1) # Process terminal
    assert db.agents["a1"]["context"]["step"] == 99
    await engine.stop()


@pytest.mark.asyncio
async def test_parallel_wait_shorthand(mock_infrastructure):
    db, bus = mock_infrastructure
    
    class MultiCtx(BaseModel):
        sub: SignalCtx = SignalCtx()

    class ParallelWaitNode(Node[MultiCtx, None]):
        async def run(self, ctx, payload=None):
            return Parallel(
                branches=[
                    Wait(signal_id="p-sig", resume_action=SuccessNode, sub_context_path="sub")
                ],
                join_node=SuccessNode
            )

    engine = Engine(persistence=db, event_bus=bus, nodes=[ParallelWaitNode, SuccessNode])
    await engine.start_worker() # CRITICAL
    await engine.trigger_agent("p1", ParallelWaitNode, MultiCtx())
    await asyncio.sleep(0.1) # Allow node to run and branches to park
    assert "p-sig" in db.waiting_room
    await engine.release_signal("p-sig")
    await asyncio.sleep(0.1) 
    assert db.agents["p1"]["node"] == "TERMINAL"
    await engine.stop()
