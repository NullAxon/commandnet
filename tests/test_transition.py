import pytest
import asyncio
from pydantic import BaseModel
from typing import Type
from commandnet import Engine, Node, Transition

# 1. Define State and Payload Models
class TransitionCtx(BaseModel):
    received_message: str = ""
    step_count: int = 0

class TransitionPayload(BaseModel):
    content: str
    code: int

# 2. Define the Destination Node
class DestinationNode(Node[TransitionCtx, TransitionPayload]):
    async def run(self, ctx: TransitionCtx, payload: TransitionPayload) -> None:
        # Verify the payload was delivered correctly
        ctx.received_message = f"{payload.content}:{payload.code}"
        ctx.step_count += 1
        return None

# 3. Define the Source Node
class SourceNode(Node[TransitionCtx, None]):
    async def run(self, ctx: TransitionCtx, payload: None) -> Transition:
        ctx.step_count += 1
        # Explicitly return a Transition object with a payload
        return Transition(
            node_cls=DestinationNode,
            payload=TransitionPayload(content="Hello from Transition", code=200)
        )

@pytest.mark.asyncio
async def test_transition_with_payload(mock_infrastructure):
    """
    Tests that a node can return a Transition object to trigger the next node
    with an explicit Pydantic payload.
    """
    db, bus = mock_infrastructure
    
    # Initialize Engine with both nodes
    engine = Engine(
        persistence=db, 
        event_bus=bus, 
        nodes=[SourceNode, DestinationNode]
    )
    
    await engine.start_worker()

    # Trigger the first node
    subject_id = "trans-test-1"
    initial_ctx = TransitionCtx()
    await engine.trigger_subject(subject_id, SourceNode, initial_ctx)
    
    # Wait for execution to finish (Source -> Destination -> Terminal)
    await asyncio.sleep(0.2)
    
    # Fetch results from DB
    node_name, ctx_dict = await db.lock_and_load(subject_id)
    await db.unlock_subject(subject_id)
    
    # Assertions
    assert node_name == "TERMINAL"
    assert ctx_dict["step_count"] == 2
    assert ctx_dict["received_message"] == "Hello from Transition:200"

@pytest.mark.asyncio
async def test_transition_fallback_payload(mock_infrastructure):
    """
    Tests that a Transition without a payload inherits the payload 
    from the current event (propagation).
    """
    db, bus = mock_infrastructure
    
    class SimpleNode(Node[TransitionCtx, TransitionPayload]):
        async def run(self, ctx: TransitionCtx, payload: TransitionPayload) -> Transition:
            # Return transition without specifying payload
            return Transition(node_cls=DestinationNode)

    engine = Engine(
        persistence=db, 
        event_bus=bus, 
        nodes=[SimpleNode, DestinationNode]
    )
    await engine.start_worker()

    subject_id = "trans-test-2"
    original_payload = TransitionPayload(content="Inherited", code=500)
    
    await engine.trigger_subject(subject_id, SimpleNode, TransitionCtx(), payload=original_payload)
    await asyncio.sleep(0.2)
    
    _, ctx_dict = await db.lock_and_load(subject_id)
    await db.unlock_subject(subject_id)
    
    # DestinationNode should have received the payload passed to SimpleNode
    assert ctx_dict["received_message"] == "Inherited:500"
