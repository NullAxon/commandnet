import pytest
import asyncio
from commandnet import Engine, Node, Event
from example import AgentContext, AuthCheck

@pytest.mark.asyncio
async def test_engine_execution_flow(mock_infrastructure):
    db, bus = mock_infrastructure
    engine = Engine(persistence=db, event_bus=bus)
    await engine.start_worker()

    # Trigger agent
    ctx = AgentContext(user_query="test_query")
    await engine.trigger_agent("agent-1", AuthCheck, ctx)
    
    # Wait for processing (AuthCheck -> Executing -> Terminal)
    await asyncio.sleep(0.1)
    
    # Verify state directly via dictionary to avoid lock interference
    agent_data = db.agents.get("agent-1")
    assert agent_data is not None
    
    # State remains "Executing" in DB because terminal states don't trigger save_state
    assert agent_data["node"] == "Executing" 
    assert agent_data["context"]["user_query"] == "test_query"
    assert agent_data["context"]["is_authenticated"] is True

@pytest.mark.asyncio
async def test_node_failure_handling(mock_infrastructure):
    class FailingNode(Node):
        async def run(self, ctx):
            raise ValueError("Boom!")

    db, bus = mock_infrastructure
    engine = Engine(persistence=db, event_bus=bus)
    
    # Setup state so the engine doesn't exit early
    evt = Event(agent_id='a1', node_name='FailingNode')
    await db.save_state("a1", "FailingNode", {}, evt)
    
    # Execution should raise exception in the worker loop
    with pytest.raises(ValueError):
        await engine.process_event(evt)
