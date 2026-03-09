import pytest
import asyncio
from commandnet import Event

@pytest.mark.asyncio
async def test_db_locking():
    # Test the updated per-agent lock behavior in the example code
    from example import InMemoryDB
    db = InMemoryDB()
    
    # Pre-populate state
    await db.save_state("a1", "Start", {"val": 1}, None)
    
    # Simulate an engine worker loading and locking the agent row
    node, ctx = await db.load_and_lock_agent("a1")
    
    assert node == "Start"
    assert ctx["val"] == 1
    
    # Assert that a lock was created for 'a1' and is currently held
    assert "a1" in db.locks
    assert db.locks["a1"].locked() is True
    
    # Assert that saving state releases the lock
    await db.save_state("a1", "Next", {"val": 2}, None)
    assert db.locks["a1"].locked() is False

@pytest.mark.asyncio
async def test_db_recompose():
    from example import InMemoryDB
    db = InMemoryDB()
    
    # Setup initial parent state
    await db.save_state("p1", "Start", {"base": 1, "sub1": {"v": 0}}, None)
    
    # Save a modified sub-state independently
    await db.save_sub_state("p1#sub1", "p1", "Done", {"v": 99}, None)
    
    # Execute structural merge
    merged = await db.recompose_parent("p1")
    
    # Assert the sub-context was patched back into the main tree
    assert merged["sub1"]["v"] == 99
    
    # Assert the sub-context table was cleaned up
    assert "p1#sub1" not in db.sub_agents
