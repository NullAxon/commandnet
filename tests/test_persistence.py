import pytest
import asyncio

@pytest.mark.asyncio
async def test_db_locking():
    # Test the specific lock behavior in the example code
    from example import InMemoryDB
    db = InMemoryDB()
    
    await db.save_state("a1", "Start", {"val": 1}, None)
    node, ctx = await db.load_and_lock_agent("a1")
    
    assert node == "Start"
    assert db.lock.locked() is True
    
    db.lock.release()
