import pytest
import asyncio
from commandnet import Event

@pytest.mark.asyncio
async def test_db_locking():
    from example import InMemoryDB
    db = InMemoryDB()
    
    await db.save_state("a1", "Start", {"val": 1}, None)
    
    node, ctx = await db.lock_and_load("a1")
    assert node == "Start"
    assert ctx["val"] == 1
    
    assert "a1" in db.locks
    assert db.locks["a1"].locked() is True
    
    # Save state triggers unlock_subject
    await db.save_state("a1", "Next", {"val": 2}, None)
    assert db.locks["a1"].locked() is False

@pytest.mark.asyncio
async def test_db_recompose():
    from example import InMemoryDB
    db = InMemoryDB()
    
    await db.save_state("p1", "Start", {"base": 1, "sub1": {"v": 0}}, None)
    await db.save_sub_state("p1#sub1", "p1", "Done", {"v": 99}, None)
    
    merged = await db.recompose_parent("p1")
    assert merged["sub1"]["v"] == 99
    assert "p1#sub1" not in db.sub_subjects

