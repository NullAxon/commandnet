import pytest
from pydantic import BaseModel
from commandnet import GraphAnalyzer, Node
from typing import Union, Type, Optional

class Ctx(BaseModel): pass

class TestFinal(Node[Ctx, None]):
    async def run(self, ctx, payload=None): return None

class TestMid(Node[Ctx, None]):
    async def run(self, ctx, payload=None) -> Type[TestFinal]: return TestFinal

class TestStart(Node[Ctx, None]):
    async def run(self, ctx, payload=None) -> Union[Type[TestMid], Type[TestFinal]]: return TestMid

class RobustTypeHintNode(Node[Ctx, None]):
    async def run(self, ctx, payload=None) -> Optional[Type[TestStart]]: return TestStart

# Create a registry for the tests to use
TEST_REGISTRY = {
    "TestStart": TestStart,
    "TestMid": TestMid,
    "TestFinal": TestFinal,
    "RobustTypeHintNode": RobustTypeHintNode
}

def test_graph_analyzer():
    # Pass the registry explicitly
    dag = GraphAnalyzer.build_graph(TestStart, TEST_REGISTRY)
    assert "TestStart" in dag
    assert "TestMid" in dag["TestStart"]
    assert "TestFinal" in dag["TestMid"]
    assert len(dag["TestFinal"]) == 0

def test_robust_graph_analyzer():
    # Pass the registry explicitly
    dag = GraphAnalyzer.build_graph(RobustTypeHintNode, TEST_REGISTRY)
    assert "TestStart" in dag["RobustTypeHintNode"]

def test_graph_extracts_generics():
    c_type = GraphAnalyzer.get_context_type(TestStart)
    assert c_type == Ctx
    
def test_static_validation():
    # Should not raise any exceptions when registry is provided
    assert GraphAnalyzer.validate(TestStart, TEST_REGISTRY) is True

def test_static_validation_missing_node():
    # Test that it fails if a node is missing from the registry
    incomplete_registry = {"TestStart": TestStart} # Missing TestMid/TestFinal
    with pytest.raises(RuntimeError, match="references unknown node"):
        GraphAnalyzer.validate(TestStart, incomplete_registry)

