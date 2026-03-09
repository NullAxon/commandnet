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
    # Tests the updated analyzer logic for Optional and generic Type[]
    async def run(self, ctx, payload=None) -> Optional[Type[TestStart]]: return TestStart

def test_graph_analyzer():
    dag = GraphAnalyzer.build_graph(TestStart)
    assert "TestStart" in dag
    assert "TestMid" in dag["TestStart"]
    assert "TestFinal" in dag["TestMid"]
    assert len(dag["TestFinal"]) == 0

def test_robust_graph_analyzer():
    dag = GraphAnalyzer.build_graph(RobustTypeHintNode)
    assert "TestStart" in dag["RobustTypeHintNode"]

def test_graph_extracts_generics():
    c_type = GraphAnalyzer.get_context_type(TestStart)
    assert c_type == Ctx
    
def test_static_validation():
    # Should not raise any exceptions
    assert GraphAnalyzer.validate(TestStart) is True
