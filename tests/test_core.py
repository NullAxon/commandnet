import pytest
from commandnet import GraphAnalyzer, Node
from typing import Union, Type

# Test DAG definition
class Final(Node):
    async def run(self, ctx):
        return None
class Mid(Node):
    async def run(self, ctx) -> Final:
        return Final()
class Start(Node):
    async def run(self, ctx) -> Union[Mid, Final]:
        return Mid()

def test_graph_analyzer():
    dag = GraphAnalyzer.build_graph(Start)
    assert "Start" in dag
    assert "Mid" in dag["Start"]
    assert "Final" in dag["Mid"]
    assert len(dag["Final"]) == 0
