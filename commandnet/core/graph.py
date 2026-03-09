import typing
from typing import Any, Dict, List, Set, Type, Union, get_args, get_origin
from .node import Node, NODE_REGISTRY

class GraphAnalyzer:
    @staticmethod
    def _get_generic_arg(node_cls: Type[Node], index: int) -> Type[Any]:
        """Extracts C (index 0) or P (index 1) from Node[C, P]."""
        for base in getattr(node_cls, "__orig_bases__", []):
            origin = get_origin(base)
            if origin is Node or (isinstance(origin, type) and issubclass(origin, Node)):
                args = get_args(base)
                if len(args) > index:
                    return args[index]
        return dict

    @staticmethod
    def get_context_type(node_cls: Type[Node]) -> Type[Any]:
        return GraphAnalyzer._get_generic_arg(node_cls, 0)

    @staticmethod
    def get_payload_type(node_cls: Type[Node]) -> Type[Any]:
        return GraphAnalyzer._get_generic_arg(node_cls, 1)

    @staticmethod
    def _get_node_names(type_hint: Any) -> List[str]:
        origin = typing.get_origin(type_hint)
        
        if origin is Union:
            return [name for arg in typing.get_args(type_hint) for name in GraphAnalyzer._get_node_names(arg)]
            
        # Fix: Extract class name from Type[NodeClass]
        if origin is type:
            arg = typing.get_args(type_hint)[0]
            if isinstance(arg, type) and issubclass(arg, Node):
                return [arg.__name__]
            if isinstance(arg, typing.ForwardRef):
                return [arg.__forward_arg__]
            if isinstance(arg, str):
                return [arg]
                
        if isinstance(type_hint, type) and issubclass(type_hint, Node):
            return [type_hint.__name__]
            
        return []

    @staticmethod
    def get_transitions(node_cls: Type[Node]) -> Set[Type[Node]]:
        ret_annotation = node_cls.run.__annotations__.get("return")
        if not ret_annotation: return set()
        node_names = GraphAnalyzer._get_node_names(ret_annotation)
        return {NODE_REGISTRY[name] for name in node_names if name in NODE_REGISTRY}

    @staticmethod
    def build_graph(start_node: Type[Node]) -> Dict[str, List[str]]:
        graph = {}
        visited, queue = set(), [start_node]
        while queue:
            current = queue.pop(0)
            if current in visited: continue
            visited.add(current)
            transitions = GraphAnalyzer.get_transitions(current)
            graph[current.__name__] = [t.__name__ for t in transitions]
            for t in transitions:
                if t not in visited: queue.append(t)
        return graph
