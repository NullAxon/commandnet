import typing
import inspect
from typing import Any, Dict, List, Set, Type, Union, get_args, get_origin
from .node import Node

class GraphAnalyzer:
    @staticmethod
    def _get_generic_arg(node_cls: Type[Node], index: int) -> Type[Any]:
        for base in getattr(node_cls, "__orig_bases__", []):
            origin = get_origin(base)
            if origin is Node or (inspect.isclass(origin) and issubclass(origin, Node)):
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
            names = []
            for arg in typing.get_args(type_hint):
                if arg is type(None): continue
                names.extend(GraphAnalyzer._get_node_names(arg))
            return names
        if origin in (type, typing.Type):
            arg = typing.get_args(type_hint)[0]
            if inspect.isclass(arg) and issubclass(arg, Node):
                return [arg.get_node_name()]
            if isinstance(arg, typing.ForwardRef):
                return [arg.__forward_arg__]
            if isinstance(arg, str):
                return [arg]
        if inspect.isclass(type_hint) and issubclass(type_hint, Node):
            return [type_hint.get_node_name()]
        return []

    @staticmethod
    def get_transitions(node_cls: Type[Node], registry: Dict[str, Type[Node]]) -> Set[Type[Node]]:
        ret_annotation = node_cls.run.__annotations__.get("return")
        if not ret_annotation: return set()
        
        node_names = GraphAnalyzer._get_node_names(ret_annotation)
        transitions = set()
        
        for name in node_names:
            if name not in registry:
                raise RuntimeError(
                    f"Graph Error: Node '{node_cls.get_node_name()}' references unknown node '{name}'. "
                    "Ensure it is passed to the Engine/Registry."
                )
            transitions.add(registry[name])
        return transitions

    @staticmethod
    def build_graph(start_node: Type[Node], registry: Dict[str, Type[Node]]) -> Dict[str, List[str]]:
        graph = {}
        visited, queue = set(), [start_node]
        while queue:
            current = queue.pop(0)
            if current in visited: continue
            visited.add(current)
            transitions = GraphAnalyzer.get_transitions(current, registry)
            graph[current.get_node_name()] = [t.get_node_name() for t in transitions]
            for t in transitions:
                if t not in visited: queue.append(t)
        return graph

    @staticmethod
    def validate(start_node: Type[Node], registry: Dict[str, Type[Node]]):
        graph = GraphAnalyzer.build_graph(start_node, registry)
        for node_name, edges in graph.items():
            node_cls = registry.get(node_name)
            if not node_cls:
                raise ValueError(f"Validation Error: Node '{node_name}' missing from registry.")
            
            source_ctx = GraphAnalyzer.get_context_type(node_cls)
            for edge in edges:
                target_cls = registry.get(edge)
                if not target_cls:
                    raise ValueError(f"Validation Error: Edge '{edge}' from '{node_name}' does not exist.")
                
                target_ctx = GraphAnalyzer.get_context_type(target_cls)
                if source_ctx is not target_ctx and not issubclass(source_ctx, target_ctx):
                    raise TypeError(
                        f"Type Error in Graph: '{node_name}' transitions to '{edge}', "
                        f"but their Context types do not match! "
                        f"({source_ctx.__name__} -> {target_ctx.__name__})"
                    )
        return True

