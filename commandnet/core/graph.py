import typing
from typing import Any, Dict, List, Set, Type, Union
from .node import Node, NODE_REGISTRY

class GraphAnalyzer:
    """Introspects Python annotations to derive transitions using the Node Registry."""

    @staticmethod
    def get_context_type(node_cls: Type[Node]) -> Type[Any]:
        """Extracts the expected Context type from the run() method signature."""
        hints = typing.get_type_hints(node_cls.run)
        for param_name, param_type in hints.items():
            if param_name != 'return':
                return param_type
        return dict  # Fallback

    @staticmethod
    def _get_node_names(type_hint: Any) -> List[str]:
        """Recursively extracts node names from a type hint (handling Union)."""
        # Handle Union types (e.g., Union[A, B] or A | B)
        origin = typing.get_origin(type_hint)
        if origin is Union:
            return [name for arg in typing.get_args(type_hint) for name in GraphAnalyzer._get_node_names(arg)]
        
        # Handle ForwardRefs (strings in annotations)
        if isinstance(type_hint, typing.ForwardRef):
            return [type_hint.__forward_arg__]
        
        # Handle actual classes
        if isinstance(type_hint, type) and issubclass(type_hint, Node):
            return [type_hint.__name__]
            
        return []

    @staticmethod
    def get_transitions(node_cls: Type[Node]) -> Set[Type[Node]]:
        """Extracts possible next nodes by looking up names in the NODE_REGISTRY."""
        # Access the raw annotation directly from the function
        ret_annotation = node_cls.run.__annotations__.get("return")
        if not ret_annotation:
            return set()

        # Get the names of the next nodes
        node_names = GraphAnalyzer._get_node_names(ret_annotation)
        
        # Resolve names to actual classes using our Registry
        return {NODE_REGISTRY[name] for name in node_names if name in NODE_REGISTRY}

    @staticmethod
    def build_graph(start_node: Type[Node]) -> Dict[str, List[str]]:
        """Recursively builds the execution DAG."""
        graph = {}
        visited = set()
        queue = [start_node]

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            
            visited.add(current)
            transitions = GraphAnalyzer.get_transitions(current)
            graph[current.__name__] = [t.__name__ for t in transitions]

            for t in transitions:
                if t not in visited:
                    queue.append(t)
                    
        return graph
