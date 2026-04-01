from __future__ import annotations


class LineageGraph:
    def __init__(self) -> None:
        self.graph: dict[str, dict[str, set[str]]] = {}
        self.added_edges: set[tuple[str, str]] = set()

    def add_edge(self, parent: str, child: str) -> None:
        if parent == child or (child, parent) in self.added_edges:
            return
        self.graph.setdefault(parent, {"children": set(), "parents": set()})
        self.graph.setdefault(child, {"children": set(), "parents": set()})
        self.graph[parent]["children"].add(child)
        self.graph[child]["parents"].add(parent)
        self.added_edges.add((parent, child))

    def get_parents(self, node: str) -> list[str]:
        return sorted(self.graph.get(node, {}).get("parents", []))

    def get_children(self, node: str) -> list[str]:
        return sorted(self.graph.get(node, {}).get("children", []))
