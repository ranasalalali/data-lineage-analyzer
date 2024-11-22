class LineageGraph:
    def __init__(self):
        self.graph = {}
        self.added_edges = set()

    def add_edge(self, parent, child):
        if parent == child or (child, parent) in self.added_edges:
            return
        if parent not in self.graph:
            self.graph[parent] = {"children": set(), "parents": set()}
        if child not in self.graph:
            self.graph[child] = {"children": set(), "parents": set()}
        self.graph[parent]["children"].add(child)
        self.graph[child]["parents"].add(parent)
        self.added_edges.add((parent, child))

    def get_parents(self, node):
        return list(self.graph.get(node, {}).get("parents", []))

    def get_children(self, node):
        return list(self.graph.get(node, {}).get("children", []))
