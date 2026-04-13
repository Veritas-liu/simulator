class Edge:
    def __init__(self, src, dst, bandwidth):
        self.src = src
        self.dst = dst
        self.bandwidth = bandwidth
        self.flow = 0
    def clear_flow(self):
        self.flow = 0
    def add_flow(self, flow):
        self.flow += flow

class Topo:
    def __init__(self, n_node, n_gpu, edges):
        self.n_node = n_node
        self.n_gpu = n_gpu
        self.edges = {}
        for src, dst, bandwidth in edges:
            self.edges[src, dst] = Edge(src, dst, bandwidth)
            self.edges[dst, src] = Edge(dst, src, bandwidth)
    def clear_flow(self):
        for edge in self.edges.values():
            edge.clear_flow()
    def add_flow(self, flow):
        for i in range(len(flow.path)-1):
            self.edges[flow.path[i], flow.path[i+1]].add_flow(flow.size)

class Flow:
    def __init__(self, id, size, src, dst, next_flows=[], path=None):
        self.id = id
        self.size = size
        self.src = src
        self.dst = dst
        self.next_flows = next_flows
        self.path = path
        self.dependency_count = 0
    
    def toStr(self):
        return f"idx: {self.id}\t{self.src} -> {self.dst}\tsize={self.size}\npath={self.path}\tnext_flows={self.next_flows}"
    
def route_flows(topo, route, flows):
    routed_flows = []
    for flow in flows:
        for path, ratio in route[flow.src][flow.dst]:
            routed_flows.append(Flow(flow.id, flow.size * ratio, flow.src, flow.dst, [], path))
    for flow in routed_flows:
        for idx2, flow2 in enumerate(routed_flows):
            if flow2.id in flows[flow.id].next_flows:
                flow.next_flows.append(idx2)
                flow2.dependency_count += 1
    for idx, flow in enumerate(routed_flows):
        flow.id = idx
    return routed_flows