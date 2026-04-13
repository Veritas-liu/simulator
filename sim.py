class Sim:
    def __init__(self, topo):
        self.topo = topo
        self.flows = []
        self.time = 0
    def finished(self):
        return len(self.flows) == 0
    def add_flow(self, flow):
        self.flows.append(flow)
    def step(self):
        if self.finished():
            return None
        self.topo.clear_flow()
        for flow in self.flows:
            self.topo.add_flow(flow)
        min_time = float('inf')
        flow_to_finish = None
        for flow in self.flows:
            flow_time = -float('inf')
            for i in range(len(flow.path)-1):
                edge = self.topo.edges[flow.path[i], flow.path[i+1]]
                edge_time = edge.flow / edge.bandwidth
                flow_time = max(flow_time, edge_time)
            if flow_time < min_time:
                min_time = flow_time
                flow_to_finish = flow
        if flow_to_finish is not None:
            self.time += min_time
            self.flows.remove(flow_to_finish)
            for flow in self.flows:
                bandwidth = float('inf')
                for i in range(len(flow.path)-1):
                    edge = self.topo.edges[flow.path[i], flow.path[i+1]]
                    bandwidth = min(bandwidth, edge.bandwidth / edge.flow * flow.size)
                flow.size -= bandwidth * min_time
                if flow.size < 1e-6:
                    flow.size = 0
            self.flows = [flow for flow in self.flows if flow.size > 1e-6]
        self.topo.clear_flow()
        return flow_to_finish