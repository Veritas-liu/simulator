import heapq

class Sim:
    def __init__(self, topo):
        self.topo = topo
        self.flows = []
        self.time = 0

        # heap: (finish_time, flow_id, version)
        self.heap = []
        self.flow_id = 0
        self.id2flow = {}

        # 初始化 edge
        for edge in self.topo.edges.values():
            edge.flow = 0.0
            edge.flows = set()

    def _compute_finish_time(self, flow):
        """计算 flow 完成时间（基于当前 edge 状态）"""
        max_time = 0.0
        for edge in flow.edges:
            if edge.bandwidth > 0:
                t = edge.flow / edge.bandwidth
                if t > max_time:
                    max_time = t
        return max_time

    def add_flow(self, flow):
        """新增 flow（支持动态加入）"""
        # 分配 id
        flow.id = self.flow_id
        self.flow_id += 1
        self.id2flow[flow.id] = flow

        # lazy version
        flow.version = 0

        # 缓存 edges
        flow.edges = [
            self.topo.edges[flow.path[i], flow.path[i+1]]
            for i in range(len(flow.path) - 1)
        ]

        self.flows.append(flow)

        # 注册到 edge
        for edge in flow.edges:
            edge.flows.add(flow)
            edge.flow += flow.size

        # 初始化 finish time
        flow.finish_time = self._compute_finish_time(flow)
        heapq.heappush(self.heap, (flow.finish_time, flow.id, flow.version))

    def finished(self):
        return len(self.flows) == 0

    def _pop_valid_flow(self):
        """从 heap 取出合法最小 flow（lazy 删除过期）"""
        while True:
            t, fid, ver = heapq.heappop(self.heap)
            flow = self.id2flow.get(fid, None)
            if flow is None:
                continue
            if flow.version == ver:
                return t, flow

    def step(self):
        if self.finished():
            return None

        # ==============================
        # 1. O(log N) 找最小完成 flow
        # ==============================
        min_time, flow_to_finish = self._pop_valid_flow()

        self.time += min_time

        # ==============================
        # 2. 找受影响 flows
        # ==============================
        affected = set()
        for edge in flow_to_finish.edges:
            for f in edge.flows:
                if f != flow_to_finish:
                    affected.add(f)

        # ==============================
        # 3. 移除完成 flow
        # ==============================
        for edge in flow_to_finish.edges:
            edge.flows.remove(flow_to_finish)
            edge.flow -= flow_to_finish.size

        self.flows.remove(flow_to_finish)
        del self.id2flow[flow_to_finish.id]

        # ==============================
        # 4. 更新受影响 flow（并重算 finish_time）
        # ==============================
        for flow in affected:
            # 更新 size
            bandwidth = float('inf')
            for edge in flow.edges:
                if edge.flow > 0:
                    bandwidth = min(
                        bandwidth,
                        edge.bandwidth / edge.flow * flow.size
                    )

            flow.size -= bandwidth * min_time

            # 如果还活着，更新 heap
            if flow.size > 1e-6:
                flow.version += 1
                flow.finish_time = self._compute_finish_time(flow)
                heapq.heappush(
                    self.heap,
                    (flow.finish_time, flow.id, flow.version)
                )
            else:
                # 已完成，标记删除
                for edge in flow.edges:
                    if flow in edge.flows:
                        edge.flows.remove(flow)
                        edge.flow -= flow.size
                if flow in self.flows:
                    self.flows.remove(flow)
                if flow.id in self.id2flow:
                    del self.id2flow[flow.id]

        return flow_to_finish

        
class Sim_slow:
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