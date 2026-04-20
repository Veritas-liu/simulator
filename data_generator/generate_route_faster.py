import json
import sys
import random
from collections import defaultdict


def build_graph(edges, n_node):
    graph = defaultdict(list)
    for u, v, bw in edges:
        graph[u].append((v, bw))
    return graph


# ✅ 核心：DP + 直接构建 (src, dst) → paths
def compute_routes(graph, n_node, max_hops):
    # layer[h][src] = 所有从 src 出发、长度为 h 的路径
    layer = [defaultdict(list) for _ in range(max_hops + 1)]

    # routes[src][dst] = 所有路径
    routes = defaultdict(lambda: defaultdict(list))

    # 初始化：0-hop
    for u in range(n_node):
        layer[0][u] = [[u]]

    # 按 hop 分层 DP
    for h in range(1, max_hops + 1):
        for src in range(n_node):
            for path in layer[h - 1][src]:
                last = path[-1]
                for nei, _ in graph[last]:
                    if nei in path:  # 保证 simple path
                        continue

                    new_path = path + [nei]

                    # ✅ 直接记录 src → dst
                    routes[src][nei].append(new_path)

                    # 用于下一层扩展
                    layer[h][src].append(new_path)

    return routes


def generate_routes(topo_file, route_file, k=3, preference=0.5):
    with open(topo_file, 'r') as f:
        topo = json.load(f)
    
    n_node = topo['n_node']
    n_gpu = topo['n_gpu']
    edges = topo['edges']
    
    graph = build_graph(edges, n_node)

    print("Precomputing routes...")
    all_routes = compute_routes(graph, n_node, k)
    print("Route precomputation completed.")

    routes = {}
    
    for src in range(n_gpu):
        routes[str(src)] = {}
        for dst in range(n_gpu):
            if src == dst:
                continue

            paths = all_routes[src][dst]

            # print(f"Found {len(paths)} paths from {src} to {dst}.")

            if not paths:
                continue

            # === 权重计算（原逻辑不变）===
            hops_list = [len(p) - 1 for p in paths]

            if preference > 0:
                weights = [1 / (h + 1) ** preference for h in hops_list]
            else:
                weights = [1.0] * len(paths)

            random_factors = [random.random() for _ in paths]
            weighted_random = [rf * w for rf, w in zip(random_factors, weights)]
            total = sum(weighted_random)
            ratios = [wr / total for wr in weighted_random]

            routes[str(src)][str(dst)] = [
                [path, ratio] for path, ratio in zip(paths, ratios)
            ]
    
    with open(route_file, 'w') as f:
        json.dump(routes, f, indent=2)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python generate_route.py <topo.json> <route.json> [k] [preference]")
        sys.exit(1)

    topo_file = sys.argv[1]
    route_file = sys.argv[2]
    k = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    preference = float(sys.argv[4]) if len(sys.argv) > 4 else 2

    generate_routes(topo_file, route_file, k, preference)