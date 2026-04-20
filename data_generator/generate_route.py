import json
import sys
import random
from collections import defaultdict

def build_graph(edges, n_node):
    graph = defaultdict(list)
    for u, v, bw in edges:
        graph[u].append((v, bw))
    return graph

def find_all_paths(graph, start, end, max_hops, path=[], hops=0):
    path = path + [start]
    if hops > max_hops:
        return []
    if start == end:
        return [path]
    if start not in graph:
        return []
    paths = []
    for node, _ in graph[start]:
        if node not in path:  # avoid cycles
            new_paths = find_all_paths(graph, node, end, max_hops, path, hops + 1)
            for new_path in new_paths:
                paths.append(new_path)
    return paths

def generate_routes(topo_file, route_file, k=3, preference=0.5):
    with open(topo_file, 'r') as f:
        topo = json.load(f)
    
    n_node = topo['n_node']
    n_gpu = topo['n_gpu']
    edges = topo['edges']
    
    graph = build_graph(edges, n_node)
    
    routes = {}
    
    for src in range(n_gpu):
        routes[str(src)] = {}
        for dst in range(n_gpu):
            if src == dst:
                continue
            print(f"Finding paths from {src} to {dst}...")
            paths = find_all_paths(graph, src, dst, k)
            print(f"Found {len(paths)} paths from {src} to {dst}.")
            if not paths:
                continue
            # Calculate weights based on preference
            hops_list = [len(p) - 1 for p in paths]  # number of hops
            if preference > 0:
                weights = [1 / (h + 1) ** preference for h in hops_list]
            else:
                weights = [1.0] * len(paths)
            # Generate random ratios with expectation proportional to weights
            random_factors = [random.random() for _ in paths]
            weighted_random = [rf * w for rf, w in zip(random_factors, weights)]
            total = sum(weighted_random)
            ratios = [wr / total for wr in weighted_random]
            routes[str(src)][str(dst)] = [[path, ratio] for path, ratio in zip(paths, ratios)]
    
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