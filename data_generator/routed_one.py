import argparse
import json
import random
import sys
import math
import os
from pathlib import Path
from collections import defaultdict


def build_flows(n_gpu, group_size, base_size, variation, algo):
    """Generate routed flows: support ring, butterfly, all2all. group_size为通信域大小。"""
    if n_gpu <= 1:
        raise ValueError('n_gpu must be greater than 1 for routed flows')
    if base_size <= 0:
        raise ValueError('base_size must be positive')
    if variation < 0:
        raise ValueError('variation must be non-negative')
    if algo not in ['ring', 'butterfly', 'all2all']:
        raise ValueError('algo must be one of "ring", "butterfly", "all2all"')

    flows = []
    stage_ids = []
    fid = 0

    if algo == 'all2all':
        # group_size为通信域大小，分组后每组内做all2all
        if group_size <= 0 or n_gpu % group_size != 0:
            raise ValueError('group_size must be a positive divisor of n_gpu for all2all')
        n_group = n_gpu // group_size
        n_group = 1
        groups = [list(range(i * group_size, (i + 1) * group_size)) for i in range(n_group)]
        for group in groups:
            group_stage_ids = []
            for t in range(1, group_size):
                ids = []
                for i in range(group_size):
                    src = group[i]
                    dst = group[(i + t) % group_size]
                    if variation <= 0:
                        size = float(base_size)
                    else:
                        delta = random.uniform(-variation, variation)
                        size = float(base_size) * max(0.0, 1.0 + delta)
                    flows.append({'id': fid, 'src': src, 'dst': dst, 'size': size, 'next_flows': []})
                    ids.append(fid)
                    fid += 1
                group_stage_ids.append(ids)
            stage_ids.extend(group_stage_ids)
        # Set dependencies: stage s -> stage s+1 by src/dst overlap
        for s in range(1, len(stage_ids)):
            for cur_id in stage_ids[s - 1]:
                cur = flows[cur_id]
                cur_src, cur_dst = cur['src'], cur['dst']
                for nxt_id in stage_ids[s]:
                    nxt = flows[nxt_id]
                    nxt_src, nxt_dst = nxt['src'], nxt['dst']
                    if (cur_src in (nxt_src, nxt_dst)) or (cur_dst in (nxt_src, nxt_dst)):
                        cur['next_flows'].append(nxt_id)
        return flows

    # dp模式（ring/butterfly）
    if group_size <= 0 or n_gpu % group_size != 0:
        raise ValueError('group_size must be a positive divisor of n_gpu')
    if algo == 'butterfly' and (group_size & (group_size - 1)) != 0:
        raise ValueError('group_size must be a power of 2 for butterfly all-reduce')
    n_group = n_gpu // group_size
    n_group = 1
    groups = [list(range(i * group_size, (i + 1) * group_size)) for i in range(n_group)]

    for group in groups:
        if algo == 'ring':
            num_stages = group_size - 1
        else:
            num_stages = int(math.log2(group_size))
        group_stage_ids = []
        for stage in range(num_stages):
            ids = []
            if algo == 'ring':
                for i in range(group_size):
                    src = group[i]
                    dst = group[(i + 1) % group_size]
                    if variation <= 0:
                        size = float(base_size)
                    else:
                        delta = random.uniform(-variation, variation)
                        size = float(base_size) * max(0.0, 1.0 + delta)
                    flows.append({'id': fid, 'src': src, 'dst': dst, 'size': size, 'next_flows': []})
                    ids.append(fid)
                    fid += 1
            else:  # butterfly
                mask = 1 << stage
                for i in range(group_size):
                    partner = i ^ mask
                    if i < partner:
                        for src_idx, dst_idx in [(i, partner), (partner, i)]:
                            src = group[src_idx]
                            dst = group[dst_idx]
                            if variation <= 0:
                                size = float(base_size)
                            else:
                                delta = random.uniform(-variation, variation)
                                size = float(base_size) * max(0.0, 1.0 + delta)
                            flows.append({'id': fid, 'src': src, 'dst': dst, 'size': size, 'next_flows': []})
                            ids.append(fid)
                            fid += 1
            group_stage_ids.append(ids)
        stage_ids.extend(group_stage_ids)
    for s in range(1, len(stage_ids)):
        for cur_id in stage_ids[s - 1]:
            cur = flows[cur_id]
            cur_src, cur_dst = cur['src'], cur['dst']
            for nxt_id in stage_ids[s]:
                nxt = flows[nxt_id]
                nxt_src, nxt_dst = nxt['src'], nxt['dst']
                if (cur_src in (nxt_src, nxt_dst)) or (cur_dst in (nxt_src, nxt_dst)):
                    cur['next_flows'].append(nxt_id)
    return flows

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

def generate_routes(origin_routes, needed_pairs, preference=0.5):
    routes = defaultdict(dict)

    for src, dst in needed_pairs:
        paths = origin_routes[src][dst]

        if not paths:
            continue

        # === 权重计算 ===
        hops_list = [len(p) - 1 for p in paths]

        if preference > 0:
            weights = [1 / (h + 1) ** preference for h in hops_list]
        else:
            weights = [1.0] * len(paths)

        random_factors = [random.random() for _ in paths]
        # print(f"Generating routes for ({src} -> {dst}), found {len(paths)} paths, hops: {hops_list}, weights: {weights}, random_factors: {random_factors}")
        weighted_random = [rf * w for rf, w in zip(random_factors, weights)]
        total = sum(weighted_random)

        # ⚠️ 防止极端情况 total=0
        if total == 0:
            ratios = [1.0 / len(paths)] * len(paths)
        else:
            ratios = [wr / total for wr in weighted_random]

        routes[src][dst] = [
            [path, ratio] for path, ratio in zip(paths, ratios)
        ]

    return routes

   
def route_flows(route, flows):
    routed_flows = []

    # 1. 先展开 flow（注意这里要 copy！否则你在改原数据）
    for flow in flows:
        for path, ratio in route[flow['src']][flow['dst']]:
            routed_flow = flow.copy()
            routed_flow['size'] = flow['size'] * ratio
            routed_flow['next_flows'] = []
            routed_flow['path'] = path
            routed_flows.append(routed_flow)

    # 2. 建立 id -> routed_flow 索引表
    id_to_indices = {}
    for idx, flow in enumerate(routed_flows):
        fid = flow['id']
        if fid not in id_to_indices:
            id_to_indices[fid] = []
        id_to_indices[fid].append(idx)

    # 3. 构建 next_flows（O(N)）
    for flow in routed_flows:
        original_id = flow['id']
        for next_id in flows[original_id]['next_flows']:
            if next_id in id_to_indices:
                flow['next_flows'].extend(id_to_indices[next_id])

    # 4. 重编号
    for idx, flow in enumerate(routed_flows):
        flow['id'] = idx

    return routed_flows

def main():
    parser = argparse.ArgumentParser(description='Generate routed flows')
    parser.add_argument('--group-size', type=int, default=64, help='通信域大小，dp/all2all均用此参数')
    parser.add_argument('--base-size', type=float, default=1000.0, help='Base data size per flow')
    parser.add_argument('--variation', type=float, default=0.0, help='Variation factor: 0.0 means fixed size')
    parser.add_argument('--seed', type=int, default=None, help='Random seed')
    parser.add_argument('--dir', type=str, default=None, help='topology dir')
    parser.add_argument('--algo', type=str, default='ring', help='communication algorithm: ring or butterfly')
    parser.add_argument('--hop', type=int, default=3, help='Number of hops for each flow')
    parser.add_argument('--n-flows', type=int, default=1, help='Number of flow sets to generate')
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    topo_file = os.path.join(args.dir, 'topo.json')
    with open(topo_file, 'r') as f:
        topo = json.load(f)
    n_node = topo['n_node']
    n_gpu = topo['n_gpu']
    edges = topo['edges']
    graph = build_graph(edges, n_node)

    flows = build_flows(n_gpu, args.group_size, args.base_size, args.variation, args.algo)

    # print("Precomputing routes...")
    all_routes = compute_routes(graph, n_node, args.hop)
    # print("Route precomputation completed.")

    for flows_id in range(args.n_flows):
        # shuffle GPU order mapping (global permutation)
        perm = list(range(n_gpu))
        random.shuffle(perm)
        for f in flows:
            f['src'] = perm[f['src']]
            f['dst'] = perm[f['dst']]
        needed_pairs = set()
        for f in flows:
            needed_pairs.add((f['src'], f['dst']))
        for routes_id in range(5):
            # print(f"Generating routes for flows_id={flows_id}, routes_id={routes_id}...")
            routes = generate_routes(all_routes, needed_pairs, preference=0.5)
            # print(f"Generated routes for {len(routes)} src nodes.")
            routed_flows = route_flows(routes, flows)
            # print(f"Routed flows generated: {len(routed_flows)}")
            # output routed flows (compact list format)
            output_file = os.path.join(args.dir, f'routed_{args.algo}_flows_{flows_id}_{routes_id}.json')
            compact_flows = []
            for flow in routed_flows:
                # id, src, dst, size (2 decimals), next_flows (list of int)
                compact_flows.append([
                    flow['id'],
                    flow['src'],
                    flow['dst'],
                    round(flow['size'], 2),
                    flow['next_flows'],
                    flow['path']  # 直接输出路径信息，供后续分析使用
                ])
            with open(output_file, 'w') as f:
                json.dump(compact_flows, f, separators=(',', ':'))
            # print(f"Generated {len(compact_flows)} routed flows for {flows_id}_{routes_id}.")
    print(f"Data generation completed for algo={args.algo}, dir={args.dir}.")

if __name__ == '__main__':
    main()
