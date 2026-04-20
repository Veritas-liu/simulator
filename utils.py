import json
from model import Topo, Flow


def _parse_int_key(key):
    try:
        return int(key)
    except (ValueError, TypeError):
        return key


def load_topo(path):
    """Load topology JSON and return a Topo instance.

    Expected format:
      {
        "n_node": int,
        "n_gpu": int,
        "edges": [[src,dst,bandwidth], ...] (or dict entries)
      }

    验证：
      1. n_gpu <= n_node
      2. gpu 节点为 [0, n_gpu-1]
      3. switch 节点编号从 n_gpu 开始
    """
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError('topo file must contain a JSON object')

    n_node = data.get('n_node')
    n_gpu = data.get('n_gpu')
    if n_node is None or n_gpu is None:
        raise ValueError('topo must include n_node and n_gpu')
    if not isinstance(n_node, int) or not isinstance(n_gpu, int):
        raise ValueError('n_node and n_gpu must be integers')
    if n_gpu < 0 or n_node < 0 or n_gpu > n_node:
        raise ValueError('invalid n_node / n_gpu values')

    edges_raw = data.get('edges')
    if edges_raw is None:
        raise ValueError('topo must include edges')

    edges = []
    for e in edges_raw:
        if isinstance(e, (list, tuple)) and len(e) == 3:
            src, dst, bandwidth = e
        elif isinstance(e, dict):
            src = e.get('src')
            dst = e.get('dst')
            bandwidth = e.get('bandwidth')
        else:
            raise ValueError('edge item must be [src,dst,bandwidth] or object with keys src,dst,bandwidth')

        if not (isinstance(src, int) and isinstance(dst, int)):
            raise ValueError('edge src and dst must be int')
        if not (0 <= src < n_node and 0 <= dst < n_node):
            raise ValueError('edge endpoints must be within [0, n_node-1]')
        if src == dst:
            raise ValueError('edge src and dst must differ')

        try:
            bandwidth = float(bandwidth)
        except (TypeError, ValueError):
            raise ValueError('edge bandwidth must be numeric')
        if bandwidth <= 0:
            raise ValueError('edge bandwidth must be positive')

        edges.append((src, dst, bandwidth))

    # ensure GPU编号是小的
    if n_gpu > 0:
        # 虽然在 topo 里没有特别记录每个节点类型，要求由编号确定
        # gpu 编号 [0,n_gpu-1]；switch 由 n_gpu...n_node-1
        pass

    return Topo(n_node, n_gpu, edges)

def load_routed_flows(path):
    """Load routed flows JSON and return a list of Flow
    JSON example:
    [[0,1,0,1000.0,[4,5,7],[1,4,0]],[1,0,3,1000.0,[4,5,6],[0,4,5,3]],[2,3,2,1000.0,[5,6,7],[3,5,2]],[3,2,1,1000.0,[4,6,7],[2,5,4,1]],[4,1,0,1000.0,[8,9,11],[1,4,0]],[5,0,3,1000.0,[8,9,10],[0,4,5,3]],[6,3,2,1000.0,[9,10,11],[3,5,2]],[7,2,1,1000.0,[8,10,11],[2,5,4,1]],[8,1,0,1000.0,[],[1,4,0]],[9,0,3,1000.0,[],[0,4,5,3]],[10,3,2,1000.0,[],[3,5,2]],[11,2,1,1000.0,[],[2,5,4,1]]]
    """
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError('flows file must contain a JSON array')

    flows = []
    idtoidx = {}
    for i, item in enumerate(data):
        if not isinstance(item, list):
            raise ValueError('each flow item must be an object')

        fid, src, dst, size, next_flows, path = item

        if src is None or dst is None or size is None:
            raise ValueError('flow must include src, dst, size')
        if not (isinstance(src, int) and isinstance(dst, int)):
            raise ValueError('flow src and dst must be integers')
        if src == dst:
            raise ValueError('flow src and dst must be different')

        if not isinstance(next_flows, list):
            raise ValueError('flow next_flows must be a list of flow ids')
        if not all(isinstance(x, int) for x in next_flows):
            raise ValueError('flow next_flows items must be integers')

        try:
            size = float(size)
        except (TypeError, ValueError):
            raise ValueError('flow size must be numeric')
        if size < 0:
            raise ValueError('flow size must be positive')
        if size == 0:
            print(f'warning: flow {fid} has zero size, will be ignored')
            continue

        flow = Flow(fid, size, src, dst, next_flows, path)
        idtoidx[fid] = len(flows)
        flows.append(flow)

    for f in flows:
        for nfid in f.next_flows:
            nf = flows[idtoidx.get(nfid)] if nfid in idtoidx else None
            if nf is not None:
                nf.dependency_count += 1

    return flows

    

# def load_route(path):
#     """Load route mapping JSON and return nested dict route[src][dst] -> list of (path, ratio).

#     只支持 GPU->GPU 路径（src,dst 均为 GPU 编号）。
#     """
#     with open(path, 'r', encoding='utf-8') as f:
#         data = json.load(f)

#     if not isinstance(data, dict):
#         raise ValueError('route file must contain a JSON object')

#     route = {}
#     for src_k, dsts in data.items():
#         src = _parse_int_key(src_k)
#         if not isinstance(src, int):
#             raise ValueError('route src keys must be integer-like')
#         route.setdefault(src, {})

#         if not isinstance(dsts, dict):
#             raise ValueError(f'route[{src_k}] must be a dict of destination entries')

#         for dst_k, paths in dsts.items():
#             dst = _parse_int_key(dst_k)
#             if not isinstance(dst, int):
#                 raise ValueError('route dst keys must be integer-like')
#             if not isinstance(paths, list):
#                 raise ValueError(f'route[{src}][{dst}] must be a list')

#             route[src][dst] = []
#             total_ratio = 0.0
#             for item in paths:
#                 if isinstance(item, (list, tuple)) and len(item) == 2:
#                     path, ratio = item
#                 elif isinstance(item, dict):
#                     path = item.get('path')
#                     ratio = item.get('ratio')
#                 else:
#                     raise ValueError('each route entry must be [path,ratio] or {"path":...,"ratio":...}')
#                 if not isinstance(path, list) or len(path) < 2:
#                     raise ValueError('path must be a list of node ids with length>=2')
#                 if not all(isinstance(x, int) for x in path):
#                     raise ValueError('path elements must be int')
#                 if path[0] != src or path[-1] != dst:
#                     raise ValueError('path endpoints must match route source and destination')

#                 ratio = float(ratio)
#                 if ratio <= 0 or ratio > 1:
#                     raise ValueError('route ratio must be in (0,1]')
#                 total_ratio += ratio
#                 route[src][dst].append((path, ratio))

#             if not abs(total_ratio - 1.0) < 1e-6:
#                 raise ValueError(f'route[{src}][{dst}] ratios must sum to 1')

#     return route


# def load_flows(path):
#     """Load flows JSON and return a list of Flow objects."""
#     with open(path, 'r', encoding='utf-8') as f:
#         data = json.load(f)

#     if not isinstance(data, list):
#         raise ValueError('flows file must contain a JSON array')

#     flows = []
#     for i, item in enumerate(data):
#         if not isinstance(item, dict):
#             raise ValueError('each flow item must be an object')

#         fid = item.get('id', i)
#         src = item.get('src')
#         dst = item.get('dst')
#         size = item.get('size')
#         next_flows = item.get('next_flows', [])

#         if src is None or dst is None or size is None:
#             raise ValueError('flow must include src, dst, size')
#         if not (isinstance(src, int) and isinstance(dst, int)):
#             raise ValueError('flow src and dst must be integers')
#         if src == dst:
#             raise ValueError('flow src and dst must be different')

#         if not isinstance(next_flows, list):
#             raise ValueError('flow next_flows must be a list of flow ids')
#         if not all(isinstance(x, int) for x in next_flows):
#             raise ValueError('flow next_flows items must be integers')

#         try:
#             size = float(size)
#         except (TypeError, ValueError):
#             raise ValueError('flow size must be numeric')
#         if size <= 0:
#             raise ValueError('flow size must be positive')

#         flow = Flow(fid, size, src, dst, next_flows, None)
#         flows.append(flow)

#     return flows
