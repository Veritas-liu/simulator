import argparse
import json
import random
import sys
import math
from pathlib import Path


def build_butterfly_allreduce_flows(n_gpu, base_size, variation):
    """Butterfly all-reduce: log(n) stages, each node exchanges with XOR partner."""
    if not (n_gpu & (n_gpu - 1)) == 0:
        raise ValueError('n_gpu must be a power of 2 for butterfly all-reduce')

    flows = []
    fid = 0
    stage_ids = []
    stages = int(math.log2(n_gpu))

    for stage in range(stages):
        ids = []
        mask = 1 << stage
        for i in range(n_gpu):
            partner = i ^ mask
            if i < partner:  # Avoid duplicates
                # Bidirectional: i -> partner and partner -> i
                for src, dst in [(i, partner), (partner, i)]:
                    if variation <= 0:
                        size = float(base_size)
                    else:
                        delta = random.uniform(-variation, variation)
                        size = float(base_size) * max(0.0, 1.0 + delta)

                    flows.append({
                        'id': fid,
                        'src': src,
                        'dst': dst,
                        'size': size,
                        'next_flows': []
                    })
                    ids.append(fid)
                    fid += 1
        stage_ids.append(ids)

    # Set dependencies: flows in stage s depend on stage s-1 if same src/dst involved
    for s in range(1, len(stage_ids)):
        for cur_id in stage_ids[s - 1]:
            cur = flows[cur_id]
            cur_src, cur_dst = cur['src'], cur['dst']
            for next_id in stage_ids[s]:
                nxt = flows[next_id]
                nxt_src, nxt_dst = nxt['src'], nxt['dst']
                if (cur_src in (nxt_src, nxt_dst)) or (cur_dst in (nxt_src, nxt_dst)):
                    cur['next_flows'].append(next_id)

    return flows


def main():
    parser = argparse.ArgumentParser(description='Generate butterfly all-reduce flows.json')
    parser.add_argument('--n-gpu', type=int, required=True, help='Number of GPU nodes (must be power of 2)')
    parser.add_argument('--base-size', type=float, default=1000.0, help='Base data size per flow')
    parser.add_argument('--variation', type=float, default=0.0,
                        help='Variation factor: 0.0 means fixed size; e.g. 0.2 means +/-20%')
    parser.add_argument('--seed', type=int, default=None, help='Random seed for variation')
    parser.add_argument('--output', type=str, default=None, help='Output JSON file path (default to stdout)')
    args = parser.parse_args()

    if args.n_gpu <= 1 or not (args.n_gpu & (args.n_gpu - 1)) == 0:
        raise ValueError('n_gpu must be a power of 2 > 1 for butterfly all-reduce')
    if args.base_size <= 0:
        raise ValueError('base_size must be positive')
    if args.variation < 0:
        raise ValueError('variation must be non-negative')

    if args.seed is not None:
        random.seed(args.seed)

    flows = build_butterfly_allreduce_flows(args.n_gpu, args.base_size, args.variation)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open('w', encoding='utf-8') as f:
            json.dump(flows, f, indent=2)
        print(f'Generated {len(flows)} flows in {output_path!s}')
    else:
        json.dump(flows, sys.stdout, indent=2)


if __name__ == '__main__':
    main()
