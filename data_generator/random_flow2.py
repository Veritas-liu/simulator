import argparse
import json
import random
import sys
from pathlib import Path


def build_round_based_random_flows(n_gpu, n_rounds, flows_per_round, base_size, variation):
    """Generate random flows in rounds, with dependencies between conflicting flows in adjacent rounds."""
    flows = []
    fid = 0
    round_flows = []

    for r in range(n_rounds):
        round_fids = []
        for _ in range(flows_per_round):
            src = random.randint(0, n_gpu - 1)
            dst = random.randint(0, n_gpu - 1)
            while dst == src:
                dst = random.randint(0, n_gpu - 1)

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
                'next_flows': [],
            })
            round_fids.append(fid)
            fid += 1
        round_flows.append(round_fids)

    # Set dependencies: for adjacent rounds, if flows share src or dst, add dependency
    for r in range(n_rounds - 1):
        current_round = round_flows[r]
        next_round = round_flows[r + 1]
        for cur_fid in current_round:
            cur_flow = flows[cur_fid]
            cur_nodes = {cur_flow['src'], cur_flow['dst']}
            for nxt_fid in next_round:
                nxt_flow = flows[nxt_fid]
                nxt_nodes = {nxt_flow['src'], nxt_flow['dst']}
                if cur_nodes & nxt_nodes:  # Conflict: shared node
                    cur_flow['next_flows'].append(nxt_fid)

    return flows


def main():
    parser = argparse.ArgumentParser(description='Generate round-based random flows.json')
    parser.add_argument('--n-gpu', type=int, required=True, help='Number of GPU nodes')
    parser.add_argument('--n-rounds', type=int, required=True, help='Number of rounds')
    parser.add_argument('--flows-per-round', type=int, required=True, help='Number of flows per round')
    parser.add_argument('--base-size', type=float, default=1000.0, help='Base data size per flow')
    parser.add_argument('--variation', type=float, default=0.0,
                        help='Variation factor: 0.0 means fixed size; e.g. 0.2 means +/-20%')
    parser.add_argument('--seed', type=int, default=None, help='Random seed')
    parser.add_argument('--output', type=str, default=None, help='Output JSON file path (default to stdout)')
    args = parser.parse_args()

    if args.n_gpu <= 0:
        raise ValueError('n_gpu must be positive')
    if args.n_rounds <= 0:
        raise ValueError('n_rounds must be positive')
    if args.flows_per_round <= 0:
        raise ValueError('flows_per_round must be positive')
    if args.base_size <= 0:
        raise ValueError('base_size must be positive')
    if args.variation < 0:
        raise ValueError('variation must be non-negative')

    if args.seed is not None:
        random.seed(args.seed)

    flows = build_round_based_random_flows(args.n_gpu, args.n_rounds, args.flows_per_round, args.base_size, args.variation)

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