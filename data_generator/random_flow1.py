import argparse
import json
import random
import sys
from pathlib import Path


def build_random_flows(n_gpu, num_flows, base_size, variation, max_deps=3):
    """Generate random flows with random src, dst, size, and dependencies."""
    flows = []
    for fid in range(num_flows):
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

    # Random dependencies: each flow can depend on up to max_deps random later flows
    for fid in range(num_flows):
        num_deps = random.randint(0, min(max_deps, num_flows - fid - 1))
        deps = random.sample(range(fid + 1, num_flows), num_deps)
        flows[fid]['next_flows'] = deps

    return flows


def main():
    parser = argparse.ArgumentParser(description='Generate random flows.json')
    parser.add_argument('--n-gpu', type=int, required=True, help='Number of GPU nodes')
    parser.add_argument('--num-flows', type=int, required=True, help='Number of flows to generate')
    parser.add_argument('--base-size', type=float, default=1000.0, help='Base data size per flow')
    parser.add_argument('--variation', type=float, default=0.0,
                        help='Variation factor: 0.0 means fixed size; e.g. 0.2 means +/-20%')
    parser.add_argument('--max-deps', type=int, default=3, help='Max dependencies per flow')
    parser.add_argument('--seed', type=int, default=None, help='Random seed')
    parser.add_argument('--output', type=str, default=None, help='Output JSON file path (default to stdout)')
    args = parser.parse_args()

    if args.n_gpu <= 0:
        raise ValueError('n_gpu must be positive')
    if args.num_flows <= 0:
        raise ValueError('num_flows must be positive')
    if args.base_size <= 0:
        raise ValueError('base_size must be positive')
    if args.variation < 0:
        raise ValueError('variation must be non-negative')
    if args.max_deps < 0:
        raise ValueError('max_deps must be non-negative')

    if args.seed is not None:
        random.seed(args.seed)

    flows = build_random_flows(args.n_gpu, args.num_flows, args.base_size, args.variation, args.max_deps)

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
