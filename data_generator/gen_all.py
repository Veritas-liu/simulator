import concurrent.futures
import subprocess
import os

group_size = 64
algos = ['all2all', 'ring', 'butterfly']
n_flows = ['2', '1', '1']
dirs = [f'../Data/2048_{i}' for i in range(1, 26)] + [f'../Data/4096_{i}' for i in range(1, 26)]
# dirs = [f'../Data/2048_1']

cmd_template = (
    'python3 routed_one.py --group-size={group_size} --algo={algo} --dir={dir} --n-flows={n_flows}'
)

def run_cmd(cmd):
    print(f'Running: {cmd}')
    result = subprocess.run(cmd, shell=True)
    return result.returncode

tasks = []
for d in dirs:
    for algo in algos:
        print(f"Generating data for dir: {d}, algo: {algo}, group_size: {group_size}, n_flows: {n_flows[algos.index(algo)]}")
        tasks.append(cmd_template.format(group_size=group_size, algo=algo, dir=d, n_flows=n_flows[algos.index(algo)]))

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(run_cmd, cmd) for cmd in tasks]
    for future in concurrent.futures.as_completed(futures):
        code = future.result()
        if code != 0:
            print(f'Error: Command failed with exit code {code}')
