import subprocess
from concurrent.futures import ThreadPoolExecutor

def run_command(i):
    cmd = ["python3", "main.py", f"--dir=./data/{i}"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return i, result.returncode, result.stdout, result.stderr

if __name__ == "__main__":
    max_workers = 16  # 可根据CPU核心数调整
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(run_command, i) for i in range(1, 1001)]
        for future in futures:
            i, code, out, err = future.result()
            print(f"[{i}] return code: {code}")
            if out:
                print(f"stdout:\n{out}")
            if err:
                print(f"stderr:\n{err}")