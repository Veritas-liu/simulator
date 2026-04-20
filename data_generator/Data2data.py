from pathlib import Path
import shutil

if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    src_root = base_dir.parent / "Data"
    dst_root = base_dir.parent / "data"
    dst_root.mkdir(parents=True, exist_ok=True)

    cnt = 1
    for src_dir in sorted(src_root.iterdir()):
        if not src_dir.is_dir() or src_dir.name == "example":
            continue

        topo_json = src_dir / "topo.json"
        if not topo_json.exists():
            print(f"skip missing topo: {src_dir}")
            continue

        route_files = sorted([p for p in src_dir.glob("*.json") if p.name != "topo.json"])
        for route_file in route_files:
            dst_dir = dst_root / str(cnt)
            dst_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy(route_file, dst_dir / "flow.json")
            shutil.copy(topo_json, dst_dir / "topo.json")
            print(f"copied {route_file.name} + topo.json -> {dst_dir}")
            cnt += 1

    print(f"done, created {cnt - 1} cases under {dst_root}")
