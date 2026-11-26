#!/usr/bin/env python3
"""실행 전 피어 폴더를 env.json에 맞게 초기 상태로 되돌린다.
참고: 강의 자료 HW6explanation.pdf PPT §22.3 · p.219

    python tools/reset_dirs.py --env envs/A.json [--data data]

1. env의 peers[].dir 폴더(p1~p4)와 logs/, report.json 을 지운다.
2. 폴더를 새로 만들고, files[].origin 피어의 폴더에 원본 파일을 복사한다.
3. data 폴더에 원본이 없으면 make_dummy로 먼저 만든다.
"""
import argparse
import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from make_dummy import make, MB  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True)
    ap.add_argument("--data", default="data", help="원본 파일이 있는 폴더 (기본: data)")
    a = ap.parse_args()
    with open(a.env, encoding="utf-8") as f:
        env = json.load(f)

    for p in env["peers"]:
        shutil.rmtree(p["dir"], ignore_errors=True)
    shutil.rmtree("logs", ignore_errors=True)
    if os.path.exists("report.json"):
        os.remove("report.json")
    for p in env["peers"]:
        os.makedirs(p["dir"])
    os.makedirs("logs")

    os.makedirs(a.data, exist_ok=True)
    dirs = {p["id"]: p["dir"] for p in env["peers"]}
    for f in env["files"]:
        src = os.path.join(a.data, f["name"])
        if not os.path.exists(src) or os.path.getsize(src) != f["size"]:
            mb = f["size"] // MB
            print(f"generating {src} ({mb} MB) ...")
            make(mb, a.data)
        dst = os.path.join(dirs[f["origin"]], f["name"])
        shutil.copyfile(src, dst)
        print(f"{f['name']:12} -> {dst}")
    print(f"ready for {env['name']} (run {env['run']})")


if __name__ == "__main__":
    main()
