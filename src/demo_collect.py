#!/usr/bin/env python3
"""시연(수동 실행) 뒤 결과 모으기: VM 의 tracker.jsonl · report.json 가져오기 -> verify -> 보관.

    python demo_collect.py --env envs/A.json --name demo_A        # runs_demo/demo_A/ 에 보관

demo_run.ps1 로 터미널 창을 띄워 실행한 뒤(모든 Peer 가 스스로 끝난 다음) 사용한다.
"""
import argparse
import glob
import json
import os
import shutil
import subprocess
import sys

from run_remote import DEFAULT_WORK, ROOT, VM_DIR, scp_from


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--work", default=DEFAULT_WORK)
    ap.add_argument("--runs", default=os.path.join(ROOT, "runs_demo"))
    a = ap.parse_args()
    work = os.path.abspath(a.work)
    env = json.load(open(os.path.join(work, a.env), encoding="utf-8"))
    envp = dict(os.environ, PYTHONIOENCODING="utf-8")
    scp_from(f"{VM_DIR}/logs/tracker.jsonl", os.path.join(work, "logs", "tracker.jsonl"))
    scp_from(f"{VM_DIR}/report.json", os.path.join(work, "report.json"))
    r = subprocess.run([sys.executable, "tools/verify.py", "--env", a.env], cwd=work, env=envp,
                       capture_output=True, text=True, encoding="utf-8")
    print(r.stdout)
    with open(os.path.join(work, "verify.txt"), "w", encoding="utf-8") as f:
        f.write(r.stdout)
    dst = os.path.join(a.runs, a.name)
    shutil.rmtree(dst, ignore_errors=True)
    os.makedirs(os.path.join(dst, "logs"))
    for fp in glob.glob(os.path.join(work, "logs", "*.jsonl")):
        shutil.copy(fp, os.path.join(dst, "logs"))
    for fn in ("report.json", "verify.txt"):
        shutil.copy(os.path.join(work, fn), dst)
    for p in env["peers"]:
        os.makedirs(os.path.join(dst, p["dir"]))
        for fp in glob.glob(os.path.join(work, p["dir"], "*.bitmap")) + glob.glob(os.path.join(work, p["dir"], "*.lost")):
            shutil.copy(fp, os.path.join(dst, p["dir"]))
    print(f"saved -> {dst}")


if __name__ == "__main__":
    main()
