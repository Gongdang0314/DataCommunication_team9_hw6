#!/usr/bin/env python3
"""(개발용) Tracker 도 내 PC 에서 띄워 Run 하나를 끝까지 실행 -> verify.

    python run_local.py --work ../../work --env envs/DEV.json [--policy rarest] [--port 6000]

실행 폴더(--work)에는 envs/ 와 tools/ 가 있어야 한다. 결과는 그 폴더의 logs/ p1~p4/ report.json.
"""
import argparse
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--work", required=True)
    ap.add_argument("--env", required=True)
    ap.add_argument("--policy")
    ap.add_argument("--port", default="6000")
    a = ap.parse_args()
    work = os.path.abspath(a.work)
    py = sys.executable
    envp = dict(os.environ, PYTHONIOENCODING="utf-8")
    subprocess.run([py, "tools/reset_dirs.py", "--env", a.env], cwd=work, check=True, env=envp)
    os.makedirs(os.path.join(work, "out"), exist_ok=True)
    tr = subprocess.Popen([py, os.path.join(HERE, "tracker.py"), "--env", a.env, a.port], cwd=work,
                          stdout=open(os.path.join(work, "out", "tracker.out"), "w"),
                          stderr=subprocess.STDOUT, env=envp)
    time.sleep(0.7)
    peers = []
    import json
    env = json.load(open(os.path.join(work, a.env), encoding="utf-8"))
    for p in env["peers"]:
        cmd = [py, os.path.join(HERE, "peer.py"), "--env", a.env, "--id", p["id"]]
        if a.policy:
            cmd += ["--policy", a.policy]
        cmd += ["127.0.0.1", a.port]
        peers.append(subprocess.Popen(cmd, cwd=work, stdout=open(os.path.join(work, "out", f"{p['id']}.out"), "w"),
                                      stderr=subprocess.STDOUT, env=envp))
    t0 = time.time()
    codes = [q.wait() for q in peers]
    print(f"peers exit {codes}  ({time.time() - t0:.1f}s)")
    try:
        print("tracker exit", tr.wait(timeout=40))
    except subprocess.TimeoutExpired:
        tr.kill()
        print("tracker killed")
    r = subprocess.run([py, "tools/verify.py", "--env", a.env], cwd=work, env=envp,
                       capture_output=True, text=True, encoding="utf-8")
    print(r.stdout[-3000:])
    with open(os.path.join(work, "verify.txt"), "w", encoding="utf-8") as f:
        f.write(r.stdout)


if __name__ == "__main__":
    main()
