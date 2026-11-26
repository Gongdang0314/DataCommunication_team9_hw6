#!/usr/bin/env python3
"""정식 실행: Tracker 는 외부 VM(EC2), Peer 4개는 내 PC. Run 하나를 끝까지 돌리고 결과를 runs/<이름>/ 에 보관.

    python run_remote.py --deploy                                  # VM 에 tracker 코드 · envs 복사
    python run_remote.py --env envs/A.json --name A                # Run A
    python run_remote.py --env envs/C3.json --name C3_random --policy random

순서 (과제명세서 8-3 과 같음)
  1) reset_dirs (실행 폴더의 p1~p4 · logs 초기화, 원본 배치)
  2) ssh: VM 에서 tracker 시작 (tracker 먼저)        python3 tracker.py --env envs/X.json <PORT>
  3) 내 PC: peer 4개 (독립 프로세스)                   python peer.py --env envs/X.json --id Pn <VM_IP> <PORT>
  4) 모두 스스로 끝날 때까지 대기 (ALLDONE -> BYE -> exit)
  5) scp: VM 의 logs/tracker.jsonl, report.json -> 실행 폴더
  6) verify.py -> verify.txt
  7) runs/<이름>/ 에 logs/*.jsonl · report.json · verify.txt · p1~p4/*.bitmap *.lost 보관 (큰 데이터 파일 제외)
"""
import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)                       # G7HW6/
DEFAULT_WORK = os.path.join(os.path.expanduser("~"), "hw6work")   # OneDrive 밖 실행 폴더 (240 MB 데이터)
VM_HOST = "13.125.21.50"
VM_USER = "ubuntu"
VM_PORT = "9500"
VM_DIR = "hw6"
KEY = os.path.join(os.path.expanduser("~"), ".ssh", "daeman5.pem")


def ssh(cmd, check=True, capture=False):
    args = ["ssh", "-i", KEY, "-o", "ConnectTimeout=10", f"{VM_USER}@{VM_HOST}", cmd]
    return subprocess.run(args, check=check, capture_output=capture, text=True)


def scp_to(local, remote):
    subprocess.run(["scp", "-q", "-i", KEY] + local + [f"{VM_USER}@{VM_HOST}:{remote}"], check=True)


def scp_from(remote, local):
    subprocess.run(["scp", "-q", "-i", KEY, f"{VM_USER}@{VM_HOST}:{remote}", local], check=True)


def deploy():
    ssh(f"mkdir -p {VM_DIR}/lib {VM_DIR}/envs")
    scp_to([os.path.join(HERE, "tracker.py"), os.path.join(HERE, "hw6net.py")], f"{VM_DIR}/")
    scp_to(glob.glob(os.path.join(HERE, "lib", "*.py")), f"{VM_DIR}/lib/")
    scp_to(glob.glob(os.path.join(HERE, "envs", "*.json")), f"{VM_DIR}/envs/")
    print(ssh(f"cd {VM_DIR} && ls -R && python3 --version", capture=True).stdout)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--deploy", action="store_true")
    ap.add_argument("--env")
    ap.add_argument("--name")
    ap.add_argument("--policy")
    ap.add_argument("--work", default=DEFAULT_WORK)
    ap.add_argument("--runs", default=os.path.join(ROOT, "runs"))
    a = ap.parse_args()
    if a.deploy:
        deploy()
        if not a.env:
            return
    work = os.path.abspath(a.work)
    py = sys.executable
    envp = dict(os.environ, PYTHONIOENCODING="utf-8")
    env = json.load(open(os.path.join(work, a.env), encoding="utf-8"))
    name = a.name or env["name"]

    # 1) 폴더 초기화
    subprocess.run([py, "tools/reset_dirs.py", "--env", a.env], cwd=work, check=True, env=envp)
    out_dir = os.path.join(work, "out")
    os.makedirs(out_dir, exist_ok=True)
    # 2) VM 에서 tracker 먼저
    ssh(f"cd {VM_DIR} && (pkill -f '^python3 tracker.py' || true) && rm -rf logs report.json tracker.out && "
        f"(nohup python3 tracker.py --env {a.env} {VM_PORT} > tracker.out 2>&1 < /dev/null &) && sleep 0.3 && "
        f"pgrep -f '^python3 tracker.py' > /dev/null && echo tracker-started")
    time.sleep(1.0)
    # 3) peer 4개
    t0 = time.time()
    procs = []
    for p in env["peers"]:
        cmd = [py, os.path.join(HERE, "peer.py"), "--env", a.env, "--id", p["id"]]
        if a.policy:
            cmd += ["--policy", a.policy]
        cmd += [VM_HOST, VM_PORT]
        procs.append((p["id"], subprocess.Popen(cmd, cwd=work, env=envp, stderr=subprocess.STDOUT,
                                                stdout=open(os.path.join(out_dir, f"{p['id']}.out"), "w"))))
    # 4) 모두 스스로 끝날 때까지
    codes = {pid: pr.wait() for pid, pr in procs}
    print(f"[{name}] peers exit {codes}  ({time.time() - t0:.1f}s)")
    for _ in range(90):
        r = ssh(f"pgrep -f '^python3 tracker.py' > /dev/null && echo running || echo stopped", capture=True)
        if "stopped" in r.stdout:
            break
        time.sleep(1.0)
    else:
        print("tracker still running -> kill")
        ssh(f"pkill -f '^python3 tracker.py' || true", check=False)
    # 5) 결과 가져오기
    scp_from(f"{VM_DIR}/logs/tracker.jsonl", os.path.join(work, "logs", "tracker.jsonl"))
    scp_from(f"{VM_DIR}/report.json", os.path.join(work, "report.json"))
    scp_from(f"{VM_DIR}/tracker.out", os.path.join(out_dir, "tracker.out"))
    # 6) verify
    r = subprocess.run([py, "tools/verify.py", "--env", a.env], cwd=work, env=envp,
                       capture_output=True, text=True, encoding="utf-8")
    with open(os.path.join(work, "verify.txt"), "w", encoding="utf-8") as f:
        f.write(r.stdout)
    print(r.stdout.strip().splitlines()[-1])
    # 7) 보관
    dst = os.path.join(a.runs, name)
    shutil.rmtree(dst, ignore_errors=True)
    os.makedirs(os.path.join(dst, "logs"))
    for fp in glob.glob(os.path.join(work, "logs", "*.jsonl")):
        shutil.copy(fp, os.path.join(dst, "logs"))
    shutil.copy(os.path.join(work, "report.json"), dst)
    shutil.copy(os.path.join(work, "verify.txt"), dst)
    for p in env["peers"]:
        pd = os.path.join(dst, p["dir"])
        os.makedirs(pd)
        for fp in glob.glob(os.path.join(work, p["dir"], "*.bitmap")) + glob.glob(os.path.join(work, p["dir"], "*.lost")):
            shutil.copy(fp, pd)
    rep = json.load(open(os.path.join(work, "report.json"), encoding="utf-8"))
    print(f"[{name}] T_alldone={rep['T_alldone']} T_rep={rep['T_rep']} p_rep={rep['p_rep']} "
          f"T_destroy={rep['T_destroy']} T_dead={rep['T_dead']} abort={rep['abort']}")


if __name__ == "__main__":
    main()
