#!/usr/bin/env python3
"""Run 결과 분석 (Readme 9-5 지표 · 9-4 정합성): 속도 · 출처 분포 · crash/ban/fail 수 · report.json 확인.

    python analyze.py <runs/RUN 폴더> <env.json>
"""
import collections
import json
import os
import sys

CHUNK_KB = 64


def load(path):
    return [json.loads(ln) for ln in open(path, encoding="utf-8") if ln.strip()]


def main():
    run, envp = sys.argv[1], sys.argv[2]
    env = json.load(open(envp, encoding="utf-8"))
    cs = env["chunk_size"]
    ids = [p["id"] for p in env["peers"]]
    lim = {p["id"]: (p["up_kbps"], p["down_kbps"]) for p in env["peers"]}
    ev = {i: load(os.path.join(run, "logs", f"{i}.jsonl")) for i in ids}
    rep = json.load(open(os.path.join(run, "report.json"), encoding="utf-8"))
    recv = {i: [e for e in ev[i] if e["ev"] == "RECV"] for i in ids}
    print(f"== {run}")
    print("peer  recv  avg_down KB/s (limit)  %   | sent  avg_up KB/s (limit)  %  | CRASH BAN FAIL RETRY | EXIT")
    src_table = collections.defaultdict(collections.Counter)
    for i in ids:
        for e in recv[i]:
            src_table[e["src"]][i] += 1
    for i in ids:
        r = recv[i]
        if len(r) > 1:
            dt = r[-1]["t"] - r[0]["t"]
            dn = len(r) * cs / 1024 / dt if dt > 0 else 0
        else:
            dn = 0
        # 업로드 = 다른 Peer 의 RECV 중 src 가 나. 시간은 Peer 마다 시계가 달라 각 수신자 시계로 첫/끝을 잡는다(근사)
        ups = [(j, e["t"]) for j in ids for e in recv[j] if e["src"] == i]
        if len(ups) > 1:
            ts = [t for _, t in ups]
            dtu = max(ts) - min(ts)
            up = len(ups) * cs / 1024 / dtu if dtu > 0 else 0
        else:
            up = 0
        c = collections.Counter(e["ev"] for e in ev[i])
        last = ev[i][-1]
        print(f"{i:4} {len(r):5}  {dn:8.1f} ({lim[i][1]:4})  {100 * dn / lim[i][1]:5.1f} | {len(ups):5}  {up:8.1f} "
              f"({lim[i][0]:4}) {100 * up / lim[i][0]:5.1f} | {c['CRASH']:5} {c['BAN']:3} {c['FAIL']:4} {c['RETRY']:5} | "
              f"{last['ev']} {last.get('code')}")
    print("source table (row = sender, col = receiver):")
    print("       " + " ".join(f"{i:>5}" for i in ids) + "   total")
    for s in ids:
        print(f"  {s:4} " + " ".join(f"{src_table[s][i]:5}" for i in ids) + f"   {sum(src_table[s].values()):5}")
    # 정합성 (9-4)
    bad = []
    tr = rep["T_rep"]
    if rep["T_alldone"] is not None:
        if any(v is not None and v > rep["T_alldone"] for v in tr.values()):
            bad.append("T_rep > T_alldone")
        if rep["T_last_done"] is not None and rep["T_last_done"] > rep["T_alldone"]:
            bad.append("T_last_done > T_alldone")
        if rep["T_exit"] < rep["T_alldone"]:
            bad.append("T_exit < T_alldone")
    if len(rep["crashes_sent"]) != len(env.get("crashes") or []):
        bad.append(f"crashes_sent {len(rep['crashes_sent'])} != env {len(env.get('crashes') or [])}")
    if env.get("destroy"):
        v = env["destroy"]["peer"]
        if rep["T_destroy"] is None or rep["peers"].get(v) != "DEAD":
            bad.append("destroy not reflected")
    for i in ids:
        bm = 0
        orig = 0
        for f in env["files"]:
            p = os.path.join(run, i.lower(), f["name"] + ".bitmap")
            if os.path.exists(p):
                b = open(p).read().strip()
                if f["origin"] == i:
                    orig += b.count("1")
                else:
                    bm += b.count("1")
        if bm != len(recv[i]):
            bad.append(f"{i}: RECV {len(recv[i])} != bitmap 1s {bm}")
    total = sum(len(recv[i]) for i in ids)
    print(f"RECV total {total} / progress_total_chunks {env['progress_total_chunks']}")
    print("consistency:", "OK" if not bad else bad)
    print(f"report: T_alldone={rep['T_alldone']} T_done={rep['T_done']} T_rep={tr} p_rep={rep['p_rep']} "
          f"dup={rep['dup_ratio']} T_destroy={rep['T_destroy']} p_destroy={rep['p_destroy']} T_dead={rep['T_dead']}")


if __name__ == "__main__":
    main()
