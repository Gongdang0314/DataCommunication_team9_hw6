#!/usr/bin/env python3
"""실행이 끝난 뒤 결과를 점검한다 (제출 전 체크리스트의 자동 버전).
참고: 강의 자료 HW6explanation.pdf PPT §20.2 · p.204, 부록 B 제출 전 체크리스트 p.203~204

    python tools/verify.py --env envs/C2.json

점검 항목
  [1] 종료 코드   logs/<id>.jsonl 의 마지막 EXIT 이벤트: 생존자 0, 희생자 3
  [2] 파일 내용   완성 파일은 SHA-256, 불완전 파일(.incomplete)은 bitmap이 1인 청크만 원본과 비교
                  (.corrupt = SHA-256 재시도 한도 초과로 포기한 파일 -> FAIL)
  [3] 수렴        생존자들의 파일별 .bitmap 이 모두 같은가 (= 생존자 전원의 OR)
  [4] 손실        파괴가 없으면(A) 손실 0. 파괴된 피어가 어떤 파일의 원본도 아니면 손실 0, STALLED 0 (C1 유형)
  [5] 로그 형식   logs/*.jsonl 의 모든 줄이 JSON 인가

원본 내용은 make_dummy와 같은 방법으로 메모리에서 다시 만들어 비교하므로 원본 파일이 없어도 된다.
"""
import argparse
import glob
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from make_dummy import block_stream, MB  # noqa: E402

_cache = {}
problems = []


def expected_bytes(size):
    if size not in _cache:
        _cache[size] = b"".join(block_stream(size // MB))
    return _cache[size]


def fail(msg):
    problems.append(msg)
    print("  FAIL  " + msg)


def ok(msg):
    print("  ok    " + msg)


def read_events(path):
    evs, bad = [], 0
    if not os.path.exists(path):
        return None, 0
    with open(path, encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                evs.append(json.loads(ln))
            except json.JSONDecodeError:
                bad += 1
    return evs, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True)
    ap.add_argument("--tracker-log", default=os.path.join("logs", "tracker.jsonl"),
                    help="외부 VM 에서 가져온 tracker 로그 (기본: logs/tracker.jsonl)")
    a = ap.parse_args()
    env = json.load(open(a.env, encoding="utf-8"))
    cs = env["chunk_size"]
    victim = env["destroy"]["peer"] if env.get("destroy") else None
    peers = [p for p in env["peers"]]
    survivors = [p for p in peers if p["id"] != victim]
    print(f"== {env['name']} (run {env['run']})   victim: {victim or '-'}")

    # [5] log format + [1] exit codes
    print("[5] 로그 형식 / [1] 종료 코드")
    stalled_seen = 0
    for p in peers + [{"id": "tracker"}]:
        path = a.tracker_log if p["id"] == "tracker" else os.path.join("logs", f"{p['id']}.jsonl")
        evs, bad = read_events(path)
        if evs is None:
            if p["id"] == "tracker":
                fail(f"tracker: {path} 없음 (외부 tracker 라면 VM 의 logs/tracker.jsonl 을 이 폴더의 logs/ 로 복사할 것)")
            else:
                fail(f"{p['id']}: logs/{p['id']}.jsonl 없음")
            continue
        if bad:
            fail(f"{p['id']}: JSON이 아닌 줄 {bad}개")
        stalled_seen += sum(1 for e in evs if e.get("ev") == "STALLED")
        exits = [e for e in evs if e.get("ev") == "EXIT"]
        want = 3 if p["id"] == victim else 0
        if p["id"] == victim and not exits and any(e.get("ev") == "DESTROY" for e in evs):
            ok(f"{p['id']}: DESTROY 기록 확인 (희생자), 이벤트 {len(evs)}줄")
            continue
        if not exits:
            fail(f"{p['id']}: EXIT 이벤트 없음 (정상 종료하지 못했거나 기록 누락)")
        elif exits[-1].get("code") != want:
            fail(f"{p['id']}: exit {exits[-1].get('code')} (기대값 {want})")
        else:
            ok(f"{p['id']}: exit {want}, 이벤트 {len(evs)}줄")

    # [2] contents
    print("[2] 파일 내용")
    bitmaps = {}  # file -> {peer: str}
    for p in survivors:
        for f in env["files"]:
            n = (f["size"] + cs - 1) // cs
            full = os.path.join(p["dir"], f["name"])
            inc = full + ".incomplete"
            bmp = full + ".bitmap"
            bm = open(bmp).read().strip() if os.path.exists(bmp) else None
            if bm is not None and len(bm) != n:
                fail(f"{p['id']} {f['name']}: .bitmap 길이 {len(bm)} (기대 {n})")
                bm = None
            if os.path.exists(full):
                data = open(full, "rb").read()
                good = hashlib.sha256(data).digest() == hashlib.sha256(expected_bytes(f["size"])).digest()
                if good:
                    ok(f"{p['id']} {f['name']}: 완성, 해시 일치")
                else:
                    fail(f"{p['id']} {f['name']}: 완성 파일인데 해시 불일치")
                if bm is None:
                    fail(f"{p['id']} {f['name']}: .bitmap 없음 (완성 파일에도 남겨야 함)")
                    bm = "1" * n
                elif "0" in bm:
                    fail(f"{p['id']} {f['name']}: 완성 파일인데 .bitmap에 0이 있음")
            elif os.path.exists(inc):
                if bm is None:
                    fail(f"{p['id']} {f['name']}: .incomplete는 있는데 .bitmap 없음")
                    continue
                data = open(inc, "rb").read()
                exp = expected_bytes(f["size"])
                wrong = [i for i, c in enumerate(bm)
                         if c == "1" and data[i * cs:(i + 1) * cs] != exp[i * cs:(i + 1) * cs]]
                have = bm.count("1")
                if wrong:
                    fail(f"{p['id']} {f['name']}: 보유 청크 {have}개 중 {len(wrong)}개가 원본과 다름 (예: #{wrong[0]})")
                else:
                    ok(f"{p['id']} {f['name']}: 불완전 {have}/{n}, 보유 청크 모두 원본과 일치")
                if not os.path.exists(full + ".lost"):
                    fail(f"{p['id']} {f['name']}: .lost 파일 없음")
            else:
                if os.path.exists(full + ".corrupt"):
                    fail(f"{p['id']} {f['name']}: SHA-256 불일치로 포기한 파일(.corrupt)")
                else:
                    fail(f"{p['id']} {f['name']}: 파일도 .incomplete도 없음")
                continue
            bitmaps.setdefault(f["name"], {})[p["id"]] = bm

    # [3] convergence, [4] loss
    print("[3] 수렴 / [4] 손실")
    victim_is_origin = any(f["origin"] == victim for f in env["files"]) if victim else False
    for f in env["files"]:
        bms = bitmaps.get(f["name"], {})
        if not bms:
            continue
        n = len(next(iter(bms.values())))
        union = ["1" if any(b[i] == "1" for b in bms.values()) else "0" for i in range(n)]
        union = "".join(union)
        lost = union.count("0")
        diff = [pid for pid, b in bms.items() if b != union]
        if diff:
            fail(f"{f['name']}: {', '.join(diff)} 의 bitmap이 생존자 OR와 다름 (받을 수 있는 청크를 다 받지 않고 끝남)")
        else:
            ok(f"{f['name']}: 생존자 {len(bms)}명 bitmap 동일, 커버리지 {100 * (n - lost) / n:.1f}% (유실 {lost})")
        if victim and not victim_is_origin and lost:
            fail(f"{f['name']}: 원본이 아닌 피어 파괴인데 유실 {lost}개")
        if not victim and lost:
            fail(f"{f['name']}: 파괴가 없는 시나리오인데 유실 {lost}개 (모두 받지 못하고 끝남)")
    if victim and not victim_is_origin:
        if stalled_seen:
            fail(f"원본이 아닌 피어 파괴인데 STALLED 이벤트 {stalled_seen}개 (오판)")
        else:
            ok("STALLED 오판 없음")

    print()
    if problems:
        print(f"RESULT: FAIL ({len(problems)}개 문제)")
        sys.exit(1)
    print("RESULT: PASS")


if __name__ == "__main__":
    main()
