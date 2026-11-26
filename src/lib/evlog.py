"""HW6 이벤트 로거 (Python).
참고: 강의 자료 HW6explanation.pdf PPT §18.3~§18.4 · p.173~174, 결과 로그 규격 · 이벤트 사전 §18.10~§18.25 · p.180~195

이벤트 하나를 ① logs/<id>.jsonl 에 JSON 한 줄로 쓰고 ② 화면용 최근 이벤트 버퍼에 넣는다.
여러 스레드에서 동시에 불러도 안전하다.

    from evlog import EvLog
    log = EvLog("P4")                                   # logs/P4.jsonl
    log.event("START", env="C2", policy="rarest", up_kbps=540, down_kbps=460)
    log.event("RECV", file="share30.bin", idx=311, src="P3")
    log.event("CRASH", sec=9.9)
    lines = log.recent(5)                               # 화면 아래쪽에 보여 줄 최근 이벤트 문자열들
    log.event("EXIT", code=0); log.close()

화면 버퍼에는 기본적으로 RECV 를 넣지 않는다(너무 많음). EvLog("P4", screen_all=True)면 모두 넣는다.
필드 이름으로 t · peer · ev 는 쓰지 말 것 (로거가 쓰는 칸이다. 쓰면 t_ · peer_ · ev_ 로 바꿔 저장한다).
"""
import collections
import json
import os
import threading
import time

QUIET = {"RECV", "SEND"}


class EvLog:
    def __init__(self, peer_id, path=None, keep=50, screen_all=False):
        self.peer = peer_id
        self.t0 = time.time()
        path = path or os.path.join("logs", f"{peer_id}.jsonl")
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._f = open(path, "a", encoding="utf-8")
        self._lock = threading.Lock()
        self._recent = collections.deque(maxlen=keep)
        self._screen_all = screen_all

    def now(self):
        """프로그램 시작 기준 경과 초."""
        return time.time() - self.t0

    def event(self, ev, **fields):
        rec = {"t": round(self.now(), 2), "peer": self.peer, "ev": ev}
        for k, v in fields.items():          # t / peer / ev 는 로거가 쓰는 칸 → 같은 이름이면 뒤에 _ 를 붙여 보존
            rec[k + "_" if k in ("t", "peer", "ev") else k] = v
        line = json.dumps(rec, ensure_ascii=False)
        with self._lock:
            self._f.write(line + "\n")
            self._f.flush()
            if self._screen_all or ev not in QUIET:
                detail = " ".join(f"{k}={v}" for k, v in fields.items())
                self._recent.append(f"{rec['t']:7.1f}  {ev:<8} {detail}")

    def recent(self, n=5):
        with self._lock:
            return list(self._recent)[-n:]

    def close(self):
        with self._lock:
            self._f.close()
