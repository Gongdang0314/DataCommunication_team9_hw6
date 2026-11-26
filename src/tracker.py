#!/usr/bin/env python3
"""HW6 Mini-Torrent Tracker (7조).

    python3 tracker.py --env envs/B.json            # 0.0.0.0:<env tracker.port>
    python3 tracker.py --env envs/B.json 9500       # 다른 포트

Tracker 는 명단(누가 어떤 파일을 가졌나) · 생존 판정 · 지시(CMD) · 관찰(REPORT)만 다룬다.
청크 데이터는 절대 다루지 않는다. Peer 에게 먼저 연결하지 않고, 지시는 HEARTBEAT 응답으로만 보낸다.

Thread
  main       accept(timeout 0.5) -> 연결마다 handler. finished 가 되면 report.json -> EXIT
  handler    연결당 1. 한 줄 읽고 한 줄 답한다. 응답은 lock 안에서 만들고 보내기는 lock 밖
  scheduler  0.5초. DEAD 판정 · crash 일정 -> pending · destroy(replica_safe) · ALLDONE · ABORT
  observer   1초. REPORT 로 T_rep · p_rep · upstat · dup_ratio
  screen     1초. (터미널일 때만) 상태판
모든 상태는 lock 하나(self.lock)로 보호한다.
"""
import json
import os
import socket
import sys
import threading
import time
from collections import deque

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "lib"))
from evlog import EvLog  # noqa: E402
from tui import SplitScreen  # noqa: E402
from hw6net import Conn, ConnClosed, EnvError, load_env, usage_exit  # noqa: E402

USAGE = "usage: python3 tracker.py --env <env.json> [PORT]"
SCHED_SEC = 0.5          # 구현 상수: scheduler 주기
OBS_SEC = 1.0            # 구현 상수: observer 주기
ABORT_WAIT_SEC = 15.0    # 구현 상수: ABORT 뒤 BYE 를 기다리는 시간


def parse_args(argv):
    env_path, port = None, None
    rest = list(argv)
    while rest:
        a = rest.pop(0)
        if a == "--env":
            if not rest:
                usage_exit("--env needs a path", USAGE)
            env_path = rest.pop(0)
        elif a.startswith("--env="):
            env_path = a.split("=", 1)[1]
        elif a in ("-h", "--help"):
            print(USAGE)
            sys.exit(0)
        elif port is None and not a.startswith("-"):
            if not a.isdigit() or not (0 < int(a) < 65536):
                usage_exit(f"PORT must be a number: {a}", USAGE)
            port = int(a)
        else:
            usage_exit(f"unknown argument: {a}", USAGE)
    if not env_path:
        usage_exit("--env is required", USAGE)
    return env_path, port


class Tracker:
    def __init__(self, env, port):
        self.env = env
        self.T = env["timing"]
        self.port = port
        self.ids = [p["id"] for p in env["peers"]]
        self.pinfo = {p["id"]: p for p in env["peers"]}
        self.files = {f["name"]: f for f in env["files"]}
        self.total = int(env["progress_total_chunks"])
        self.dead_sec = float(self.T["dead_sec"])
        self.lock = threading.Lock()
        os.makedirs("logs", exist_ok=True)
        open(os.path.join("logs", "tracker.jsonl"), "w").close()   # 한 번의 실행 = 새 로그
        self.log = EvLog("tracker")
        self.scr = SplitScreen(top=9)
        # ---- 상태 (모두 self.lock) ----
        self.joined = {}            # id -> n (참가 순서)
        self.T0 = None              # START (env 의 4명 모두 HELLO)
        self.last_seen = {}
        self.have = {i: 0 for i in self.ids}
        self.dead = set()
        self.T_dead = {}
        self.byes = set()
        self.done = {}              # id -> (kind, detail)
        self.T_done = {}
        self.registered = {n: set() for n in self.files}   # 완성 파일 보유 (REGISTER)
        self.partial = {n: set() for n in self.files}      # 받는 중 (HAVE)
        self.pending = {i: deque() for i in self.ids}
        self.crash_sent = [False] * len(env.get("crashes") or [])
        self.crashes_sent = []
        self.destroy = env.get("destroy")
        self.destroy_sent = False
        self.T_destroy = None
        self.p_destroy = None
        self.reports = {i: {} for i in self.ids}            # id -> file -> (bitmap, ts)
        self.upstat = {n: (0, 0) for n in self.files}       # 원본 보유자의 (sent, distinct)
        self.T_rep = {n: None for n in self.files}
        self.upstat_at_rep = {}
        self.dup_ratio = {}
        self.p_rep = None
        self.alldone = False
        self.T_alldone = None
        self.abort = None
        self.abort_at = None
        self.prog_val = -1.0
        self.prog_since = None
        self.finished = False
        self.exit_code = 0
        self.recent = deque(maxlen=6)

    # ------------------------------------------------------------ util
    def t(self):
        """START 기준 초 (START 전이면 None)."""
        return None if self.T0 is None else time.time() - self.T0

    def ts(self):
        v = self.t()
        return round(v, 2) if v is not None else None

    def event(self, ev, **kw):
        self.log.event(ev, **kw)
        det = " ".join(f"{k}={v}" for k, v in kw.items())
        line = f"{self.log.now():7.1f} {ev:<13} {det}"
        self.recent.append(line)
        self.scr.log(line)

    def alive(self, pid, now=None):
        now = now or time.time()
        return (pid in self.joined and pid not in self.byes and pid not in self.dead
                and now - self.last_seen.get(pid, 0) <= self.dead_sec)

    def progress(self):
        return sum(self.have.values()) / self.total if self.total else 0.0

    def touch(self, pid):
        """어떤 메시지든 받으면 last_seen 갱신. DEAD 였으면 BACK."""
        if pid not in self.joined:
            return
        self.last_seen[pid] = time.time()
        if pid in self.dead:
            self.dead.discard(pid)
            self.event("BACK", id=pid)

    # ------------------------------------------------------------ 메시지 처리 (lock 안에서 호출)
    def handle(self, parts, conn_state, src_ip):
        cmd = parts[0] if parts else ""
        n = len(parts)
        if cmd == "HELLO" and n == 2:
            pid = parts[1]
            if pid not in self.pinfo:
                return "ERR MISMATCH"
            conn_state["id"] = pid
            if pid in self.joined:
                self.byes.discard(pid)
                self.event("REJOIN", id=pid)
            else:
                self.joined[pid] = len(self.joined) + 1
                self.event("JOIN", id=pid, n=self.joined[pid], src_ip=src_ip)
            self.touch(pid)
            if self.T0 is None and all(i in self.joined for i in self.ids):
                self.T0 = time.time()
                self.prog_since = self.T0
                self.event("START", peers=len(self.ids))
            return "OK"
        if cmd == "REGISTER" and n == 6:
            pid, _port, fname, size, chunk = parts[1:]
            f = self.files.get(fname)
            if pid not in self.pinfo or pid not in self.joined:
                return "ERR MISMATCH"
            if f is None:
                return "ERR NOFILE"
            if not (size.isdigit() and chunk.isdigit()) or int(size) != f["size"] \
                    or int(chunk) != int(self.env["chunk_size"]):
                return "ERR MISMATCH"
            self.touch(pid)
            self.registered[fname].add(pid)
            self.partial[fname].discard(pid)
            self.event("REGISTER", id=pid, file=fname, src_ip=src_ip)
            return "OK"
        if cmd == "HAVE" and n == 4:
            pid, _port, fname = parts[1:]
            if pid not in self.joined:
                return "ERR MISMATCH"
            if fname not in self.files or not self.registered[fname]:
                return "ERR NOFILE"
            self.touch(pid)
            if pid not in self.registered[fname]:
                self.partial[fname].add(pid)
            return "OK"
        if cmd == "LIST" and n == 1:
            pid = conn_state.get("id")
            if pid:
                self.touch(pid)
            rows = []
            for name, f in self.files.items():
                if not self.registered[name]:
                    continue                           # 원본이 아직 등록되지 않은 파일은 안 보인다
                holders = self.registered[name] | self.partial[name]
                rows.append(f"{name} {f['size']} {self.env['chunk_size']} {f['nchunks']} {len(holders)}")
            return "\n".join([f"FILES {len(rows)}"] + rows)
        if cmd == "PEERS" and n == 2:
            fname = parts[1]
            if fname not in self.files:
                return "ERR NOFILE"
            pid = conn_state.get("id")
            if pid:
                self.touch(pid)
            now = time.time()
            rows = []
            for hid in self.ids:                       # env 순서
                if hid in self.registered[fname] or hid in self.partial[fname]:
                    if self.alive(hid, now):
                        p = self.pinfo[hid]
                        rows.append(f"{hid} {p['host']} {p['port']}")   # 연결에서 본 IP 가 아니라 env 의 host
            return "\n".join([f"PEERS {len(rows)}"] + rows)
        if cmd == "HEARTBEAT" and n == 3:
            pid = parts[1]
            if pid not in self.joined:
                return "ERR MISMATCH"
            self.touch(pid)
            try:
                self.have[pid] = int(parts[2])
            except ValueError:
                return "ERR BADCMD"
            q = self.pending[pid]
            if q:
                c = q.popleft()
                if self.alldone or self.abort:
                    self.drop_cmd(pid, c)
                    while q:
                        self.drop_cmd(pid, q.popleft())
                    return "OK"
                self.event("CMD_DELIVERED", id=pid, cmd=c)
                return c
            return "OK"
        if cmd == "REPORT" and n == 6:
            pid, fname, sent, distinct, bm = parts[1:]
            if pid not in self.joined or fname not in self.files:
                return "ERR NOFILE"
            self.touch(pid)
            if len(bm) != self.files[fname]["nchunks"]:
                return "ERR BADCMD"
            self.reports[pid][fname] = (bm, time.time())
            if self.files[fname]["origin"] == pid:
                try:
                    self.upstat[fname] = (int(sent), int(distinct))
                except ValueError:
                    return "ERR BADCMD"
            return "OK"
        if cmd == "DONE" and n >= 3 and parts[2] in ("FULL", "PARTIAL"):
            pid, kind = parts[1], parts[2]
            if pid not in self.joined:
                return "ERR MISMATCH"
            self.touch(pid)
            detail = " ".join(parts[3:])
            self.done[pid] = (kind, detail)
            self.T_done[pid] = self.ts()
            self.event("DONE", id=pid, kind=kind, detail=detail)
            return "OK"
        if cmd == "UNDONE" and n == 2:
            pid = parts[1]
            self.touch(pid)
            self.done.pop(pid, None)
            self.T_done.pop(pid, None)
            self.event("UNDONE", id=pid)
            return "OK"
        if cmd == "STATE" and n == 1:
            pid = conn_state.get("id")
            if pid:
                self.touch(pid)
            return self.state_reply()
        if cmd == "BYE" and n == 2:
            pid = parts[1]
            if pid in self.joined and pid not in self.byes:
                self.touch(pid)
                self.byes.add(pid)
                self.event("BYE", id=pid)
            return "OK"
        return "ERR BADCMD"

    def drop_cmd(self, pid, c):
        self.event("CMD_DROPPED", id=pid, cmd=c)
        if c.startswith("CMD DISCONNECT"):
            for rec in self.crashes_sent:
                if rec["peer"] == pid and f"CMD DISCONNECT {rec['sec']}" == c and not rec.get("_drop"):
                    rec["_drop"] = True
                    break

    def live_list(self):
        now = time.time()
        return [p for p in self.ids if self.alive(p, now)]

    def state_reply(self):
        live = self.live_list()
        d = [p for p in live if p in self.done]
        head = f"STATE {len(d)} {len(live)}"
        if self.abort:
            return f"{head} ABORT {self.abort}"
        return head + (" ALLDONE" if self.alldone else " WAIT")

    # ------------------------------------------------------------ replica_safe
    def fresh_reports(self, fname, now):
        stale = float(self.T["report_stale_sec"])
        out = {}
        for pid in self.ids:
            r = self.reports[pid].get(fname)
            if r and now - r[1] <= stale and self.alive(pid, now):
                out[pid] = r[0]
        return out

    def replica_safe(self, victim):
        """희생자가 가진 모든 파일의 모든 1 청크가, 희생자가 아닌 live Peer 의 bitmap OR 에도 있으면 참."""
        now = time.time()
        stale = float(self.T["report_stale_sec"])
        vrep = self.reports.get(victim, {})
        if not vrep or not any(now - ts <= stale for _, ts in vrep.values()):
            return False                               # 모르면 기다린다
        for fname in self.files:
            r = vrep.get(fname)
            if r is None:
                return False
            vbm = r[0]
            others = [bm for pid, bm in self.fresh_reports(fname, now).items() if pid != victim]
            for i, ch in enumerate(vbm):
                if ch == "1" and not any(bm[i] == "1" for bm in others):
                    return False
        return True

    # ------------------------------------------------------------ scheduler (0.5초)
    def scheduler(self):
        while not self.finished:
            time.sleep(SCHED_SEC)
            with self.lock:
                if self.T0 is None:
                    continue
                now = time.time()
                t = now - self.T0
                for p in self.ids:
                    if p in self.joined and p not in self.dead and p not in self.byes \
                            and now - self.last_seen.get(p, now) > self.dead_sec:
                        self.dead.add(p)
                        silent = round(now - self.last_seen[p], 1)
                        if p not in self.T_dead:
                            self.T_dead[p] = round(t, 2)
                        self.event("DEAD", id=p, silent_sec=silent)
                prog = self.progress()
                if not self.alldone and not self.abort:
                    for k, c in enumerate(self.env.get("crashes") or []):
                        if not self.crash_sent[k] and t >= float(c["at_sec"]):
                            self.crash_sent[k] = True
                            cmd = f"CMD DISCONNECT {c['duration_sec']}"
                            self.pending[c["peer"]].append(cmd)
                            self.crashes_sent.append({"t": round(t, 2), "peer": c["peer"],
                                                      "cmd": "DISCONNECT", "sec": c["duration_sec"]})
                            self.event("CMD", target=c["peer"], cmd="DISCONNECT", sec=c["duration_sec"])
                    d = self.destroy
                    if d and not self.destroy_sent:
                        trig, val = d["trigger"], float(d["value"])
                        if trig == "time":
                            due = t >= val
                        elif trig == "progress":
                            due = prog >= val
                        else:                          # after_replication
                            due = self.p_rep is not None and prog >= self.p_rep + val * (1 - self.p_rep)
                        if due and (not d.get("replica_safe") or self.replica_safe(d["peer"])):
                            self.pending[d["peer"]].append("CMD DESTROY")
                            self.destroy_sent = True
                            self.T_destroy = round(t, 2)
                            self.p_destroy = round(prog, 4)
                            self.event("CMD", target=d["peer"], cmd="DESTROY", progress=round(prog, 4))
                    live = self.live_list()
                    if live and all(p in self.done for p in live):
                        self.alldone = True
                        self.T_alldone = round(t, 2)
                        self.event("ALLDONE", live=live, done={p: self.done[p][0] for p in self.done})
                    else:
                        if prog != self.prog_val:
                            self.prog_val, self.prog_since = prog, now
                        elif now - self.prog_since >= float(self.T["hang_sec"]):
                            self.set_abort("stalled", prog)
                        if t > float(self.T["deadline_sec"]):
                            self.set_abort("deadline", prog)
                else:
                    for k, c in enumerate(self.env.get("crashes") or []):
                        if not self.crash_sent[k] and t >= float(c["at_sec"]):
                            self.crash_sent[k] = True
                            self.event("CMD_DROPPED", id=c["peer"], cmd=f"CMD DISCONNECT {c['duration_sec']}")
                # 종료 판정: 남은 live Peer 가 모두 BYE (DEAD 는 기다리지 않음), 또는 ABORT 뒤 15초
                if self.alldone or self.abort:
                    waiting = [p for p in self.live_list()]
                    if not waiting or (self.abort and now - self.abort_at >= ABORT_WAIT_SEC):
                        self.finished = True

    def set_abort(self, reason, prog):
        if self.abort:
            return
        self.abort = reason
        self.abort_at = time.time()
        self.exit_code = 2
        self.event("ABORT", reason=reason, progress=round(prog, 4))

    # ------------------------------------------------------------ observer (1초)
    def observer(self):
        while not self.finished:
            time.sleep(OBS_SEC)
            with self.lock:
                if self.T0 is None:
                    continue
                now = time.time()
                for fname, f in self.files.items():
                    if self.T_rep[fname] is not None:
                        continue
                    bms = [bm for pid, bm in self.fresh_reports(fname, now).items() if pid != f["origin"]]
                    if not bms:
                        continue
                    n = f["nchunks"]
                    if all(any(bm[i] == "1" for bm in bms) for i in range(n)):
                        t = round(now - self.T0, 2)
                        self.T_rep[fname] = t
                        sent, distinct = self.upstat[fname]
                        self.upstat_at_rep[fname] = [sent, distinct]
                        self.dup_ratio[fname] = round((sent - distinct) / sent, 4) if sent else 0.0
                        self.event("REPLICATED", file=fname, t_start=t, upstat=[sent, distinct],
                                   progress=round(self.progress(), 4))
                if self.p_rep is None and all(v is not None for v in self.T_rep.values()):
                    self.p_rep = round(self.progress(), 4)
                    self.event("ALL_REPLICATED", t_start=max(self.T_rep.values()), p_rep=self.p_rep)

    # ------------------------------------------------------------ 화면 (터미널일 때만)
    def screen(self):
        while not self.finished:
            time.sleep(1.0)
            if not self.scr.tty:
                continue
            with self.lock:
                lines = self.status_lines()
            self.scr.status(lines)

    def status_lines(self):
        now = time.time()
        t = self.t()
        prog = self.progress()
        st = "ABORT " + self.abort if self.abort else ("ALLDONE" if self.alldone else "WAIT")
        bar = "#" * int(40 * prog) + "." * (40 - int(40 * prog))
        head = (f"TRACKER {self.env['name']} t={t if t is None else round(t, 1)}s "
                f"{'START+' if self.T0 else 'waiting HELLO'} {st}  port {self.port}")
        out = [head, f"  progress [{bar}] {100 * prog:5.1f}%"]
        for p in self.ids:
            if p not in self.joined:
                out.append(f"  {p}  ---    not joined")
                continue
            if p in self.byes:
                s = "BYE"
            elif p in self.dead:
                s = "DEAD"
            else:
                s = "LIVE"
            last = now - self.last_seen.get(p, now)
            dn = self.done.get(p, ("-",))[0]
            out.append(f"  {p}  {s:<4}  have {self.have[p]:5}  last {last:5.1f}s  done {dn}")
        rep = "  ".join(f"{n}={v if v is not None else '-'}" for n, v in self.T_rep.items())
        out.append(f"  T_rep {rep}  p_rep={self.p_rep}")
        d = self.destroy
        dd = "-" if not d else (f"{d['peer']} {d['trigger']}={d['value']} "
                                f"{'sent@' + str(self.T_destroy) if self.destroy_sent else '(wait)'}")
        nxt = [f"{c['peer']}@{c['at_sec']}" for k, c in enumerate(self.env.get("crashes") or [])
               if not self.crash_sent[k]]
        out.append(f"  destroy {dd}   next crash {' '.join(nxt[:4]) or '-'}")
        return out

    # ------------------------------------------------------------ 연결 처리
    def handler(self, sock, addr):
        conn = Conn(sock)
        conn.settimeout(0.5)
        state = {"id": None}
        try:
            while not self.finished:
                try:
                    line = conn.read_line()
                except socket.timeout:
                    continue
                parts = line.split()
                if not parts:
                    continue
                with self.lock:                        # 응답은 lock 안에서 만들고
                    reply = self.handle(parts, state, addr[0])
                conn.send_line(reply)                  # 보내기는 lock 밖에서
        except (ConnClosed, OSError, ValueError):
            pass
        finally:
            conn.close()

    def run(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind(("0.0.0.0", self.port))
        except OSError as e:
            print(f"cannot listen on port {self.port}: {e}", file=sys.stderr)
            sys.exit(1)
        srv.listen(64)
        srv.settimeout(0.5)
        self.event("START_PROGRAM", env=self.env["name"], run=self.env["run"], port=self.port)
        for fn in (self.scheduler, self.observer, self.screen):
            threading.Thread(target=fn, daemon=True).start()
        try:
            while not self.finished:
                try:
                    s, addr = srv.accept()
                except socket.timeout:
                    continue
                except OSError:
                    continue
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                threading.Thread(target=self.handler, args=(s, addr), daemon=True).start()
        except KeyboardInterrupt:
            with self.lock:
                self.set_abort("interrupted", self.progress())
        srv.close()
        with self.lock:
            report = self.build_report()
            with open("report.json", "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
                f.write("\n")
            self.event("EXIT", code=self.exit_code, alldone=self.T_alldone, t_exit=report["T_exit"])
        self.scr.close()
        self.log.close()
        return self.exit_code

    def build_report(self):
        peers = {}
        for p in self.ids:
            if p in self.byes:
                peers[p] = "BYE"
            elif p in self.T_dead:
                peers[p] = "DEAD"
            else:
                peers[p] = "NO_BYE"
        done_vals = [v for v in self.T_done.values() if v is not None]
        sent = [{k: v for k, v in c.items() if not k.startswith("_")}
                for c in self.crashes_sent if not c.get("_drop")]
        return {
            "env": self.env["name"], "run": self.env["run"], "seed": self.env.get("seed"),
            "policy": self.env["download"].get("policy"),
            "destroy_plan": self.destroy,
            "crashes_sent": sent,
            "T_rep": self.T_rep,
            "p_rep": self.p_rep,
            "upstat_at_rep": self.upstat_at_rep,
            "dup_ratio": self.dup_ratio,
            "T_destroy": self.T_destroy, "p_destroy": self.p_destroy,
            "T_dead": self.T_dead,
            "T_done": self.T_done,
            "T_last_done": max(done_vals) if done_vals else None,
            "T_alldone": self.T_alldone,
            "T_exit": self.ts(),
            "done_kind": {p: v[0] for p, v in self.done.items()},
            "peers": peers,
            "abort": self.abort,
        }


def main():
    env_path, port = parse_args(sys.argv[1:])
    try:
        env, _ = load_env(env_path)
    except EnvError as e:
        usage_exit(str(e), USAGE)
    port = port or int(env["tracker"]["port"])
    tr = Tracker(env, port)
    sys.exit(tr.run())


if __name__ == "__main__":
    main()
