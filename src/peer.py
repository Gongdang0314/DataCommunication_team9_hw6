#!/usr/bin/env python3
"""HW6 Mini-Torrent Peer (7조).

    python peer.py --env envs/A.json --id P1 <TRACKER_IP> <TRACKER_PORT>
    python peer.py --env envs/C3.json --id P1 --policy rarest <TRACKER_IP> <TRACKER_PORT>

Peer 는 서버이자 클라이언트다. 업로드 서버(연결당 Thread)와 다운로드 워커 N개가 동시에 돈다.

Thread                         하는 일
  main                           시작 절차 · 종료 절차 (join -> 결과 저장 -> BYE -> EXIT)
  listener (1)                   자기 포트 accept -> 연결마다 upload handler. 가상 crash 중에는 소켓을 닫는다
  upload handler (연결당 1)      BITMAP · GET 응답. GET 은 bit 확인 -> pread -> UP.take(len) -> DATA
  worker (download.workers)      청크 고르기 -> 소스 고르기 -> DOWN.take -> GET -> DATA -> pwrite -> bit = 1
  control (1, 1초)               HEARTBEAT(CMD) · PEERS · BITMAP · REPORT · STALLED · DONE/UNDONE · STATE · tracker_lost
  screen (1, 1초)                ASCII 상태판 (터미널일 때만)
  disconnect (crash 때만 1)      n초 침묵 뒤 rejoin

Lock (위에서 아래 순서로만 잡는다 - 실제로는 둘을 겹쳐 잡지 않는다)
  f.lock       파일별 bitmap · have · in_progress · state · sources · sent
  remote_lock  원격 bitmap 캐시 · holders · 주소
  sel_lock     inflight · fails · banned
  limiter.lock 속도 제한기 예약 시각 (예약만 lock 안, sleep 은 밖)
  link.lock    Tracker 연결 (요청 한 줄 -> 응답 한 줄의 쌍)
  pf.io_lock   Windows 에는 os.pwrite/pread 가 없어 seek+read/write 한 묶음만 보호 (공유 자료 lock 아님)
  log lock     evlog 내부 / 화면 출력은 tui 내부 lock
send · recv · 파일 I/O · sleep 은 f.lock / remote_lock / sel_lock 안에서 하지 않는다.
"""
import hashlib
import os
import random
import socket
import sys
import threading
import time
from collections import Counter, deque

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "lib"))
from evlog import EvLog  # noqa: E402
from tui import SplitScreen  # noqa: E402
from hw6net import (Conn, ConnClosed, EnvError, chunk_len, errname, load_env,  # noqa: E402
                    ranges, usage_exit)

USAGE = ("usage: python peer.py --env <env.json> --id <Pn> [--policy random|rarest] <TRACKER_IP> <TRACKER_PORT>\n"
         "  (TRACKER_IP 와 TRACKER_PORT 는 항상 둘 다 적는다. C3 처럼 env 의 policy 가 null 이면 --policy 필수)")
NET_TIMEOUT = 5.0        # 구현 상수: GET 응답 · 연결 timeout
BITMAP_TIMEOUT = 2.0     # 구현 상수: control 의 BITMAP 요청 timeout (heartbeat 가 밀리지 않게)
PEER_CONNECT_TIMEOUT = 1.0  # 구현 상수: Peer 끼리(같은 PC) 연결 timeout. Windows 는 닫힌 포트의 refused 가
                            # SYN 재시도 때문에 약 2초 걸리므로 짧게 둔다 (정상 연결은 1 ms 안쪽)
UPLOAD_DRAIN_SEC = 3.0   # 구현 상수: 종료 시 진행 중 업로드 마무리 최대 시간
MB = 1 << 20
O_BIN = getattr(os, "O_BINARY", 0)
HAS_PWRITE = hasattr(os, "pwrite") and hasattr(os, "pread")


# ====================================================================== 인자
def parse_args(argv):
    a = {"env": None, "id": None, "policy": None}
    pos = []
    rest = list(argv)
    while rest:
        x = rest.pop(0)
        if x in ("-h", "--help"):
            print(USAGE)
            sys.exit(0)
        if x.startswith("--") and "=" in x:
            k, v = x[2:].split("=", 1)
            rest.insert(0, v)
            x = "--" + k
        if x in ("--env", "--id", "--policy"):
            if not rest:
                usage_exit(f"{x} needs a value", USAGE)
            a[x[2:]] = rest.pop(0)
        elif x.startswith("-"):
            usage_exit(f"unknown option {x}", USAGE)
        else:
            pos.append(x)
    if not a["env"] or not a["id"]:
        usage_exit("--env and --id are required", USAGE)
    if a["policy"] is not None and a["policy"] not in ("random", "rarest"):
        usage_exit("--policy must be random or rarest", USAGE)
    if len(pos) != 2:
        usage_exit("tracker IP and PORT are both required (last two arguments)", USAGE)
    if not pos[1].isdigit() or not (0 < int(pos[1]) < 65536):
        usage_exit(f"PORT must be a number: {pos[1]}", USAGE)
    return a, pos[0], int(pos[1])


# ====================================================================== 속도 제한기
class RateLimiter:
    """방향(UP/DOWN)마다 Peer 전체에 하나. 모든 Thread 가 공유한다.
    take(n): 다음 빈 시간 칸을 예약(lock 안)하고, 그 시각까지 잔다(lock 밖)."""

    def __init__(self, bytes_per_sec):
        self.rate = float(bytes_per_sec)
        self.next_t = 0.0
        self.lock = threading.Lock()

    def take(self, n):
        with self.lock:
            start = max(time.time(), self.next_t)
            self.next_t = start + n / self.rate
            wake = self.next_t
        time.sleep(max(0.0, wake - time.time()))


# ====================================================================== 위치 지정 파일 I/O
class PFile:
    """청크 위치에 직접 쓰고 읽는다. POSIX 는 os.pwrite/os.pread.
    Windows 에는 둘 다 없어서 seek+write / seek+read 를 io_lock 하나로 묶는다 (pwrite_guide 의 방법).
    Windows 는 열린 파일의 이름을 바꿀 수 없으므로 rename 은 io_lock 안에서 닫고 -> 바꾸고 -> 다시 연다."""

    def __init__(self, path, size=None):
        self.path = path
        self.io_lock = threading.Lock()
        flags = os.O_RDWR | O_BIN
        if size is not None:
            flags |= os.O_CREAT | os.O_TRUNC
        self.fd = os.open(path, flags, 0o644)
        if size is not None:
            os.ftruncate(self.fd, size)               # 전체 크기로 미리 (못 받은 자리는 0)

    def pwrite(self, data, off):
        if HAS_PWRITE:
            os.pwrite(self.fd, data, off)
            return
        with self.io_lock:
            os.lseek(self.fd, off, os.SEEK_SET)
            view, done = memoryview(data), 0
            while done < len(data):
                done += os.write(self.fd, view[done:])

    def pread(self, n, off):
        if HAS_PWRITE:
            return os.pread(self.fd, n, off)
        with self.io_lock:
            os.lseek(self.fd, off, os.SEEK_SET)
            out = bytearray()
            while len(out) < n:
                d = os.read(self.fd, n - len(out))
                if not d:
                    break
                out += d
            return bytes(out)

    def rename(self, new_path):
        if HAS_PWRITE:                                 # POSIX: 열린 파일도 이름을 바꿀 수 있다
            os.replace(self.path, new_path)
            self.path = new_path
            return
        with self.io_lock:
            os.close(self.fd)
            os.replace(self.path, new_path)
            self.path = new_path
            self.fd = os.open(new_path, os.O_RDWR | O_BIN)

    def close(self):
        with self.io_lock:
            try:
                os.close(self.fd)
            except OSError:
                pass


def sha256_of(pf, size):
    """1 MB 씩 읽어 누적한다 (파일 전체를 메모리에 올리지 않는다)."""
    h = hashlib.sha256()
    off = 0
    while off < size:
        n = min(MB, size - off)
        h.update(pf.pread(n, off))
        off += n
    return h.hexdigest()


# ====================================================================== 파일 상태
class FileState:
    def __init__(self, spec, cs, dirpath, is_origin):
        self.name = spec["name"]
        self.size = spec["size"]
        self.n = spec["nchunks"]
        self.cs = cs
        self.sha = spec["sha256"].lower()
        self.origin = spec["origin"]
        self.is_origin = is_origin
        self.final_path = os.path.join(dirpath, self.name)
        self.part_path = self.final_path + ".part"
        self.lock = threading.Lock()
        self.bitmap = bytearray(self.n)
        self.have = 0                      # 받은 청크 수 (원본 파일은 0)
        self.in_progress = set()
        self.state = "SEED" if is_origin else "DOWN"
        self.zero_since = None
        self.hash_fail = 0
        self.sent = 0
        self.sent_set = set()
        self.sources = Counter()
        self.t_start = None
        self.pf = None

    def clen(self, i):
        return chunk_len(self.size, self.cs, i)

    def bits(self):
        with self.lock:
            return self.bitmap.translate(BITS_TABLE).decode()

    def count(self):
        with self.lock:
            return self.bitmap.count(1)


BITS_TABLE = bytes.maketrans(b"\x00\x01", b"01")


class NoChunk(ValueError):
    """상대가 ERR NOCHUNK 로 답함 (내 캐시의 그 상대 bitmap 이 낡았다)."""


# ====================================================================== Tracker 연결
class TrackerLink:
    """Peer 가 연 Tracker 연결 하나를 control · main · worker 가 lock 으로 나눠 쓴다 (요청 한 줄 -> 응답)."""

    def __init__(self, peer, host, port):
        self.peer = peer
        self.host, self.port = host, port
        self.lock = threading.Lock()
        self.conn = None
        self.last_ok = time.time()

    def request(self, line, multi=False, force=False):
        """응답 첫 줄(과 multi 면 n줄)을 돌려준다. 실패하면 OSError/ConnClosed.
        가상 crash 중에는 보내지 않는다 (force 는 rejoin 의 announce 만)."""
        with self.lock:
            if not self.peer.online.is_set() and not force:
                raise ConnectionError("offline")
            try:
                if self.conn is None:
                    self.conn = Conn.connect(self.host, self.port, NET_TIMEOUT)
                c = self.conn
                c.send_line(line)
                first = c.read_line()
                rows = []
                if multi and first.split()[0] in ("FILES", "PEERS"):
                    for _ in range(int(first.split()[1])):
                        rows.append(c.read_line())
                self.last_ok = time.time()
                return (first, rows) if multi else first
            except (OSError, ConnClosed, ValueError, IndexError):
                if self.conn is not None:
                    self.conn.close()
                self.conn = None
                raise

    def drop(self):
        """가상 crash: 응답 없이 shutdown (Tracker 쪽으로도 침묵). lock 을 기다리지 않는다."""
        c = self.conn
        self.conn = None
        if c is not None:
            c.close()


# ====================================================================== Peer
class Peer:
    def __init__(self, env, me, tracker_ip, tracker_port):
        self.env = env
        self.me = me
        self.id = me["id"]
        self.T = env["timing"]
        self.cs = int(env["chunk_size"])
        self.policy = me["policy"]
        self.workers_n = int(env["download"]["workers"])
        self.per_peer_max = int(env["download"]["per_peer_max"])
        self.fail_limit = int(self.T["fail_limit"])
        self.ban_sec = float(self.T["ban_sec"])
        self.rng = random.Random(me["rng_seed"])
        self.rng_lock = threading.Lock()
        self.UP = RateLimiter(me["up_Bps"])
        self.DOWN = RateLimiter(me["down_Bps"])
        self.dir = me["dir"]
        self.log = EvLog(self.id)
        self.scr = SplitScreen(top=7)
        self.tracker_addr = f"{tracker_ip}:{tracker_port}"
        self.link = TrackerLink(self, tracker_ip, tracker_port)

        self.files = {}
        for spec in env["files"]:
            self.files[spec["name"]] = FileState(spec, self.cs, self.dir, spec["origin"] == self.id)
        self.down_files = [f for f in self.files.values() if not f.is_origin]
        self.order = []                     # random 정책: (파일, 청크) 전체를 한 번 섞은 순서

        self.online = threading.Event()     # alive (가상 crash 중에는 꺼짐)
        self.online.set()
        self.stopping = threading.Event()
        self.exit_code = 0
        self.epoch = 0                      # crash 할 때마다 +1 (그 전에 시작한 일을 버리기 위해)
        self.offline_lock = threading.Lock()

        self.remote_lock = threading.Lock()
        self.remote = {f.name: {} for f in self.down_files}   # file -> {pid: '0101..'}
        self.holders = {f.name: set() for f in self.down_files}  # file -> PEERS 로 받은 보유자
        self.addr = {}                                        # pid -> (host, port)
        self.sel_lock = threading.Lock()
        self.inflight = Counter()
        self.fails = Counter()
        self.banned_until = {}

        self.srv = None
        self.srv_lock = threading.Lock()
        self.up_conns = set()
        self.up_lock = threading.Lock()
        self.active_uploads = 0

        self.bytes_up = 0
        self.bytes_down = 0
        self.stat_lock = threading.Lock()
        self.rate_hist = deque()            # (t, up, down) 화면용 실측 속도
        self.reported = None                # 보낸 DONE 종류
        self.force_refresh = threading.Event()
        self.threads = []
        self.saved = False

    # ------------------------------------------------------------ 로그 / 화면
    def ev(self, name, **kw):
        self.log.event(name, **kw)
        if name != "RECV":
            det = " ".join(f"{k}={v}" for k, v in kw.items())
            self.scr.log(f"{self.log.now():7.1f} {name:<8} {det}")

    # ------------------------------------------------------------ 시작
    def startup(self):
        me = self.me
        self.ev("START", env=self.env["name"], run=self.env["run"], policy=self.policy,
                up_kbps=me["up_kbps"], down_kbps=me["down_kbps"], rng_seed=me["rng_seed"],
                tracker=self.tracker_addr, workers=self.workers_n, per_peer_max=self.per_peer_max)
        os.makedirs(self.dir, exist_ok=True)
        # 원본 확인 (다르면 exit 1)
        for f in self.files.values():
            if not f.is_origin:
                continue
            if not os.path.exists(f.final_path) or os.path.getsize(f.final_path) != f.size:
                self.fatal(f"original file missing or wrong size: {f.final_path}")
            f.pf = PFile(f.final_path)
            t0 = time.time()
            got = sha256_of(f.pf, f.size)
            if got != f.sha:
                self.fatal(f"SHA-256 mismatch for original {f.final_path}")
            f.bitmap[:] = b"\x01" * f.n
            self.ev("HASH", file=f.name, ok=True, sec=round(time.time() - t0, 2), origin=True)
        self.open_listener()
        threading.Thread(target=self.listener, daemon=True, name="listener").start()
        # HELLO -> 원본 REGISTER -> LIST 에 env 의 파일이 모두 보일 때까지
        lost = float(self.T["tracker_lost_sec"])
        t0 = time.time()
        while True:
            try:
                r = self.link.request(f"HELLO {self.id}")
                if r != "OK":
                    self.fatal(f"tracker refused HELLO: {r}")
                break
            except (OSError, ConnClosed, ValueError):
                if time.time() - t0 > lost:
                    self.ev("ABORT", reason="tracker_lost", silent_sec=round(time.time() - t0, 1))
                    self.exit_code = 2
                    return False
                time.sleep(1.0)
        self.register_seeds()
        need = {f.name for f in self.files.values()}
        while True:
            try:
                first, rows = self.link.request("LIST", multi=True)
                seen = {r.split()[0] for r in rows if r.split()}
                if need <= seen:
                    break
            except (OSError, ConnClosed, ValueError):
                pass
            if time.time() - self.link.last_ok > lost:
                self.ev("ABORT", reason="tracker_lost", silent_sec=round(time.time() - self.link.last_ok, 1))
                self.exit_code = 2
                return False
            time.sleep(0.5)
        # 받을 파일마다 .part (전체 크기) · HAVE
        for f in self.down_files:
            f.pf = PFile(f.part_path, f.size)
            f.t_start = time.time()
            self.ev("GET", file=f.name, chunks=f.n)
            self.tracker_try(f"HAVE {self.id} {self.me['port']} {f.name}")
        if self.policy == "random":
            order = [(f.name, i) for f in self.down_files for i in range(f.n)]
            self.rng.shuffle(order)
            self.order = order
        self.refresh(force_peers=True)         # PEERS · BITMAP 첫 지도
        return True

    def fatal(self, msg):
        print(f"error: {msg}", file=sys.stderr)
        self.scr.close()
        self.log.event("EXIT", code=1, reason=msg)
        os._exit(1)

    def register_seeds(self, force=False):
        for f in self.files.values():
            if f.state == "SEED":
                self.tracker_try(f"REGISTER {self.id} {self.me['port']} {f.name} {f.size} {self.cs}", force)

    def tracker_try(self, line, force=False):
        try:
            return self.link.request(line, force=force)
        except (OSError, ConnClosed, ValueError):
            return None

    # ------------------------------------------------------------ listener / upload
    def open_listener(self):
        while True:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if os.name != "nt":
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((self.me["host"], int(self.me["port"])))
                s.listen(64)
                s.settimeout(0.2)
                with self.srv_lock:
                    self.srv = s
                return
            except OSError as e:
                if self.stopping.is_set():
                    return
                print(f"listen {self.me['port']} failed ({e}), retry", file=sys.stderr)
                time.sleep(0.3)

    def close_listener(self):
        with self.srv_lock:
            s, self.srv = self.srv, None
        if s is not None:
            try:
                s.close()
            except OSError:
                pass

    def listener(self):
        while not self.stopping.is_set():
            with self.srv_lock:
                s = self.srv
            if s is None or not self.online.is_set():
                time.sleep(0.2)
                continue
            try:
                c, _ = s.accept()
            except (socket.timeout, OSError):
                continue
            if not self.online.is_set() or self.stopping.is_set():
                c.close()
                continue
            c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=self.upload_handler, args=(c,), daemon=True).start()

    def upload_handler(self, sock):
        conn = Conn(sock)
        conn.settimeout(1.0)
        epoch = self.epoch
        with self.up_lock:
            self.up_conns.add(conn)
        try:
            while not self.stopping.is_set() and self.online.is_set() and epoch == self.epoch:
                try:
                    line = conn.read_line()
                except socket.timeout:
                    continue
                parts = line.split()
                if not parts:
                    continue
                if parts[0] == "BITMAP" and len(parts) == 2:
                    f = self.files.get(parts[1])
                    if f is None:
                        conn.send_line("ERR NOFILE")
                        continue
                    bits = f.bits()
                    conn.send_line(f"BITMAP {f.n} {bits}")
                elif parts[0] == "GET" and len(parts) == 3:
                    f = self.files.get(parts[1])
                    if f is None:
                        conn.send_line("ERR NOFILE")
                        continue
                    try:
                        i = int(parts[2])
                    except ValueError:
                        conn.send_line("ERR BADCMD")
                        continue
                    with f.lock:
                        ok = 0 <= i < f.n and f.bitmap[i] == 1     # bit 확인 뒤에 읽는다
                    if not ok:
                        conn.send_line("ERR NOCHUNK")
                        continue
                    n = f.clen(i)
                    data = f.pf.pread(n, i * self.cs)
                    with self.up_lock:
                        self.active_uploads += 1
                    try:
                        self.UP.take(len(data))                    # 업로드 제한기 (모든 handler 공유)
                        if not self.online.is_set() or epoch != self.epoch:
                            return                                 # crash 중이면 DATA 없이 끝
                        conn.send_bytes(f"DATA {i} {len(data)}\n".encode() + data)
                    finally:
                        with self.up_lock:
                            self.active_uploads -= 1
                    with f.lock:
                        f.sent += 1
                        f.sent_set.add(i)
                    with self.stat_lock:
                        self.bytes_up += len(data)
                else:
                    conn.send_line("ERR BADCMD")                   # 연결은 끊지 않는다
        except (ConnClosed, OSError, ValueError):
            pass
        finally:
            with self.up_lock:
                self.up_conns.discard(conn)
            conn.close()

    def drop_upload_conns(self):
        with self.up_lock:
            conns = list(self.up_conns)
        for c in conns:
            c.shutdown()

    # ------------------------------------------------------------ 소스 상태 (sel_lock)
    def is_banned(self, pid, now=None):
        now = now or time.time()
        return self.banned_until.get(pid, 0) > now

    def available_sources(self):
        """BAN 이 아니고 inflight < per_peer_max 인 상대 집합, BAN 아닌 상대 집합."""
        with self.remote_lock:
            known = list(self.addr)
        now = time.time()
        with self.sel_lock:
            unbanned = {p for p in known if not self.is_banned(p, now)}
            avail = {p for p in unbanned if self.inflight[p] < self.per_peer_max}
        return avail, unbanned

    def mark(self, src, ok, counted):
        with self.sel_lock:
            self.inflight[src] -= 1
            if ok:
                self.fails[src] = 0
                return
            if not counted:
                return
            self.fails[src] += 1
            if self.fails[src] >= self.fail_limit:
                self.fails[src] = 0
                self.banned_until[src] = time.time() + self.ban_sec
                ban = True
            else:
                ban = False
        if ban:
            self.ev("BAN", target=src, sec=self.ban_sec)

    # ------------------------------------------------------------ 청크 · 소스 고르기
    def remote_snapshot(self):
        with self.remote_lock:
            return {fn: dict(bms) for fn, bms in self.remote.items()}, dict(self.addr)

    def pick_next(self):
        """(f, i, src) 를 고르고 in_progress · inflight 를 표시한다. 없으면 None."""
        avail, unbanned = self.available_sources()
        if not avail:
            return None
        remote, _ = self.remote_snapshot()
        cands_files = [f for f in self.down_files if f.state in ("DOWN", "STALLED")]
        if not cands_files:
            return None
        snaps = {}
        for f in cands_files:
            with f.lock:
                snaps[f.name] = (bytes(f.bitmap), set(f.in_progress))
        if self.policy == "random":
            choice = None
            for fn, i in self.order:
                if fn not in snaps:
                    continue
                bm, inp = snaps[fn]
                if bm[i] or i in inp:
                    continue
                if any(b[i] == "1" for p, b in remote[fn].items() if p in avail):
                    choice = (self.files[fn], i)
                    break
        else:
            best, cands = None, []
            for f in cands_files:
                bm, inp = snaps[f.name]
                rb = [(p, b) for p, b in remote[f.name].items() if p in unbanned]
                if not rb:
                    continue
                av = [b for p, b in rb if p in avail]
                if not av:
                    continue
                for i in range(f.n):
                    if bm[i] or i in inp:
                        continue
                    r = 0
                    for _, b in rb:
                        if b[i] == "1":
                            r += 1
                    if r == 0 or not any(b[i] == "1" for b in av):
                        continue
                    if best is None or r < best:
                        best, cands = r, [(f, i)]
                    elif r == best:
                        cands.append((f, i))
            choice = None
            if cands:
                with self.rng_lock:
                    choice = self.rng.choice(cands)
        if choice is None:
            return None
        f, i = choice
        with f.lock:                                  # check-then-act 를 한 lock 안에서
            if f.bitmap[i] or i in f.in_progress:
                return None
            f.in_progress.add(i)
        src = self.pick_source(f, i, remote[f.name])
        if src is None:
            self.release(f, i)
            return None
        return f, i, src

    def pick_source(self, f, i, bms):
        holders = [p for p, b in bms.items() if b[i] == "1"]
        now = time.time()
        with self.sel_lock:
            ok = [p for p in holders if not self.is_banned(p, now) and self.inflight[p] < self.per_peer_max]
            if not ok:
                return None
            low = min(self.inflight[p] for p in ok)
            best = [p for p in ok if self.inflight[p] == low]
            with self.rng_lock:
                src = self.rng.choice(best)
            self.inflight[src] += 1
        return src

    def release(self, f, i):
        with f.lock:
            f.in_progress.discard(i)

    def forget_remote(self, pid, fname=None):
        with self.remote_lock:
            if fname:
                self.remote.get(fname, {}).pop(pid, None)
            else:
                for bms in self.remote.values():
                    bms.pop(pid, None)

    # ------------------------------------------------------------ 워커
    def worker(self):
        conns = {}
        while not self.stopping.is_set():
            if not self.online.is_set():
                for c in conns.values():
                    c.close()
                conns.clear()
                self.online.wait(0.5)
                continue
            pick = self.pick_next()
            if pick is None:
                time.sleep(0.05)
                continue
            f, i, src = pick
            epoch = self.epoch
            ok, counted = False, True
            n = f.clen(i)
            try:
                self.DOWN.take(n)                       # Peer 전체에 하나인 다운로드 제한기
                if not self.online.is_set() or epoch != self.epoch or self.stopping.is_set():
                    counted = False
                    continue
                data = self.fetch(conns, src, f, i, n)
                if not self.online.is_set() or epoch != self.epoch:
                    counted = False                     # crash 중 도착한 데이터는 버린다 (상대 탓 아님)
                    continue
                f.pf.pwrite(data, i * self.cs)          # ① 디스크 먼저
                with f.lock:                            # ② bit 나중
                    last = False
                    if not f.bitmap[i]:
                        f.bitmap[i] = 1
                        f.have += 1
                        f.sources[src] += 1
                        last = f.have == f.n and f.state in ("DOWN", "STALLED")
                with self.stat_lock:
                    self.bytes_down += n
                self.ev("RECV", file=f.name, idx=i, src=src)
                ok = True
                if last:
                    self.finish_file(f)
            except (OSError, ConnClosed, ValueError, IndexError) as e:
                c = conns.pop(src, None)
                if c:
                    c.close()
                if not self.online.is_set() or epoch != self.epoch or self.stopping.is_set():
                    counted = False
                if counted:
                    err = "ERR NOCHUNK" if isinstance(e, NoChunk) else errname(e)
                    self.ev("FAIL", file=f.name, idx=i, src=src, err=err)
            finally:
                self.release(f, i)                      # 빼먹으면 그 청크는 영원히 hang
                self.mark(src, ok, counted)
        for c in conns.values():
            c.close()

    def fetch(self, conns, src, f, i, n):
        """GET 한 번. 재사용하던 연결이 낡았으면(상대가 crash 에서 돌아옴) 새 연결로 한 번만 다시."""
        c = conns.get(src)
        reused = c is not None
        for attempt in (0, 1):
            if c is None:
                with self.remote_lock:
                    host, port = self.addr.get(src, (None, None))
                if host is None:
                    raise ConnClosed("unknown peer")
                try:
                    c = Conn.connect(host, port, NET_TIMEOUT, PEER_CONNECT_TIMEOUT)
                except socket.timeout:
                    # 같은 PC(loopback)에서 연결 timeout = 닫힌 포트 (Windows 는 refused 를 늦게 알려 준다)
                    raise ConnectionRefusedError("connect timeout on loopback")
                conns[src] = c
            try:
                c.send_line(f"GET {f.name} {i}")
                h = c.read_line().split()
            except (ConnClosed, ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
                conns.pop(src, None)
                c.close()
                if reused and attempt == 0:
                    self.ev("RETRY", file=f.name, idx=i, src=src, err=errname(e))
                    c, reused = None, False
                    continue
                raise
            if h and h[0] == "ERR":
                if len(h) > 1 and h[1] == "NOCHUNK":
                    self.forget_remote(src, f.name)     # 그 상대의 bitmap 이 낡았다 -> 버리고 다음 갱신 때 새로
                    raise NoChunk("ERR NOCHUNK")
                raise ValueError(" ".join(h))
            if len(h) != 3 or h[0] != "DATA" or int(h[1]) != i or int(h[2]) != n:
                raise ValueError(f"bad header {h}")
            return c.read_exact(n)
        raise ConnClosed("retry failed")

    def finish_file(self, f):
        """마지막 청크를 쓴 worker 만. SHA-256 -> rename -> REGISTER (또는 CORRUPT)."""
        t0 = time.time()
        got = sha256_of(f.pf, f.size)
        ok = got == f.sha
        self.ev("HASH", file=f.name, ok=ok, sec=round(time.time() - t0, 2))
        if ok:
            f.pf.rename(f.final_path)
            with f.lock:
                f.state = "SEED"
                f.zero_since = None
                f.hash_fail = 0
                srcs = dict(f.sources)
            self.ev("COMPLETE", file=f.name, sec=round(time.time() - f.t_start, 1), sources=srcs)
            if self.online.is_set():
                self.tracker_try(f"REGISTER {self.id} {self.me['port']} {f.name} {f.size} {self.cs}")
            return
        with f.lock:
            f.hash_fail += 1
            tries = f.hash_fail
            give_up = tries >= 2
            if give_up:
                f.state = "FAILED"
            else:
                f.bitmap[:] = bytes(f.n)               # 처음부터 다시
                f.have = 0
                f.sources.clear()
        if give_up:
            f.pf.rename(f.final_path + ".corrupt")
        self.ev("CORRUPT", file=f.name, action="give_up" if give_up else "redownload", tries=tries)

    # ------------------------------------------------------------ control (1초)
    def total_have(self):
        return sum(f.count() for f in self.down_files)

    def control(self):
        hb_every = float(self.T["heartbeat_sec"])
        peers_every = float(self.T["peers_refresh_sec"])
        period = float(self.T["bitmap_refresh_sec"])
        lost = float(self.T["tracker_lost_sec"])
        next_hb = 0.0
        next_peers = 0.0
        while not self.stopping.is_set():
            loop_start = time.time()
            if not self.online.is_set():
                time.sleep(0.1)
                continue
            epoch = self.epoch
            now = time.time()
            # 1) HEARTBEAT (CMD 는 이 응답으로만 온다)
            if now >= next_hb:
                next_hb = now + hb_every
                try:
                    r = self.link.request(f"HEARTBEAT {self.id} {self.total_have()}")
                except (OSError, ConnClosed, ValueError):
                    r = None
                if r and r.startswith("CMD DISCONNECT"):
                    try:
                        sec = float(r.split()[2])
                    except (IndexError, ValueError):
                        sec = 0.0
                    self.go_offline()                     # 지금 이 순간부터 아무것도 보내지 않는다
                    self.ev("CRASH", sec=sec)
                    threading.Thread(target=self.virtual_disconnect, args=(sec,), daemon=True).start()
                    continue
                if r == "CMD DESTROY":
                    self.virtual_destroy()
            if epoch != self.epoch or not self.online.is_set():
                continue
            # 2) PEERS (3초) · BITMAP (1초) -> rarity
            force = self.force_refresh.is_set()
            if force:
                self.force_refresh.clear()
            do_peers = force or time.time() >= next_peers
            if do_peers:
                next_peers = time.time() + peers_every
            self.refresh(force_peers=do_peers)
            if epoch != self.epoch or not self.online.is_set():
                continue
            # 3) REPORT (관찰용, 파일마다)
            for f in self.files.values():
                with f.lock:
                    sent, distinct = f.sent, len(f.sent_set)
                    bits = f.bitmap.translate(BITS_TABLE).decode()
                self.tracker_try(f"REPORT {self.id} {f.name} {sent} {distinct} {bits}")
            # 4) STALLED 판정 -> DONE / UNDONE
            self.judge_stall()
            # 5) STATE
            try:
                st = self.link.request("STATE")
            except (OSError, ConnClosed, ValueError):
                st = None
            if st and epoch == self.epoch:
                p = st.split()
                if len(p) >= 4 and p[3] == "ALLDONE":
                    self.ev("ALLDONE", done=int(p[1]), alive=int(p[2]))
                    self.exit_code = 0
                    self.stopping.set()
                    break
                if len(p) >= 4 and p[3] == "ABORT":
                    self.ev("ABORT", reason=p[4] if len(p) > 4 else "unknown")
                    self.exit_code = 2
                    self.stopping.set()
                    break
            # 6) tracker_lost (가상 crash 시간 제외: rejoin 때 last_ok 를 다시 잰다)
            if self.online.is_set() and time.time() - self.link.last_ok > lost:
                self.ev("ABORT", reason="tracker_lost", silent_sec=round(time.time() - self.link.last_ok, 1))
                self.exit_code = 2
                self.stopping.set()
                break
            time.sleep(max(0.0, period - (time.time() - loop_start)))

    def refresh(self, force_peers=False):
        """보유자 목록(PEERS)과 각 보유자의 BITMAP 을 갱신한다. 실패한 상대는 캐시에서 즉시 뺀다."""
        epoch = self.epoch
        failed = set()
        targets = [f for f in self.down_files if f.state not in ("SEED", "FAILED")]
        for f in targets:
            if force_peers:
                try:
                    first, rows = self.link.request(f"PEERS {f.name}", multi=True)
                except (OSError, ConnClosed, ValueError):
                    continue
                if not first.startswith("PEERS"):
                    continue
                holders = {}
                for row in rows:
                    p = row.split()
                    if len(p) == 3 and p[0] != self.id:          # 자기 자신은 건너뛴다
                        holders[p[0]] = (p[1], int(p[2]))
                with self.remote_lock:
                    self.addr.update(holders)
                    bms = self.remote[f.name]
                    for pid in list(bms):
                        if pid not in holders:
                            bms.pop(pid)                          # DEAD · BYE 한 Peer 는 목록에서 빠진다
                    self.holders[f.name] = set(holders)
            with self.remote_lock:
                holders = set(self.holders[f.name])
            for pid in sorted(holders):
                if epoch != self.epoch or not self.online.is_set():
                    return
                bits = None if pid in failed else self.fetch_bitmap(pid, f)
                if bits is None:
                    failed.add(pid)                    # 이번 바퀴에는 그 상대에게 다시 묻지 않는다
                with self.remote_lock:
                    if bits is None:
                        self.remote[f.name].pop(pid, None)
                    else:
                        self.remote[f.name][pid] = bits

    def fetch_bitmap(self, pid, f):
        with self.remote_lock:
            host, port = self.addr.get(pid, (None, None))
        if host is None:
            return None
        if not hasattr(self, "_bm_conns"):
            self._bm_conns = {}
        c = self._bm_conns.get(pid)
        for attempt in (0, 1):
            fresh = c is None
            try:
                if c is None:
                    c = Conn.connect(host, port, BITMAP_TIMEOUT, PEER_CONNECT_TIMEOUT)
                    self._bm_conns[pid] = c
                c.send_line(f"BITMAP {f.name}")
                p = c.read_line().split()
                if len(p) == 3 and p[0] == "BITMAP" and int(p[1]) == f.n and len(p[2]) == f.n:
                    return p[2]
                return None
            except (OSError, ConnClosed, ValueError):
                self._bm_conns.pop(pid, None)
                if c is not None:
                    c.close()
                c = None
                if attempt == 0 and not fresh:
                    continue                       # 낡은 연결이었을 수 있다 -> 새 연결로 한 번
                return None
        return None

    def rarity(self, f):
        """청크별 rarity = 그 청크를 가진 (캐시에 있고 BAN 아닌) 다른 Peer 수."""
        with self.remote_lock:
            bms = dict(self.remote.get(f.name, {}))
        now = time.time()
        with self.sel_lock:
            bms = {p: b for p, b in bms.items() if not self.is_banned(p, now)}
        r = [0] * f.n
        for b in bms.values():
            for i, ch in enumerate(b):
                if ch == "1":
                    r[i] += 1
        return r

    def judge_stall(self):
        now = time.time()
        stall_sec = float(self.T["stall_sec"])
        for f in self.down_files:
            if f.state in ("SEED", "FAILED"):
                continue
            r = self.rarity(f)
            with f.lock:
                missing = [i for i in range(f.n) if not f.bitmap[i]]
            zero = bool(missing) and all(r[i] == 0 for i in missing)   # 남은 것 '전부' 가 어디에도 없다
            ev = None
            with f.lock:
                if f.state in ("SEED", "FAILED"):
                    continue
                if zero:
                    f.zero_since = f.zero_since or now
                    if f.state != "STALLED" and now - f.zero_since >= stall_sec:
                        f.state = "STALLED"
                        ev = ("STALLED", len(missing))
                else:
                    f.zero_since = None
                    if f.state == "STALLED":
                        f.state = "DOWN"
                        ev = ("UNSTALLED", None)
            if ev and ev[0] == "STALLED":
                self.ev("STALLED", file=f.name, lost=ev[1])
            elif ev:
                self.ev("UNSTALLED", file=f.name)
        states = {f.name: f.state for f in self.down_files}
        if all(s in ("SEED", "STALLED", "FAILED") for s in states.values()):
            kind = "FULL" if all(s == "SEED" for s in states.values()) else "PARTIAL"
            details = []
            for f in self.down_files:
                if f.state == "STALLED":
                    details.append(f"{f.name}:{f.n - f.count()}")
                elif f.state == "FAILED":
                    details.append(f"{f.name}:FAILED")
            detail = " ".join(details)
            key = (kind, detail)
            if self.reported != key:
                line = f"DONE {self.id} {kind}" + (f" {detail}" if detail else "")
                if self.tracker_try(line) == "OK":
                    self.reported = key
                    self.ev("DONE", kind=kind, detail=detail)
        elif self.reported:
            if self.tracker_try(f"UNDONE {self.id}") == "OK":
                self.reported = None
                self.ev("UNDONE")

    # ------------------------------------------------------------ 가상 crash / destroy
    def go_offline(self):
        with self.offline_lock:
            self.online.clear()                  # 모든 스레드에 알림 (워커 · control 정지, heartbeat 중단)
            self.epoch += 1
            self.close_listener()                # 새 연결 -> connection refused
            self.drop_upload_conns()             # 업로드 연결은 응답 없이 shutdown
            self.link.drop()                     # Tracker 쪽으로도 침묵
            for c in list(getattr(self, "_bm_conns", {}).values()):
                c.close()
            self._bm_conns = {}

    def virtual_disconnect(self, sec):
        end = time.time() + sec
        while time.time() < end:                 # 침묵 중에는 지시를 못 받음
            if self.stopping.is_set():
                return
            time.sleep(min(0.2, max(0.0, end - time.time())))
        with self.sel_lock:                      # 돌아오면 소스 상태는 새로
            self.fails.clear()
            self.banned_until.clear()
        for f in self.down_files:
            with f.lock:
                f.zero_since = None              # 내가 crash 에서 돌아오면 zero_since 를 지우고 새로 센다
        self.open_listener()                     # listen -> HELLO -> 원본·완성 REGISTER · HAVE
        self.link.last_ok = time.time()
        try:
            self.link.request(f"HELLO {self.id}", force=True)
        except (OSError, ConnClosed, ValueError):
            pass
        self.register_seeds(force=True)
        for f in self.down_files:
            if f.state != "SEED":
                self.tracker_try(f"HAVE {self.id} {self.me['port']} {f.name}", force=True)
        self.force_refresh.set()                 # PEERS · BITMAP 즉시 강제 갱신 (control 이 바로 다음 바퀴에)
        self.link.last_ok = time.time()
        self.online.set()                        # 받던 곳부터 이어받기 (디스크 · bitmap 그대로)
        self.ev("REJOIN")

    def virtual_destroy(self):
        self.go_offline()
        self.ev("DESTROY")
        self.save_results()
        self.log.event("EXIT", code=3, up_mb=round(self.bytes_up / MB, 1), down_mb=round(self.bytes_down / MB, 1))
        self.scr.close()
        os._exit(3)                              # BYE 를 보내지 않는다

    # ------------------------------------------------------------ 결과 저장
    def save_results(self):
        if self.saved:
            return
        self.saved = True
        for f in self.files.values():
            with f.lock:
                bits = f.bitmap.translate(BITS_TABLE).decode()
                state = f.state
            with open(f.final_path + ".bitmap", "w", encoding="ascii", newline="") as fp:
                fp.write(bits)
            if state == "SEED":
                continue
            if f.pf is not None:
                if state == "FAILED":
                    pass                         # 이미 .corrupt
                else:
                    try:
                        f.pf.rename(f.final_path + ".incomplete")
                    except OSError as e:
                        print(f"rename failed: {e}", file=sys.stderr)
            with open(f.final_path + ".lost", "w", encoding="ascii", newline="") as fp:
                fp.write(ranges(bits))

    # ------------------------------------------------------------ 화면
    def screen(self):
        while not self.stopping.is_set():
            now = time.time()
            with self.stat_lock:
                self.rate_hist.append((now, self.bytes_up, self.bytes_down))
            while self.rate_hist and now - self.rate_hist[0][0] > 5.0:
                self.rate_hist.popleft()
            if self.scr.tty:
                self.scr.status(self.status_lines())
            time.sleep(1.0)

    def status_lines(self):
        t = self.log.now()
        h = self.rate_hist
        if len(h) >= 2 and h[-1][0] > h[0][0]:
            dt = h[-1][0] - h[0][0]
            up = (h[-1][1] - h[0][1]) / dt / 1024
            dn = (h[-1][2] - h[0][2]) / dt / 1024
        else:
            up = dn = 0
        st = "ONLINE" if self.online.is_set() else "CRASHED"
        out = [f"+-- {self.id} t={t:6.1f}s {st} policy={self.policy} | up {up:.0f}/{self.me['up_kbps']} "
               f"| down {dn:.0f}/{self.me['down_kbps']} KB/s"]
        for f in self.files.values():
            with f.lock:
                bm = bytes(f.bitmap)
                state = f.state
            r = self.rarity(f) if state == "STALLED" else None
            cells = []
            for k in range(40):
                a, b = k * f.n // 40, max(k * f.n // 40 + 1, (k + 1) * f.n // 40)
                seg = bm[a:b]
                cnt = seg.count(1)
                if r is not None and any(not bm[i] and r[i] == 0 for i in range(a, b)):
                    cells.append("x")
                elif cnt == len(seg):
                    cells.append("#")
                elif cnt:
                    cells.append("+")
                else:
                    cells.append(".")
            c = bm.count(1)
            out.append(f"| {f.name:<12}[{''.join(cells)}] {100 * c / f.n:5.1f}% {c:>4}/{f.n:<4} {state}")
        out.append(f"| tracker {self.tracker_addr}  listen {self.me['host']}:{self.me['port']}")
        out.append("+" + "-" * 72)
        return out

    # ------------------------------------------------------------ 실행 · 종료
    def run(self):
        if not self.startup():
            self.save_results()
            self.finish_exit()
        for k in range(self.workers_n):
            t = threading.Thread(target=self.worker, daemon=True, name=f"worker{k}")
            t.start()
            self.threads.append(t)
        ctl = threading.Thread(target=self.control, daemon=True, name="control")
        ctl.start()
        self.threads.append(ctl)
        threading.Thread(target=self.screen, daemon=True, name="screen").start()
        try:
            while not self.stopping.wait(0.5):
                pass
        except KeyboardInterrupt:
            self.exit_code = 2
            self.stopping.set()
        # 정상 종료: 새 연결 거부 -> 진행 중 업로드 마무리(<=3초) -> join -> 결과 저장 -> BYE -> EXIT
        self.close_listener()
        t_end = time.time() + UPLOAD_DRAIN_SEC
        while time.time() < t_end:
            with self.up_lock:
                if self.active_uploads == 0:
                    break
            time.sleep(0.05)
        self.drop_upload_conns()
        for t in self.threads:
            t.join(timeout=NET_TIMEOUT + 1)
        self.save_results()
        self.finish_exit()

    def finish_exit(self):
        if self.online.is_set():
            self.tracker_try(f"BYE {self.id}")
        self.link.drop()
        for f in self.files.values():
            if f.pf is not None:
                f.pf.close()
        self.log.event("EXIT", code=self.exit_code, up_mb=round(self.bytes_up / MB, 1),
                       down_mb=round(self.bytes_down / MB, 1))
        self.scr.close()
        self.log.close()
        sys.stdout.flush()
        os._exit(self.exit_code)


def main():
    args, tip, tport = parse_args(sys.argv[1:])
    try:
        env, me = load_env(args["env"], args["id"], args["policy"])
    except EnvError as e:
        usage_exit(str(e), USAGE)
    Peer(env, me, tip, tport).run()


if __name__ == "__main__":
    main()
