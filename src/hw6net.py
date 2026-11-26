"""hw6net.py - Tracker 와 Peer 가 함께 쓰는 코드 (7조 HW6).

  * load_env   : env.json 읽기 (키트 lib/env_example.py 의 load_env 와 같은 규칙)
  * Conn       : TCP 연결 하나 + 수신 버퍼 하나 (read_line / read_exact / send_line)
  * ConnClosed : 상대가 연결을 닫았을 때

메시지는 UTF-8 텍스트 한 줄(\n)이고, 바이너리는 DATA 응답의 본문뿐이다.
TCP 는 메시지 경계를 지켜 주지 않으므로 recv() 로 받은 바이트를 rbuf 에 모아 두고
read_line 은 \n 까지, read_exact 는 정확히 n 바이트를 잘라 준다.
"""
import json
import math
import socket
import sys


class ConnClosed(Exception):
    """상대가 연결을 닫음 (recv 가 b'' 를 돌려줌)."""


class EnvError(Exception):
    """env.json 형식 오류 / 인자 오류 -> exit 1."""


def load_env(path, my_id=None, cli_policy=None):
    try:
        with open(path, encoding="utf-8") as f:
            env = json.load(f)
    except (OSError, ValueError) as e:
        raise EnvError(f"cannot read env {path}: {e}")
    if env.get("version") != 1:
        raise EnvError(f"unsupported env version: {env.get('version')}")
    cs = int(env["chunk_size"])
    for f in env["files"]:
        f["size"] = int(f["size"])
        f["nchunks"] = math.ceil(f["size"] / cs)      # 올림 (마지막 청크가 짧을 수 있다)
    if my_id is None:
        return env, None
    me = next((p for p in env["peers"] if p["id"] == my_id), None)
    if me is None:
        raise EnvError(f"{my_id} is not listed in {path}")
    dl = env["download"]
    policy = cli_policy or dl.get("policy")           # 명령행이 우선
    if policy not in ("random", "rarest"):
        raise EnvError("policy is null in env: give --policy random|rarest")
    me["policy"] = policy
    me["number"] = env["peers"].index(me) + 1
    digits = "".join(ch for ch in my_id if ch.isdigit())
    me["rng_seed"] = int(dl["policy_seed"]) + (int(digits) if digits else me["number"])
    me["up_Bps"] = int(me["up_kbps"]) * 1024           # 1 KB = 1024 B
    me["down_Bps"] = int(me["down_kbps"]) * 1024
    me["my_files"] = [f["name"] for f in env["files"] if f["origin"] == my_id]
    return env, me


def chunk_len(size, cs, idx):
    return min(cs, size - idx * cs)


def ranges(bits):
    """'110100' 같은 bitmap 에서 0 인 번호를 '2,4-5' 형식으로 (연속 구간은 a-b)."""
    out, i, n = [], 0, len(bits)
    while i < n:
        if bits[i] == 1 or bits[i] == "1":
            i += 1
            continue
        j = i
        while j + 1 < n and not (bits[j + 1] == 1 or bits[j + 1] == "1"):
            j += 1
        out.append(f"{i}" if i == j else f"{i}-{j}")
        i = j + 1
    return ",".join(out)


class Conn:
    """연결 하나 = 소켓 하나 + 수신 버퍼 하나. 그 연결을 만든 Thread 하나만 쓴다(소유권)."""

    MAX_LINE = 1 << 16

    def __init__(self, sock):
        self.sock = sock
        self.rbuf = bytearray()

    @classmethod
    def connect(cls, host, port, timeout=5.0, connect_timeout=None):
        s = socket.create_connection((host, int(port)), timeout=connect_timeout or timeout)
        s.settimeout(timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return cls(s)

    def settimeout(self, t):
        self.sock.settimeout(t)

    def _fill(self):
        d = self.sock.recv(65536)
        if not d:
            raise ConnClosed("closed")
        self.rbuf += d

    def read_line(self):
        while True:
            k = self.rbuf.find(b"\n")
            if k >= 0:
                line = bytes(self.rbuf[:k])
                del self.rbuf[:k + 1]
                return line.decode("utf-8", errors="replace").rstrip("\r")
            if len(self.rbuf) > self.MAX_LINE:
                raise ValueError("line too long")
            self._fill()

    def read_exact(self, n):
        while len(self.rbuf) < n:
            self._fill()
        out = bytes(self.rbuf[:n])
        del self.rbuf[:n]
        return out

    def send_line(self, s):
        self.sock.sendall((s + "\n").encode("utf-8"))

    def send_bytes(self, b):
        self.sock.sendall(b)

    def shutdown(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def close(self):
        self.shutdown()
        try:
            self.sock.close()
        except OSError:
            pass


def errname(e):
    """로그용 실패 이름 (ConnClosed · ConnectionRefused · timeout · ...)."""
    if isinstance(e, ConnClosed):
        return "ConnClosed"
    if isinstance(e, ConnectionRefusedError):
        return "ConnectionRefused"
    if isinstance(e, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
        return "ConnClosed"
    if isinstance(e, socket.timeout):
        return "timeout"
    return type(e).__name__


def usage_exit(msg, usage):
    print(f"error: {msg}", file=sys.stderr)
    print(usage, file=sys.stderr)
    sys.exit(1)
