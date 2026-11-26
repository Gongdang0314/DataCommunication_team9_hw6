#!/usr/bin/env python3
"""명령 한 줄을 보내고 응답을 보여 주는 디버깅 도구 (nc 대용).
참고: 강의 자료 HW6explanation.pdf PPT §6.3 · p.53, §9.4 · p.78

    python tools/probe.py 127.0.0.1 6000 "LIST"
    python tools/probe.py 127.0.0.1 7003 "BITMAP share30.bin"
    python tools/probe.py 127.0.0.1 7003 "GET share30.bin 5"      # DATA 본문은 길이와 앞부분만 표시

첫 응답은 최대 5초 기다리고(속도 제한 때문에 GET 응답은 늦게 올 수 있다), 그 뒤 0.7초 동안
더 오는 것이 없으면 끝낸다. DATA 헤더를 받으면 본문 길이만큼 읽고 요약한다.
"""
import socket
import sys
import time


def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)
    host, port, line = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    s = socket.create_connection((host, port), timeout=3)
    s.sendall((line + "\n").encode())
    s.settimeout(5.0)
    buf = b""
    t0 = time.time()
    while True:
        try:
            d = s.recv(65536)
        except socket.timeout:
            break
        s.settimeout(0.7)
        if not d:
            break
        buf += d
        if buf.startswith(b"DATA ") and b"\n" in buf:
            head, body = buf.split(b"\n", 1)
            need = int(head.split()[2])
            s.settimeout(3)
            while len(body) < need:
                d = s.recv(need - len(body))
                if not d:
                    break
                body += d
            print(head.decode())
            print(f"[{len(body)} bytes of {need}] first 16: {body[:16].hex()}")
            print(f"({(time.time() - t0) * 1000:.0f} ms)")
            return
    text = buf.decode(errors="replace")
    for ln in text.splitlines():
        print(ln if len(ln) <= 160 else ln[:160] + f"... ({len(ln)} chars)")
    if not text:
        print("(no response)")


if __name__ == "__main__":
    main()
