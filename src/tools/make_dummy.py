#!/usr/bin/env python3
"""HW6 더미 파일 생성기.
참고: 강의 자료 HW6explanation.pdf PPT §3.2 · p.20

    python tools/make_dummy.py 10 20 30            -> data/share10.bin, share20.bin, share30.bin
    python tools/make_dummy.py 3 5 7 --out data    (DEV 용)

크기마다 seed가 고정되어 있어 누가 언제 만들어도 바이트 단위로 같은 파일이 나온다.
원본 해시는 <out>/original_sha256.txt 에 기록된다.
"""
import argparse
import hashlib
import os
import random

MB = 1 << 20


def block_stream(mb):
    """share<mb>.bin 의 내용을 1 MB씩 돌려준다 (verify.py도 이 함수를 쓴다)."""
    rng = random.Random(mb)
    for _ in range(mb):
        yield rng.randbytes(MB)


def make(mb, out_dir):
    """share<mb>.bin 을 만들고 <out_dir>/original_sha256.txt 에 해시를 기록(같은 파일 줄은 교체)."""
    name = f"share{mb}.bin"
    path = os.path.join(out_dir, name)
    h = hashlib.sha256()
    with open(path, "wb") as f:
        for block in block_stream(mb):
            f.write(block)
            h.update(block)
    digest = h.hexdigest()
    rec = os.path.join(out_dir, "original_sha256.txt")
    old = []
    if os.path.exists(rec):
        with open(rec, encoding="utf-8") as f:
            old = [ln for ln in f.read().splitlines() if ln and not ln.endswith("  " + name)]
    with open(rec, "w", encoding="utf-8") as f:
        f.write("\n".join(old + [f"{digest}  {name}"]) + "\n")
    return path, digest


def main():
    ap = argparse.ArgumentParser(description="HW6 dummy file generator")
    ap.add_argument("sizes", type=int, nargs="+", help="크기(MB), 예: 10 20 30")
    ap.add_argument("--out", default="data", help="저장 폴더 (기본: data)")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    for mb in a.sizes:
        path, digest = make(mb, a.out)
        print(f"{path}  {digest}")


if __name__ == "__main__":
    main()
