"""tui.py - 터미널 화면 분할: 위쪽 N줄은 고정 상태판(ASCII 진행 막대), 아래는 로그가 흘러내림.  HW6 kit.
참고: 강의 자료 HW6explanation.pdf PPT §18.5~§18.7 · p.175~177

    scr = SplitScreen(top=8)          # 위 8줄 = 상태판
    scr.log("12.3  RECV  share30.bin #311 <- P3")     # 아래 영역에 한 줄 추가 (위로 스크롤)
    scr.status([...8줄 이하 문자열...])               # 상태판 다시 그리기 (제자리)
    scr.close()                                       # 끝: 스크롤 영역 해제

원리 (ANSI/VT100 제어 문자)
  ESC[t;br   스크롤 영역(DECSTBM)을 t~b 행으로 제한 -> 그 아래에서 줄바꿈해도 위 N줄은 안 움직인다
  ESC7 ESC8  커서 위치 저장/복원
  ESC[r;cH   커서 이동,  ESC[K  줄 끝까지 지우기
- stdout 이 터미널이 아니면(파일/파이프로 리다이렉트) 상태판은 그리지 않고 로그만 그냥 출력한다.
- Windows 10 이상 콘솔/Windows Terminal 도 VT 모드를 켜면 같은 방식으로 동작한다.
- 여러 Thread가 동시에 부르므로 출력은 lock 하나로 묶는다 (한 줄이 섞이지 않도록).
- 기록용 로그(logs/<id>.jsonl)는 evlog 가 따로 쓴다. 이 화면 출력은 기록하지 않는다.
"""
import os
import shutil
import sys
import threading


class SplitScreen:
    def __init__(self, top=8, stream=None):
        self.out = stream or sys.stdout
        self.top = top
        self.lock = threading.Lock()
        self.tty = self.out.isatty()
        self.size = None
        if self.tty:
            if os.name == "nt":
                os.system("")                      # Windows: VT(ANSI) 처리 켜기
            self._setup()

    def _setup(self):
        cols, rows = shutil.get_terminal_size((100, 30))
        self.size = (cols, rows)
        self._w("\x1b[2J")                          # 화면 지우기
        self._w(f"\x1b[{self.top + 2};{rows}r")     # 스크롤 영역 = 상태판+구분선 아래 ~ 마지막 행
        self._w(f"\x1b[{self.top + 1};1H" + "-" * (cols - 1))
        self._w(f"\x1b[{rows};1H")                  # 커서를 맨 아래로
        self.out.flush()

    def _w(self, s):
        self.out.write(s)

    def _check_resize(self):
        if shutil.get_terminal_size((100, 30)) != self.size:
            self._setup()

    def log(self, line):
        with self.lock:
            if not self.tty:
                self.out.write(line + "\n")
                self.out.flush()
                return
            self._check_resize()
            cols, rows = self.size
            # 맨 아래 행에서 줄바꿈 -> 스크롤 영역만 한 줄 위로 밀리고, 빈 맨 아래 행에 새 줄을 쓴다
            self._w(f"\x1b[{rows};1H\n" + line[:cols - 1] + "\x1b[K")
            self.out.flush()

    def status(self, lines):
        if not self.tty:
            return                                  # 리다이렉트 중에는 상태판 생략
        with self.lock:
            self._check_resize()
            cols, _ = self.size
            buf = ["\x1b7"]                         # 커서 저장
            for i in range(self.top):
                s = lines[i] if i < len(lines) else ""
                buf.append(f"\x1b[{i + 1};1H" + s[:cols - 1] + "\x1b[K")
            buf.append("\x1b8")                     # 커서 복원 (로그 위치로)
            self._w("".join(buf))
            self.out.flush()

    def close(self):
        with self.lock:
            if self.tty:
                self._w("\x1b[r")                   # 스크롤 영역 해제
                self._w(f"\x1b[{self.size[1]};1H\n")
                self.out.flush()


if __name__ == "__main__":                          # 데모: python tui.py
    import random
    import time
    scr = SplitScreen(top=6)
    have = {"share10.bin": 0, "share20.bin": 0, "share30.bin": 0}
    n = {"share10.bin": 160, "share20.bin": 320, "share30.bin": 480}
    t0 = time.time()
    while any(have[f] < n[f] for f in have):
        f = random.choice([f for f in have if have[f] < n[f]])
        have[f] = min(n[f], have[f] + random.randint(5, 25))
        scr.log(f"{time.time() - t0:7.1f}  RECV     file={f} idx={random.randrange(n[f])} src=P{random.randint(1, 3)}")
        bars = [f"+-- P4 t={time.time() - t0:6.1f}s ONLINE  policy=random"]
        for k in have:
            fill = 40 * have[k] // n[k]
            bars.append(f"|  {k:<12}[{'#' * fill}{'.' * (40 - fill)}] {100 * have[k] / n[k]:5.1f}%")
        scr.status(bars)
        time.sleep(0.03)
    scr.log("done")
    scr.close()
