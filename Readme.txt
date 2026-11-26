================================================================================
HW#6 - Mini-Torrent (Tracker 가 조율하는 4-Peer 하이브리드 P2P 파일 공유)
9조
================================================================================

문서 수정일: 2026-11-27 KST


1. 조원 정보
================================================================================

  김대영 (20223087)
    - Tracker 구현 (scheduler · observer · report.json), EC2 배포, 원격 실행 스크립트

  김민재 (20223089)
    - Peer 구현 (업로드 서버 · 다운로드 워커 · 속도 제한기 · 청크 선택 · STALLED)

  황태웅 (20223152)
    - 가상 crash/destroy · 종료 처리, 검증 도구 실행과 반복 테스트, 로그 명세, 문서 정리, 시연 영상


2. 프로그램 개요
================================================================================

  - Python 표준 라이브러리 + 키트 lib(evlog.py · tui.py)만 사용하는 TCP 기반 하이브리드 P2P
  - Tracker 1개: AWS EC2(서울)에서 0.0.0.0:9500 으로 listen. 명단 · 생존 판정 · 지시(CMD) · 관찰만 한다.
    청크 데이터는 절대 다루지 않는다. Peer 에게 먼저 연결하지 않는다(지시는 HEARTBEAT 응답으로만).
  - Peer 4개(P1 ~ P4): 내 PC(Windows 11)의 독립 프로세스. 서버이자 클라이언트.
    업로드 서버(연결당 Thread)와 다운로드 워커 8개가 동시에 돌고, 받은 청크는 디스크에 쓰고 bit 를 1 로
    바꾸는 즉시 다른 Peer 에게 내준다(부분 재공유). Peer 끼리는 env 의 host(127.0.0.1)로 직접 연결.
  - 모든 실험 조건(포트 · 속도 · 시간 상수 · crash 일정 · destroy 계획 · 정책 · 피어 이름)은 env.json 에서 읽는다.
    코드에 있는 숫자는 env 에 없는 구현 상수(scheduler 0.5초, GET/연결 timeout 5초, ABORT 뒤 BYE 대기 15초,
    업로드 마무리 3초, Peer 끼리 연결 timeout 1초, BITMAP 요청 timeout 2초)뿐이다.

  프로그램 구성요소 (src/)

    tracker.py         Tracker (main · handler · scheduler · observer · screen)
    peer.py            Peer (main · listener · upload handler · worker × N · control · screen)
    hw6net.py          공통: load_env, Conn(read_line · read_exact · send_line), ranges(.lost 형식)
    lib/evlog.py       키트 이벤트 로거 (그대로 사용)
    lib/tui.py         키트 화면 분할(상태판 고정 + 로그 흐름) (그대로 사용)
    envs/              키트 시나리오 6개 (수정하지 않음)
    tools/             키트 도구 make_dummy · reset_dirs · verify · probe (수정하지 않음)
    run_remote.py      정식 실행 1회: reset_dirs -> VM tracker 시작 -> peer 4개 -> 대기 -> scp -> verify -> runs/ 보관
    run_local.py       (개발용) tracker 도 내 PC 에서 실행 -> verify
    demo_run.ps1       (시연용) Windows Terminal 에 tracker 화면(ssh) + peer 4개 ASCII 화면(2x2) 띄우기
    demo_collect.py    (시연용) 수동 실행 뒤 tracker 로그 · report.json 가져오기 -> verify -> 보관
    analyze.py         결과 분석 (평균 속도 · 출처 분포 · CRASH/BAN/FAIL 수 · 9-4 정합성)


3. 실행 환경 · 빌드 · 실행 방법
================================================================================

실행 환경

  Tracker:    AWS EC2 t3.micro, ap-northeast-2c (서울), Ubuntu 24.04.5 LTS, Python 3.12.3
  Peer:       로컬 PC Windows 11, Python 3.13 (Peer 4개 모두 같은 PC, 서로 127.0.0.1:7001~7004)
  포트:       Tracker 9500 (env 의 6000 대신 실행 인자로 지정 — env 파일은 고치지 않음),
              Peer 7001 ~ 7004 (env 그대로)
  왕복 지연:  로컬 PC <-> EC2(서울) TCP 약 20 ms
  빌드:       Python 이므로 컴파일 단계 없음 (pip 설치 불필요)

실행 폴더

  Peer 는 "실행 폴더"를 현재 폴더로 실행한다. 실행 폴더에는 envs/ 와 tools/ 가 있고, 실행 중에
  data/(원본) · p1~p4/ · logs/ · report.json 이 생긴다 (src/ 를 그대로 실행 폴더로 써도 된다).
  우리는 OneDrive 동기화 폴더 밖의 C:\Users\<user>\hw6work 를 실행 폴더로 썼다
  (OneDrive 가 폴더를 잡고 있으면 reset_dirs 의 폴더 삭제가 실패하고, 실행마다 240 MB 가 동기화되기 때문).

한 Run 을 직접 실행하는 방법 (과제명세서 8-3 과 같은 순서, 실행 폴더에서)

  ① 폴더 초기화 (원본 생성 · 배치)
       python tools/reset_dirs.py --env envs/A.json
  ② VM 에서 tracker 먼저 (VM 의 ~/hw6 에 tracker.py · hw6net.py · lib/ · envs/ 를 둔다)
       vm$ cd ~/hw6 && python3 tracker.py --env envs/A.json 9500
  ③ 내 PC 에서 peer 4개 (각각 다른 터미널, 또는 백그라운드). 마지막 두 인자 = VM 공인 IP · PORT
       python <src>/peer.py --env envs/A.json --id P1 13.125.21.50 9500
       python <src>/peer.py --env envs/A.json --id P2 13.125.21.50 9500
       python <src>/peer.py --env envs/A.json --id P3 13.125.21.50 9500
       python <src>/peer.py --env envs/A.json --id P4 13.125.21.50 9500
       (C3 는 4개 모두 --policy random 또는 --policy rarest 를 붙인다)
  ④ 모두 스스로 끝난다 (ALLDONE -> BYE -> exit 0, 희생자는 exit 3)
  ⑤ VM 의 결과 가져오기
       scp -i daeman5.pem ubuntu@13.125.21.50:hw6/logs/tracker.jsonl logs/
       scp -i daeman5.pem ubuntu@13.125.21.50:hw6/report.json .
  ⑥ python tools/verify.py --env envs/A.json > verify.txt

  개발 중에는 tracker 도 내 PC 에서: python <src>/tracker.py --env envs/DEV.json  +  peer 끝을 127.0.0.1 6000

자동 실행 (우리가 제출 결과를 만든 방법)

  cd src
  python run_remote.py --deploy                                         VM 에 tracker 코드 · envs 복사
  python run_remote.py --env envs/DEV.json --name DEV                   (A, B, C1, C2 도 같은 방식)
  python run_remote.py --env envs/C3.json  --name C3_random --policy random
  python run_remote.py --env envs/C3.json  --name C3_rarest --policy rarest
  -> runs/<이름>/ 에 logs/*.jsonl · report.json · verify.txt · p1~p4/*.bitmap *.lost

종료 코드

  0 정상 (ALLDONE 뒤 BYE, FULL · PARTIAL 모두)
  1 인자 · 설정 오류 (IP 또는 PORT 누락, PORT 가 숫자가 아님, env version != 1, env 에 없는 id,
    policy 가 null 인데 --policy 없음, 원본 SHA-256 불일치) — 사용법 출력
  2 ABORT (stalled · deadline) 또는 Tracker 응답이 tracker_lost_sec(30초) 동안 없음
  3 가상 파괴된 희생자 (CMD DESTROY -> 결과 저장 -> BYE 없이 종료)

Windows 에서 실행하며 해결한 것 (키트 README 가 각 조가 해결하라고 한 부분)

  - os.pwrite / os.pread 가 없다 -> 파일마다 io_lock 하나로 lseek+write / lseek+read 를 한 묶음으로 보호
    (pwrite_guide 의 방법). 이 lock 은 공유 자료가 아니라 파일 위치 포인터만 보호하며, 다른 lock 을 잡은 채
    들어가지 않는다. POSIX 에서는 그대로 os.pwrite / os.pread 를 쓴다.
  - 파일은 os.open(..., O_BINARY) 로 연다 (텍스트 모드 변환 방지).
  - 열린 파일의 이름을 바꿀 수 없다 -> .part -> 완성 이름 / .incomplete / .corrupt 로 바꿀 때 io_lock 안에서
    닫고 -> os.replace -> 다시 연다.
  - 닫힌 loopback 포트로의 connect 가 refused 를 받기까지 약 2초 걸린다(SYN 재전송) -> Peer 끼리의 connect 는
    timeout 1초로 두고, loopback 에서 connect timeout 이면 ConnectionRefused 로 기록한다.
    (이것을 안 하면 crash 한 Peer 에게 요청한 워커와 BITMAP 갱신이 2초씩 멈춰 heartbeat 가 밀린다)
  - 화면: tui.py 가 os.system("") 로 VT 모드를 켠다. Windows Terminal 에서 ASCII 상태판이 그대로 동작한다.
    출력을 파일로 돌리면(run_remote.py) 상태판은 그리지 않고 사람용 로그 줄만 남긴다.


4. 배포 정보
================================================================================

  VM:         AWS EC2 t3.micro, 리전 ap-northeast-2 (서울), 가용 영역 ap-northeast-2c
  공인 IP:    13.125.21.50
  OS:         Ubuntu 24.04.5 LTS, Python 3.12.3
  보안 그룹:  TCP 22 (SSH), TCP 9500 (Tracker) — 실행 PC 의 공인 IP 만 허용
  Tracker:    python3 tracker.py --env envs/<X>.json 9500   (0.0.0.0:9500)
  실험 후:    인스턴스 중지


5. Thread 구조와 Lock 대응표
================================================================================

5-1. Tracker (외부 VM)

  Thread        개수 · 주기     하는 일
  main          1              accept(timeout 0.5초) -> 연결마다 handler. finished 이면 report.json -> EXIT
  handler       연결당 1        한 줄 읽고(timeout 0.5초) 한 줄 답한다. 응답은 lock 안에서 만들고 보내기는 lock 밖
  scheduler     1 · 0.5초       DEAD 판정 · crash 일정 -> pending · destroy(due + replica_safe) · ALLDONE ·
                               hang/deadline ABORT · 종료 판정(live 전원 BYE 또는 ABORT 뒤 15초)
  observer      1 · 1초         REPORT 로 T_rep · p_rep · upstat_at_rep · dup_ratio (네트워크 I/O 없음)
  screen        1 · 1초         SSH 터미널일 때만 상태판 (진행률 · Peer 별 LIVE/DEAD/BYE · have · T_rep · 다음 일정)

  공유 자료 (Tracker)                                   누가                        Lock
  joined · last_seen · have · dead · byes · done ·      handler(쓰기) · scheduler ·  self.lock 하나
  registered/partial(명단) · pending · reports ·        observer · screen(읽기)
  upstat · T_rep · alldone · abort
  소켓 · 수신 버퍼                                       그 연결의 handler 하나        lock 불필요(소유권)

5-2. Peer (내 PC, 4개 프로세스)

  Thread              개수 · 주기          하는 일
  main                1                   시작 절차 / 종료 절차 (새 연결 거부 -> 업로드 마무리 ≤3초 -> join ->
                                          결과 저장 -> BYE -> EXIT)
  listener            1 · accept 0.2초     자기 포트 accept -> 연결마다 upload handler. 가상 crash 중에는 소켓을 닫음
  upload handler      연결당 1             BITMAP · GET 응답. GET 은 bit 확인 -> pread -> UP.take(len) -> DATA
  worker              8 (download.workers) 청크 고르기 -> 소스 고르기 -> DOWN.take -> GET -> DATA -> pwrite -> bit = 1
  control             1 · 1초              HEARTBEAT(2초, CMD 처리) · PEERS(3초) · BITMAP(1초) 갱신 · REPORT ·
                                          STALLED 판정 · DONE/UNDONE · STATE · tracker_lost
  screen              1 · 1초              ASCII 상태판 (터미널일 때만)
  disconnect          crash 때만 1         n 초 침묵 -> listen -> HELLO · REGISTER · HAVE -> REJOIN

  공유 자료 (Peer)                              누가 읽고 쓰나                               Lock
  파일별 bitmap · have · in_progress · state ·  worker(쓰기) · upload handler(읽기) ·       f.lock (파일마다)
  zero_since · sources · sent · sent_set        control · screen(읽기)
  원격 bitmap 캐시 · holders · 주소             control(쓰기) · worker(읽기)                 remote_lock
  inflight · fails · banned_until              worker(쓰기) · control(읽기: rarity 계산)    sel_lock
  속도 제한기 예약 시각 (UP · DOWN)             upload handler(UP) · worker(DOWN)            limiter.lock (예약만, sleep 은 밖)
  Tracker 연결 (요청 한 줄 -> 응답)             control · main · worker(REGISTER) · rejoin   TrackerLink.lock
  파일 위치 포인터 (Windows)                    worker(pwrite) · upload handler(pread)       pf.io_lock
  업로드 연결 목록 · 진행 중 업로드 수          listener · upload handler · go_offline       up_lock
  보낸/받은 바이트 수                           upload handler · worker · screen            stat_lock
  로그 파일 · 화면                              모든 Thread                                  evlog 내부 lock · tui 내부 lock
  소켓 · 수신 버퍼                              그 연결을 만든 Thread 하나                    lock 불필요(소유권)

  규칙: f.lock · remote_lock · sel_lock 안에서는 send · recv · 파일 I/O · sleep 을 하지 않는다. 필요한 값만 lock
  안에서 복사(snapshot)하고 lock 밖에서 보낸다. 이 세 lock 은 서로 겹쳐 잡지 않는다(교착 없음).
  (pf.io_lock 과 TrackerLink.lock 은 각각 파일 하나 · 소켓 하나의 "요청 한 쌍"만 감싸는 I/O 직렬화용이며,
   그 안에서 다른 lock 을 잡지 않는다.)


6. 설계
================================================================================

6-1. 메시지 경계 (Conn)
  소켓마다 수신 버퍼(bytearray) 하나. read_line 은 \n 까지 모아 한 줄, read_exact(n) 은 버퍼에 남은 것부터 정확히
  n 바이트. DATA 헤더 뒤 본문을 read_exact 로 읽으므로 헤더와 본문 앞부분이 한 recv 에 붙어 와도 밀리지 않는다.

6-2. 속도 제한 (RateLimiter)
  방향마다 Peer 전체에 하나 (UP = up_kbps × 1024, DOWN = down_kbps × 1024 B/s). take(n) 은 lock 안에서
  next_t = max(now, next_t) + n / rate 로 다음 빈 시간 칸만 예약하고, lock 밖에서 그 시각까지 잔다.
  워커 8개는 GET 을 보내기 직전에 DOWN.take(청크 길이), upload handler 는 DATA 를 보내기 직전에 UP.take(len).
  측정 결과 모든 Run 에서 업로드 · 다운로드 실행 전체 평균이 한도 이하 (8-1 표).

6-3. 청크 선택 · 소스 선택
  - random: 시작할 때 (파일, 청크) 전체를 Random(policy_seed + 피어 번호)로 한 번 섞은 순서대로,
    아직 없고 · 받는 중이 아니고 · 지금 요청할 수 있는 보유자가 있는 첫 청크.
  - rarest: 받는 모든 파일의 청크 중 rarity(캐시에 있고 BAN 아닌 다른 Peer 중 그 청크를 가진 수)가 가장 작은 것,
    같으면 무작위.
  - 고르기와 in_progress 표시는 f.lock 하나 안에서 (check-then-act). 다른 워커와 같은 청크를 고르지 않는다.
  - 소스: 그 청크를 가진 Peer 중 BAN 아니고 inflight < per_peer_max 인 Peer 에서 inflight 가 가장 적은 쪽(같으면 무작위).
  - 개선: 고를 때 "지금 요청할 수 있는 보유자가 있는 청크"만 후보로 둔다. random 에서 순서상 첫 청크의 유일한
    보유자가 per_peer_max 로 꽉 차 있으면 8개 워커가 모두 그 청크를 골랐다 놓았다 반복하는 문제를 막는다.
  - 실패: ConnClosed · refused · timeout · ERR NOCHUNK · DATA 헤더 불일치 -> FAIL, 같은 상대 연속 3번 -> BAN 5초.
    ERR NOCHUNK 이면 그 상대의 캐시 bitmap 을 버린다. crash 중이라 내가 버린 요청은 상대 탓으로 세지 않는다.
  - 낡은 연결: 재사용하던 연결이 끊겨 있으면(상대가 crash 에서 돌아옴) 새 연결로 한 번만 다시 보낸다(RETRY).

6-4. 디스크 먼저, bit 나중 · 파일 완성
  받는 파일은 시작할 때 .part 를 전체 크기로 만든다(ftruncate). pwrite(i × chunk_size) 가 끝난 뒤에 f.lock 안에서
  bit = 1, have + 1. 업로드 쪽은 bit 를 확인한 뒤 pread. 마지막 청크를 쓴 워커가 SHA-256(1 MB 씩) 비교 ->
  같으면 rename -> COMPLETE -> REGISTER, 다르면 CORRUPT -> 처음부터(2회 연속이면 .corrupt, FAILED).

6-5. STALLED · DONE PARTIAL
  control 이 1초마다, 받는 파일마다 "남은 청크 전부가 rarity 0" 인지 본다. 그 상태가 stall_sec(30초) 계속되면
  STALLED. 하나라도 받을 수 있게 되면 UNSTALLED. BITMAP 이 실패하거나 BAN 된 상대, PEERS 에서 빠진 상대는
  rarity 에서 뺀다(파괴된 Peer 의 옛 bitmap 을 세지 않음). 내가 crash 에서 돌아오면 zero_since 를 지운다.
  받는 파일이 모두 SEED · STALLED · FAILED 이면 DONE FULL | PARTIAL(detail = 파일:남은 수)을 한 번 보내고,
  ALLDONE 까지 업로드를 계속한다. 다시 받을 것이 생기면 UNDONE.

6-6. 가상 crash · destroy
  - CMD DISCONNECT n: control 이 그 자리에서 go_offline() (alive 끄기 · epoch+1 · listen 소켓 닫기 · 업로드 연결
    shutdown · Tracker 연결 닫기) -> CRASH 기록 -> disconnect Thread 가 n 초 침묵 -> listen -> HELLO -> REGISTER ·
    HAVE -> PEERS · BITMAP 강제 갱신 표시 -> alive 켜기 -> REJOIN. 침묵 중에는 heartbeat · REPORT · STATE · BYE 를
    포함해 아무것도 보내지 않는다(TrackerLink 가 offline 이면 거부). 디스크 · bitmap 은 그대로 두고 이어받는다.
    침묵 중 도착한 DATA 는 버린다(epoch 비교).
  - CMD DESTROY: go_offline() -> DESTROY -> 결과 저장(.bitmap · .incomplete · .lost) -> EXIT code=3 -> os._exit(3).
    정상 종료 함수를 쓰지 않으므로 BYE 를 보내지 않는다 -> Tracker 는 20초 뒤 DEAD 판정.

6-7. Tracker 의 판정
  - DEAD: 어떤 메시지든 last_seen 갱신, dead_sec 침묵이면 DEAD(PEERS · ALLDONE 에서 제외), 다시 오면 BACK.
  - crash 일정: START 기준 at_sec 이 되면 pending 큐에 "CMD DISCONNECT sec", 다음 HEARTBEAT 응답으로 전달.
  - destroy: due(time · progress · after_replication) 이고 (replica_safe 스위치 false 또는 replica_safe(v) 참)인
    첫 순간 한 번. replica_safe(v) = 최근 report_stale_sec 안의 REPORT 기준으로 v 가 가진 모든 1 청크가 v 가 아닌
    live Peer 의 bitmap OR 에도 있음 (v 의 REPORT 가 없으면 거짓).
  - ALLDONE: env 의 4명이 모두 참가했고 live 전원이 DONE. ABORT: 진행률이 hang_sec 동안 그대로(stalled) ·
    deadline_sec 초과. ALLDONE · ABORT 뒤 지시는 CMD_DROPPED.
  - 종료: live Peer 가 모두 BYE(DEAD 는 기다리지 않음) 또는 ABORT 뒤 15초 -> report.json -> EXIT.


7. 실행 결과 (필수 7회, 모두 VM Tracker 구성 — runs/)
================================================================================

  시각은 report.json 기준(START = 4명 모두 HELLO 한 순간, 초).

  Run        정책    crash  희생자 · 파괴(T_destroy, p_destroy)   DONE     T_alldone  종료 코드(T P1 P2 P3 P4)  verify
  DEV        random  2      -                                    FULL      19.07     0 0 0 0 0                PASS
  A          random  8      -                                    FULL     155.61     0 0 0 0 0                PASS
  B          random  3      P3 · 97.59 s (0.7764) replica_safe   FULL     155.61     0 0 0 3 0                PASS
  C1         rarest  0      P4 · 55.07 s (0.4771)                FULL     127.10     0 0 0 0 3                PASS
  C2         rarest  0      P3 · 38.55 s (0.3132)                PARTIAL  152.09     0 0 0 3 0                PASS
  C3_random  random  0      P1 · 103.11 s (0.8545)               FULL     145.63     0 3 0 0 0                PASS
  C3_rarest  rarest  0      P1 · 97.07 s (0.7896)                FULL     151.59     0 3 0 0 0                PASS

  Run        T_rep 10 / 20 / 30 (s)        p_rep   dup_ratio 10 / 20 / 30      T_dead           T_done (s)
  DEV        8.57 / 15.57 / 16.57          0.8556  0.473 / 0.339 / 0.417      -                P3 15.28 P2 15.90 P1 18.05 P4 18.99
  A          77.55 / 92.56 / 107.56        0.7983  0.458 / 0.263 / 0.144      -                P3 92.60 P2 108.32 P1 137.46 P4 155.19
  B          68.55 / 72.55 / 97.55         0.7764  0.441 / 0.269 / 0.195      P3 119.59        P3 72.21 P2 98.49 P1 130.34 P4 155.20
  C1         24.56 / 50.56 / 78.57         0.6250  0.135 / 0.119 / 0.178      P4 77.08         P3 64.55 P2 88.64 P1 126.89
  C2         26.54 / 76.55 / null          null    0.150 / 0.281 / -          P3 60.55         P2 95.07 P1 129.74 P4 151.91
  C3_random  57.54 / 63.55 / 83.55         0.7556  0.463 / 0.203 / 0.126      P1 125.12        P3 63.18 P2 82.70 P4 145.14
  C3_rarest  23.54 / 49.55 / 76.55         0.6566  0.135 / 0.131 / 0.116      P1 119.08        P3 64.20 P2 82.75 P4 151.19

  Run 별로 확인한 것
  - DEV · A: crash(2회 · 8회) 뒤 모두 20초 안에 REJOIN, DEAD 오판 없음(T_dead {}), 네 명 모두 FULL.
  - B: env 의 파괴 시각은 34초였지만 share30 이 P3 에만 있는 청크가 남아 replica_safe 가 기다렸고, share30 의
    T_rep(97.55) 직후 97.59 초에 CMD DESTROY -> P3 는 BYE 없이 exit 3 -> 20초 뒤 DEAD(119.59) -> 생존자 FULL.
  - C1: 원본이 없는 P4 가 진행률 47.7% 에서 파괴 -> 유실 0, STALLED 0 (verify [4] ok).
  - C2: share30 의 원본 P3 가 진행률 31.3%(38.55 s)에서 즉시 파괴 -> 60.55 s DEAD -> 생존자 P2 · P1 · P4 가 각각
    STALLED + DONE PARTIAL(share30.bin:160) -> 152.09 s ALLDONE -> 모두 exit 0. 생존자 세 명의 share30 bitmap 은
    320/480 으로 같고 .lost 도 같다 (verify [3] "생존자 3명 bitmap 동일, 커버리지 66.7% (유실 160)").
  - C3: 같은 env, 정책만 바꿔 두 번. 둘 다 복제 후 파괴라 유실 0.


8. 성능 지표 (9-5) · 분석
================================================================================

8-1. Peer 별 평균 수신 · 송신 속도 (analyze.py)
  수신 = 내 RECV 수 × 64 KB ÷ (마지막 RECV − 첫 RECV), 송신 = 다른 Peer 의 RECV 중 src 가 나인 것으로 같은 방법.
  ( ) 안은 한도, % 는 한도 대비.

  Run        P1 수신/송신 KB/s          P2                       P3                       P4
  DEV        747(79%) / 533(49%)        674(71%) / 631(62%)      579(61%) / 873(91%)      828(84%) / 578(54%)
  A          375(81%) / 321(60%)        385(77%) / 350(70%)      337(70%) / 329(70%)      400(84%) / 228(45%)
  B          398(85%) / 330(70%)        424(79%) / 396(73%)      431(82%) / 450(99%)      400(89%) / 226(43%)
  C1         408(90%) / 428(83%)        469(95%) / 350(74%)      482(91%) / 407(82%)      354(77%) / 291(56%)
  C2         418(90%) / 361(75%)        479(89%) / 428(88%)      439(83%) / 545(100%*)    425(92%) / 324(60%)
  C3_random  427(94%) / 423(78%)        503(94%) / 372(77%)      493(96%) / 375(81%)      427(95%) / 261(56%)
  C3_rarest  386(85%) / 396(73%)        503(94%) / 376(77%)      486(94%) / 363(79%)      410(91%) / 276(60%)

  모든 Run 에서 업로드 · 다운로드 실행 전체 평균이 한도 이하다.
  (*) C2 의 P3 송신 100.3% 는 측정 공식의 효과다: 청크 N 개의 첫 도착 ~ 마지막 도착 사이에는 간격이 N−1 개뿐인데
      분자에 N 을 넣으므로 N/(N−1) 만큼 크게 나온다. P3 는 파괴 전 38초 동안 업로드 한도를 꽉 채워 보냈고
      (334 청크), 간격 기준으로 다시 재면 333 × 64 ÷ Δt = 543.8 KB/s ≤ 544. 제한기는 Peer 전체에 하나이며
      예약만 lock 안에서 한다 (6-2).

8-2. 출처 분포 (보낸 Peer = 행, 받은 Peer = 열, 청크 수) — Run A
            P1    P2    P3    P4   합
    P1       0   278   182   311   771
    P2     315     0   205   323   843
    P3     258   205     0   326   789
    P4     227   157    93     0   477
  (모든 Run 의 표: python src/analyze.py runs/<Run> src/envs/<env>.json)

8-3. crash · ban · 실패 수 (CRASH / BAN / FAIL / RETRY)
  DEV  P1 1/2/6/4    P2 1/1/4/0    P3 0/2/6/9    P4 0/3/11/8
  A    P1 2/12/36/23 P2 2/8/29/8   P3 2/8/25/17  P4 2/10/34/25
  B    P1 1/4/15/11  P2 1/1/6/4    P3 0/4/13/11  P4 1/4/16/16
  C1   P1 0/0/2/2    P2 0/2/6/4    P3 0/1/5/5    P4(희생자) 0/0/0/0
  C2   P1 0/2/6/4    P2 0/2/6/4    P3(희생자) 0/0/0/0  P4 0/2/6/4
  C3_random  P4 0/1/3/3 (나머지 0)     C3_rarest  P4 0/1/3/3 (나머지 0)
  FAIL · BAN 은 crash 순간(끊긴 요청 -> refused)과 파괴 직후에만 생긴다. 상대가 사라지면 3번 실패 -> 5초 BAN ->
  그 사이 BITMAP 갱신이 실패해 그 상대가 보유자에서 빠지므로 FAIL 이 끝없이 반복되지 않는다.

8-4. 분석 질문에 대한 답

  ① 평균 수신 속도는 한도의 몇 % 였나, 못 미친 이유는?
     본 시나리오에서 70 ~ 96%. crash 가 없는 C3 는 90 ~ 96%, crash 가 8번인 A 는 70 ~ 84% 로 가장 낮다.
     - 소스의 업로드 여유: 처음에는 원본 보유자 한 명만 그 파일을 가져서, 받는 쪽 세 명이 원본 한 명의 업로드
       한도(약 500 KB/s)를 나눠 쓴다. 내 다운로드 한도보다 소스들의 업로드 여유가 작은 구간이 생긴다.
     - crash: 사라진 Peer 는 그 동안 받지 못하고, 다른 Peer 는 그 Peer 를 소스로 못 쓴다 (A: 8번 × 약 6 ~ 11초).
     - 끝부분: 마지막 몇 청크는 보유자 수가 적고 per_peer_max 로 동시에 요청할 수 있는 수도 줄어든다.
       원본이 없는 P4 는 받을 양(960 청크)이 가장 많아 가장 늦게 끝난다(A 155 s, B 155 s, C3 145 / 151 s).

  ② 원본이 없던 P4 는 얼마나 올려 주었나 — 부분 재공유가 없다면 총 시간은?
     A 에서 P4 는 477 청크(29.8 MB)를 올려 주었다(B 437, C2 368, C3 405 / 330). 부분 재공유가 없어 원본만
     올려 준다면 P3 혼자 share30 을 세 명에게 1,440 청크(90 MB) 보내야 하므로, P3 한도 468 KB/s 로
     1,440 × 64 ÷ 468 ≈ 197 초가 하한이다. 실제 A 는 155.6 초 — 부분 재공유로 약 21% 이상 빨라졌다.
     (A 에서 원본이 보낸 청크 비율은 54% 뿐이고 나머지 46% 는 받는 중인 Peer 끼리 주고받았다.)

  ③ C2 에서 잃은 청크 수를 "P3 가 사라지기 전까지 share30 을 얼마나 내보냈나" 로 설명
     P3 는 파괴(38.55 s) 전까지 share30 청크를 334 번 보냈고, 그중 서로 다른 청크는 320 개였다
     (P1 · P2 · P4 의 RECV 중 file=share30, src=P3 를 센 값). 파괴 뒤 share30 의 청크는 생존자끼리만 오갈 수
     있으므로 생존자 전원의 합집합은 정확히 이 320 개이고, 480 − 320 = 160 개가 유실 — verify 의 커버리지
     66.7% (유실 160) 와 각 생존자의 STALLED lost=160 과 일치한다. rarest 라서 원본에만 있는 청크부터 받았기
     때문에 중복 송출이 14 번(4%)뿐이었고, 그만큼 P3 의 38초 업로드가 거의 모두 "새 청크" 로 퍼졌다.
     (참조 구현의 99 개보다 많은 것은 우리 실행에서 진행률 30.8% 에 더 빨리(38.6 s vs 46 s) 도달해 P3 가
     share30 을 내보낸 시간이 더 짧았기 때문이다.)

  ④ C3 에서 rarest 의 이득(T_rep · dup)과 손해(전체 완료)
                         random      rarest
     T_rep 10/20/30 (s)   57.5/63.6/83.6   23.5/49.6/76.6
     T_rep 최댓값          83.55           76.55
     dup_ratio 평균        0.264           0.127
     p_rep                0.7556          0.6566
     T_destroy (s)        103.11          97.07
     T_alldone − T_destroy 42.5            54.5
     T_alldone (s)        145.63          151.59
     파괴 시점까지 원본에서 받은 청크 비율   58.0%   63.7%
     - 이득: rarest 는 원본에만 있는 청크부터 받아 원본의 합집합이 거의 일직선으로 늘어난다 -> T_rep 가 앞당겨지고
       (share10 57.5 -> 23.5 s), 원본이 같은 청크를 다시 보내는 비율 dup 이 절반 이하가 된다.
     - 손해: 모두가 "가장 드문 청크" 로 몰려 원본 보유자에게 요청이 집중된다(파괴 시점까지 원본에서 받은 비율
       58.0% -> 63.7%). 원본의 업로드 한도는 정해져 있으니, 받는 중인 다른 Peer 의 업로드 여유가 덜 쓰인다.
       또 p_rep 가 낮아(0.66) 같은 u 라도 파괴가 6초 일렀고, 파괴 시점의 진행률도 낮아(0.79 vs 0.85) 희생자 P1 의
       업로드 없이 받아야 할 양이 더 많았다 -> 파괴 후 완료까지 54.5 s (random 42.5 s), 전체 완료는 6초 늦었다.
     (반복 실행 결과 · 평균은 8-5)

  ⑤ 모두가 같은 청크로 몰리는 문제를 줄이려면
     - rarest 후보를 "가장 작은 rarity 하나" 가 아니라 rarity 하위 k 개 중 무작위로 고른다(동점만 무작위인 지금보다
       분산이 크다).
     - 소스 기준으로 고르기: 지금 한가한(inflight 가 적은) 보유자가 가진 청크 중에서 rarest 를 고른다. 원본이
       바쁘면 다른 Peer 가 가진 청크를 받게 되어 업로드 여유가 고르게 쓰인다.
     - 하이브리드: 시작 직후 몇 청크는 random(BitTorrent 의 random first piece), 원본이 한 부 복제될 때(p_rep)까지는
       rarest 로 복제를 앞당기고, 그 뒤에는 random 으로 바꿔 요청을 여러 소스로 흩는다. 끝부분에서는 endgame
       모드(남은 몇 청크를 여러 소스에 동시에 요청).

8-5. C3 반복 실행 (runs_extra/, 필수 아님 — 평균과 범위)

  정책별 3회 (runs/C3_<정책> + runs_extra/C3_<정책>_2, _3). 모두 verify PASS. 값: 1회 / 2회 / 3회 -> 평균 (범위)

  random
    T_rep 최댓값 (s)        83.5 / 82.6 / 83.5     -> 83.2  (82.6 ~ 83.5)
    dup_ratio 평균          0.264 / 0.259 / 0.260  -> 0.261 (0.259 ~ 0.264)
    p_rep                   0.756 / 0.746 / 0.756  -> 0.752 (0.746 ~ 0.756)
    T_destroy (s)           103.1 / 101.1 / 103.1  -> 102.4 (101.1 ~ 103.1)
    T_alldone − T_destroy   42.5 / 44.0 / 42.5     -> 43.0  (42.5 ~ 44.0)
    T_alldone (s)           145.6 / 145.1 / 145.6  -> 145.4 (145.1 ~ 145.6)
  rarest
    T_rep 최댓값 (s)        76.5 / 73.6 / 74.5     -> 74.9  (73.6 ~ 76.5)
    dup_ratio 평균          0.127 / 0.127 / 0.101  -> 0.119 (0.101 ~ 0.127)
    p_rep                   0.657 / 0.644 / 0.643  -> 0.648 (0.643 ~ 0.657)
    T_destroy (s)           97.1 / 95.1 / 95.1     -> 95.7  (95.1 ~ 97.1)
    T_alldone − T_destroy   54.5 / 56.0 / 56.0     -> 55.5  (54.5 ~ 56.0)
    T_alldone (s)           151.6 / 151.1 / 151.1  -> 151.3 (151.1 ~ 151.6)

  세 번 모두 같은 방향이다: rarest 는 복제가 약 8초 빠르고(T_rep 최댓값) 원본의 중복 송출이 절반 이하지만,
  p_rep 가 낮아 파괴가 약 7초 일찍 오고 파괴 뒤 완료에 12초 더 걸려 전체 완료는 약 6초 늦다. 범위가 1 ~ 3초로
  좁아(스레드 순서 · 네트워크 버퍼에 따른 변동) 두 정책의 차이는 우연이 아니다.


9. 정합성 확인 (9-4) — 7회 모두 OK (analyze.py)
================================================================================

  1 시각 순서     모든 Run: T_rep ≤ T_alldone ≤ T_exit, T_last_done ≤ T_alldone. 일어나지 않은 일은 null
                  (A 의 T_destroy, C2 share30 의 T_rep · p_rep).
  2 crash 지시 수  crashes_sent = env 의 crashes 수 (DEV 2, A 8, B 3, 나머지 0). 파괴가 있는 Run 은 T_destroy 와
                  희생자 peers = "DEAD" 가 있다.
  3 RECV 줄 수     Peer 마다 RECV 줄 수 = 끝에 가진 받은 청크 수(.bitmap 의 1 − 원본 청크 수).
                  파괴 · 유실이 없는 DEV · A · B 는 4명 합 = progress_total_chunks (720 · 2,880 · 2,880).
                  (B 는 희생자 P3 가 파괴 전에 이미 FULL 이라 합이 2,880. C1 2,227 · C2 2,192 · C3 2,771 / 2,669 는
                   희생자가 못 받은 양과 C2 의 유실만큼 적다)
  4 속도          업로드 · 다운로드 전체 평균 ≤ 한도 (8-1).


10. 구현 한계 · 추가 구현 · 느낀 점
================================================================================

구현 한계
  - Peer 는 Windows 에서 실행했다. os.pwrite 가 없어 파일마다 io_lock 으로 lseek+write 를 묶었으므로, 같은 파일에
    대한 디스크 쓰기 · 읽기는 동시에 하나씩만 된다(64 KB 쓰기는 매우 짧아 속도에는 영향이 없었다).
  - Peer 로그의 t 는 Peer 시작 기준이라 Tracker START 와 0.5초 안팎 차이가 있다. 파괴 시점까지 자른 출처 비율(8-4 ④)은
    이 차이를 무시한 근사값이다.
  - 측정 공식(8-1) 때문에 파괴 직전까지 한도를 꽉 채운 Peer 의 송신 속도가 100% 를 아주 조금 넘게 계산될 수 있다.
  - Tracker 장애 복구는 하지 않는다(명세대로 tracker_lost 30초 뒤 exit 2).

추가 구현
  - RETRY: 재사용하던 연결이 낡았으면(상대가 crash 에서 돌아옴) 새 연결로 한 번 다시 보내 실패로 세지 않는다
    (QnA Q16 의 "복귀 직후의 낡은 연결" 개선 팁).
  - crash 중이라 내가 버린 요청은 상대 탓(FAIL · BAN)으로 세지 않는다 (QnA Q16 개선 팁).
  - 청크를 고를 때 "지금 요청할 수 있는 보유자가 있는 청크" 만 후보로 둔다(6-3).
  - Peer 끼리 connect timeout 1초 (Windows 의 느린 refused 대응), BITMAP 갱신 중 실패한 상대는 그 바퀴에 다시 묻지
    않아 heartbeat 가 밀리지 않는다.
  - Tracker 도 SSH 터미널에 상태판(진행률 · Peer 별 LIVE/DEAD/BYE · have · T_rep · 다음 crash/destroy)을 그린다.
  - run_remote.py 로 VM tracker 시작 -> peer 4개 -> scp -> verify -> runs/ 보관까지 한 번에, analyze.py 로 지표 계산.

배우고 느낀 점
  - HW5 의 별 모양 구조와 달리 데이터는 Peer 끼리 직접 오가고 Tracker 는 명단 · 지시만 한다. 부분 재공유 덕분에
    원본만 올려 주는 경우보다 20% 이상 빨랐고, P4 처럼 원본이 없는 Peer 도 30 MB 가까이 올려 주었다.
  - 시간 상수의 순서(crash ≤ 12 < DEAD 20 < STALLED 30 < hang 60)가 "잠깐 사라짐 / 영원히 사라짐 / 유실 /
    전체 멈춤" 을 저절로 구별해 준다는 것을 실제 로그로 확인했다.
  - "디스크 먼저, bit 나중", "lock 안에서는 I/O · sleep 금지", "모든 대기에 timeout" 같은 규칙을 하나라도 어기면
    verify 가 바로 잡아낸다. 특히 Windows 에서는 pwrite 부재, 열린 파일 rename, 느린 connection refused 처럼
    OS 차이를 직접 해결해야 했다.
  - rarest 가 복제를 앞당기지만 전체 완료는 늦을 수 있다는 것을 같은 env 두 번의 실측으로 확인했다.
