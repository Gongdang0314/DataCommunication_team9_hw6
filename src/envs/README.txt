======================================================================
 HW6 env.json 시나리오 세트 (공개용)
======================================================================

Tracker와 Peer는 실행할 때 같은 env 파일을 읽는다.
(참고: 강의 자료 HW6explanation.pdf PPT §4.4 ~ §4.10 · p.32 ~ 38)
Tracker는 외부 VM(AWS / GCP)에서, Peer 4개는 내 PC 한 대에서 실행한다.

  (VM)    python tracker.py --env envs/C2.json
  (내 PC) python peer.py    --env envs/C2.json --id P1 <VM공인IP> 6000   (P1~P4 각각)
  (내 PC) python peer.py    --env envs/C3.json --id P1 --policy rarest <VM공인IP> 6000
                                                                    (C3만 --policy 필요)
  Tracker 주소(IP PORT)는 peer 명령의 마지막 두 인자로만 준다 (필수). env 파일은 고치지 않는다.
  개발 중에는 tracker 도 내 PC 에서 돌려도 된다 (peer 명령 끝을 127.0.0.1 6000 으로).


1. 필드 설명
----------------------------------------------------------------------

필드                       읽는 쪽   의미
-------------------------  --------  ------------------------------------------------
version                    둘 다     형식 버전. 1이 아니면 실행을 거부할 것
name, run, seed,           둘 다     시나리오 이름, Run 종류(A/B/C1/C2/C3),
description                          생성 seed(기록용), 설명.
                                     로그와 보고서 첫 줄에 남길 것
tracker.port               Tracker   Tracker 의 기본 listen 포트 (0.0.0.0:6000). env 에 Tracker IP 는 없다.
                                     peer 는 실행 인자로 주소를 받는다: ... --id P1 213.123.45.7 6000
                                     (로컬 시험은 127.0.0.1 6000)
                                     다른 포트: tracker.py --env ... 6100  +  peer ... 213.123.45.7 6100
                                     (두 쪽 포트가 같아야 한다. env 는 고치지 않는다)
                                     peer 는 6000 이어도 IP PORT 둘 다 적는다 (tracker 만 포트 생략 가능)
chunk_size                 둘 다     청크 크기(바이트). 64 KB
files[]                    둘 다     name, size(바이트),
                                     origin(처음부터 이 파일을 가진 피어 = 원본 seed),
                                     sha256(원본 전체의 SHA-256, 소문자 16진수 64자):
                                     원본 seed 는 시작할 때, 받는 쪽은 완성 직후 비교
peers[]                    둘 다     id, host, port, dir(저장 폴더),
                                     host = 피어끼리 연결할 주소 (127.0.0.1, 4명 모두 같은 PC).
                                     tracker 는 PEERS 응답에 이 값을 돌려준다
                                     (연결에서 본 IP 가 아니라),
                                     up_kbps / down_kbps (업로드/다운로드 한도 KB/s,
                                     1 KB = 1024 B)
progress_total_chunks      Tracker   전체 진행률의 분모
                                     (모든 피어가 받아야 할 청크 수의 합)
download.workers           Peer      다운로드 워커 스레드 수
download.per_peer_max      Peer      한 상대 피어에게 동시에 보낼 수 있는 요청 수
download.policy            Peer      "random" 또는 "rarest".
                                     null이면 명령행 --policy 가 필수 (C3)
download.policy_seed       Peer      청크 선택용 난수 seed.
                                     실제 seed = policy_seed + 피어 번호 (P1 -> +1 ... P4 -> +4)
timing.*                   둘 다     시간 상수(초).
                                     heartbeat 2, BITMAP 갱신 1, PEERS 갱신 3,
                                     DEAD 20, STALLED 30, HANG 60, DEADLINE 900,
                                     상태 화면 5, 연속 실패 한도 3회, 제외 5초,
                                     tracker_lost_sec 30 (peer 가 tracker 응답을 30초 못 받으면
                                     결과 저장 후 스스로 exit 2, 가상 crash 시간 제외),
                                     report_stale_sec 3 (REPORT 가 3초 넘게 오래되면
                                     tracker 관찰에서 뺀다)
crashes[]                  Tracker   가상 crash 일정. at_sec(START 기준 초: 4명 모두 HELLO한 순간)에
                                     peer 에게 "CMD DISCONNECT <duration_sec>" 지시
destroy                    Tracker   영구 파괴 계획. 없으면 null. 아래 참고


2. destroy 객체
----------------------------------------------------------------------

peer          파괴할 피어
trigger       "time"               START 후 value 초가 지나면 파괴 (Run B)
              "progress"           전체 진행률이 value(0~1)를 넘으면 파괴 (C1, C2)
              "after_replication"  모든 원본 파일이 한 부 복제된 시점의 진행률을 p_rep라 할 때,
                                   진행률이 p_rep + value x (1 - p_rep) 를 넘으면 파괴 (C3)
replica_safe  true 이면 조건이 되어도, 희생자의 모든 청크가 다른 살아 있는 피어에
              있을 때까지 기다렸다가 파괴 (Run B)

파괴 지시는 heartbeat 응답 "CMD DESTROY" 로 전달한다.
희생자는 BYE 없이 사라지고 exit 3으로 스스로 종료한다.


3. 공개 시나리오 목록
----------------------------------------------------------------------

파일       Run  정책       가상 crash       영구 파괴                의도
---------  ---  ---------  ---------------  -----------------------  ------------------------------
DEV        A    random     2회              -                        개발용: 3/5/7 MB,
                                                                     1,000 KB/s, 약 20초
A          A    random     8회 (전원 2회)   -                        crash 와 재합류
B          B    random     3회              P3, 34초 (replica_safe)  가장 큰 파일의 원본 seed 이탈
                                                                     (복제될 때까지 대기)
C1         C1   rarest     -                P4, 진행률 47.5%         원본이 없는 피어의 늦은 이탈
C2         C2   rarest     -                P3, 진행률 30.8%         share30 원본 손실 (PARTIAL)
C3         C3   --policy   -                P1, 복제 후 u = 0.369    random vs rarest 비교
                                                                     (--policy random, rarest 각 1회)

공유 파일: share10 / share20 / share30 = 10 / 20 / 30 MB (청크 160 / 320 / 480개),
진행률 분모 2,880. DEV 를 뺀 실행 1회는 약 2.5 ~ 3분.
필수 실행 7회: DEV, A, B, C1, C2, C3(random), C3(rarest).

시나리오는 반드시 한 번에 하나씩 실행한다 (동시 실행 금지: CPU 부족 -> heartbeat 지연 -> DEAD 오판).

채점에는 같은 형식이지만 값이 다른 비공개 시나리오를 사용한다.
특정 피어 이름이나 시각을 코드에 하드코딩하지 말 것.
