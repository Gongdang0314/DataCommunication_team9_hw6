# 시연 영상용 실행 (Windows Terminal): VM Tracker 화면 1개 + Peer 4개의 ASCII 상태 화면(2x2)
#
#   powershell -ExecutionPolicy Bypass -File demo_run.ps1 -EnvFile envs\A.json
#   powershell -ExecutionPolicy Bypass -File demo_run.ps1 -EnvFile envs\C3.json -Policy rarest
#
# 순서: reset_dirs -> (창 1) ssh 로 VM 에서 tracker 실행(화면 보임) -> (창 2) peer 4개 2x2 분할
# 모두 끝나면:  python demo_collect.py --env envs\A.json --name demo_A   (tracker 로그 가져오기 + verify)
param(
    [Parameter(Mandatory = $true)][string]$EnvFile,
    [string]$Policy = "",
    [string]$VmHost = "13.125.21.50",
    [string]$Port = "9500",
    [string]$Work = "$env:USERPROFILE\hw6work",
    [string]$Key = "$env:USERPROFILE\.ssh\daeman5.pem"
)
$src = Split-Path -Parent $MyInvocation.MyCommand.Path
$envUnix = $EnvFile -replace '\\', '/'
Push-Location $Work
python tools\reset_dirs.py --env $EnvFile
Pop-Location

# 창 1: Tracker (VM). -t 로 터미널을 붙여야 상태판이 그려진다
$trk = "ssh -t -i `"$Key`" ubuntu@$VmHost `"cd hw6 && pkill -f '^python3 tracker.py'; rm -rf logs report.json; python3 tracker.py --env $envUnix $Port; echo; echo '[tracker 종료 - Enter]'; read x`""
Start-Process wt.exe -ArgumentList "-w new new-tab --title Tracker cmd /c $trk"
Start-Sleep -Seconds 3

$pol = ""
if ($Policy -ne "") { $pol = "--policy $Policy" }
function PeerCmd($id) { return "-d `"$Work`" --title $id cmd /k python `"$src\peer.py`" --env $EnvFile --id $id $pol $VmHost $Port" }

# 창 2: Peer 4개 (2x2)
$args2 = "-w new new-tab $(PeerCmd 'P1') ; split-pane -V $(PeerCmd 'P2') ; move-focus left ; split-pane -H $(PeerCmd 'P3') ; move-focus right ; split-pane -H $(PeerCmd 'P4')"
Start-Process wt.exe -ArgumentList $args2
Write-Host "started. 끝나면: cd $src ; python demo_collect.py --env $EnvFile --name demo_<이름>"
