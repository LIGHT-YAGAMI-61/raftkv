param(
  [ValidateSet('follower-pause','follower-partition','leader-isolate','leader-kill','cluster-kill')]
  [string[]]$Faults = @('follower-pause','follower-partition','leader-isolate','leader-kill','cluster-kill'),
  [int]$Duration = 60,
  [int]$Keys = 30,
  [string]$OutDir = 'logs\rerun'
)

Set-Location (Split-Path $PSScriptRoot)
New-Item -ItemType Directory -Force $OutDir | Out-Null
$Net = 'raftkv_default'
$Nodes = 'node1','node2','node3'

function Cid($svc) { docker compose ps -q $svc }

# Compose logs keep every past election, so the highest term is the current leader.
function Get-Leader {
  $best = $null
  docker compose logs --no-color 2>$null | ForEach-Object {
    if ($_ -match '(node\d+): becoming LEADER for term (\d+)') {
      $t = [int]$Matches[2]
      if (-not $best -or $t -gt $best.Term) { $best = @{ Node = $Matches[1]; Term = $t } }
    }
  }
  if (-not $best) { throw 'no leader found in logs' }
  $best.Node
}

# Reconnect needs --alias, or the node loses its compose DNS name.
function Reconnect($node) {
  docker network connect --alias $node $Net (Cid $node) | Out-Null
}

function Inject($fault) {
  $leader = Get-Leader
  $follower = ($Nodes | Where-Object { $_ -ne $leader })[0]
  Write-Host "  leader=$leader, follower=$follower"
  switch ($fault) {
    'follower-pause'     { docker pause (Cid $follower) | Out-Null; Start-Sleep 15; docker unpause (Cid $follower) | Out-Null }
    'follower-partition' { docker network disconnect $Net (Cid $follower) | Out-Null; Start-Sleep 15; Reconnect $follower }
    'leader-isolate'     { docker network disconnect $Net (Cid $leader) | Out-Null; Start-Sleep 15; Reconnect $leader }
    'leader-kill'        { docker kill (Cid $leader) | Out-Null; Start-Sleep 15; docker start (Cid $leader) | Out-Null }
    'cluster-kill'       { foreach ($n in $Nodes) { docker kill (Cid $n) | Out-Null }
                           Start-Sleep 10
                           foreach ($n in $Nodes) { docker start (Cid $n) | Out-Null } }
  }
}

docker compose up -d | Out-Null
Start-Sleep 10
$summary = Join-Path $OutDir 'faults-summary.txt'
Clear-Content $summary -ErrorAction SilentlyContinue

foreach ($f in $Faults) {
  Write-Host "== $f"
  $out = Join-Path $OutDir "fault-$f.txt"
  $err = Join-Path $OutDir "fault-$f.err"
  $runArgs = @('compose','run','--rm','--no-deps','-T','--entrypoint','/app/checker','bench',
               '-nodes','node1:5001,node2:5001,node3:5001','-keys',"$Keys",'-duration',"${Duration}s")
  $p = Start-Process docker -ArgumentList $runArgs -RedirectStandardOutput $out -RedirectStandardError $err -PassThru -NoNewWindow
  Start-Sleep 15
  Inject $f
  $p | Wait-Process
  # Decide from the checker's own RESULT line, not the process exit code.
  $result = (Select-String -Path $out -Pattern 'RESULT:' | Select-Object -Last 1).Line
  "$f : $result" | Tee-Object -FilePath $summary -Append
  Start-Sleep 10
}