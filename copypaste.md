BASE="http://127.0.0.1:8000"

for ep in \
"/intelligence/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=300" \
"/intelligence/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=0" \
"/intelligence/projects/proj_001/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=0" \
"/intelligence/do/projects/ranked?org_id=org_001&team_id=team_001&user_id=user_001&limit=20" \
"/intelligence/projects/proj_001/do/tasks/ranked?org_id=org_001&team_id=team_001&user_id=user_001&limit=20" \
"/intelligence/projects/proj_001/plan/tasks?org_id=org_001&team_id=team_001" \
"/intelligence/ux/wellbeing?org_id=org_001&scope_type=team&scope_id=team_001&limit=200" \
"/intelligence/thermo/dashboard?org_id=org_001&include_history=true&limit=200"
do
  echo "==== $ep"
  curl -s -o /dev/null -w "status=%{http_code} total=%{time_total}s connect=%{time_connect}s starttransfer=%{time_starttransfer}s\n" "$BASE$ep"
done


$BASE = "http://localhost:8000"

$endpoints = @(
  "/intelligence/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=300",   # warm/cache-hit
  "/intelligence/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=0",     # cold/cache-miss
  "/intelligence/projects/proj_001/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=0",
  "/intelligence/do/projects/ranked?org_id=org_001&team_id=team_001&user_id=user_001&limit=20",
  "/intelligence/projects/proj_001/do/tasks/ranked?org_id=org_001&team_id=team_001&user_id=user_001&limit=20",
  "/intelligence/projects/proj_001/plan/tasks?org_id=org_001&team_id=team_001",
  "/intelligence/ux/wellbeing?org_id=org_001&scope_type=team&scope_id=team_001&limit=200",
  "/intelligence/thermo/dashboard?org_id=org_001&include_history=true&limit=200"
)

function Measure-Api($url, $runs = 10) {
  $times = @()
  for ($i=0; $i -lt $runs; $i++) {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    try { Invoke-WebRequest -Uri $url -Method GET -UseBasicParsing | Out-Null } catch {}
    $sw.Stop()
    $times += $sw.Elapsed.TotalMilliseconds
  }
  $sorted = $times | Sort-Object
  $avg = ($times | Measure-Object -Average).Average
  $p95 = $sorted[[math]::Min([int]($runs*0.95), $runs-1)]
  [pscustomobject]@{
    Url = $url
    AvgMs = [math]::Round($avg,2)
    P95Ms = [math]::Round($p95,2)
    MinMs = [math]::Round(($sorted[0]),2)
    MaxMs = [math]::Round(($sorted[-1]),2)
  }
}

$results = foreach ($ep in $endpoints) { Measure-Api "$BASE$ep" 15 }
$results | Format-Table -AutoSize
