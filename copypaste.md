


#!/usr/bin/env bash
BASE="http://localhost:8000"

endpoints=(
"/intelligence/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=300"
"/intelligence/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=0"
"/intelligence/projects/proj_001/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=0"
"/intelligence/do/projects/ranked?org_id=org_001&team_id=team_001&user_id=user_001&limit=20"
"/intelligence/projects/proj_001/do/tasks/ranked?org_id=org_001&team_id=team_001&user_id=user_001&limit=20"
"/intelligence/projects/proj_001/plan/tasks?org_id=org_001&team_id=team_001"
"/intelligence/ux/wellbeing?org_id=org_001&scope_type=team&scope_id=team_001&limit=200"
"/intelligence/thermo/dashboard?org_id=org_001&include_history=true&limit=200"
)

runs=15

for ep in "${endpoints[@]}"; do
  echo "=== $ep"
  times=()
  for ((i=1;i<=runs;i++)); do
    t=$(curl -s -o /dev/null -w "%{time_total}" "$BASE$ep")
    ms=$(awk "BEGIN {printf \"%.2f\", $t*1000}")
    times+=("$ms")
  done

  sorted=$(printf "%s\n" "${times[@]}" | sort -n)
  min=$(echo "$sorted" | head -n1)
  max=$(echo "$sorted" | tail -n1)
  avg=$(printf "%s\n" "${times[@]}" | awk '{s+=$1} END {printf "%.2f", s/NR}')
  p95_index=$(( (runs*95 + 99)/100 ))
  p95=$(echo "$sorted" | sed -n "${p95_index}p")

  echo "avg=${avg}ms p95=${p95}ms min=${min}ms max=${max}ms"
  echo
done

$results = foreach ($ep in $endpoints) { Measure-Api "$BASE$ep" 15 }
$results | Format-Table -AutoSize
