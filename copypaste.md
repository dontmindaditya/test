BASE="http://127.0.0.1:8000"
endpoints=(
  "/intelligence/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=300"
  "/intelligence/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=0"
  "/intelligence/projects/proj_001/decide/directions?org_id=org_001&team_id=team_001&limit=10&max_age_s=0"
  "/intelligence/do/projects/ranked?org_id=org_001&team_id=team_001&user_id=user_001&limit=20"
  "/intelligence/projects/proj_001/do/tasks/ranked?org_id=org_001&team_id=team_001&user_id=user_001&limit=20"
  "/intelligence/projects/proj_001/plan/tasks?org_id=org_001&team_id=team_001"
  "/intelligence/ux/wellbeing?org_id=org_001&scope_type=team&scope_id=team_001&limit=200"
)
runs=15
for ep in "${endpoints[@]}"; do
  echo "=== $ep ==="
  times=()
  for ((i=1; i<=runs; i++)); do
    t=$(curl -s -o /dev/null -w "%{time_total}" "$BASE$ep")
    ms=$(awk "BEGIN {printf \"%.2f\", $t*1000}")
    times+=("$ms")
  done
  printf "%s\n" "${times[@]}" | awk 'BEGIN{min=999999;max=0;sum=0;count=0} {if($1<min)min=$1; if($1>max)max=$1; sum+=$1; count++} END{printf "Min: %.2f ms | Avg: %.2f ms | Max: %.2f ms\n\n", min, sum/count, max}'
done



