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
