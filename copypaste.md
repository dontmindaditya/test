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



Make the GET API responses faster for the Discourse/Decide/Do endpoints in DB (Postgres) mode. The slow endpoints are:

GET /intelligence/decide/directions
GET /intelligence/projects/{project_id}/decide/directions
GET /intelligence/do/projects/ranked
GET /intelligence/projects/{project_id}/do/tasks/ranked
GET /intelligence/projects/{project_id}/plan/tasks
GET /intelligence/ux/wellbeing
GET /intelligence/thermo/dashboard
GET /intelligence/project-cards
GET /intelligence/project-cards/{project_id}
Fix the following issues:

Cache bypass bug: get_decide_cache() doesn't skip the cache when max_age_s=0 — add an explicit early return.
Full event scans: Endpoints load ALL org events then filter in Python — add list_raw_events_scoped() and list_vote_events_scoped() methods to storage_postgres.py that push filtering (org, team, project, event_type) into SQL WHERE clauses.
Vote tally recomputation: _compute_vote_tallies runs every request — add a module-level _VOTE_TALLY_CACHE dict with TTL-based caching (default 60s).
Do ranking not cached: Add a feed cache for the Do ranking endpoints keyed on org/team/user/project + FSA state version.
Thermo dashboard recomputation: Change /thermo/dashboard to return the latest stored snapshot by default, only recomputing when ?refresh=true is passed.
Missing DB indexes: Create backend/sql/010_get_perf_indexes.sql with indexes on events_raw(team_id), events_raw(user_id), composite indexes on (org_id, project_id, occurred_at DESC) and (org_id, team_id, project_id, occurred_at DESC), a partial index for vote event types, and a freshness index on decide_rankings_cache.
Connection pool: Increase PostgresStore pool from maxconn=5 to maxconn=10.
Compatibility: Add hasattr guard before calling store.upsert_memcubes_batch().
