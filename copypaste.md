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



Improvements List
🔴 Critical Issues
1. Bug in pipeline/orchestrator.py:49-50 - Analysis stage is executed twice (line 49 & 50)
2. Missing agent implementations - execute_agent() has commented-out imports, agents dictionary is empty (lines 124-130)
3. No authentication/authorization - API endpoints are unprotected
⚠️ Security Improvements
4. Add rate limiting - No rate limiting on API endpoints
5. Environment variables - .env file is committed (check .gitignore)
6. Add API key authentication for agent endpoints
7. Input validation - Add more robust validation on all endpoints
🔧 Code Quality
8. Deprecated model name - gpt-4-turbo-preview is deprecated in config.py:48, use gpt-4o
9. Type hints - Missing type hints in many functions
10. Add docstrings - Many modules lack documentation
11. Error handling - Improve error messages and exception handling
12. Logging consistency - Mix of logger setups (get_logger, setup_logger)
🧪 Testing
13. Add more tests - Only 2 test files exist
14. Add pytest fixtures in conftest.py for the main app
15. Add integration tests for WebSocket endpoints
🏗️ Architecture
16. Add dependency injection - Use FastAPI's Depends() for services
17. Database migrations - Add Alembic or similar for Supabase schema
18. Modular config - Split config.py into separate files per domain
19. Health checks - Add detailed health checks (DB, external APIs)
⚡ Performance
20. Add caching - Cache agent registry, settings
21. Connection pooling - Configure for database/HTTP clients
22. Async optimization - Review blocking calls in async functions
📦 Dependencies
23. Update pinned versions - Many packages have * or no version constraints in requirements
24. Remove unused dependencies - Audit requirements.txt


