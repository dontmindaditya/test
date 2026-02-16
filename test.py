from __future__ import annotations

import copy
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from typing import Any
import uuid
from functools import wraps

import jwt
from django.conf import settings
from django.core.cache import cache
from jwt import ExpiredSignatureError, InvalidTokenError
from rest_framework import status
from rest_framework.response import Response
from supabase import create_client, Client

from backend.env import load_env

load_env()


try:
    from collectium_intelligence.classifier import classify_event
    from collectium_intelligence.discourse import build_knowledge_cards, extract_insights, generate_strategic_directions
    from collectium_intelligence.fsa import apply_org_fsa_event, apply_project_fsa_event
    from collectium_intelligence.hypergraph_cf import (
        build_hypergraph,
        derive_user_weights_from_profiles,
        rank_directions_for_user,
        rank_directions_for_group,
    )
    from collectium_intelligence.hypergraph_learn import refine_hypergraph_weights
    from collectium_intelligence.interactions import interaction_from_event
    from collectium_intelligence.memos import (
        MEMOS_SCHEMA_MEMCUBE_CONTEXT,
        build_memos_schema_memcubes,
        ingest_memos_message,
    )
    from collectium_intelligence.memcubes import (
        PSYCHODYNAMIC_TELEMETRY_CONTEXT,
        build_psychodynamic_exchange_bundle,
        build_psychodynamic_telemetry_memcube,
    )
    from collectium_intelligence.profiler_team import build_team_profile as _build_team_profile_base
    from collectium_intelligence.profiler_user import build_user_profile as _build_user_profile_base
    from collectium_intelligence.ranker import rank_feed
    from collectium_intelligence.ux_control import recommend_ux_interventions, evaluate_ux_policy
    from collectium_intelligence.ux_policy import (
        DEFAULT_POLICY_EPSILON,
        DEFAULT_POLICY_MIN_SAMPLES,
        offline_policy_eval,
        select_intervention,
    )
    from collectium_intelligence.ux_taxonomy import default_ux_interventions
    from collectium_intelligence.wellbeing import normalize_wellbeing_proxies
    from collectium_intelligence.context_blocks import context_block_from_event, context_block_info_from_event
    from collectium_intelligence.dag import compute_levels, iter_level_groups, topological_sort, would_create_cycle
    from collectium_intelligence.drift import drift_report_user
    from collectium_intelligence.online_block_matrix import update_online_block_matrix_record
    from collectium_intelligence.psychodynamics import animal_name, classify_animal_event, frobenius_diff
    from collectium_intelligence.te_online import get_te_registry, update_te_on_event, TeamTETracker
    from collectium_intelligence.paper_report import build_paper_report
    from collectium_intelligence.state_schemas import (
        STATE_SCHEMA_MEMCUBE_CONTEXT,
        build_state_schema_memcube,
        default_animals_state_schema,
        default_steiner_state_schema,
    )
    from collectium_intelligence.steiner_model import SteinerPrototypeModel
    from collectium_intelligence.utils.time import parse_iso8601, utc_now_iso8601
    from collectium_intelligence.utils.math import cosine_similarity
    from collectium_intelligence.monitoring import (
        LatencyTracker,
        export_prometheus_metrics,
        get_global_collector,
        get_health_status,
    )
    from collectium_intelligence.telemetry_validation import build_telemetry_instrumentation_report
except ModuleNotFoundError as e:
    raise RuntimeError(
        "collectium_intelligence is not installed; from `intelligence/` run: `pip install -e '.[backend]'`"
    ) from e


from backend.models import (
    IngestEventResponse,
    MemcubeExchangeImport,
    MemosMessage,
    RankFeedRequest,
    RankFeedResponse,
    RawEvent,
    UxApplyRequest,
    UxExposureRequest,
    UxIntervention,
    UxInterventionRun,
    WellbeingWindow,
    ConsentRecord,
)
from backend.models import Memcube
from backend.storage import InMemoryStore


PIPELINE_VERSION = os.getenv("INTELLIGENCE_PIPELINE_VERSION", "beta-v1")
INFLUENCE_LAYER_SCHEMA_VERSION = "team-influence-layer-v1"
ANIMAL_CLASSIFIER_MODE = os.getenv("TRACKA_ANIMAL_CLASSIFIER", "heuristic")
STEINER_MODEL_PATH = os.getenv("TRACKA_STEINER_MODEL_PATH", "")
HYPERGRAPH_REFINE_MODE = os.getenv("TRACKA_HYPERGRAPH_REFINE", "off")
HYPERGRAPH_REFINE_DIMS = os.getenv("TRACKA_HYPERGRAPH_REFINE_DIMS", "64")
HYPERGRAPH_REFINE_RETAIN = os.getenv("TRACKA_HYPERGRAPH_REFINE_RETAIN", "0.7")
_STEINER_MODEL: SteinerPrototypeModel | None = None
_STEINER_MODEL_ERROR: str | None = None


class SupabaseUserService:
    _instance: "SupabaseUserService | None" = None

    def __new__(cls) -> "SupabaseUserService":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        self._supabase_url = getattr(settings, "SUPABASE_URL", None)
        self._supabase_service_key = getattr(settings, "SUPABASE_SERVICE_KEY", None)
        self._jwt_secret = getattr(settings, "SUPABASE_JWT_SECRET", None)
        if not self._supabase_url or not self._supabase_service_key or not self._jwt_secret:
            raise RuntimeError("Supabase settings are missing from Django settings.")
        self._client: Client = create_client(self._supabase_url, self._supabase_service_key)

    def verify_token(self, token: str) -> dict[str, Any]:
        if not token:
            raise InvalidTokenError("Missing token.")
        payload = jwt.decode(
            token,
            self._jwt_secret,
            algorithms=["HS256"],
            audience="authenticated",
        )
        if not isinstance(payload, dict):
            raise InvalidTokenError("Invalid token payload.")
        return payload

    def get_user_by_id(self, user_id: str) -> dict[str, Any]:
        cache_key = f"supabase_user_{user_id}"
        cached = cache.get(cache_key)
        if isinstance(cached, dict):
            return cached
        try:
            response = self._client.auth.admin.get_user_by_id(user_id)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Supabase user lookup failed: {exc}") from exc

        user_data = {}
        if isinstance(response, dict):
            user_data = response.get("user") or response.get("data") or response
        elif hasattr(response, "user"):
            user_data = response.user  # type: ignore[assignment]
        elif hasattr(response, "data"):
            user_data = response.data  # type: ignore[assignment]

        if hasattr(user_data, "model_dump"):
            user_data = user_data.model_dump()  # type: ignore[assignment]
        elif hasattr(user_data, "dict"):
            user_data = user_data.dict()  # type: ignore[assignment]
        elif not isinstance(user_data, dict):
            user_data = {"id": user_id}

        cache.set(cache_key, user_data, timeout=300)
        return user_data


def _supabase_auth_enabled() -> bool:
    return str(os.getenv("ENABLE_SUPABASE_AUTH", "true")).strip().lower() in {"1", "true", "yes", "on"}


def get_user_display_name(user_data: dict[str, Any]) -> str | None:
    metadata = user_data.get("user_metadata") if isinstance(user_data, dict) else {}
    if not isinstance(metadata, dict):
        metadata = {}
    for key in ("full_name", "name", "display_name", "username", "preferred_username"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def get_user_avatar(user_data: dict[str, Any]) -> str | None:
    metadata = user_data.get("user_metadata") if isinstance(user_data, dict) else {}
    if not isinstance(metadata, dict):
        metadata = {}
    for key in ("avatar_url", "avatar", "picture", "photo_url"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def get_request_user_info(request: Any) -> dict[str, Any]:
    user_data = getattr(request, "supabase_user", None)
    user_id = getattr(request, "supabase_user_id", None)
    email = getattr(request, "supabase_user_email", None)
    display_name = get_user_display_name(user_data) if isinstance(user_data, dict) else None
    return {
        "user_id": user_id,
        "email": email,
        "display_name": display_name,
        "authenticated": bool(user_id),
    }


def _attach_user_to_request(
    request: Any,
    *,
    user_id: str | None,
    email: str | None,
    user_data: dict[str, Any],
    authenticated: bool,
) -> None:
    setattr(request, "supabase_user", user_data)
    setattr(request, "supabase_user_id", user_id)
    setattr(request, "supabase_user_email", email)
    if hasattr(request, "state"):
        request.state.supabase_user = user_data
        request.state.supabase_user_id = user_id
        request.state.supabase_user_email = email
        request.state.supabase_authenticated = authenticated


def _extract_request(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any | None:
    request = kwargs.get("request")
    if request is not None:
        return request
    for arg in args:
        if hasattr(arg, "headers"):
            return arg
    return None


def _extract_bearer_token(request: Any) -> str | None:
    header = None
    if hasattr(request, "headers"):
        header = request.headers.get("Authorization") if request.headers else None
    if not header:
        return None
    parts = header.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


def token_required(func: Any) -> Any:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        request = _extract_request(args, kwargs)
        if request is None:
            return Response({"detail": "Request object is required."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if not _supabase_auth_enabled():
            mock_user = {
                "id": "mock-user",
                "email": "mock@example.com",
                "user_metadata": {"display_name": "Mock User"},
            }
            _attach_user_to_request(
                request,
                user_id=mock_user["id"],
                email=mock_user["email"],
                user_data=mock_user,
                authenticated=False,
            )
            return func(*args, **kwargs)

        token = _extract_bearer_token(request)
        if not token:
            return Response({"detail": "Authorization header missing or invalid."}, status=status.HTTP_401_UNAUTHORIZED)

        try:
            service = SupabaseUserService()
        except Exception:
            return Response({"detail": "Authentication service unavailable."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        try:
            payload = service.verify_token(token)
        except ExpiredSignatureError:
            return Response({"detail": "Token has expired."}, status=status.HTTP_401_UNAUTHORIZED)
        except InvalidTokenError:
            return Response({"detail": "Invalid token."}, status=status.HTTP_401_UNAUTHORIZED)
        except Exception:
            return Response({"detail": "Token verification failed."}, status=status.HTTP_401_UNAUTHORIZED)

        user_id = payload.get("sub") or payload.get("user_id") or payload.get("id")
        if not isinstance(user_id, str) or not user_id.strip():
            return Response({"detail": "Token missing user identifier."}, status=status.HTTP_401_UNAUTHORIZED)

        user_id = user_id.strip()
        email = payload.get("email")
        user_data: dict[str, Any] = {}
        try:
            user_data = service.get_user_by_id(user_id)
        except Exception:
            user_data = {}
        if not email and isinstance(user_data, dict):
            email = user_data.get("email")

        _attach_user_to_request(
            request,
            user_id=user_id,
            email=email if isinstance(email, str) else None,
            user_data=user_data if isinstance(user_data, dict) else {},
            authenticated=True,
        )
        return func(*args, **kwargs)

    return wrapper


def _load_steiner_model() -> SteinerPrototypeModel | None:
    global _STEINER_MODEL, _STEINER_MODEL_ERROR
    if _STEINER_MODEL is not None or _STEINER_MODEL_ERROR is not None:
        return _STEINER_MODEL
    if not STEINER_MODEL_PATH:
        _STEINER_MODEL_ERROR = "unset"
        return None
    try:
        _STEINER_MODEL = SteinerPrototypeModel.load_json(STEINER_MODEL_PATH)
    except Exception as exc:  # noqa: BLE001
        _STEINER_MODEL_ERROR = str(exc)
        _STEINER_MODEL = None
        print(f"[tracka] Steiner model load failed: {exc}")
    return _STEINER_MODEL


def _psychodynamics_config() -> dict[str, Any]:
    cfg: dict[str, Any] = {"animal_classifier": str(ANIMAL_CLASSIFIER_MODE or "heuristic")}
    model = _load_steiner_model()
    if model is not None:
        cfg["steiner_model"] = model
    mte_mode = os.getenv("TRACKA_MTE_MODE")
    if mte_mode:
        cfg["mte_mode"] = str(mte_mode)
    mte_alpha = os.getenv("TRACKA_MTE_ALPHA")
    if mte_alpha:
        try:
            cfg["mte_alpha"] = float(mte_alpha)
        except Exception:
            pass
    mte_sigma = os.getenv("TRACKA_MTE_SIGMA")
    if mte_sigma:
        try:
            cfg["mte_sigma"] = float(mte_sigma)
        except Exception:
            pass
    mte_embed = os.getenv("TRACKA_MTE_EMBEDDING_NAME")
    if mte_embed:
        cfg["mte_embedding_name"] = str(mte_embed)
    mte_dims = os.getenv("TRACKA_MTE_EMBEDDING_DIMS")
    if mte_dims:
        try:
            cfg["mte_embedding_dims"] = int(mte_dims)
        except Exception:
            pass
    mte_max_t = os.getenv("TRACKA_MTE_MAX_T")
    if mte_max_t:
        try:
            cfg["mte_max_T"] = int(mte_max_t)
        except Exception:
            pass
    mte_max_team = os.getenv("TRACKA_MTE_MAX_TEAM")
    if mte_max_team:
        try:
            cfg["mte_max_team"] = int(mte_max_team)
        except Exception:
            pass
    primary_state_space = os.getenv("TRACKA_PRIMARY_STATE_SPACE")
    if primary_state_space:
        cfg["primary_state_space"] = str(primary_state_space)
    secondary_state_spaces = os.getenv("TRACKA_SECONDARY_STATE_SPACES")
    if secondary_state_spaces:
        cfg["secondary_state_spaces"] = str(secondary_state_spaces)
    return cfg


def _hypergraph_refine_enabled() -> bool:
    return str(HYPERGRAPH_REFINE_MODE or "off").strip().lower() in {"1", "true", "on", "yes"}


def _maybe_refine_hypergraph(hg: Any) -> Any:
    if not _hypergraph_refine_enabled():
        return hg
    try:
        dims = max(4, int(HYPERGRAPH_REFINE_DIMS))
    except Exception:
        dims = 64
    try:
        retain = float(HYPERGRAPH_REFINE_RETAIN)
    except Exception:
        retain = 0.7
    try:
        return refine_hypergraph_weights(hg, dims=dims, retain_weight=retain)
    except Exception:
        return hg


def build_user_profile(
    user_id: str,
    org_id: str,
    events: list[dict[str, Any]],
    existing_profile: dict[str, Any] | None = None,
    *,
    psychodynamics_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = _psychodynamics_config()
    if isinstance(psychodynamics_config, dict):
        cfg.update(psychodynamics_config)
    return _build_user_profile_base(
        user_id,
        org_id,
        events,
        existing_profile,
        psychodynamics_config=cfg,
    )


def build_team_profile(
    team_id: str,
    org_id: str,
    member_profiles: list[dict[str, Any]],
    team_events: list[dict[str, Any]],
    existing_profile: dict[str, Any] | None = None,
    *,
    psychodynamics_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = _psychodynamics_config()
    if isinstance(psychodynamics_config, dict):
        cfg.update(psychodynamics_config)
    return _build_team_profile_base(
        team_id,
        org_id,
        member_profiles,
        team_events,
        existing_profile,
        psychodynamics_config=cfg,
    )


def _recompute_block_matrices_for_scope(
    *,
    store: Any,
    events: list[dict[str, Any]],
    org_id: str,
    scope_type: str,
    scope_id: str,
    pipeline_version: str,
) -> list[dict[str, Any]]:
    records_by_ctx: dict[str, dict[str, Any]] = {}
    mode = str(ANIMAL_CLASSIFIER_MODE or "heuristic")
    steiner_model = _load_steiner_model()

    for event in events:
        try:
            ctx_block = str(context_block_from_event(event)).strip().lower()
        except Exception:
            continue

        try:
            animal_state = int(
                classify_animal_event(
                    event,
                    mode=mode,
                    steiner_model=steiner_model,
                )
            )
        except Exception:
            continue

        prev = records_by_ctx.get(ctx_block)
        updated = update_online_block_matrix_record(
            prev,
            org_id=org_id,
            scope_type=scope_type,
            scope_id=scope_id,
            context_block=ctx_block,
            pipeline_version=pipeline_version,
            updated_at=utc_now_iso8601(),
            new_state=int(animal_state),
        )
        records_by_ctx[ctx_block] = updated

    for record in records_by_ctx.values():
        try:
            store.upsert_psychodynamic_block_matrix(record)
        except Exception:
            pass

    return list(records_by_ctx.values())


def _model_dump(obj: Any) -> dict[str, Any]:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()  # type: ignore[no-any-return]
    return obj.dict()  # type: ignore[no-any-return,attr-defined]


def _fingerprint_request(payload: dict[str, Any]) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()[:16]


def _ensure_memcube_id(prefix: str, object_id: Optional[str] = None) -> str:
    if isinstance(object_id, str) and object_id:
        return f"{prefix}:{object_id}"
    return f"{prefix}:{uuid.uuid4()}"


def _raw_event_type(event: dict[str, Any]) -> str:
    value = event.get("event_type")
    if isinstance(value, str) and value:
        return value
    value = event.get("action")
    if isinstance(value, str) and value:
        return value
    return "unknown"


def _raw_event_data(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event.get("event_data"), dict):
        return event["event_data"]
    ctx = event.get("context")
    if isinstance(ctx, dict) and isinstance(ctx.get("event_data"), dict):
        return ctx["event_data"]
    return {}


def _is_discourse_candidate_event(event: dict[str, Any]) -> bool:
    """Return True if this raw event should contribute to Discourse→Decide content synthesis."""
    event_type = _raw_event_type(event)
    # Keep voting/proposal mechanics out of the discourse synthesis so direction IDs
    # remain stable (votes should not change the content candidates they vote on).
    return not (
        event_type.startswith("vote.")
        or event_type.startswith("proposal.")
        or event_type.startswith("decide.")
    )


def _normalize_vote_choice(value: Any) -> str:
    choice = str(value or "").strip().lower()
    if choice in {"support", "yes", "y", "up", "approve", "for"}:
        return "support"
    if choice in {"oppose", "no", "n", "down", "reject", "against"}:
        return "oppose"
    return "abstain"


def _compute_vote_tallies(
    *,
    events: list[dict[str, Any]],
    direction_ids: set[str],
    team_id: str | None,
    project_id: str | None,
    user_id: str | None,
) -> tuple[dict[str, dict[str, int]], dict[str, str]]:
    """Compute vote tallies from raw `vote.cast` events.

    Rules:
    - Counts only the latest vote per (direction_id, user_id).
    - Tallies are scoped by `project_id` (org-level uses `project_id is None`).
    - If `team_id` is provided, votes must match `event.team_id == team_id`.
    """
    if not direction_ids:
        return {}, {}

    latest: dict[tuple[str, str], dict[str, str]] = {}
    for event in events:
        if _raw_event_type(event) != "vote.cast":
            continue
        if isinstance(team_id, str) and team_id and event.get("team_id") != team_id:
            continue

        ev_project = event.get("project_id")
        if project_id is None:
            if ev_project not in (None, ""):
                continue
        else:
            if ev_project != project_id:
                continue

        data = _raw_event_data(event)
        proposal_id = data.get("proposal_id") or data.get("direction_id")
        if not isinstance(proposal_id, str) or not proposal_id.strip():
            continue
        direction_id = proposal_id.strip()
        if direction_id not in direction_ids:
            continue

        voter = event.get("user_id") or event.get("subject_id") or event.get("actor_id")
        if not isinstance(voter, str) or not voter.strip():
            continue
        voter_id = voter.strip()

        timestamp = str(event.get("timestamp") or "")
        choice = _normalize_vote_choice(data.get("choice"))

        key = (direction_id, voter_id)
        prev = latest.get(key)
        if prev is None or timestamp >= str(prev.get("timestamp") or ""):
            latest[key] = {"timestamp": timestamp, "choice": choice}

    tallies: dict[str, dict[str, int]] = {
        did: {"support": 0, "oppose": 0, "abstain": 0, "total": 0} for did in sorted(direction_ids)
    }
    user_votes: dict[str, str] = {}

    for (direction_id, voter_id), vote in latest.items():
        choice = str(vote.get("choice") or "abstain")
        row = tallies.setdefault(direction_id, {"support": 0, "oppose": 0, "abstain": 0, "total": 0})
        if choice not in {"support", "oppose", "abstain"}:
            choice = "abstain"
        row[choice] = int(row.get(choice) or 0) + 1
        row["total"] = int(row.get("total") or 0) + 1
        if isinstance(user_id, str) and user_id and voter_id == user_id:
            user_votes[direction_id] = choice

    return tallies, user_votes


def create_app(*, store: Optional[Any] = None) -> Any:
    """Create a FastAPI app wrapping Track A intelligence functions.

    Returns `Any` to avoid importing FastAPI types in environments that only run unit tests.
    """
    try:
        from fastapi import Body, FastAPI, HTTPException, Query, Request, Response
        from fastapi.middleware.cors import CORSMiddleware
        from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(
            "fastapi is required for the backend; from `intelligence/` run: `pip install -e '.[backend]'`"
        ) from e

    app = FastAPI(title="Collectium Intelligence Backend", version=PIPELINE_VERSION)
    enable_debug = str(os.getenv("INTELLIGENCE_ENABLE_UI") or "1").strip().lower() in {"1", "true", "yes"}
    enable_monitoring = str(os.getenv("INTELLIGENCE_ENABLE_MONITORING") or "1").strip().lower() in {"1", "true", "yes"}
    collector = get_global_collector()

    # CORS for the polished Collectium frontend (Vite dev server).
    # Keep defaults dev-friendly while remaining explicit.
    cors_env = str(os.getenv("INTELLIGENCE_CORS_ORIGINS") or "").strip()
    default_origins = [
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]
    origins = (
        ["*"]
        if cors_env == "*"
        else [o.strip() for o in cors_env.split(",") if o.strip()] or default_origins
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if store is None:
        force_memory = str(os.getenv("INTELLIGENCE_FORCE_MEMORY") or "").strip().lower() in {"1", "true", "yes"}
        if force_memory:
            store = InMemoryStore()
        else:
            dsn = os.getenv("INTELLIGENCE_DATABASE_URL") or os.getenv("DATABASE_URL")
            if dsn:
                try:
                    from backend.storage_postgres import PostgresStore

                    schema = os.getenv("INTELLIGENCE_DB_SCHEMA")
                    store = PostgresStore.from_dsn(dsn, ensure_schema=True, schema=schema)
                except Exception as e:  # noqa: BLE001
                    raise RuntimeError(
                        "Failed to initialize Postgres storage; check INTELLIGENCE_DATABASE_URL / DATABASE_URL."
                    ) from e
            else:
                store = InMemoryStore()

    @app.get("/auth/test")
    def auth_test() -> dict[str, Any]:
        enabled = _supabase_auth_enabled()
        if not enabled:
            return {"status": "disabled", "enabled": False}
        try:
            _ = SupabaseUserService()
            return {"status": "ok", "enabled": True}
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "enabled": True, "error": str(exc)}

    @token_required
    @app.get("/auth/test/protected")
    def auth_test_protected(request: Request) -> dict[str, Any]:
        return {"status": "ok", "user": get_request_user_info(request)}

    @token_required
    @app.get("/auth/me")
    def auth_me(request: Request) -> dict[str, Any]:
        return {
            "status": "ok",
            "user": get_request_user_info(request),
            "supabase_user": getattr(request, "supabase_user", None),
        }

    def _ux_policy_config() -> dict[str, Any]:
        mode = str(os.getenv("TRACKA_UX_POLICY_MODE") or "off").strip().lower()
        if mode not in {"off", "shadow", "on"}:
            mode = "off"
        try:
            epsilon = float(os.getenv("TRACKA_UX_POLICY_EPSILON") or DEFAULT_POLICY_EPSILON)
        except Exception:
            epsilon = float(DEFAULT_POLICY_EPSILON)
        try:
            min_samples = int(os.getenv("TRACKA_UX_POLICY_MIN_SAMPLES") or DEFAULT_POLICY_MIN_SAMPLES)
        except Exception:
            min_samples = int(DEFAULT_POLICY_MIN_SAMPLES)
        return {"mode": mode, "epsilon": float(epsilon), "min_samples": int(min_samples)}

    def _ensure_default_ux_interventions(org_id: str) -> list[dict[str, Any]]:
        """Ensure the intervention taxonomy exists for an org (idempotent)."""
        interventions: list[dict[str, Any]] = []
        now = utc_now_iso8601()
        for row in default_ux_interventions():
            rec = dict(row)
            rec["org_id"] = org_id
            rec.setdefault("created_at", now)
            rec.setdefault("updated_at", now)
            try:
                interventions.append(store.upsert_ux_intervention(rec))
            except Exception:
                # Keep bootstrapping best-effort in demo mode.
                continue
        return interventions

    def _list_ux_interventions(org_id: str, *, limit: int = 200) -> list[dict[str, Any]]:
        try:
            return store.list_ux_interventions(org_id=org_id, limit=int(limit))
        except Exception:
            return []

    def _lookup_ux_intervention(org_id: str, intervention_key: str) -> dict[str, Any] | None:
        key = str(intervention_key or "")
        if not key:
            return None
        for row in _list_ux_interventions(org_id, limit=500):
            if str(row.get("intervention_key") or "") == key:
                return row
        return None

    def _log_wellbeing_window(
        *,
        org_id: str,
        scope_type: str,
        scope_id: str,
        window_end: str | None,
        pipeline_version: str,
        proxies: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        consent_granted: bool | None = None,
    ) -> None:
        if not hasattr(store, "upsert_wellbeing_window"):
            return
        if not org_id or not scope_type or not scope_id or not pipeline_version:
            return
        ts = str(window_end or "").strip() or utc_now_iso8601()
        normalized = normalize_wellbeing_proxies(proxies, consent_granted=consent_granted)
        meta = metadata if isinstance(metadata, dict) else {}
        meta = dict(meta)
        meta["wellbeing_schema_version"] = normalized.get("schema_version")
        dropped = normalized.get("dropped")
        if isinstance(dropped, list) and dropped:
            meta["wellbeing_dropped"] = dropped
        if normalized.get("missing_consent") is True:
            meta["wellbeing_missing_consent"] = True
        payload = {
            "org_id": org_id,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "window_end": ts,
            "pipeline_version": pipeline_version,
            "proxies": normalized.get("proxies") or {},
            "consent_granted": normalized.get("consent_granted"),
            "metadata": meta,
            "created_at": ts,
        }
        try:
            store.upsert_wellbeing_window(payload)
        except Exception:
            return

    def _persist_team_influence_layers(
        *,
        org_id: str,
        team_id: str,
        pipeline_version: str,
        influence_layers: dict[str, Any] | None,
        updated_at: str | None = None,
    ) -> None:
        if not hasattr(store, "upsert_psychodynamic_influence_layer"):
            return
        if not org_id or not team_id or not pipeline_version:
            return
        layers = influence_layers if isinstance(influence_layers, dict) else {}
        if not layers:
            return
        ts = str(updated_at or "").strip() or utc_now_iso8601()
        for ctx, payload in layers.items():
            if not isinstance(ctx, str) or not ctx.strip():
                continue
            if not isinstance(payload, dict):
                continue
            ctx_key = ctx.strip().lower()
            layer_payload = {k: v for k, v in payload.items() if k not in {"events", "T"}}
            record = {
                "schema_version": INFLUENCE_LAYER_SCHEMA_VERSION,
                "org_id": org_id,
                "team_id": team_id,
                "context_block": ctx_key,
                "pipeline_version": pipeline_version,
                "updated_at": ts,
                "events": payload.get("events"),
                "T": payload.get("T"),
                "layers": layer_payload,
            }
            try:
                store.upsert_psychodynamic_influence_layer(record)
            except Exception:
                continue

    def _persist_psychodynamics_memcube(
        *,
        org_id: str,
        scope_type: str,
        scope_id: str,
        pipeline_version: str,
        psychodynamics: dict[str, Any],
        block_matrices: list[dict[str, Any]] | None = None,
        source: str | None = None,
        created_at: str | None = None,
    ) -> dict[str, Any] | None:
        if not org_id or not scope_type or not scope_id or not pipeline_version:
            return None
        if not isinstance(psychodynamics, dict) or not psychodynamics:
            return None
        meta: dict[str, Any] = {}
        if isinstance(source, str) and source:
            meta["source"] = source
        try:
            memcube = build_psychodynamic_telemetry_memcube(
                org_id=org_id,
                scope_type=scope_type,
                scope_id=scope_id,
                pipeline_version=pipeline_version,
                telemetry=psychodynamics,
                block_matrices=block_matrices,
                metadata=meta,
                created_at=created_at,
                context_type=PSYCHODYNAMIC_TELEMETRY_CONTEXT,
            )
        except Exception:
            return None
        try:
            store.upsert_memcube(memcube)
        except Exception:
            return None
        return memcube

    def _upsert_team_profile(
        team_profile: dict[str, Any],
        *,
        org_id: str,
        team_id: str,
        pipeline_version: str,
        updated_at: str | None = None,
    ) -> None:
        store.upsert_team_profile(team_profile)
        psych = team_profile.get("psychodynamics") if isinstance(team_profile, dict) else {}
        psych = psych if isinstance(psych, dict) else {}
        influence_layers = psych.get("influence_layers") if isinstance(psych.get("influence_layers"), dict) else {}
        _persist_team_influence_layers(
            org_id=org_id,
            team_id=team_id,
            pipeline_version=pipeline_version,
            influence_layers=influence_layers,
            updated_at=updated_at,
        )
        _persist_psychodynamics_memcube(
            org_id=org_id,
            scope_type="team",
            scope_id=team_id,
            pipeline_version=pipeline_version,
            psychodynamics=psych,
            source="team_profile",
            created_at=updated_at,
        )

    def _upsert_user_profile(
        user_profile: dict[str, Any],
        *,
        org_id: str,
        user_id: str,
        pipeline_version: str,
        updated_at: str | None = None,
    ) -> None:
        store.upsert_user_profile(user_profile)
        psych = user_profile.get("psychodynamics") if isinstance(user_profile, dict) else {}
        psych = psych if isinstance(psych, dict) else {}
        _persist_psychodynamics_memcube(
            org_id=org_id,
            scope_type="user",
            scope_id=user_id,
            pipeline_version=pipeline_version,
            psychodynamics=psych,
            source="user_profile",
            created_at=updated_at,
        )

    def _ensure_user_profile(org_id: str, user_id: str) -> dict[str, Any]:
        prof = store.get_user_profile(org_id=org_id, user_id=user_id)
        if prof is None:
            events = store.list_user_classified_events(org_id=org_id, user_id=user_id)
            prof = build_user_profile(user_id, org_id, events)
            _upsert_user_profile(
                prof,
                org_id=org_id,
                user_id=user_id,
                pipeline_version=PIPELINE_VERSION,
                updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
            )
        return prof

    def _ensure_team_profile(org_id: str, team_id: str) -> dict[str, Any]:
        prof = store.get_team_profile(org_id=org_id, team_id=team_id)
        if prof is None:
            team_events = store.list_team_classified_events(org_id=org_id, team_id=team_id)
            member_ids = sorted({str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)})
            member_profiles: list[dict[str, Any]] = []
            for uid in member_ids:
                member_profiles.append(_ensure_user_profile(org_id, uid))
            prof = build_team_profile(team_id, org_id, member_profiles, team_events)
            _upsert_team_profile(
                prof,
                org_id=org_id,
                team_id=team_id,
                pipeline_version=PIPELINE_VERSION,
            )
        return prof

    def _state_schema_memcubes_for_org(org_id: str) -> list[dict[str, Any]]:
        if not isinstance(org_id, str) or not org_id:
            return []
        existing = store.list_memcubes(org_id=org_id, context_type=STATE_SCHEMA_MEMCUBE_CONTEXT, limit=50)
        if existing:
            return existing
        animals = build_state_schema_memcube(
            org_id=org_id,
            schema=default_animals_state_schema(),
            metadata={"schema_name": "animals"},
        )
        steiner = build_state_schema_memcube(
            org_id=org_id,
            schema=default_steiner_state_schema(),
            metadata={"schema_name": "steiner"},
        )
        for cube in (animals, steiner):
            try:
                store.upsert_memcube(cube)
            except Exception:
                continue
        return [animals, steiner]

    def _memos_schema_memcubes_for_org(org_id: str) -> list[dict[str, Any]]:
        if not isinstance(org_id, str) or not org_id:
            return []
        existing = store.list_memcubes(org_id=org_id, context_type=MEMOS_SCHEMA_MEMCUBE_CONTEXT, limit=50)
        if existing:
            return existing
        memcubes = build_memos_schema_memcubes(org_id)
        stored: list[dict[str, Any]] = []
        for cube in memcubes:
            try:
                stored.append(store.upsert_memcube(cube))
            except Exception:
                continue
        return stored

    def _psychodynamics_memcube_for_scope(
        *,
        org_id: str,
        scope_type: str,
        scope_id: str,
        pipeline_version: str,
        include_blocks: bool = False,
        source: str | None = None,
    ) -> dict[str, Any] | None:
        scope_type = str(scope_type or "").strip().lower()
        scope_id = str(scope_id or "").strip()
        if scope_type not in {"user", "team"} or not scope_id:
            return None

        if scope_type == "user":
            prof = _ensure_user_profile(org_id, scope_id)
        else:
            prof = _ensure_team_profile(org_id, scope_id)

        psych = prof.get("psychodynamics") if isinstance(prof, dict) else {}
        psych = psych if isinstance(psych, dict) else {}
        updated_at = prof.get("updated_at") if isinstance(prof, dict) else None

        block_matrices: list[dict[str, Any]] | None = None
        if include_blocks and hasattr(store, "list_psychodynamic_block_matrices"):
            try:
                block_matrices = store.list_psychodynamic_block_matrices(
                    org_id=org_id,
                    scope_type=scope_type,
                    scope_id=scope_id,
                    pipeline_version=pipeline_version,
                    limit=200,
                )
            except Exception:
                block_matrices = None

        return _persist_psychodynamics_memcube(
            org_id=org_id,
            scope_type=scope_type,
            scope_id=scope_id,
            pipeline_version=pipeline_version,
            psychodynamics=psych,
            block_matrices=block_matrices,
            source=source,
            created_at=updated_at,
        )

    def _prepare_exchange_memcube(
        memcube: dict[str, Any],
        *,
        org_id: str,
        source: str | None,
        now: str,
    ) -> dict[str, Any] | None:
        if not isinstance(memcube, dict):
            return None
        rec = copy.deepcopy(memcube)
        if not isinstance(rec.get("org_id"), str) or not rec.get("org_id"):
            rec["org_id"] = org_id

        content = rec.get("content") if isinstance(rec.get("content"), dict) else {}
        if not isinstance(rec.get("context_type"), str) or not rec.get("context_type"):
            if "telemetry_version" in content:
                rec["context_type"] = PSYCHODYNAMIC_TELEMETRY_CONTEXT
            elif "schema_version" in content and "schema" in content:
                rec["context_type"] = STATE_SCHEMA_MEMCUBE_CONTEXT

        if not isinstance(rec.get("level"), str) or not rec.get("level"):
            scope_type = content.get("scope_type")
            if isinstance(scope_type, str) and scope_type:
                rec["level"] = scope_type
            elif rec.get("context_type") == STATE_SCHEMA_MEMCUBE_CONTEXT:
                rec["level"] = "org"

        if not isinstance(rec.get("entity_id"), str) or not rec.get("entity_id"):
            scope_id = content.get("scope_id")
            if isinstance(scope_id, str) and scope_id:
                rec["entity_id"] = scope_id
            else:
                schema_id = content.get("schema_id")
                if isinstance(schema_id, str) and schema_id:
                    rec["entity_id"] = schema_id

        if not isinstance(rec.get("context_type"), str) or not rec.get("context_type"):
            return None
        if not isinstance(rec.get("level"), str) or not rec.get("level"):
            return None
        if not isinstance(rec.get("entity_id"), str) or not rec.get("entity_id"):
            return None

        if not isinstance(rec.get("memcube_id"), str) or not rec.get("memcube_id"):
            rec["memcube_id"] = f"{rec['context_type']}:{rec['entity_id']}:{uuid.uuid4()}"

        meta = rec.get("metadata") if isinstance(rec.get("metadata"), dict) else {}
        exchange = meta.get("exchange") if isinstance(meta.get("exchange"), dict) else {}
        exchange["imported_at"] = now
        if isinstance(source, str) and source:
            exchange["source"] = source
        meta["exchange"] = exchange
        rec["metadata"] = meta

        return rec

    def _snapshot_scope_state(*, org_id: str, scope_type: str, scope_id: str, pipeline_version: str) -> dict[str, Any]:
        """Build an audit-friendly snapshot used for pre/post measurement."""
        scope_type = str(scope_type)
        if scope_type == "user":
            prof = _ensure_user_profile(org_id, scope_id)
            blocks = store.list_psychodynamic_block_matrices(
                org_id=org_id, scope_type="user", scope_id=scope_id, pipeline_version=pipeline_version
            )
            return {
                "scope_type": "user",
                "scope_id": scope_id,
                "user_profile_psychodynamics": prof.get("psychodynamics") or {},
                "block_matrices": blocks,
            }

        if scope_type == "team":
            team_profile = _ensure_team_profile(org_id, scope_id)
            team_events = store.list_team_classified_events(org_id=org_id, team_id=scope_id)
            member_ids = sorted({str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)})
            member_profiles = [_ensure_user_profile(org_id, uid) for uid in member_ids]
            ux = recommend_ux_interventions(user_profiles=member_profiles, team_profile=team_profile)
            return {
                "scope_type": "team",
                "scope_id": scope_id,
                "team_profile_psychodynamics": team_profile.get("psychodynamics") or {},
                "group_metrics": ux.get("group_metrics") or {},
            }

        raise ValueError("Unsupported scope_type")

    def _delta_numeric(pre: dict[str, Any], post: dict[str, Any]) -> dict[str, float]:
        out: dict[str, float] = {}
        keys = set(pre.keys()) | set(post.keys())
        for k in sorted(keys):
            a = pre.get(k)
            b = post.get(k)
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                out[str(k)] = float(b) - float(a)
        return out

    def _delta_block_matrices(pre_records: list[dict[str, Any]], post_records: list[dict[str, Any]]) -> dict[str, Any]:
        """Compute a compact delta summary across context×timescale kernel blocks."""
        pre_by_ctx = {str(r.get("context_block") or ""): r for r in pre_records if isinstance(r, dict)}
        post_by_ctx = {str(r.get("context_block") or ""): r for r in post_records if isinstance(r, dict)}
        out: dict[str, Any] = {}

        for ctx in sorted(set(pre_by_ctx.keys()) | set(post_by_ctx.keys())):
            pre = pre_by_ctx.get(ctx) if isinstance(pre_by_ctx.get(ctx), dict) else {}
            post = post_by_ctx.get(ctx) if isinstance(post_by_ctx.get(ctx), dict) else {}
            pre_ts = pre.get("timescales") if isinstance(pre.get("timescales"), dict) else {}
            post_ts = post.get("timescales") if isinstance(post.get("timescales"), dict) else {}

            ctx_out: dict[str, Any] = {}
            for ts in sorted(set(pre_ts.keys()) | set(post_ts.keys())):
                a = pre_ts.get(ts) if isinstance(pre_ts.get(ts), dict) else {}
                b = post_ts.get(ts) if isinstance(post_ts.get(ts), dict) else {}
                Ka = a.get("kernel") if isinstance(a.get("kernel"), list) else None
                Kb = b.get("kernel") if isinstance(b.get("kernel"), list) else None
                kernel_change = float(frobenius_diff(Ka, Kb)) if isinstance(Ka, list) and isinstance(Kb, list) else 0.0  # type: ignore[arg-type]
                ctx_out[ts] = {
                    "kernel_change": kernel_change,
                    "entropy_rate_delta": (
                        float(b.get("entropy_rate")) - float(a.get("entropy_rate"))
                        if isinstance(a.get("entropy_rate"), (int, float)) and isinstance(b.get("entropy_rate"), (int, float))
                        else 0.0
                    ),
                    "mean_certainty_delta": (
                        float(b.get("mean_certainty")) - float(a.get("mean_certainty"))
                        if isinstance(a.get("mean_certainty"), (int, float)) and isinstance(b.get("mean_certainty"), (int, float))
                        else 0.0
                    ),
                }
            out[ctx] = ctx_out
        return out

    def _store_counts() -> dict[str, int]:
        if hasattr(store, "counts") and callable(getattr(store, "counts")):
            return store.counts()  # type: ignore[no-any-return]
        # In-memory store.
        return {
            "events_raw": int(len(getattr(store, "events_raw", {}) or {})),
            "events_classified": int(len(getattr(store, "events_classified", {}) or {})),
            "user_profiles": int(len(getattr(store, "user_profiles", {}) or {})),
            "team_profiles": int(len(getattr(store, "team_profiles", {}) or {})),
            "feed_rankings_cache": int(len(getattr(store, "feed_rankings_cache", {}) or {})),
            "decide_rankings_cache": int(len(getattr(store, "decide_rankings_cache", {}) or {})),
            "org_fsa_state": int(len(getattr(store, "org_fsa_state", {}) or {})),
            "project_fsa_state": int(len(getattr(store, "project_fsa_state", {}) or {})),
            "interactions": int(len(getattr(store, "interactions", {}) or {})),
            "memcubes": int(len(getattr(store, "memcubes", {}) or {})),
            "ux_interventions": int(len(getattr(store, "ux_interventions", {}) or {})),
            "ux_intervention_runs": int(len(getattr(store, "ux_intervention_runs", {}) or {})),
            "wellbeing_windows": int(len(getattr(store, "wellbeing_windows", {}) or {})),
            "ux_exposures": int(len(getattr(store, "ux_exposures", {}) or {})),
            "psychodynamic_block_matrices": int(len(getattr(store, "psychodynamic_block_matrices", {}) or {})),
            "psychodynamic_influence_layers": int(len(getattr(store, "psychodynamic_influence_layers", {}) or {})),
        }

    def _reset_inmemory_store() -> None:
        if not isinstance(store, InMemoryStore):
            raise ValueError("reset supported only for InMemoryStore")
        store.events_raw.clear()
        store.events_classified.clear()
        store.user_profiles.clear()
        store.team_profiles.clear()
        store.decide_rankings_cache.clear()
        store.feed_rankings_cache.clear()
        store.org_fsa_state.clear()
        store.project_fsa_state.clear()
        store.interactions.clear()
        store.memcubes.clear()
        store.ux_interventions.clear()
        store.ux_intervention_runs.clear()
        store.wellbeing_windows.clear()
        store.ux_exposures.clear()
        store.psychodynamic_block_matrices.clear()
        store.psychodynamic_influence_layers.clear()
        store.team_to_org.clear()
        store.user_to_org.clear()

    def _load_sample_events(kind: str) -> list[dict[str, Any]]:
        kind = (kind or "").strip().lower()
        filename = "sample_discourse_events.json" if kind in {"discourse", "sample_discourse"} else "sample_events.json"
        path = Path(__file__).resolve().parents[1] / "data" / filename
        if not path.exists():
            raise FileNotFoundError(str(path))
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError(f"{filename} must contain a JSON list")
        return [e for e in data if isinstance(e, dict)]

    def _ingest_raw_event(
        raw: dict[str, Any],
        *,
        llm_mode: str,
        update_profiles: bool,
        team_layer_mode: str | None = None,
    ) -> str:
        with LatencyTracker("event_ingest"):
            store.upsert_event_raw(raw)

        classified = classify_event(raw, llm_mode=llm_mode)
        # Normalize context and attach derived context partition info for downstream pipelines.
        ctx = classified.get("context")
        if not isinstance(ctx, dict):
            ctx = {}

        # Ensure raw event_data is always present (and wins on conflicts).
        raw_event_data = raw.get("event_data") if isinstance(raw.get("event_data"), dict) else {}
        ctx_event_data = ctx.get("event_data") if isinstance(ctx.get("event_data"), dict) else {}
        ctx["event_data"] = {**ctx_event_data, **raw_event_data}

        # Merge raw context signals (authoritative) without clobbering event_data.
        raw_ctx = raw.get("context") if isinstance(raw.get("context"), dict) else {}
        for k, v in raw_ctx.items():
            if k == "event_data":
                continue
            ctx[k] = v

        # Paper-aligned context block (Task/Role/Value) with provenance fields.
        try:
            info = context_block_info_from_event({**classified, "context": ctx})
            ctx["context_block"] = info.get("context_block")
            ctx["context_block_source"] = info.get("source")
            ctx["context_block_field"] = info.get("field")
            ctx["context_block_reason"] = info.get("reason")
        except Exception:
            # Keep ingestion robust; fallback is to omit the block.
            pass

        # Store per-event psychodynamic label (Animals) for transparent debugging.
        animal_state: int | None = None
        try:
            animal_state = int(classify_animal_event({**classified, "context": ctx}))
            ctx["animal_state"] = int(animal_state)
            ctx["animal"] = animal_name(int(animal_state))
        except Exception:
            animal_state = None

        classified["context"] = ctx
        store.upsert_event_classified(classified)
        collector.record_event_processed()

        interaction = interaction_from_event(raw_event=raw, classified_event=classified)
        if interaction is not None:
            store.upsert_interaction(interaction)

        org_id = str(raw.get("org_id") or "")
        user_id = str(raw.get("user_id") or "")
        team_id = raw.get("team_id")

        # Incrementally persist user block-matrix kernels (context × timescale) for fast reads.
        try:
            if org_id and user_id and hasattr(store, "get_psychodynamic_block_matrix"):
                context_block = str(ctx.get("context_block") or context_block_from_event(classified)).strip().lower()
                prev = store.get_psychodynamic_block_matrix(
                    org_id=org_id,
                    scope_type="user",
                    scope_id=user_id,
                    context_block=context_block,
                    pipeline_version=PIPELINE_VERSION,
                )
                if animal_state is None:
                    animal_state = int(classify_animal_event(classified))
                updated = update_online_block_matrix_record(
                    prev,
                    org_id=org_id,
                    scope_type="user",
                    scope_id=user_id,
                    context_block=context_block,
                    pipeline_version=PIPELINE_VERSION,
                    updated_at=utc_now_iso8601(),
                    new_state=int(animal_state),
                )
                store.upsert_psychodynamic_block_matrix(updated)
                collector.record_kernel_update()
        except Exception:
            # Do not block ingestion if incremental storage fails (dev-safe default).
            pass

        # Update online Transfer Entropy tracking for team influence graphs.
        # This incrementally updates pairwise TE sufficient statistics without
        # requiring full recomputation of team profiles.
        try:
            if org_id and team_id and user_id and animal_state is not None:
                te_updates = update_te_on_event(
                    {
                        "org_id": org_id,
                        "team_id": team_id,
                        "user_id": user_id,
                        "timestamp": raw.get("timestamp"),
                    },
                    animal_state=int(animal_state),
                )
                # Optionally persist incremental TE updates (for debugging/audit).
                if te_updates and hasattr(store, "upsert_te_incremental"):
                    store.upsert_te_incremental(
                        org_id=org_id,
                        team_id=team_id,
                        updates=te_updates,
                        timestamp=raw.get("timestamp") or utc_now_iso8601(),
                    )
                if te_updates:
                    collector.record_te_computation()
        except Exception:
            # Do not block ingestion if TE update fails.
            pass

        action = str(classified.get("action") or raw.get("event_type") or "")
        timestamp = str(raw.get("timestamp") or "")
        event_id = str(raw.get("event_id") or "")
        if org_id and action:
            prev = store.get_org_fsa_state(org_id=org_id)
            org_state = apply_org_fsa_event(
                prev,
                org_id=org_id,
                action=action,
                context={
                    "event_data": raw.get("event_data") or {},
                    "project_id": raw.get("project_id"),
                    "team_id": raw.get("team_id"),
                },
                event_id=event_id or None,
                user_id=user_id or None,
                timestamp=timestamp or None,
            )
            store.upsert_org_fsa_state(org_state)

            project_id = raw.get("project_id")
            if isinstance(project_id, str) and project_id:
                prev_proj = store.get_project_fsa_state(org_id=org_id, project_id=project_id)
                proj_state = apply_project_fsa_event(
                    prev_proj,
                    org_id=org_id,
                    project_id=project_id,
                    action=action,
                    context={"event_data": raw.get("event_data") or {}},
                    event_id=event_id or None,
                    user_id=user_id or None,
                    timestamp=timestamp or None,
                )
                store.upsert_project_fsa_state(proj_state)

        if update_profiles:
            window_end = timestamp or utc_now_iso8601()
            if org_id and user_id:
                user_events = store.list_user_classified_events(org_id=org_id, user_id=user_id)
                user_profile = build_user_profile(user_id, org_id, user_events)
                _upsert_user_profile(
                    user_profile,
                    org_id=org_id,
                    user_id=user_id,
                    pipeline_version=PIPELINE_VERSION,
                    updated_at=user_profile.get("updated_at") if isinstance(user_profile, dict) else None,
                )
                try:
                    psych = user_profile.get("psychodynamics") if isinstance(user_profile, dict) else {}
                    psych = psych if isinstance(psych, dict) else {}
                    wb = psych.get("wellbeing_proxies") if isinstance(psych.get("wellbeing_proxies"), dict) else {}
                    _log_wellbeing_window(
                        org_id=org_id,
                        scope_type="user",
                        scope_id=user_id,
                        window_end=window_end,
                        pipeline_version=PIPELINE_VERSION,
                        proxies=wb or {},
                        metadata={"source": "event_ingest", "event_id": event_id, "user_id": user_id},
                    )
                except Exception:
                    pass

            if org_id and isinstance(team_id, str) and team_id:
                team_events = store.list_team_classified_events(org_id=org_id, team_id=team_id)
                member_ids = sorted({str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)})
                member_profiles: list[dict[str, Any]] = []
                for uid in member_ids:
                    prof = store.get_user_profile(org_id=org_id, user_id=uid)
                    if prof is None:
                        u_events = store.list_user_classified_events(org_id=org_id, user_id=uid)
                        prof = build_user_profile(uid, org_id, u_events)
                        _upsert_user_profile(
                            prof,
                            org_id=org_id,
                            user_id=uid,
                            pipeline_version=PIPELINE_VERSION,
                            updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
                        )
                    member_profiles.append(prof)
                layer_mode = str(team_layer_mode or "").strip().lower()
                if layer_mode not in {"off", "auto", "on"}:
                    layer_mode = "off"
                team_cfg = {"layer_mode": layer_mode} if layer_mode else {}
                team_profile = build_team_profile(
                    team_id,
                    org_id,
                    member_profiles,
                    team_events,
                    psychodynamics_config=team_cfg or None,
                )
                _upsert_team_profile(
                    team_profile,
                    org_id=org_id,
                    team_id=team_id,
                    pipeline_version=PIPELINE_VERSION,
                    updated_at=window_end,
                )
                try:
                    psych = team_profile.get("psychodynamics") if isinstance(team_profile, dict) else {}
                    psych = psych if isinstance(psych, dict) else {}
                    wb = psych.get("wellbeing_proxies") if isinstance(psych.get("wellbeing_proxies"), dict) else {}
                    _log_wellbeing_window(
                        org_id=org_id,
                        scope_type="team",
                        scope_id=team_id,
                        window_end=window_end,
                        pipeline_version=PIPELINE_VERSION,
                        proxies=wb or {},
                        metadata={"source": "event_ingest", "event_id": event_id, "team_id": team_id},
                    )
                except Exception:
                    pass

        return str(raw.get("event_id") or "")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "pipeline_version": PIPELINE_VERSION, "store": store.__class__.__name__}

    if enable_monitoring:
        @app.get("/intelligence/monitoring/health")
        def monitoring_health() -> dict[str, Any]:
            status = get_health_status(store=store)
            return {
                "healthy": status.healthy,
                "status": status.status,
                "checks": status.checks,
                "metrics": status.metrics,
                "warnings": status.warnings,
                "errors": status.errors,
            }

        @app.get("/intelligence/monitoring/summary")
        def monitoring_summary() -> dict[str, Any]:
            summary = collector.get_summary()
            return {
                "latency_p50": summary.latency_p50,
                "latency_p95": summary.latency_p95,
                "latency_p99": summary.latency_p99,
                "latency_mean": summary.latency_mean,
                "latency_count": summary.latency_count,
                "interventions_applied": summary.interventions_applied,
                "interventions_measured": summary.interventions_measured,
                "effects_positive": summary.effects_positive,
                "effects_negative": summary.effects_negative,
                "effects_neutral": summary.effects_neutral,
                "mean_effect_size": summary.mean_effect_size,
                "events_processed": summary.events_processed,
                "profiles_updated": summary.profiles_updated,
                "te_computations": summary.te_computations,
                "kernel_updates": summary.kernel_updates,
                "errors": summary.errors,
                "warnings": summary.warnings,
            }

        @app.get("/intelligence/monitoring/metrics")
        def monitoring_metrics() -> Response:
            return Response(export_prometheus_metrics(), media_type="text/plain")

        @app.get("/metrics", include_in_schema=False)
        def metrics_root() -> Response:
            return Response(export_prometheus_metrics(), media_type="text/plain")

    @app.get("/", include_in_schema=False)
    def root() -> Any:
        if enable_debug:
            return RedirectResponse(url="/ui")
        return health()

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon() -> Any:
        # Avoid noisy 404 logs in local dev.
        return Response(status_code=204)

    if enable_debug:
        from backend.ui import render_ui_html

        @app.get("/ui", response_class=HTMLResponse)
        def ui() -> Any:
            return HTMLResponse(
                render_ui_html(pipeline_version=PIPELINE_VERSION),
                headers={"Cache-Control": "no-store"},
            )

        @app.get("/intelligence/debug/counts")
        def debug_counts() -> dict[str, Any]:
            return {"store": store.__class__.__name__, "counts": _store_counts()}

        @app.post("/intelligence/debug/reset")
        def debug_reset() -> dict[str, Any]:
            try:
                _reset_inmemory_store()
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e
            return {"ok": True, "counts": _store_counts()}

        @app.get("/intelligence/debug/sample-events")
        def debug_sample_events(*, kind: str = "discourse") -> dict[str, Any]:
            try:
                events = _load_sample_events(kind)
            except Exception as e:  # noqa: BLE001
                raise HTTPException(status_code=404, detail=f"sample events not available: {e}") from e
            return {"kind": kind, "count": len(events), "events": events}

        @app.get("/intelligence/debug/events/raw")
        def debug_list_raw_events(
            *,
            org_id: Optional[str] = None,
            team_id: Optional[str] = None,
            user_id: Optional[str] = None,
            limit: int = 200,
        ) -> dict[str, Any]:
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")
            events = store.list_raw_events(org_id=resolved_org, limit=max(1, int(limit)))
            if isinstance(team_id, str) and team_id:
                events = [e for e in events if e.get("team_id") == team_id]
            if isinstance(user_id, str) and user_id:
                events = [e for e in events if e.get("user_id") == user_id or e.get("subject_id") == user_id]
            return {"org_id": resolved_org, "count": len(events), "events": events}

        @app.get("/intelligence/debug/telemetry/instrumentation")
        def debug_telemetry_instrumentation(
            *,
            org_id: Optional[str] = None,
            team_id: Optional[str] = None,
            user_id: Optional[str] = None,
            limit: int = 2000,
            require_explicit: bool = True,
            required_field_coverage: Optional[float] = None,
            team_id_coverage: Optional[float] = None,
            explicit_block_coverage: Optional[float] = None,
            explicit_phase_coverage: Optional[float] = None,
        ) -> dict[str, Any]:
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")

            events = store.list_raw_events(org_id=resolved_org, limit=max(1, int(limit)))
            if isinstance(team_id, str) and team_id:
                events = [e for e in events if e.get("team_id") == team_id]
            if isinstance(user_id, str) and user_id:
                events = [e for e in events if e.get("user_id") == user_id or e.get("subject_id") == user_id]

            thresholds: dict[str, float] = {}
            if required_field_coverage is not None:
                thresholds["required_field_coverage"] = float(required_field_coverage)
            if team_id_coverage is not None:
                thresholds["team_id_coverage"] = float(team_id_coverage)
            if explicit_block_coverage is not None:
                thresholds["explicit_block_coverage"] = float(explicit_block_coverage)
            if explicit_phase_coverage is not None:
                thresholds["explicit_phase_coverage"] = float(explicit_phase_coverage)

            report = build_telemetry_instrumentation_report(
                events,
                thresholds=thresholds or None,
                require_explicit=bool(require_explicit),
            )
            return {"org_id": resolved_org, "count": len(events), "report": report}

        @app.get("/intelligence/debug/events/classified")
        def debug_list_classified_events(
            *,
            org_id: Optional[str] = None,
            team_id: Optional[str] = None,
            user_id: Optional[str] = None,
            limit: int = 200,
        ) -> dict[str, Any]:
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")

            events: list[dict[str, Any]]
            if isinstance(team_id, str) and team_id:
                events = store.list_team_classified_events(org_id=resolved_org, team_id=team_id, limit=max(1, int(limit)))
            elif isinstance(user_id, str) and user_id:
                events = store.list_user_classified_events(org_id=resolved_org, user_id=user_id, limit=max(1, int(limit)))
            else:
                # Postgres store doesn't expose an org-level classified list; fall back to in-memory if available.
                if hasattr(store, "events_classified"):
                    rows = [e for e in (getattr(store, "events_classified", {}) or {}).values() if e.get("org_id") == resolved_org]
                    rows.sort(key=lambda r: str(r.get("timestamp") or ""))
                    events = rows[-max(1, int(limit)) :]
                else:
                    raise HTTPException(status_code=400, detail="team_id or user_id is required for classified events.")

            return {"org_id": resolved_org, "count": len(events), "events": events}

        @app.post("/intelligence/debug/psychodynamics/block-matrix/recompute")
        def debug_recompute_block_matrix(
            *,
            org_id: Optional[str] = None,
            user_id: Optional[str] = None,
            team_id: Optional[str] = None,
            scope_type: str = "user",
            pipeline_version: Optional[str] = None,
            limit: int = 5000,
        ) -> dict[str, Any]:
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")

            scope_type = str(scope_type or "").strip().lower()
            if scope_type not in {"user", "team"}:
                raise HTTPException(status_code=400, detail="scope_type must be user|team")

            scope_id = user_id if scope_type == "user" else team_id
            if not isinstance(scope_id, str) or not scope_id:
                raise HTTPException(status_code=400, detail="scope_id is required (user_id or team_id)")

            if scope_type == "user":
                events = store.list_user_classified_events(
                    org_id=resolved_org,
                    user_id=scope_id,
                    limit=max(1, int(limit)),
                )
            else:
                events = store.list_team_classified_events(
                    org_id=resolved_org,
                    team_id=scope_id,
                    limit=max(1, int(limit)),
                )

            pv = str(pipeline_version or PIPELINE_VERSION)
            records = _recompute_block_matrices_for_scope(
                store=store,
                events=events,
                org_id=resolved_org,
                scope_type=scope_type,
                scope_id=scope_id,
                pipeline_version=pv,
            )

            return {
                "org_id": resolved_org,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "pipeline_version": pv,
                "records": records,
                "event_count": len(events),
            }

        @app.get("/intelligence/debug/paper-report")
        def debug_paper_report(
            *,
            org_id: Optional[str] = None,
            team_id: Optional[str] = None,
            user_id: Optional[str] = None,
            limit: int = 500,
            llm_mode: str = "off",
            pipeline_version: Optional[str] = None,
            significance_mode: str = "on",
            layer_mode: str = "on",
            window: int = 200,
        ) -> dict[str, Any]:
            """Generate a deterministic “paper report” for transparency (dev-only)."""
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")

            events = store.list_raw_events(org_id=resolved_org, limit=max(1, int(limit)))
            if isinstance(team_id, str) and team_id:
                events = [e for e in events if e.get("team_id") == team_id]
            if isinstance(user_id, str) and user_id:
                events = [e for e in events if e.get("user_id") == user_id]

            cfg = {
                "significance_mode": str(significance_mode or "on"),
                "layer_mode": str(layer_mode or "on"),
                "window": int(window),
            }
            pv = str(pipeline_version or PIPELINE_VERSION)
            try:
                return build_paper_report(
                    events,
                    llm_mode=llm_mode,
                    pipeline_version=pv,
                    team_psychodynamics_config=cfg,
                    report_mode="audit",
                )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e

        @app.get("/intelligence/paper-report")
        def paper_report(
            *,
            org_id: Optional[str] = None,
            team_id: Optional[str] = None,
            user_id: Optional[str] = None,
            limit: int = 500,
            llm_mode: str = "auto",
            pipeline_version: Optional[str] = None,
            significance_mode: str = "on",
            layer_mode: str = "on",
            window: int = 200,
        ) -> dict[str, Any]:
            """Generate a production-ready paper report from live telemetry."""
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")

            events = store.list_raw_events(org_id=resolved_org, limit=max(1, int(limit)))
            if isinstance(team_id, str) and team_id:
                events = [e for e in events if e.get("team_id") == team_id]
            if isinstance(user_id, str) and user_id:
                events = [e for e in events if e.get("user_id") == user_id]

            cfg = {
                "significance_mode": str(significance_mode or "on"),
                "layer_mode": str(layer_mode or "on"),
                "window": int(window),
            }
            pv = str(pipeline_version or PIPELINE_VERSION)

            memcubes: list[dict[str, Any]] = []
            memcubes.extend(_state_schema_memcubes_for_org(resolved_org))
            scope_type = "team" if isinstance(team_id, str) and team_id else "user"
            scope_id = team_id if scope_type == "team" else user_id
            if isinstance(scope_id, str) and scope_id:
                telemetry = _psychodynamics_memcube_for_scope(
                    org_id=resolved_org,
                    scope_type=scope_type,
                    scope_id=scope_id,
                    pipeline_version=pv,
                    source="paper-report",
                )
                if isinstance(telemetry, dict):
                    memcubes.append(telemetry)

            try:
                return build_paper_report(
                    events,
                    llm_mode=llm_mode,
                    pipeline_version=pv,
                    team_psychodynamics_config=cfg,
                    report_mode="production",
                    memcubes=memcubes if memcubes else None,
                )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e

        @app.get("/intelligence/debug/ux-policy/eval")
        def debug_ux_policy_eval(
            *,
            org_id: Optional[str] = None,
            team_id: Optional[str] = None,
            user_id: Optional[str] = None,
            scope_type: str = "team",
            scope_id: Optional[str] = None,
            limit: int = 500,
        ) -> dict[str, Any]:
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")

            scope_type = str(scope_type or "team").strip().lower()
            sid = str(scope_id or "").strip()
            if not sid:
                if scope_type == "team":
                    sid = str(team_id or "").strip()
                elif scope_type == "user":
                    sid = str(user_id or "").strip()
            if not sid:
                raise HTTPException(status_code=400, detail="scope_id (or team_id/user_id) is required.")

            try:
                runs = store.list_ux_intervention_runs(
                    org_id=resolved_org,
                    scope_type=scope_type,
                    scope_id=sid,
                    limit=int(limit),
                )
            except Exception:
                runs = []

            summary = offline_policy_eval(runs)
            cfg = _ux_policy_config()
            return {
                "org_id": resolved_org,
                "scope_type": scope_type,
                "scope_id": sid,
                "policy_mode": cfg.get("mode"),
                "summary": summary,
            }

        @app.get("/intelligence/debug/replay-snapshot")
        def debug_replay_snapshot(
            *,
            org_id: Optional[str] = None,
            team_id: Optional[str] = None,
            user_id: Optional[str] = None,
            max_events: int = 25,
            limit: int = 2000,
            include_timeline: bool = True,
            include_events: bool = True,
            include_hypergraph: bool = True,
            llm_mode: str = "off",
            window: int = 200,
            layer_mode: str = "on",
            significance_mode: str = "auto",
            direction_limit: int = 10,
        ) -> dict[str, Any]:
            """Compute a non-mutating snapshot for replay/scrubbing UIs (dev-only).

            This endpoint is designed for interactive frontend demos:
              - it takes the already-ingested raw events in the store
              - selects the first `max_events` chronologically
              - computes intermediate artifacts for Discourse → Decide → Do without writing to the store
            """
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")

            raw_events = store.list_raw_events(org_id=resolved_org, limit=max(1, int(limit)))
            if isinstance(team_id, str) and team_id:
                raw_events = [e for e in raw_events if e.get("team_id") == team_id]
            if isinstance(user_id, str) and user_id and not (isinstance(team_id, str) and team_id):
                # Only filter by user if team_id isn't provided; otherwise team replay stays team-wide.
                raw_events = [e for e in raw_events if e.get("user_id") == user_id]

            raw_events.sort(key=lambda r: str(r.get("timestamp") or ""))
            total = len(raw_events)

            def _enrich_classified(raw: dict[str, Any]) -> dict[str, Any]:
                classified = classify_event(raw, llm_mode=llm_mode)

                ctx = classified.get("context")
                if not isinstance(ctx, dict):
                    ctx = {}

                # Ensure raw event_data always exists (and wins on conflicts).
                raw_event_data = raw.get("event_data") if isinstance(raw.get("event_data"), dict) else {}
                ctx_event_data = ctx.get("event_data") if isinstance(ctx.get("event_data"), dict) else {}
                ctx["event_data"] = {**ctx_event_data, **raw_event_data}

                # Merge raw context signals without clobbering event_data.
                raw_ctx = raw.get("context") if isinstance(raw.get("context"), dict) else {}
                for k, v in raw_ctx.items():
                    if k == "event_data":
                        continue
                    ctx[k] = v

                # Paper-aligned context block (Task/Role/Value) with provenance fields.
                try:
                    info = context_block_info_from_event({**classified, "context": ctx})
                    ctx["context_block"] = info.get("context_block")
                    ctx["context_block_source"] = info.get("source")
                    ctx["context_block_field"] = info.get("field")
                    ctx["context_block_reason"] = info.get("reason")
                except Exception:
                    pass

                # Per-event Animal state label for visualization.
                try:
                    animal_state = int(classify_animal_event({**classified, "context": ctx}))
                    ctx["animal_state"] = int(animal_state)
                    ctx["animal"] = animal_name(int(animal_state))
                except Exception:
                    pass

                classified["context"] = ctx
                return classified

            # Clamp `max_events`.
            m = int(max_events)
            if m < 0:
                m = 0
            if m > total:
                m = total

            if total == 0:
                return {
                    "org_id": resolved_org,
                    "team_id": team_id,
                    "total_events": 0,
                    "max_events": 0,
                    "timeline": [],
                    "snapshot": None,
                }

            # If requested, compute a full timeline with derived fields (for slider labeling).
            timeline: list[dict[str, Any]] = []
            classified_all: list[dict[str, Any]] | None = None
            if bool(include_timeline):
                classified_all = [_enrich_classified(e) for e in raw_events]
                for i, ev in enumerate(classified_all):
                    ctx = ev.get("context") if isinstance(ev.get("context"), dict) else {}
                    timeline.append(
                        {
                            "i": i + 1,
                            "event_id": ev.get("event_id"),
                            "timestamp": ev.get("timestamp"),
                            "user_id": ev.get("user_id"),
                            "action": ev.get("action"),
                            "object_type": ev.get("object_type"),
                            "object_id": ev.get("object_id"),
                            "context_block": ctx.get("context_block"),
                            "animal": ctx.get("animal"),
                            "classification_confidence": ev.get("classification_confidence"),
                        }
                    )

            # Build subset.
            subset_raw = raw_events[:m]
            if not subset_raw:
                return {
                    "org_id": resolved_org,
                    "team_id": team_id,
                    "total_events": total,
                    "max_events": m,
                    "timeline": timeline if include_timeline else None,
                    "snapshot": None,
                }

            classified_subset = (
                (classified_all or [])[:m] if classified_all is not None else [_enrich_classified(e) for e in subset_raw]
            )

            team_id_local = str(team_id or subset_raw[0].get("team_id") or "team_001")
            user_ids = sorted({str(e.get("user_id")) for e in subset_raw if isinstance(e.get("user_id"), str)})

            member_profiles: list[dict[str, Any]] = []
            for uid in user_ids:
                member_profiles.append(build_user_profile(uid, resolved_org, classified_subset))

            team_profile = build_team_profile(
                team_id_local,
                resolved_org,
                member_profiles,
                classified_subset,
                psychodynamics_config={
                    "window": int(window),
                    "layer_mode": str(layer_mode),
                    "significance_mode": str(significance_mode),
                },
            )

            # Discourse → Decide artifacts.
            try:
                insights = extract_insights(org_id=resolved_org, events=subset_raw, llm_mode=llm_mode)
            except Exception:
                insights = []
            try:
                cards = build_knowledge_cards(org_id=resolved_org, insights=insights)
            except Exception:
                cards = []
            try:
                directions = generate_strategic_directions(
                    org_id=resolved_org, cards=cards, max_directions=max(1, int(direction_limit))
                )
            except Exception:
                directions = []

            ranked: list[dict[str, Any]] = []
            hg_payload: dict[str, Any] | None = None
            user_weights: dict[str, float] | None = None
            if directions and user_ids:
                try:
                    user_weights = derive_user_weights_from_profiles(member_profiles)
                except Exception:
                    user_weights = None
                try:
                    hg = build_hypergraph(user_ids=user_ids, insights=insights, cards=cards, directions=directions)
                    hg = _maybe_refine_hypergraph(hg)
                    ranked = rank_directions_for_group(
                        user_ids=user_ids,
                        hg=hg,
                        user_weights=user_weights or {},
                        limit=max(1, int(direction_limit)),
                    )
                    if bool(include_hypergraph):
                        hg_payload = {
                            "nodes": len(hg.node_ids),
                            "edges": len(hg.edges),
                            "node_ids": hg.node_ids,
                            "node_types": hg.node_types,
                            "hyperedges": hg.edges,
                            "edge_weights": hg.edge_weights,
                        }
                except Exception:
                    ranked = []
                    hg_payload = None

            # Do: simulate org/project FSA state evolution on the subset.
            org_state: dict[str, Any] | None = None
            project_states: dict[str, Any] = {}
            for raw, classified in zip(subset_raw, classified_subset):
                action = str(classified.get("action") or raw.get("event_type") or "")
                if not action:
                    continue
                ts = str(raw.get("timestamp") or "") or None
                eid = str(raw.get("event_id") or "") or None
                uid = str(raw.get("user_id") or "") or None
                org_state = apply_org_fsa_event(
                    org_state,
                    org_id=resolved_org,
                    action=action,
                    context={"event_data": raw.get("event_data") or {}},
                    event_id=eid,
                    user_id=uid,
                    timestamp=ts,
                )

                project_id = raw.get("project_id")
                if isinstance(project_id, str) and project_id:
                    prev_proj = project_states.get(project_id)
                    project_states[project_id] = apply_project_fsa_event(
                        prev_proj,
                        org_id=resolved_org,
                        project_id=project_id,
                        action=action,
                        context={"event_data": raw.get("event_data") or {}},
                        event_id=eid,
                        user_id=uid,
                        timestamp=ts,
                    )

            # Surface a small, inspectable subset of events for UI detail panes.
            events_out: list[dict[str, Any]] | None = None
            if bool(include_events):
                events_out = []
                for raw, classified in zip(subset_raw, classified_subset):
                    ctx = classified.get("context") if isinstance(classified.get("context"), dict) else {}
                    events_out.append(
                        {
                            "event_id": raw.get("event_id"),
                            "timestamp": raw.get("timestamp"),
                            "user_id": raw.get("user_id"),
                            "team_id": raw.get("team_id"),
                            "project_id": raw.get("project_id"),
                            "event_type": raw.get("event_type"),
                            "action": classified.get("action"),
                            "object_type": classified.get("object_type"),
                            "object_id": classified.get("object_id"),
                            "context_block": ctx.get("context_block"),
                            "animal": ctx.get("animal"),
                            "context_block_source": ctx.get("context_block_source"),
                            "classification_confidence": classified.get("classification_confidence"),
                            "event_data": ctx.get("event_data") if isinstance(ctx.get("event_data"), dict) else {},
                        }
                    )

            return {
                "org_id": resolved_org,
                "team_id": team_id_local,
                "total_events": total,
                "max_events": m,
                "timeline": timeline if include_timeline else None,
                "snapshot": {
                    "range": {
                        "events_included": m,
                        "start_ts": str(subset_raw[0].get("timestamp") or ""),
                        "end_ts": str(subset_raw[-1].get("timestamp") or ""),
                    },
                    "events": events_out,
                    "discourse": {
                        "insights": insights,
                        "cards": cards,
                        "directions": directions,
                    },
                    "decide": {
                        "ranked": ranked,
                        "hypergraph": hg_payload,
                        "user_weights": user_weights,
                    },
                    "psychometrics": {
                        "users": [
                            {
                                "user_id": p.get("user_id"),
                                "psychodynamics": p.get("psychodynamics"),
                                "pattern_summary": p.get("pattern_summary"),
                            }
                            for p in member_profiles
                        ],
                        "team": {
                            "team_id": team_profile.get("team_id"),
                            "psychodynamics": team_profile.get("psychodynamics"),
                            "collective_patterns": team_profile.get("collective_patterns"),
                        },
                    },
                    "fsa": {"org": org_state, "projects": project_states},
                },
            }

        @app.post("/intelligence/debug/ingest-sample")
        def debug_ingest_sample(
            *,
            kind: str = "discourse",
            llm_mode: str = "off",
            update_profiles: bool = True,
            team_layer_mode: str = "auto",
        ) -> dict[str, Any]:
            try:
                events = _load_sample_events(kind)
            except Exception as e:  # noqa: BLE001
                raise HTTPException(status_code=404, detail=f"sample events not available: {e}") from e

            ingested: list[str] = []
            for raw in events:
                event_id = _ingest_raw_event(
                    raw,
                    llm_mode=llm_mode,
                    update_profiles=bool(update_profiles),
                    team_layer_mode=team_layer_mode,
                )
                ingested.append(event_id)
            return {"ok": True, "kind": kind, "ingested": len(ingested), "counts": _store_counts()}

        @app.get("/intelligence/debug/metrics/discourse")
        def debug_discourse_metrics(*, org_id: Optional[str] = None, team_id: Optional[str] = None) -> dict[str, Any]:
            resolved_org = org_id or store.resolve_org_id(team_id=team_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")

            raw_events = store.list_raw_events(org_id=resolved_org)
            insights = extract_insights(org_id=resolved_org, events=raw_events, llm_mode="off")
            cards = build_knowledge_cards(org_id=resolved_org, insights=insights)

            # Card coherence: mean similarity between each insight embedding and its card centroid.
            by_insight = {str(i.get("insight_id")): i for i in insights if isinstance(i.get("insight_id"), str)}
            intra_sims: list[float] = []
            card_sizes: list[int] = []
            for card in cards:
                centroid = card.get("embedding")
                if not isinstance(centroid, list) or not centroid:
                    continue
                ids = card.get("insight_ids") if isinstance(card.get("insight_ids"), list) else []
                ids = [str(x) for x in ids if isinstance(x, str)]
                card_sizes.append(len(ids))
                for iid in ids:
                    ins = by_insight.get(iid)
                    emb = ins.get("embedding") if isinstance(ins, dict) else None
                    if isinstance(emb, list) and emb:
                        intra_sims.append(float(cosine_similarity(centroid, emb)))

            # Inter-card similarity (centroids).
            centroids = [c.get("embedding") for c in cards if isinstance(c.get("embedding"), list) and c.get("embedding")]
            inter_sims: list[float] = []
            for i in range(len(centroids)):
                for j in range(i + 1, len(centroids)):
                    inter_sims.append(float(cosine_similarity(centroids[i], centroids[j])))

            def _mean(xs: list[float]) -> float:
                return float(sum(xs) / len(xs)) if xs else 0.0

            def _min(xs: list[int]) -> int:
                return int(min(xs)) if xs else 0

            def _max(xs: list[int]) -> int:
                return int(max(xs)) if xs else 0

            types: dict[str, int] = {}
            for ins in insights:
                t = str(ins.get("insight_type") or "other")
                types[t] = int(types.get(t, 0)) + 1

            per_user: dict[str, int] = {}
            for ins in insights:
                uid = str(ins.get("user_id") or "")
                if uid:
                    per_user[uid] = int(per_user.get(uid, 0)) + 1

            return {
                "org_id": resolved_org,
                "events": len(raw_events),
                "insights": {"count": len(insights), "types": types, "per_user": per_user},
                "cards": {
                    "count": len(cards),
                    "size_min": _min(card_sizes),
                    "size_max": _max(card_sizes),
                    "size_mean": float(sum(card_sizes) / len(card_sizes)) if card_sizes else 0.0,
                    "intra_similarity_mean": _mean(intra_sims),
                    "inter_similarity_mean": _mean(inter_sims),
                },
            }

        @app.get("/intelligence/debug/profiles/user")
        def debug_user_profile(
            *,
            user_id: str,
            org_id: Optional[str] = None,
            team_id: Optional[str] = None,
        ) -> dict[str, Any]:
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
            prof = store.get_user_profile(org_id=resolved_org, user_id=user_id)
            if prof is None:
                events = store.list_user_classified_events(org_id=resolved_org, user_id=user_id)
                prof = build_user_profile(user_id, resolved_org, events)
                _upsert_user_profile(
                    prof,
                    org_id=resolved_org,
                    user_id=user_id,
                    pipeline_version=PIPELINE_VERSION,
                    updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
                )
            return prof

        @app.get("/intelligence/debug/drift/user")
        def debug_user_drift(
            *,
            user_id: str,
            org_id: Optional[str] = None,
            team_id: Optional[str] = None,
            window: int = 50,
        ) -> dict[str, Any]:
            resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
            events = store.list_user_classified_events(org_id=resolved_org, user_id=user_id, limit=5000)
            return drift_report_user(events, user_id=user_id, window=int(window))

        @app.get("/intelligence/debug/profiles/team")
        def debug_team_profile(
            *,
            team_id: str,
            org_id: Optional[str] = None,
            recompute: bool = False,
            window: int = 200,
            layer_mode: str = "off",
            significance_mode: str = "auto",
            significance_M: int = 200,
            significance_q: float = 0.05,
        ) -> dict[str, Any]:
            resolved_org = org_id or store.resolve_org_id(team_id=team_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")

            prof = store.get_team_profile(org_id=resolved_org, team_id=team_id)
            if prof is None or bool(recompute):
                team_events = store.list_team_classified_events(org_id=resolved_org, team_id=team_id)
                member_ids = sorted({str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)})
                member_profiles: list[dict[str, Any]] = []
                for uid in member_ids:
                    up = store.get_user_profile(org_id=resolved_org, user_id=uid)
                    if up is None:
                        u_events = store.list_user_classified_events(org_id=resolved_org, user_id=uid)
                        up = build_user_profile(uid, resolved_org, u_events)
                        _upsert_user_profile(
                            up,
                            org_id=resolved_org,
                            user_id=uid,
                            pipeline_version=PIPELINE_VERSION,
                            updated_at=up.get("updated_at") if isinstance(up, dict) else None,
                        )
                    member_profiles.append(up)
                prof = build_team_profile(
                    team_id,
                    resolved_org,
                    member_profiles,
                    team_events,
                    psychodynamics_config={
                        "window": int(window),
                        "layer_mode": str(layer_mode),
                        "significance_mode": str(significance_mode),
                        "significance_M": int(significance_M),
                        "significance_q": float(significance_q),
                    },
                )
                _upsert_team_profile(
                    prof,
                    org_id=resolved_org,
                    team_id=team_id,
                    pipeline_version=PIPELINE_VERSION,
                )
            return prof

    def _dag_view_from_graph(graph: dict[str, Any]) -> dict[str, Any]:
        nodes_raw = graph.get("nodes") if isinstance(graph.get("nodes"), dict) else {}
        edges_raw = graph.get("edges") if isinstance(graph.get("edges"), list) else []

        node_ids: set[str] = {str(k) for k in nodes_raw.keys() if isinstance(k, str) and k}
        edge_pairs: list[tuple[str, str]] = []
        blocked_by: dict[str, list[str]] = {}
        for e in edges_raw:
            if not isinstance(e, dict):
                continue
            if str(e.get("type") or "depends_on") != "depends_on":
                continue
            src = e.get("from")
            dst = e.get("to")
            if not isinstance(src, str) or not src:
                continue
            if not isinstance(dst, str) or not dst:
                continue
            node_ids.add(src)
            node_ids.add(dst)
            edge_pairs.append((src, dst))
            blocked_by.setdefault(dst, []).append(src)

        for nid in node_ids:
            blocked_by.setdefault(nid, [])
        for nid, preds in blocked_by.items():
            blocked_by[nid] = sorted(set(preds))

        topo = topological_sort(nodes=sorted(node_ids), edges=edge_pairs)
        is_dag = topo is not None
        levels = compute_levels(nodes=sorted(node_ids), edges=edge_pairs) if is_dag else {}
        level_groups = [{"level": lvl, "node_ids": ids} for (lvl, ids) in iter_level_groups(levels)]

        return {
            "is_dag": bool(is_dag),
            "topo_order": topo or [],
            "levels": levels,
            "level_groups": level_groups,
            "blocked_by": blocked_by,
            "edge_pairs": edge_pairs,
            "node_ids": sorted(node_ids),
        }

    def _ready_map(*, blocked_by: dict[str, list[str]], state_by_id: dict[str, str], done_states: set[str]) -> dict[str, bool]:
        out: dict[str, bool] = {}
        for nid, preds in blocked_by.items():
            if not preds:
                out[nid] = True
                continue
            ok = True
            for pred in preds:
                if state_by_id.get(pred) not in done_states:
                    ok = False
                    break
            out[nid] = ok
        return out

    def _block_status(
        *,
        meta: dict[str, Any],
        now: datetime,
        state_by_id: dict[str, str],
        done_states: set[str],
        wait_key: str,
    ) -> tuple[bool, dict[str, Any]]:
        if not isinstance(meta, dict):
            return False, {}
        block_type = str(meta.get("block_type") or "").strip().lower()
        blocked_until = meta.get("blocked_until")
        reason = meta.get("block_reason") or meta.get("reason")
        note = meta.get("block_note")

        # Time blocks.
        if block_type == "time" or (isinstance(blocked_until, str) and blocked_until.strip()):
            until_str = blocked_until if isinstance(blocked_until, str) and blocked_until.strip() else None
            if until_str is None:
                return True, {"block_type": "time", "blocked_until": None, "reason": reason, "note": note}
            try:
                until_dt = parse_iso8601(until_str)
                return until_dt > now, {"block_type": "time", "blocked_until": until_str, "reason": reason, "note": note}
            except Exception:
                return True, {"block_type": "time", "blocked_until": until_str, "reason": reason, "note": note}

        # Memory blocks.
        waiting_for = meta.get(wait_key)
        waiting_for = waiting_for if isinstance(waiting_for, str) and waiting_for.strip() else None
        waiting_for_external = meta.get("waiting_for_external")
        if block_type == "memory" or waiting_for is not None or bool(waiting_for_external):
            resolved_by_dependency = waiting_for is not None and state_by_id.get(waiting_for) in done_states
            if resolved_by_dependency and not bool(waiting_for_external):
                return False, {}
            return True, {
                "block_type": "memory",
                "waiting_for": waiting_for,
                "waiting_for_external": bool(waiting_for_external),
                "reason": reason,
                "note": note,
            }

        return False, {}

    @app.get("/intelligence/state/org")
    def get_org_state(*, org_id: Optional[str] = None, team_id: Optional[str] = None) -> dict[str, Any]:
        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        state = store.get_org_fsa_state(org_id=resolved_org)
        if state is None:
            raise HTTPException(status_code=404, detail="org_fsa_state not found; ingest events first.")
        return state

    @app.get("/intelligence/plan/org/projects")
    def get_org_projects_plan(*, org_id: Optional[str] = None, team_id: Optional[str] = None) -> dict[str, Any]:
        """Org-level project dependency plan (DAG) derived from org_fsa_state.project_graph."""
        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        state = store.get_org_fsa_state(org_id=resolved_org)
        if state is None:
            raise HTTPException(status_code=404, detail="org_fsa_state not found; ingest events first.")

        graph = state.get("project_graph") if isinstance(state.get("project_graph"), dict) else {"nodes": {}, "edges": []}
        view = _dag_view_from_graph(graph)
        nodes = graph.get("nodes") if isinstance(graph.get("nodes"), dict) else {}
        state_by_id = {
            str(pid): str(meta.get("state") or "")
            for pid, meta in nodes.items()
            if isinstance(pid, str) and isinstance(meta, dict)
        }
        deps_ready = _ready_map(blocked_by=view["blocked_by"], state_by_id=state_by_id, done_states={"completed"})

        now = datetime.now(timezone.utc)
        blocks: dict[str, dict[str, Any]] = {}
        blocked: dict[str, bool] = {}
        for pid in view["node_ids"]:
            meta = nodes.get(pid) if isinstance(nodes.get(pid), dict) else {}
            is_blocked, info = _block_status(
                meta=meta if isinstance(meta, dict) else {},
                now=now,
                state_by_id=state_by_id,
                done_states={"completed"},
                wait_key="waiting_for_project_id",
            )
            blocked[pid] = bool(is_blocked)
            if info:
                blocks[pid] = info

        ready = {pid: bool(deps_ready.get(pid, True)) and not bool(blocked.get(pid)) for pid in view["node_ids"]}
        return {
            "org_id": resolved_org,
            "graph": graph,
            "view": view,
            "ready": ready,
            "deps_ready": deps_ready,
            "blocked": blocked,
            "blocks": blocks,
        }

    @app.post("/intelligence/plan/org/projects/edges")
    def mutate_org_projects_edges(payload: dict[str, Any]) -> dict[str, Any]:
        """Add/remove project dependency edges with DAG validation and audit-friendly ingestion."""
        resolved_org = str(payload.get("org_id") or "") or store.resolve_org_id(team_id=str(payload.get("team_id") or "") or None)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        user_id = str(payload.get("user_id") or "")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required.")
        op = str(payload.get("op") or "add").strip().lower()
        src = str(payload.get("from") or "")
        dst = str(payload.get("to") or "")
        if not src or not dst:
            raise HTTPException(status_code=400, detail="from and to are required.")

        reason = payload.get("reason")
        evidence = payload.get("evidence_event_ids")
        dry_run = bool(payload.get("dry_run"))
        team_id = payload.get("team_id")

        state = store.get_org_fsa_state(org_id=resolved_org)
        if state is None:
            raise HTTPException(status_code=404, detail="org_fsa_state not found; ingest events first.")

        graph = state.get("project_graph") if isinstance(state.get("project_graph"), dict) else {"nodes": {}, "edges": []}
        view = _dag_view_from_graph(graph)

        if op == "add":
            if would_create_cycle(nodes=view["node_ids"], edges=view["edge_pairs"], new_edge=(src, dst)):
                raise HTTPException(status_code=400, detail="edge would create a cycle; DAG invariant violated.")
        elif op == "remove":
            pass
        else:
            raise HTTPException(status_code=400, detail="op must be 'add' or 'remove'.")

        if dry_run:
            return {"ok": True, "dry_run": True, "op": op, "from": src, "to": dst, "would_create_cycle": False}

        now = utc_now_iso8601()
        event_type = "project.depends_on.add" if op == "add" else "project.depends_on.remove"
        raw_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_data": {
                "project_id": dst,
                "depends_on_project_id": src,
                "reason": reason,
                "evidence_event_ids": evidence if isinstance(evidence, list) else [],
            },
            "context": {
                "collectium_context": {"phase": "do", "block": "task"},
                "attribution": {"actor_user_id": user_id, "reason": reason},
            },
            "user_id": user_id,
            "org_id": resolved_org,
            "team_id": team_id,
            "project_id": None,
            "timestamp": now,
        }
        _ingest_raw_event(raw_event, llm_mode="off", update_profiles=False)

        updated = store.get_org_fsa_state(org_id=resolved_org)
        return {"ok": True, "org_id": resolved_org, "state": updated}

    @app.post("/intelligence/plan/org/projects/blocks")
    def mutate_org_projects_blocks(payload: dict[str, Any]) -> dict[str, Any]:
        """Set/clear a project time/memory block (org-level planning)."""
        resolved_org = str(payload.get("org_id") or "") or store.resolve_org_id(team_id=str(payload.get("team_id") or "") or None)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        user_id = str(payload.get("user_id") or "")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required.")
        project_id = str(payload.get("project_id") or "")
        if not project_id:
            raise HTTPException(status_code=400, detail="project_id is required.")
        block_type = str(payload.get("block_type") or "clear").strip().lower()
        team_id = payload.get("team_id")

        if block_type not in {"time", "memory", "clear"}:
            raise HTTPException(status_code=400, detail="block_type must be 'time', 'memory', or 'clear'.")

        reason = payload.get("reason")
        evidence = payload.get("evidence_event_ids")
        blocked_until = payload.get("blocked_until")
        waiting_for_project_id = payload.get("waiting_for_project_id")
        waiting_for_external = payload.get("waiting_for_external")

        now = utc_now_iso8601()
        if block_type == "clear":
            event_type = "project.block.clear"
        elif block_type == "time":
            event_type = "project.time_block.set"
        else:
            event_type = "project.memory_block.set"

        raw_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_data": {
                "project_id": project_id,
                "block_type": block_type if block_type != "clear" else None,
                "blocked_until": blocked_until,
                "waiting_for_project_id": waiting_for_project_id,
                "waiting_for_external": bool(waiting_for_external) if waiting_for_external is not None else None,
                "block_reason": reason,
                "evidence_event_ids": evidence if isinstance(evidence, list) else [],
            },
            "context": {
                "collectium_context": {"phase": "do", "block": "task"},
                "attribution": {"actor_user_id": user_id, "reason": reason},
            },
            "user_id": user_id,
            "org_id": resolved_org,
            "team_id": team_id,
            "project_id": None,
            "timestamp": now,
        }
        _ingest_raw_event(raw_event, llm_mode="off", update_profiles=False)

        updated = store.get_org_fsa_state(org_id=resolved_org)
        return {"ok": True, "org_id": resolved_org, "state": updated}

    @app.get("/intelligence/state/project")
    def get_project_state(
        *,
        project_id: str,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
    ) -> dict[str, Any]:
        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        if not project_id:
            raise HTTPException(status_code=400, detail="project_id is required.")
        state = store.get_project_fsa_state(org_id=resolved_org, project_id=project_id)
        if state is None:
            raise HTTPException(status_code=404, detail="project_fsa_state not found; ingest project events first.")
        return state

    @app.get("/intelligence/projects/{project_id}/plan/tasks")
    def get_project_tasks_plan(
        project_id: str,
        *,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Project-level task dependency plan (DAG) derived from project_fsa_state.task_graph."""
        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        state = store.get_project_fsa_state(org_id=resolved_org, project_id=project_id)
        if state is None:
            raise HTTPException(status_code=404, detail="project_fsa_state not found; ingest project events first.")
        graph = state.get("task_graph") if isinstance(state.get("task_graph"), dict) else {"nodes": {}, "edges": []}
        view = _dag_view_from_graph(graph)

        tasks = state.get("tasks") if isinstance(state.get("tasks"), dict) else {}
        state_by_id = {str(k): str(v) for k, v in tasks.items() if isinstance(k, str) and isinstance(v, str)}
        deps_ready = _ready_map(blocked_by=view["blocked_by"], state_by_id=state_by_id, done_states={"completed_approved"})

        nodes = graph.get("nodes") if isinstance(graph.get("nodes"), dict) else {}
        now = datetime.now(timezone.utc)
        blocks: dict[str, dict[str, Any]] = {}
        blocked: dict[str, bool] = {}
        for tid in view["node_ids"]:
            meta = nodes.get(tid) if isinstance(nodes.get(tid), dict) else {}
            is_blocked, info = _block_status(
                meta=meta if isinstance(meta, dict) else {},
                now=now,
                state_by_id=state_by_id,
                done_states={"completed_approved"},
                wait_key="waiting_for_task_id",
            )
            blocked[tid] = bool(is_blocked)
            if info:
                blocks[tid] = info

        ready = {tid: bool(deps_ready.get(tid, True)) and not bool(blocked.get(tid)) for tid in view["node_ids"]}

        return {
            "org_id": resolved_org,
            "project_id": project_id,
            "graph": graph,
            "view": view,
            "ready": ready,
            "deps_ready": deps_ready,
            "blocked": blocked,
            "blocks": blocks,
        }

    @app.post("/intelligence/projects/{project_id}/plan/tasks/edges")
    def mutate_project_task_edges(
        project_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Add/remove task dependency edges with DAG validation and audit-friendly ingestion."""
        resolved_org = str(payload.get("org_id") or "") or store.resolve_org_id(team_id=str(payload.get("team_id") or "") or None)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        team_id = payload.get("team_id")
        if not isinstance(team_id, str) or not team_id:
            raise HTTPException(status_code=400, detail="team_id is required.")
        user_id = str(payload.get("user_id") or "")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required.")
        op = str(payload.get("op") or "add").strip().lower()
        src = str(payload.get("from") or "")
        dst = str(payload.get("to") or "")
        if not src or not dst:
            raise HTTPException(status_code=400, detail="from and to are required.")

        reason = payload.get("reason")
        evidence = payload.get("evidence_event_ids")
        dry_run = bool(payload.get("dry_run"))

        state = store.get_project_fsa_state(org_id=resolved_org, project_id=project_id)
        if state is None:
            raise HTTPException(status_code=404, detail="project_fsa_state not found; ingest project events first.")

        graph = state.get("task_graph") if isinstance(state.get("task_graph"), dict) else {"nodes": {}, "edges": []}
        view = _dag_view_from_graph(graph)

        if op == "add":
            if would_create_cycle(nodes=view["node_ids"], edges=view["edge_pairs"], new_edge=(src, dst)):
                raise HTTPException(status_code=400, detail="edge would create a cycle; DAG invariant violated.")
        elif op == "remove":
            pass
        else:
            raise HTTPException(status_code=400, detail="op must be 'add' or 'remove'.")

        if dry_run:
            return {"ok": True, "dry_run": True, "op": op, "from": src, "to": dst, "would_create_cycle": False}

        now = utc_now_iso8601()
        event_type = "task.depends_on.add" if op == "add" else "task.depends_on.remove"
        raw_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_data": {
                "task_id": dst,
                "depends_on_task_id": src,
                "reason": reason,
                "evidence_event_ids": evidence if isinstance(evidence, list) else [],
            },
            "context": {
                "collectium_context": {"phase": "do", "block": "task"},
                "attribution": {"actor_user_id": user_id, "reason": reason},
            },
            "user_id": user_id,
            "org_id": resolved_org,
            "team_id": team_id,
            "project_id": project_id,
            "timestamp": now,
        }
        _ingest_raw_event(raw_event, llm_mode="off", update_profiles=False)

        updated = store.get_project_fsa_state(org_id=resolved_org, project_id=project_id)
        return {"ok": True, "org_id": resolved_org, "project_id": project_id, "state": updated}

    @app.post("/intelligence/projects/{project_id}/plan/tasks/blocks")
    def mutate_project_task_blocks(
        project_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Set/clear a task time/memory block (project-level planning)."""
        resolved_org = str(payload.get("org_id") or "") or store.resolve_org_id(team_id=str(payload.get("team_id") or "") or None)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        team_id = payload.get("team_id")
        if not isinstance(team_id, str) or not team_id:
            raise HTTPException(status_code=400, detail="team_id is required.")
        user_id = str(payload.get("user_id") or "")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required.")
        task_id = str(payload.get("task_id") or "")
        if not task_id:
            raise HTTPException(status_code=400, detail="task_id is required.")
        block_type = str(payload.get("block_type") or "clear").strip().lower()

        if block_type not in {"time", "memory", "clear"}:
            raise HTTPException(status_code=400, detail="block_type must be 'time', 'memory', or 'clear'.")

        reason = payload.get("reason")
        evidence = payload.get("evidence_event_ids")
        blocked_until = payload.get("blocked_until")
        waiting_for_task_id = payload.get("waiting_for_task_id")
        waiting_for_external = payload.get("waiting_for_external")

        now = utc_now_iso8601()
        if block_type == "clear":
            event_type = "task.block.clear"
        elif block_type == "time":
            event_type = "task.time_block.set"
        else:
            event_type = "task.memory_block.set"

        raw_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_data": {
                "task_id": task_id,
                "block_type": block_type if block_type != "clear" else None,
                "blocked_until": blocked_until,
                "waiting_for_task_id": waiting_for_task_id,
                "waiting_for_external": bool(waiting_for_external) if waiting_for_external is not None else None,
                "block_reason": reason,
                "evidence_event_ids": evidence if isinstance(evidence, list) else [],
            },
            "context": {
                "collectium_context": {"phase": "do", "block": "task"},
                "attribution": {"actor_user_id": user_id, "reason": reason},
            },
            "user_id": user_id,
            "org_id": resolved_org,
            "team_id": team_id,
            "project_id": project_id,
            "timestamp": now,
        }
        _ingest_raw_event(raw_event, llm_mode="off", update_profiles=False)

        updated = store.get_project_fsa_state(org_id=resolved_org, project_id=project_id)
        return {"ok": True, "org_id": resolved_org, "project_id": project_id, "state": updated}

    @app.get("/intelligence/ux/recommendations")
    def get_ux_recommendations(*, org_id: Optional[str] = None, team_id: Optional[str] = None) -> dict[str, Any]:
        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        if not isinstance(team_id, str) or not team_id:
            raise HTTPException(status_code=400, detail="team_id is required for UX recommendations.")

        _ensure_default_ux_interventions(resolved_org)

        team_profile = store.get_team_profile(org_id=resolved_org, team_id=team_id)
        if team_profile is None:
            team_events = store.list_team_classified_events(org_id=resolved_org, team_id=team_id)
            member_ids = sorted(
                {str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)}
            )
            member_profiles: list[dict[str, Any]] = []
            for uid in member_ids:
                prof = store.get_user_profile(org_id=resolved_org, user_id=uid)
                if prof is None:
                    u_events = store.list_user_classified_events(org_id=resolved_org, user_id=uid)
                    prof = build_user_profile(uid, resolved_org, u_events)
                    _upsert_user_profile(
                        prof,
                        org_id=resolved_org,
                        user_id=uid,
                        pipeline_version=PIPELINE_VERSION,
                        updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
                    )
                member_profiles.append(prof)
            team_profile = build_team_profile(team_id, resolved_org, member_profiles, team_events)
            _upsert_team_profile(
                team_profile,
                org_id=resolved_org,
                team_id=team_id,
                pipeline_version=PIPELINE_VERSION,
            )

        team_events = store.list_team_classified_events(org_id=resolved_org, team_id=team_id)
        member_ids = sorted({str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)})
        user_profiles: list[dict[str, Any]] = []
        for uid in member_ids:
            prof = store.get_user_profile(org_id=resolved_org, user_id=uid)
            if prof is None:
                u_events = store.list_user_classified_events(org_id=resolved_org, user_id=uid)
                prof = build_user_profile(uid, resolved_org, u_events)
                _upsert_user_profile(
                    prof,
                    org_id=resolved_org,
                    user_id=uid,
                    pipeline_version=PIPELINE_VERSION,
                    updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
                )
            user_profiles.append(prof)

        interventions = _list_ux_interventions(resolved_org, limit=200)
        policy_context: dict[str, Any] = {}
        if isinstance(team_profile, dict):
            psych = team_profile.get("psychodynamics")
            if isinstance(psych, dict):
                wb = psych.get("wellbeing_proxies")
                if isinstance(wb, dict):
                    policy_context["wellbeing_proxies"] = wb

        out = recommend_ux_interventions(
            user_profiles=user_profiles,
            team_profile=team_profile,
            interventions=interventions,
            policy_context=policy_context,
        )
        policy_cfg = _ux_policy_config()
        if policy_cfg.get("mode") != "off":
            try:
                runs = store.list_ux_intervention_runs(
                    org_id=resolved_org,
                    scope_type="team",
                    scope_id=team_id,
                    limit=500,
                )
            except Exception:
                runs = []
            decision = select_intervention(
                interventions=interventions,
                history=runs,
                policy_context=policy_context,
                epsilon=float(policy_cfg.get("epsilon", DEFAULT_POLICY_EPSILON)),
                min_samples=int(policy_cfg.get("min_samples", DEFAULT_POLICY_MIN_SAMPLES)),
                random_seed=0,
            )
            out["policy_decision"] = decision
            out["policy_mode"] = policy_cfg.get("mode")
        out["org_id"] = resolved_org
        out["team_id"] = team_id
        out["pipeline_version"] = PIPELINE_VERSION
        out["interventions"] = interventions

        try:
            if isinstance(team_profile, dict):
                psych = team_profile.get("psychodynamics")
                psych = psych if isinstance(psych, dict) else {}
                wb = psych.get("wellbeing_proxies") if isinstance(psych.get("wellbeing_proxies"), dict) else {}
                _log_wellbeing_window(
                    org_id=resolved_org,
                    scope_type="team",
                    scope_id=team_id,
                    window_end=utc_now_iso8601(),
                    pipeline_version=PIPELINE_VERSION,
                    proxies=wb or {},
                    metadata={"source": "ux.recommendations", "team_id": team_id},
                )
        except Exception:
            pass
        return out

    @app.get("/intelligence/psychodynamics/block-matrix")
    def get_psychodynamics_block_matrix(
        *,
        scope_type: str = "user",
        scope_id: str,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        context_block: Optional[str] = None,
        pipeline_version: Optional[str] = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        if not hasattr(store, "list_psychodynamic_block_matrices"):
            raise HTTPException(status_code=501, detail="psychodynamic_block_matrices not supported by this store.")

        scope_type = str(scope_type or "").strip().lower()
        scope_id = str(scope_id or "").strip()
        if scope_type not in {"user", "team"} or not scope_id:
            raise HTTPException(status_code=400, detail="scope_type must be user|team and scope_id is required.")

        resolved_org = org_id
        if not isinstance(resolved_org, str) or not resolved_org:
            if scope_type == "user":
                resolved_org = store.resolve_org_id(team_id=team_id, user_id=scope_id)
            else:
                resolved_org = store.resolve_org_id(team_id=scope_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")

        pv = str(pipeline_version or PIPELINE_VERSION)
        if isinstance(context_block, str) and context_block.strip():
            rec = store.get_psychodynamic_block_matrix(
                org_id=resolved_org,
                scope_type=scope_type,
                scope_id=scope_id,
                context_block=context_block.strip().lower(),
                pipeline_version=pv,
            )
            records = [rec] if isinstance(rec, dict) else []
        else:
            records = store.list_psychodynamic_block_matrices(
                org_id=resolved_org,
                scope_type=scope_type,
                scope_id=scope_id,
                pipeline_version=pv,
                limit=int(limit),
            )

        return {
            "org_id": resolved_org,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "pipeline_version": pv,
            "records": records,
        }

    @app.get("/intelligence/psychodynamics/influence-layers")
    def get_psychodynamics_influence_layers(
        *,
        team_id: str,
        org_id: Optional[str] = None,
        context_block: Optional[str] = None,
        pipeline_version: Optional[str] = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        if not hasattr(store, "list_psychodynamic_influence_layers"):
            raise HTTPException(status_code=501, detail="psychodynamic_influence_layers not supported by this store.")

        team_id = str(team_id or "").strip()
        if not team_id:
            raise HTTPException(status_code=400, detail="team_id is required.")

        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")

        pv = str(pipeline_version or PIPELINE_VERSION)
        if isinstance(context_block, str) and context_block.strip():
            rec = store.get_psychodynamic_influence_layer(
                org_id=resolved_org,
                team_id=team_id,
                context_block=context_block.strip().lower(),
                pipeline_version=pv,
            )
            records = [rec] if isinstance(rec, dict) else []
        else:
            records = store.list_psychodynamic_influence_layers(
                org_id=resolved_org,
                team_id=team_id,
                pipeline_version=pv,
                limit=int(limit),
            )

        return {
            "org_id": resolved_org,
            "team_id": team_id,
            "pipeline_version": pv,
            "records": records,
        }

    # =========================================================================
    # Real-Time Transfer Entropy Endpoints
    # =========================================================================

    @app.get("/intelligence/psychodynamics/te/live")
    def get_live_te_matrix(
        *,
        team_id: str,
        org_id: Optional[str] = None,
        min_te: float = 0.001,
    ) -> dict[str, Any]:
        """
        Get the current Transfer Entropy matrix computed from streaming data.

        This uses incrementally-updated sufficient statistics, providing
        near-real-time TE estimates without full recomputation.
        """
        team_id = str(team_id or "").strip()
        if not team_id:
            raise HTTPException(status_code=400, detail="team_id is required.")

        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")

        registry = get_te_registry()
        tracker = registry.get_tracker(resolved_org, team_id)

        agents, te_matrix = tracker.compute_te_matrix()
        edges = tracker.get_influence_edges(min_te=float(min_te))

        # Compute summary stats
        total_flow = sum(e["te"] for e in edges)
        n_active_edges = len(edges)

        # Top influencers/influenced
        outgoing_by_user: dict[str, float] = {}
        incoming_by_user: dict[str, float] = {}
        for e in edges:
            outgoing_by_user[e["source"]] = outgoing_by_user.get(e["source"], 0.0) + e["te"]
            incoming_by_user[e["target"]] = incoming_by_user.get(e["target"], 0.0) + e["te"]

        top_influencer = max(outgoing_by_user.items(), key=lambda x: x[1])[0] if outgoing_by_user else None
        top_influenced = max(incoming_by_user.items(), key=lambda x: x[1])[0] if incoming_by_user else None

        return {
            "org_id": resolved_org,
            "team_id": team_id,
            "agents": agents,
            "te_matrix": te_matrix,
            "edges": edges,
            "summary": {
                "total_flow": total_flow,
                "n_active_edges": n_active_edges,
                "n_agents": len(agents),
                "top_influencer": top_influencer,
                "top_influenced": top_influenced,
                "outgoing_by_user": outgoing_by_user,
                "incoming_by_user": incoming_by_user,
            },
            "tracker_stats": {
                "updates_since_broadcast": tracker.updates_since_last_broadcast,
                "n_pairwise_stats": len(tracker.pairwise_stats),
            },
        }

    @app.get("/intelligence/psychodynamics/te/user")
    def get_user_te_influence(
        *,
        user_id: str,
        team_id: str,
        org_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Get Transfer Entropy influence summary for a specific user.

        Returns who influences this user and who this user influences.
        """
        user_id = str(user_id or "").strip()
        team_id = str(team_id or "").strip()
        if not user_id or not team_id:
            raise HTTPException(status_code=400, detail="user_id and team_id are required.")

        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")

        registry = get_te_registry()
        tracker = registry.get_tracker(resolved_org, team_id)

        summary = tracker.get_user_influence_summary(user_id)

        return {
            "org_id": resolved_org,
            "team_id": team_id,
            **summary,
        }

    @app.get("/intelligence/psychodynamics/te/stream")
    async def stream_te_updates(
        *,
        team_id: str,
        org_id: Optional[str] = None,
    ):
        """
        Server-Sent Events stream for real-time TE updates.

        Clients can subscribe to this endpoint to receive TE updates
        as they happen when team members perform actions.
        """
        from starlette.responses import StreamingResponse
        import asyncio

        team_id = str(team_id or "").strip()
        if not team_id:
            raise HTTPException(status_code=400, detail="team_id is required.")

        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")

        registry = get_te_registry()
        tracker = registry.get_tracker(resolved_org, team_id)

        async def event_generator():
            # Send initial state
            agents, te_matrix = tracker.compute_te_matrix()
            edges = tracker.get_influence_edges()

            initial = {
                "type": "initial",
                "agents": agents,
                "te_matrix": te_matrix,
                "edges": edges[:20],  # Limit to top 20 edges
            }
            yield f"data: {json.dumps(initial)}\n\n"

            # Track last update count
            last_update_count = tracker.updates_since_last_broadcast

            # Poll for updates (in production, use proper pub/sub)
            while True:
                await asyncio.sleep(2.0)  # Poll every 2 seconds

                if tracker.updates_since_last_broadcast > last_update_count:
                    agents, te_matrix = tracker.compute_te_matrix()
                    edges = tracker.get_influence_edges()

                    update = {
                        "type": "update",
                        "agents": agents,
                        "te_matrix": te_matrix,
                        "edges": edges[:20],
                        "updates_since_last": tracker.updates_since_last_broadcast - last_update_count,
                    }
                    yield f"data: {json.dumps(update)}\n\n"

                    last_update_count = tracker.updates_since_last_broadcast

                # Send heartbeat
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )

    @app.post("/intelligence/psychodynamics/te/reset")
    def reset_te_tracker(
        *,
        team_id: str,
        org_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Reset the TE tracker for a team.

        This clears all accumulated sufficient statistics.
        Use with caution - primarily for debugging/testing.
        """
        team_id = str(team_id or "").strip()
        if not team_id:
            raise HTTPException(status_code=400, detail="team_id is required.")

        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")

        registry = get_te_registry()
        # Create fresh tracker
        registry._trackers[f"{resolved_org}:{team_id}"] = TeamTETracker(
            org_id=resolved_org,
            team_id=team_id,
        )

        return {
            "ok": True,
            "org_id": resolved_org,
            "team_id": team_id,
            "message": "TE tracker reset",
        }

    # -------------------------------------------------------------------------
    # TE Validation & Schema Exploration Endpoints
    # -------------------------------------------------------------------------

    @app.get("/intelligence/psychodynamics/te/validate")
    def validate_te_computation(
        *,
        n_steps: int = Query(500, description="Number of synthetic steps per test case"),
        n_states: int = Query(4, description="State space size (default 4 for Animals)"),
        lambda_decay: float = Query(0.98, description="Decay factor for online TE"),
        beta: float = Query(1.0, description="Dirichlet smoothing parameter"),
        seed: int = Query(42, description="Random seed for reproducibility"),
    ) -> dict[str, Any]:
        """
        Validate the Transfer Entropy computation against known ground truth cases.

        This endpoint runs a suite of synthetic tests:
        - Leader-follower patterns (should detect correct direction)
        - Independent sequences (should show low TE)
        - Common cause scenarios (should distinguish from direct causation)

        Returns a validation report with pass/fail status for each test case.
        """
        try:
            from collectium_intelligence.te_validation import build_te_validation_report

            report = build_te_validation_report(
                n_steps=int(n_steps),
                n_states=int(n_states),
                lambda_decay=float(lambda_decay),
                beta=float(beta),
                seed=int(seed),
            )
            return report
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.get("/intelligence/psychodynamics/te/sensitivity")
    def te_sensitivity_analysis(
        *,
        team_id: str,
        org_id: Optional[str] = None,
        user1: Optional[str] = None,
        user2: Optional[str] = None,
        lambda_values: str = Query("0.90,0.95,0.98,0.99,1.0", description="Comma-separated lambda values"),
        beta_values: str = Query("0.1,0.5,1.0,2.0,5.0", description="Comma-separated beta values"),
    ) -> dict[str, Any]:
        """
        Analyze sensitivity of TE to hyperparameters.

        Uses real team data if available, or runs on synthetic data.
        """
        try:
            from collectium_intelligence.te_validation import sensitivity_analysis

            resolved_org = org_id or store.resolve_org_id(team_id=team_id)
            if not isinstance(resolved_org, str) or not resolved_org:
                return {"ok": False, "error": "org_id required"}

            # Parse parameter lists
            lam_vals = [float(x.strip()) for x in lambda_values.split(",")]
            beta_vals = [float(x.strip()) for x in beta_values.split(",")]

            # Get user sequences from tracker
            registry = get_te_registry()
            tracker = registry.get_tracker(resolved_org, team_id)

            if user1 and user2 and user1 in tracker.user_last_state and user2 in tracker.user_last_state:
                # Use real data from pairwise stats
                key = (user1, user2)
                if key in tracker.pairwise_stats:
                    stats = tracker.pairwise_stats[key]
                    # For now, generate synthetic data matching the pattern
                    # (A full implementation would extract the actual sequences)
                    from collectium_intelligence.te_validation import _generate_leader_follower_sequence
                    x_seq, y_seq = _generate_leader_follower_sequence(500, noise=0.2, seed=42)
                else:
                    from collectium_intelligence.te_validation import _generate_independent_sequences
                    x_seq, y_seq = _generate_independent_sequences(500, seed=42)
            else:
                # Use synthetic data
                from collectium_intelligence.te_validation import _generate_leader_follower_sequence
                x_seq, y_seq = _generate_leader_follower_sequence(500, noise=0.2, seed=42)

            result = sensitivity_analysis(x_seq, y_seq, lambda_values=lam_vals, beta_values=beta_vals)
            result["org_id"] = resolved_org
            result["team_id"] = team_id
            return result

        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.get("/intelligence/psychodynamics/schema")
    def get_current_schema() -> dict[str, Any]:
        """
        Get the current behavioral state schema (S(3,4,8) Steiner system).

        Returns the schema definition including:
        - Block structure (14 blocks of 4 primitives each)
        - Primitive labels
        - Mathematical properties
        """
        try:
            from collectium_intelligence.schema_exploration import (
                build_s348_schema,
                compute_schema_metrics,
            )

            schema = build_s348_schema()
            metrics = compute_schema_metrics(schema)
            validation = schema.validate()

            return {
                "ok": True,
                "schema": schema.to_dict(),
                "metrics": metrics,
                "validation": validation,
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.get("/intelligence/psychodynamics/schema/validate")
    def validate_schema() -> dict[str, Any]:
        """
        Validate the Steiner S(3,4,8) schema properties.

        Checks:
        - Exhaustive 3-coverage (every triple in exactly one block)
        - Uniform block size (all blocks have 4 points)
        - Uniform incidence (each point in same number of blocks)
        - Complement closure (complement of each block is also a block)
        - Intersection property (pairwise intersections are 0 or 2)
        """
        try:
            from collectium_intelligence.steiner_validation import build_steiner_validation_report

            report = build_steiner_validation_report()
            return report
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.get("/intelligence/psychodynamics/schema/variants")
    def explore_schema_variants(
        *,
        n_samples: int = Query(5, description="Number of variants to generate"),
        seed: int = Query(42, description="Random seed"),
    ) -> dict[str, Any]:
        """
        Explore variant schemas by permuting primitives.

        All variants are isomorphic to the base S(3,4,8) schema but have
        different primitive-to-block assignments. This allows exploring
        how different interpretations of the primitives affect the model.
        """
        try:
            from collectium_intelligence.schema_exploration import (
                build_s348_schema,
                explore_schema_variants as _explore,
                build_schema_comparison_report,
            )

            schema = build_s348_schema()
            variants = _explore(schema, n_samples=int(n_samples), seed=int(seed))
            report = build_schema_comparison_report(variants)

            return report
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.get("/intelligence/psychodynamics/schema/properties")
    def get_schema_properties() -> dict[str, Any]:
        """
        Get detailed mathematical properties of the behavioral schema.

        Returns information about:
        - Combinatorial design parameters (v, k, t)
        - Incidence structure
        - Intersection distribution
        - Symmetry properties
        """
        try:
            from collectium_intelligence.schema_exploration import (
                build_s348_schema,
                compute_schema_metrics,
                SCHEMA_PROPERTIES,
            )

            schema = build_s348_schema()
            metrics = compute_schema_metrics(schema)

            properties_info = [
                {
                    "name": prop.name,
                    "description": prop.description,
                    "required": prop.required,
                }
                for prop in SCHEMA_PROPERTIES
            ]

            return {
                "ok": True,
                "design_parameters": {
                    "v": schema.v,
                    "k": schema.k,
                    "t": schema.t,
                    "n_blocks": len(schema.blocks),
                    "description": "S(3,4,8) Steiner system: every 3-subset of 8 points appears in exactly one block of size 4",
                },
                "metrics": metrics,
                "property_definitions": properties_info,
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    # =========================================================================
    # Partition Discovery Endpoints
    # =========================================================================

    @app.post("/intelligence/psychodynamics/partition/discover")
    def discover_partition(
        *,
        sequences: list[list[int]] = Body(..., description="State sequences to analyze"),
        n_states: int = Body(4, description="Number of original states"),
        min_cells: int = Body(2, description="Minimum number of partition cells"),
        max_cells: int = Body(8, description="Maximum number of partition cells"),
        beam_width: int = Body(3, description="Beam search width"),
        max_iterations: int = Body(20, description="Maximum iterations"),
    ) -> dict[str, Any]:
        """
        Discover optimal state space partition using iterative independence testing.

        The algorithm uses G², χ², and mutual information tests to evaluate
        how well different partitions satisfy the Markov property and other
        constraints.

        Returns the best partition found along with diagnostics.
        """
        try:
            from collectium_intelligence.partition_discovery import (
                DiscoveryConfig,
                build_discovery_report,
            )

            config = DiscoveryConfig(
                n_states=int(n_states),
                min_cells=int(min_cells),
                max_cells=int(max_cells),
                beam_width=int(beam_width),
                max_iterations=int(max_iterations),
                n_bootstrap=200,  # Reduced for API performance
            )

            report = build_discovery_report(
                sequences,
                config=config,
                include_comparison=True,
            )

            return report
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.post("/intelligence/psychodynamics/partition/compare")
    def compare_partitions(
        *,
        sequences: list[list[int]] = Body(..., description="State sequences to analyze"),
        n_states: int = Body(4, description="Number of original states"),
    ) -> dict[str, Any]:
        """
        Compare standard partitions (discrete, trivial, 2-cell) on given data.

        Useful for evaluating how different granularities perform on the data.
        """
        try:
            from collectium_intelligence.partition_discovery import (
                DiscoveryConfig,
                PartitionDiscoveryPipeline,
            )
            from collectium_intelligence.partition_refinement import Partition

            config = DiscoveryConfig(n_states=int(n_states))
            pipeline = PartitionDiscoveryPipeline(config)

            # Build standard partitions
            partitions = [
                Partition.discrete(int(n_states)),
                Partition.trivial(int(n_states)),
            ]

            # Add a 2-cell partition if n_states >= 2
            if n_states >= 2:
                mapping = {i: 0 if i < n_states // 2 else 1 for i in range(n_states)}
                partitions.append(Partition.from_mapping(mapping, partition_id="binary"))

            comparison = pipeline.run_comparison(sequences, partitions)

            return {"ok": True, **comparison}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.post("/intelligence/psychodynamics/partition/validate")
    def validate_partition(
        *,
        sequences: list[list[int]] = Body(..., description="State sequences to analyze"),
        cell_mapping: dict[int, int] = Body(..., description="State to cell mapping"),
    ) -> dict[str, Any]:
        """
        Validate a specific partition against constraint criteria.

        Provide a mapping from state indices to cell indices.
        """
        try:
            from collectium_intelligence.partition_discovery import validate_partition as _validate
            from collectium_intelligence.partition_refinement import Partition

            # Convert string keys if necessary (JSON keys are strings)
            mapping = {int(k): int(v) for k, v in cell_mapping.items()}
            partition = Partition.from_mapping(mapping, partition_id="user-provided")

            result = _validate(partition, sequences, verbose=True)

            return result
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.post("/intelligence/psychodynamics/independence/test")
    def independence_test(
        *,
        X: list[int] = Body(..., description="First state sequence"),
        Y: list[int] = Body(..., description="Second state sequence"),
        n_states: int = Body(4, description="Number of states"),
        alpha: float = Body(0.05, description="Significance level"),
        n_bootstrap: int = Body(200, description="Bootstrap samples"),
    ) -> dict[str, Any]:
        """
        Run independence tests on two state sequences.

        Returns G², χ², and mutual information test results.
        """
        try:
            from collectium_intelligence.independence_tests import build_independence_test_report

            report = build_independence_test_report(
                X, Y,
                S=int(n_states),
                alpha=float(alpha),
                n_bootstrap=int(n_bootstrap),
            )

            return report
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.post("/intelligence/psychodynamics/markov/test")
    def markov_property_test(
        *,
        sequences: list[list[int]] = Body(..., description="State sequences to test"),
        n_states: int = Body(4, description="Number of states"),
        order: int = Body(1, description="Markov order to test"),
        alpha: float = Body(0.05, description="Significance level"),
    ) -> dict[str, Any]:
        """
        Test if sequences satisfy the Markov property.

        Tests whether I(S_{t+k}; S_t | S_{t+1}, ..., S_{t+k-1}) ≈ 0
        for the specified Markov order k.
        """
        try:
            from collectium_intelligence.independence_tests import markov_property_test as _markov_test

            result = _markov_test(
                sequences,
                S=int(n_states),
                order=int(order),
                alpha=float(alpha),
                n_bootstrap=200,
            )

            return {
                "ok": True,
                "markov_order": order,
                "result": result.to_dict(),
                "interpretation": {
                    "holds": not result.is_significant,
                    "confidence": 1.0 - result.p_value,
                    "explanation": (
                        f"Markov property {'holds' if not result.is_significant else 'violated'} "
                        f"at order {order} (p={result.p_value:.4f})"
                    ),
                },
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    @app.post("/intelligence/ux/bootstrap", response_model=dict[str, Any])
    def bootstrap_ux_taxonomy(*, org_id: str) -> dict[str, Any]:
        """Create/refresh the default UX intervention taxonomy for an org (idempotent)."""
        interventions = _ensure_default_ux_interventions(org_id)
        return {"org_id": org_id, "interventions": interventions}

    @app.post("/intelligence/ux/runs/apply", response_model=dict[str, Any])
    def apply_ux_intervention(req: UxApplyRequest) -> dict[str, Any]:
        payload = _model_dump(req)
        org_id = str(payload.get("org_id") or "")
        key = str(payload.get("intervention_key") or "")
        scope_type = str(payload.get("scope_type") or "team")
        scope_id = str(payload.get("scope_id") or "")
        if not org_id or not key or not scope_id:
            raise HTTPException(status_code=400, detail="org_id, intervention_key, and scope_id are required.")

        pv = str(payload.get("pipeline_version") or PIPELINE_VERSION)

        # Ensure taxonomy exists (best-effort).
        _ensure_default_ux_interventions(org_id)

        decided_at = utc_now_iso8601()
        pre_state = _snapshot_scope_state(org_id=org_id, scope_type=scope_type, scope_id=scope_id, pipeline_version=pv)
        params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
        rationale = payload.get("rationale") if isinstance(payload.get("rationale"), dict) else {}

        pre_wb: dict[str, Any] = {}
        if scope_type == "user":
            psych = pre_state.get("user_profile_psychodynamics") if isinstance(pre_state, dict) else {}
            psych = psych if isinstance(psych, dict) else {}
            pre_wb = psych.get("wellbeing_proxies") if isinstance(psych.get("wellbeing_proxies"), dict) else {}
        elif scope_type == "team":
            psych = pre_state.get("team_profile_psychodynamics") if isinstance(pre_state, dict) else {}
            psych = psych if isinstance(psych, dict) else {}
            pre_wb = psych.get("wellbeing_proxies") if isinstance(psych.get("wellbeing_proxies"), dict) else {}

        intervention = _lookup_ux_intervention(org_id, key)
        constraints = intervention.get("constraints") if isinstance(intervention, dict) else {}
        constraints = constraints if isinstance(constraints, dict) else {}
        consent_flag = params.get("consent_granted") if isinstance(params.get("consent_granted"), bool) else None
        policy = evaluate_ux_policy(constraints, wellbeing=pre_wb, consent_granted=consent_flag)
        policy["constraints"] = constraints
        policy_status = str(policy.get("status") or "unknown")
        if policy_status not in {"pass", "blocked", "needs_consent", "unknown"}:
            policy_status = "unknown"

        if isinstance(intervention, dict):
            rationale.setdefault(
                "intervention",
                {
                    "intervention_key": intervention.get("intervention_key"),
                    "version": intervention.get("version"),
                    "name": intervention.get("name"),
                },
            )
        rationale["policy"] = policy

        run_id = str(uuid.uuid4())
        run = {
            "org_id": org_id,
            "run_id": run_id,
            "intervention_key": key,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "pipeline_version": pv,
            "decided_at": decided_at,
            "applied_at": decided_at if policy_status == "pass" else None,
            "status": "applied" if policy_status == "pass" else policy_status,
            "decided_by": str(payload.get("decided_by") or "human"),
            "params": params,
            "rationale": rationale,
            "pre_state": pre_state,
            "post_state": {},
            "delta": {},
        }

        record = store.upsert_ux_intervention_run(run)

        # Log a wellbeing window snapshot (best-effort).
        _log_wellbeing_window(
            org_id=org_id,
            scope_type=scope_type,
            scope_id=scope_id,
            window_end=decided_at,
            pipeline_version=pv,
            proxies=pre_wb or {},
            metadata={"source": "ux.apply", "run_id": run_id, "intervention_key": key},
            consent_granted=consent_flag,
        )

        # Exposure logging (best-effort): links recommendation → apply for auditability.
        try:
            if hasattr(store, "upsert_ux_exposure"):
                store.upsert_ux_exposure(
                    {
                        "org_id": org_id,
                        "exposure_id": str(uuid.uuid4()),
                        "run_id": run_id,
                        "intervention_key": key,
                        "pipeline_version": pv,
                        "scope_type": scope_type,
                        "scope_id": scope_id,
                        "user_id": None,
                        "exposure_type": "applied" if policy_status == "pass" else policy_status,
                        "surface": "backend.apply",
                        "occurred_at": decided_at,
                        "metadata": {
                            "source": "ux.apply",
                            "run_id": run_id,
                            "decided_by": payload.get("decided_by"),
                            "policy_status": policy_status,
                        },
                    }
                )
        except Exception:
            pass

        if policy_status != "pass":
            return JSONResponse(
                status_code=409,
                content={"run": record, "policy": policy, "status": policy_status},
            )

        try:
            collector.record_intervention_applied(key)
        except Exception:
            pass

        return {"run": record}

    @app.post("/intelligence/ux/runs/{run_id}/measure", response_model=dict[str, Any])
    def measure_ux_intervention(*, run_id: str, org_id: str) -> dict[str, Any]:
        run = store.get_ux_intervention_run(org_id=org_id, run_id=run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")

        pv = str(run.get("pipeline_version") or PIPELINE_VERSION)
        scope_type = str(run.get("scope_type") or "team")
        scope_id = str(run.get("scope_id") or "")
        if not scope_id:
            raise HTTPException(status_code=400, detail="run.scope_id missing")

        measured_at = utc_now_iso8601()
        post_state = _snapshot_scope_state(org_id=org_id, scope_type=scope_type, scope_id=scope_id, pipeline_version=pv)

        pre_state = run.get("pre_state") if isinstance(run.get("pre_state"), dict) else {}
        delta: dict[str, Any] = {}
        params = run.get("params") if isinstance(run.get("params"), dict) else {}
        consent_flag = params.get("consent_granted") if isinstance(params.get("consent_granted"), bool) else None

        # Wellbeing proxy deltas.
        pre_wb: dict[str, Any] = {}
        post_wb: dict[str, Any] = {}
        if scope_type == "user":
            pre_ps = pre_state.get("user_profile_psychodynamics") if isinstance(pre_state.get("user_profile_psychodynamics"), dict) else {}
            post_ps = post_state.get("user_profile_psychodynamics") if isinstance(post_state.get("user_profile_psychodynamics"), dict) else {}
            pre_wb = pre_ps.get("wellbeing_proxies") if isinstance(pre_ps.get("wellbeing_proxies"), dict) else {}
            post_wb = post_ps.get("wellbeing_proxies") if isinstance(post_ps.get("wellbeing_proxies"), dict) else {}
            # Block-matrix deltas (context × timescale).
            pre_blocks = pre_state.get("block_matrices") if isinstance(pre_state.get("block_matrices"), list) else []
            post_blocks = post_state.get("block_matrices") if isinstance(post_state.get("block_matrices"), list) else []
            delta["block_matrix"] = _delta_block_matrices(pre_blocks, post_blocks)
        elif scope_type == "team":
            pre_ps = pre_state.get("team_profile_psychodynamics") if isinstance(pre_state.get("team_profile_psychodynamics"), dict) else {}
            post_ps = post_state.get("team_profile_psychodynamics") if isinstance(post_state.get("team_profile_psychodynamics"), dict) else {}
            pre_wb = pre_ps.get("wellbeing_proxies") if isinstance(pre_ps.get("wellbeing_proxies"), dict) else {}
            post_wb = post_ps.get("wellbeing_proxies") if isinstance(post_ps.get("wellbeing_proxies"), dict) else {}

        delta["wellbeing_proxies"] = _delta_numeric(pre_wb, post_wb)

        intervention = _lookup_ux_intervention(org_id, str(run.get("intervention_key") or ""))
        constraints = intervention.get("constraints") if isinstance(intervention, dict) else {}
        constraints = constraints if isinstance(constraints, dict) else {}
        post_policy = evaluate_ux_policy(constraints, wellbeing=post_wb, consent_granted=consent_flag)
        post_policy["constraints"] = constraints
        rationale = run.get("rationale") if isinstance(run.get("rationale"), dict) else {}
        rationale = dict(rationale)
        rationale["post_policy"] = post_policy
        run["rationale"] = rationale

        run["measured_at"] = measured_at
        run["post_state"] = post_state
        run["delta"] = delta

        record = store.upsert_ux_intervention_run(run)

        try:
            wb_delta = delta.get("wellbeing_proxies") if isinstance(delta.get("wellbeing_proxies"), dict) else {}
            if wb_delta:
                mean_effect = sum(float(v) for v in wb_delta.values()) / max(1, len(wb_delta))
                collector.record_effect_measured(mean_effect)
        except Exception:
            pass

        _log_wellbeing_window(
            org_id=org_id,
            scope_type=scope_type,
            scope_id=scope_id,
            window_end=measured_at,
            pipeline_version=pv,
            proxies=post_wb or {},
            metadata={
                "source": "ux.measure",
                "run_id": run_id,
                "intervention_key": str(run.get("intervention_key") or ""),
            },
            consent_granted=consent_flag,
        )

        return {"run": record}

    @app.post("/intelligence/ux/interventions", response_model=dict[str, Any])
    def upsert_ux_intervention_endpoint(intervention: UxIntervention) -> dict[str, Any]:
        payload = _model_dump(intervention)
        record = store.upsert_ux_intervention(payload)
        return {"intervention": record}

    @app.get("/intelligence/ux/interventions", response_model=dict[str, Any])
    def list_ux_interventions_endpoint(*, org_id: str, limit: int = 200) -> dict[str, Any]:
        rows = store.list_ux_interventions(org_id=org_id, limit=int(limit))
        return {"interventions": rows}

    @app.post("/intelligence/ux/runs", response_model=dict[str, Any])
    def upsert_ux_run_endpoint(run: UxInterventionRun) -> dict[str, Any]:
        payload = _model_dump(run)
        payload["run_id"] = payload.get("run_id") or str(uuid.uuid4())
        payload["decided_at"] = payload.get("decided_at") or utc_now_iso8601()
        record = store.upsert_ux_intervention_run(payload)
        return {"run": record}

    @app.get("/intelligence/ux/runs", response_model=dict[str, Any])
    def list_ux_runs_endpoint(
        *,
        org_id: str,
        scope_type: Optional[str] = None,
        scope_id: Optional[str] = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        rows = store.list_ux_intervention_runs(
            org_id=org_id,
            scope_type=scope_type,
            scope_id=scope_id,
            limit=int(limit),
        )
        return {"runs": rows}

    @app.post("/intelligence/ux/wellbeing", response_model=dict[str, Any])
    def upsert_wellbeing_endpoint(window: WellbeingWindow) -> dict[str, Any]:
        payload = _model_dump(window)
        payload["created_at"] = payload.get("created_at") or utc_now_iso8601()
        consent_flag = payload.get("consent_granted") if isinstance(payload.get("consent_granted"), bool) else None
        normalized = normalize_wellbeing_proxies(payload.get("proxies"), consent_granted=consent_flag)
        payload["proxies"] = normalized.get("proxies") or {}
        payload["consent_granted"] = normalized.get("consent_granted")
        meta = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        meta = dict(meta)
        meta["wellbeing_schema_version"] = normalized.get("schema_version")
        dropped = normalized.get("dropped")
        if isinstance(dropped, list) and dropped:
            meta["wellbeing_dropped"] = dropped
        if normalized.get("missing_consent") is True:
            meta["wellbeing_missing_consent"] = True
        payload["metadata"] = meta
        record = store.upsert_wellbeing_window(payload)
        return {"window": record}

    @app.get("/intelligence/ux/wellbeing", response_model=dict[str, Any])
    def list_wellbeing_endpoint(
        *,
        org_id: str,
        scope_type: str,
        scope_id: str,
        limit: int = 200,
    ) -> dict[str, Any]:
        rows = store.list_wellbeing_windows(
            org_id=org_id,
            scope_type=scope_type,
            scope_id=scope_id,
            limit=int(limit),
        )
        return {"windows": rows}

    @app.post("/intelligence/consent", response_model=dict[str, Any])
    def upsert_consent_endpoint(record: ConsentRecord) -> dict[str, Any]:
        if not hasattr(store, "upsert_consent_record"):
            raise HTTPException(status_code=501, detail="consent registry not supported by this store.")
        payload = _model_dump(record)
        payload["created_at"] = payload.get("created_at") or utc_now_iso8601()
        stored = store.upsert_consent_record(payload)
        return {"consent": stored}

    @app.get("/intelligence/consent", response_model=dict[str, Any])
    def list_consent_endpoint(
        *,
        org_id: str,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        consent_key: Optional[str] = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        if not hasattr(store, "list_consent_records"):
            raise HTTPException(status_code=501, detail="consent registry not supported by this store.")
        rows = store.list_consent_records(
            org_id=org_id,
            subject_type=subject_type,
            subject_id=subject_id,
            consent_key=consent_key,
            limit=int(limit),
        )
        return {"consent": rows}

    @app.post("/intelligence/ux/exposures", response_model=dict[str, Any])
    def upsert_ux_exposure_endpoint(req: UxExposureRequest) -> dict[str, Any]:
        if not hasattr(store, "upsert_ux_exposure"):
            raise HTTPException(status_code=501, detail="ux_exposures not supported by this store.")

        payload = _model_dump(req)
        org_id = str(payload.get("org_id") or "")
        key = str(payload.get("intervention_key") or "")
        scope_type = str(payload.get("scope_type") or "team")
        scope_id = str(payload.get("scope_id") or "")
        if not org_id or not key or not scope_id:
            raise HTTPException(status_code=400, detail="org_id, intervention_key, and scope_id are required.")

        exposure_id = str(payload.get("exposure_id") or str(uuid.uuid4()))
        occurred_at = str(payload.get("occurred_at") or "") or utc_now_iso8601()
        pv = str(payload.get("pipeline_version") or PIPELINE_VERSION)

        record = {
            "org_id": org_id,
            "exposure_id": exposure_id,
            "run_id": payload.get("run_id"),
            "intervention_key": key,
            "pipeline_version": pv,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "user_id": payload.get("user_id"),
            "exposure_type": str(payload.get("exposure_type") or "shown"),
            "surface": payload.get("surface"),
            "occurred_at": occurred_at,
            "metadata": payload.get("metadata") or {},
        }
        stored = store.upsert_ux_exposure(record)
        return {"exposure": stored}

    @app.get("/intelligence/ux/exposures", response_model=dict[str, Any])
    def list_ux_exposures_endpoint(
        *,
        org_id: str,
        scope_type: Optional[str] = None,
        scope_id: Optional[str] = None,
        user_id: Optional[str] = None,
        intervention_key: Optional[str] = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        if not hasattr(store, "list_ux_exposures"):
            raise HTTPException(status_code=501, detail="ux_exposures not supported by this store.")
        rows = store.list_ux_exposures(
            org_id=org_id,
            scope_type=scope_type,
            scope_id=scope_id,
            user_id=user_id,
            intervention_key=intervention_key,
            limit=int(limit),
        )
        return {"exposures": rows}

    @app.post("/intelligence/memcubes", response_model=dict[str, Any])
    def upsert_memcube_endpoint(memcube: Memcube) -> dict[str, Any]:
        payload = _model_dump(memcube)
        record = store.upsert_memcube(payload)
        return {"memcube": record}

    @app.get("/intelligence/memcubes", response_model=dict[str, Any])
    def list_memcubes_endpoint(
        *,
        org_id: str,
        level: Optional[str] = None,
        entity_id: Optional[str] = None,
        context_type: Optional[str] = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        rows = store.list_memcubes(
            org_id=org_id,
            level=level,
            entity_id=entity_id,
            context_type=context_type,
            limit=int(limit),
        )
        return {"memcubes": rows}

    @app.get("/intelligence/memcubes/{memcube_id}", response_model=dict[str, Any])
    def get_memcube_endpoint(memcube_id: str, *, org_id: str) -> dict[str, Any]:
        rec = store.get_memcube(org_id=org_id, memcube_id=memcube_id)
        if rec is None:
            raise HTTPException(status_code=404, detail="memcube not found")
        return {"memcube": rec}

    @app.delete("/intelligence/memcubes/{memcube_id}", response_model=dict[str, Any])
    def delete_memcube_endpoint(memcube_id: str, *, org_id: str) -> dict[str, Any]:
        deleted = bool(store.delete_memcube(org_id=org_id, memcube_id=memcube_id))
        return {"deleted": deleted}

    @app.post("/intelligence/memos/messages", response_model=dict[str, Any])
    def ingest_memos_message_endpoint(
        payload: MemosMessage,
        *,
        upsert_schema: bool = Query(True, description="Ensure memos schema memcubes exist"),
        max_messages: Optional[int] = Query(None, description="Close memcell after N messages"),
        max_idle_seconds: Optional[int] = Query(None, description="Close memcell after idle gap (seconds)"),
    ) -> dict[str, Any]:
        data = _model_dump(payload)
        org_id = str(data.get("org_id") or "").strip()
        if not org_id:
            raise HTTPException(status_code=400, detail="org_id is required.")
        if upsert_schema:
            _memos_schema_memcubes_for_org(org_id)

        group_id = str(data.get("group_id") or "").strip() or None
        team_id = str(data.get("team_id") or "").strip() or None
        message_id = str(data.get("message_id") or "").strip()
        sender_id = str(data.get("sender") or "").strip()
        message_content = str(data.get("content") or "").strip()
        created_at = str(data.get("create_time") or "").strip() or None
        sender_name = str(data.get("sender_name") or "").strip() or None
        group_name = str(data.get("group_name") or "").strip() or None
        refer_list = data.get("refer_list") if isinstance(data.get("refer_list"), list) else []
        metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}

        config: dict[str, Any] = {}
        if isinstance(max_messages, int):
            config["max_messages"] = max_messages
        if isinstance(max_idle_seconds, int):
            config["max_idle_seconds"] = max_idle_seconds
        try:
            result = ingest_memos_message(
                store=store,
                org_id=org_id,
                message_id=message_id,
                sender_id=sender_id,
                message_content=message_content,
                created_at=created_at,
                group_id=group_id,
                team_id=team_id,
                group_name=group_name,
                sender_name=sender_name,
                refer_list=refer_list,
                metadata=metadata,
                config=config or None,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"status": "ok", **result}

    @app.get("/intelligence/psychodynamics/memcubes/export", response_model=dict[str, Any])
    def export_psychodynamics_memcubes(
        *,
        org_id: Optional[str] = None,
        scope_type: Optional[str] = None,
        scope_id: Optional[str] = None,
        pipeline_version: Optional[str] = None,
        include_state_schema: bool = True,
        include_telemetry: bool = True,
        include_block_matrices: bool = False,
        source: Optional[str] = None,
    ) -> dict[str, Any]:
        resolved_org = org_id
        if not isinstance(resolved_org, str) or not resolved_org:
            if isinstance(scope_type, str) and scope_type.lower().strip() == "team":
                resolved_org = store.resolve_org_id(team_id=scope_id)
            elif isinstance(scope_type, str) and scope_type.lower().strip() == "user":
                resolved_org = store.resolve_org_id(user_id=scope_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id is required (or scope_type/scope_id with known org).")

        pv = str(pipeline_version or PIPELINE_VERSION)
        memcubes: list[dict[str, Any]] = []

        if bool(include_state_schema):
            memcubes.extend(_state_schema_memcubes_for_org(resolved_org))

        if bool(include_telemetry):
            st = str(scope_type or "").strip().lower()
            sid = str(scope_id or "").strip()
            if st not in {"user", "team"} or not sid:
                raise HTTPException(
                    status_code=400,
                    detail="scope_type (user|team) and scope_id are required when include_telemetry=true.",
                )
            telemetry = _psychodynamics_memcube_for_scope(
                org_id=resolved_org,
                scope_type=st,
                scope_id=sid,
                pipeline_version=pv,
                include_blocks=bool(include_block_matrices),
                source=source or "export",
            )
            if telemetry is not None:
                memcubes.append(telemetry)

        bundle = build_psychodynamic_exchange_bundle(
            org_id=resolved_org,
            memcubes=memcubes,
            source=source or "tracka",
        )
        return {"bundle": bundle}

    @app.post("/intelligence/psychodynamics/memcubes/import", response_model=dict[str, Any])
    def import_psychodynamics_memcubes(payload: MemcubeExchangeImport) -> dict[str, Any]:
        data = _model_dump(payload)
        org_id = str(data.get("org_id") or "").strip()
        if not org_id:
            raise HTTPException(status_code=400, detail="org_id is required.")

        bundle = data.get("bundle") if isinstance(data.get("bundle"), dict) else {}
        source = data.get("source") or bundle.get("source")
        memcubes = data.get("memcubes") if isinstance(data.get("memcubes"), list) else []
        if isinstance(bundle.get("memcubes"), list):
            memcubes = list(memcubes) + list(bundle.get("memcubes"))

        now = utc_now_iso8601()
        imported: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for raw in memcubes:
            rec = _prepare_exchange_memcube(raw, org_id=org_id, source=source, now=now)
            if rec is None:
                skipped.append({"reason": "invalid memcube", "memcube": raw})
                continue
            try:
                stored = store.upsert_memcube(rec)
            except Exception as exc:  # noqa: BLE001
                skipped.append({"reason": str(exc), "memcube_id": rec.get("memcube_id")})
                continue
            imported.append(stored)

        return {
            "org_id": org_id,
            "imported": len(imported),
            "skipped": skipped,
            "memcubes": imported,
        }

    @token_required
    @app.post("/events", response_model=IngestEventResponse)
    def ingest_event(
        event: RawEvent,
        request: Request,
        *,
        llm_mode: str = Query("off", description="auto|required|off"),
        update_profiles: bool = Query(False, description="Recompute user/team profiles (dev only)"),
        team_layer_mode: str = Query("auto", description="Team influence layer mode (off|auto|on)"),
    ) -> IngestEventResponse:
        raw = _model_dump(event)
        if not raw.get("user_id") and getattr(request, "supabase_user_id", None):
            raw["user_id"] = request.supabase_user_id
        auth_ctx = get_request_user_info(request)
        if isinstance(raw.get("context"), dict):
            raw["context"] = dict(raw["context"])
            raw["context"]["auth"] = auth_ctx
        else:
            raw["context"] = {"auth": auth_ctx}
        event_id = _ingest_raw_event(
            raw,
            llm_mode=llm_mode,
            update_profiles=bool(update_profiles),
            team_layer_mode=team_layer_mode,
        )
        return IngestEventResponse(stored=True, event_id=event_id)

    @token_required
    @app.post("/intelligence/feeds/rank", response_model=RankFeedResponse)
    def rank_feed_endpoint(req: RankFeedRequest, request: Request) -> RankFeedResponse:
        payload = _model_dump(req)
        user_id = payload["user_id"]
        team_id = payload["team_id"]
        feed_type = payload["feed_type"]
        items = payload.get("items") or []
        context = payload.get("context") or {}
        if getattr(request, "supabase_user_id", None) and user_id != request.supabase_user_id:
            raise HTTPException(status_code=403, detail="User not authorized for this feed request.")

        org_id = store.resolve_org_id(team_id=team_id, user_id=user_id)
        if not isinstance(org_id, str) or not org_id:
            raise HTTPException(status_code=400, detail="Unknown org_id; ingest at least one event first.")

        request_fingerprint = _fingerprint_request({"items": items, "context": context, "feed_type": feed_type})
        cached = store.get_feed_cache(
            org_id=org_id,
            user_id=user_id,
            feed_type=feed_type,
            pipeline_version=PIPELINE_VERSION,
            request_fingerprint=request_fingerprint,
            max_age_s=30.0,
        )
        if cached is not None:
            ranked_items = cached.get("ranked_items") or []
            total = len(items)
            limit = int(payload.get("limit") or 20)
            offset = int(payload.get("offset") or 0)
            return RankFeedResponse(
                ranked_items=ranked_items,
                total=total,
                has_more=(offset + limit) < total,
            )

        user_profile = store.get_user_profile(org_id=org_id, user_id=user_id)
        if user_profile is None:
            events = store.list_user_classified_events(org_id=org_id, user_id=user_id)
            user_profile = build_user_profile(user_id, org_id, events)
            _upsert_user_profile(
                user_profile,
                org_id=org_id,
                user_id=user_id,
                pipeline_version=PIPELINE_VERSION,
                updated_at=user_profile.get("updated_at") if isinstance(user_profile, dict) else None,
            )

        team_profile = store.get_team_profile(org_id=org_id, team_id=team_id)
        if team_profile is None:
            team_events = store.list_team_classified_events(org_id=org_id, team_id=team_id)
            member_ids = sorted(
                {str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)}
            )
            member_profiles: list[dict[str, Any]] = []
            for uid in member_ids:
                prof = store.get_user_profile(org_id=org_id, user_id=uid)
                if prof is None:
                    u_events = store.list_user_classified_events(org_id=org_id, user_id=uid)
                    prof = build_user_profile(uid, org_id, u_events)
                    _upsert_user_profile(
                        prof,
                        org_id=org_id,
                        user_id=uid,
                        pipeline_version=PIPELINE_VERSION,
                        updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
                    )
                member_profiles.append(prof)
            team_profile = build_team_profile(team_id, org_id, member_profiles, team_events)
            _upsert_team_profile(
                team_profile,
                org_id=org_id,
                team_id=team_id,
                pipeline_version=PIPELINE_VERSION,
            )

        # Provide population context for similarity-based CF.
        user_profiles = store.list_user_profiles(org_id=org_id)
        team_profiles = store.list_team_profiles(org_id=org_id)

        ranked_items = rank_feed(
            feed_type=feed_type,
            user_id=user_id,
            team_id=team_id,
            project_id=payload.get("project_id"),
            items=items,
            context=context,
            user_profile=user_profile,
            team_profile=team_profile,
            user_profiles=user_profiles,
            team_profiles=team_profiles,
        )

        store.upsert_feed_cache(
            org_id=org_id,
            user_id=user_id,
            feed_type=feed_type,
            pipeline_version=PIPELINE_VERSION,
            request_fingerprint=request_fingerprint,
            ranked_items=ranked_items,
        )

        total = len(items)
        limit = int(payload.get("limit") or 20)
        offset = int(payload.get("offset") or 0)
        return RankFeedResponse(
            ranked_items=ranked_items[:limit],
            total=total,
            has_more=(offset + limit) < total,
        )

    @app.get("/intelligence/decide/directions")
    def get_decide_directions(
        *,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        user_id: Optional[str] = None,
        limit: int = 10,
        max_age_s: float = 300.0,
    ) -> dict[str, Any]:
        # Resolve org.
        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")

        scope_type = "team" if isinstance(team_id, str) and team_id else "org"
        scope_id = str(team_id) if scope_type == "team" else resolved_org

        # Build candidates from raw events (Discourse) then rank for the group (Decide).
        raw_events = store.list_raw_events(org_id=resolved_org)
        discourse_events = [e for e in raw_events if _is_discourse_candidate_event(e)]

        cached = store.get_decide_cache(
            org_id=resolved_org,
            scope_type=scope_type,
            scope_id=scope_id,
            pipeline_version=PIPELINE_VERSION,
            max_age_s=float(max_age_s),
        )

        if cached is not None:
            directions = cached.get("directions") or []
            ranked = cached.get("ranked") or []
            ranked_by_user = cached.get("ranked_by_user") or {}
        else:
            insights = extract_insights(org_id=resolved_org, events=discourse_events, llm_mode="off")
            cards = build_knowledge_cards(org_id=resolved_org, insights=insights)
            directions = generate_strategic_directions(org_id=resolved_org, cards=cards, max_directions=int(limit))

            user_ids = sorted({e.get("user_id") for e in raw_events if isinstance(e.get("user_id"), str)})
            profiles: list[dict[str, Any]] = []
            for uid in user_ids:
                prof = store.get_user_profile(org_id=resolved_org, user_id=uid)
                if prof is None:
                    # Use classified events for psychometrics if available.
                    u_events = store.list_user_classified_events(org_id=resolved_org, user_id=uid)
                    prof = build_user_profile(uid, resolved_org, u_events)
                    _upsert_user_profile(
                        prof,
                        org_id=resolved_org,
                        user_id=uid,
                        pipeline_version=PIPELINE_VERSION,
                        updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
                    )
                profiles.append(prof)
            weights = derive_user_weights_from_profiles(profiles)

            hg = build_hypergraph(user_ids=user_ids, insights=insights, cards=cards, directions=directions)
            hg = _maybe_refine_hypergraph(hg)
            hg = _maybe_refine_hypergraph(hg)
            ranked = rank_directions_for_group(user_ids=user_ids, hg=hg, user_weights=weights, limit=int(limit))
            ranked_by_user = {uid: rank_directions_for_user(user_id=uid, hg=hg, limit=int(limit)) for uid in user_ids}

            # Persist discourse artifacts as memcubes (for downstream reads + auditability).
            now = utc_now_iso8601()
            for ins in insights:
                memcube = {
                    "org_id": resolved_org,
                    "memcube_id": _ensure_memcube_id("insight", ins.get("insight_id")),
                    "level": "org",
                    "entity_id": resolved_org,
                    "context_type": "insight",
                    "content": ins,
                    "metadata": {"user_id": ins.get("user_id"), "source_event_id": ins.get("source_event_id")},
                    "embedding": ins.get("embedding"),
                    "created_at": now,
                    "updated_at": now,
                }
                store.upsert_memcube(memcube)

            for card in cards:
                memcube = {
                    "org_id": resolved_org,
                    "memcube_id": _ensure_memcube_id("knowledge_card", card.get("card_id")),
                    "level": "org",
                    "entity_id": resolved_org,
                    "context_type": "knowledge_card",
                    "content": card,
                    "metadata": {},
                    "embedding": card.get("embedding"),
                    "created_at": now,
                    "updated_at": now,
                }
                store.upsert_memcube(memcube)

            memcube = {
                "org_id": resolved_org,
                "memcube_id": f"strategic_directions:{scope_type}:{scope_id}:{PIPELINE_VERSION}",
                "level": "org",
                "entity_id": resolved_org,
                "context_type": "strategic_directions",
                "content": {"directions": directions, "ranked": ranked, "ranked_by_user": ranked_by_user},
                "metadata": {"scope_type": scope_type, "scope_id": scope_id, "pipeline_version": PIPELINE_VERSION},
                "embedding": None,
                "created_at": now,
                "updated_at": now,
            }
            store.upsert_memcube(memcube)

            store.upsert_decide_cache(
                org_id=resolved_org,
                scope_type=scope_type,
                scope_id=scope_id,
                pipeline_version=PIPELINE_VERSION,
                directions=directions,
                ranked=ranked,
                ranked_by_user=ranked_by_user,
            )

            # Update org coordinator (FSA) to reflect the Decide phase readiness.
            # This matches the `scripts/discourse_decide_demo.py` behavior and makes the UI stepper intuitive.
            prev_state = store.get_org_fsa_state(org_id=resolved_org)
            org_state = apply_org_fsa_event(
                prev_state,
                org_id=resolved_org,
                action="decide.ready",
                context={
                    "ranked_directions": ranked,
                    "scope_type": scope_type,
                    "scope_id": scope_id,
                    "pipeline_version": PIPELINE_VERSION,
                    "computed_at": now,
                },
                event_id=None,
                timestamp=now,
            )
            store.upsert_org_fsa_state(org_state)

        direction_ids = {
            str(d.get("direction_id"))
            for d in (directions or [])
            if isinstance(d, dict) and isinstance(d.get("direction_id"), str) and str(d.get("direction_id") or "").strip()
        }
        vote_tallies, user_votes = _compute_vote_tallies(
            events=raw_events,
            direction_ids=direction_ids,
            team_id=str(team_id) if isinstance(team_id, str) and team_id else None,
            project_id=None,
            user_id=str(user_id) if isinstance(user_id, str) and user_id else None,
        )

        return {
            "org_id": resolved_org,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "pipeline_version": PIPELINE_VERSION,
            "directions": directions,
            "ranked": ranked,
            "ranked_by_user": ranked_by_user,
            "vote_tallies": vote_tallies,
            "user_votes": user_votes,
        }

    @app.get("/intelligence/projects/{project_id}/decide/directions")
    def get_project_decide_directions(
        project_id: str,
        *,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        user_id: Optional[str] = None,
        limit: int = 10,
        max_age_s: float = 300.0,
    ) -> dict[str, Any]:
        """Project-scoped Discourse→Decide pipeline (hierarchical within org Do).

        Filters raw events to the project, builds insights/cards/directions, and ranks directions:
        - for the group (`ranked`, with per-user contributions)
        - for each user (`ranked_by_user`)
        """
        resolved_org = org_id or store.resolve_org_id(team_id=team_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id with known org) is required.")
        if not isinstance(project_id, str) or not project_id.strip():
            raise HTTPException(status_code=400, detail="project_id is required.")

        scope_type = "project"
        scope_id = project_id

        raw_events = store.list_raw_events(org_id=resolved_org)
        project_events = [e for e in raw_events if e.get("project_id") == project_id]
        if isinstance(team_id, str) and team_id:
            project_events = [e for e in project_events if e.get("team_id") == team_id]

        cached = store.get_decide_cache(
            org_id=resolved_org,
            scope_type=scope_type,
            scope_id=scope_id,
            pipeline_version=PIPELINE_VERSION,
            max_age_s=float(max_age_s),
        )
        if cached is not None:
            directions = cached.get("directions") or []
            direction_ids = {
                str(d.get("direction_id"))
                for d in (directions or [])
                if isinstance(d, dict)
                and isinstance(d.get("direction_id"), str)
                and str(d.get("direction_id") or "").strip()
            }
            vote_tallies, user_votes = _compute_vote_tallies(
                events=project_events,
                direction_ids=direction_ids,
                team_id=str(team_id) if isinstance(team_id, str) and team_id else None,
                project_id=project_id,
                user_id=str(user_id) if isinstance(user_id, str) and user_id else None,
            )
            return {
                "org_id": resolved_org,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "project_id": project_id,
                "pipeline_version": PIPELINE_VERSION,
                "directions": directions,
                "ranked": cached.get("ranked") or [],
                "ranked_by_user": cached.get("ranked_by_user") or {},
                "vote_tallies": vote_tallies,
                "user_votes": user_votes,
            }

        if not project_events:
            raise HTTPException(status_code=404, detail="no raw events found for this project_id (ingest events first).")

        discourse_events = [e for e in project_events if _is_discourse_candidate_event(e)]

        insights = extract_insights(org_id=resolved_org, events=discourse_events, llm_mode="off")
        cards = build_knowledge_cards(org_id=resolved_org, insights=insights)
        directions = generate_strategic_directions(org_id=resolved_org, cards=cards, max_directions=int(limit))

        user_ids = sorted({e.get("user_id") for e in project_events if isinstance(e.get("user_id"), str)})
        profiles: list[dict[str, Any]] = []
        for uid in user_ids:
            prof = store.get_user_profile(org_id=resolved_org, user_id=uid)
            if prof is None:
                u_events = store.list_user_classified_events(org_id=resolved_org, user_id=uid)
                prof = build_user_profile(uid, resolved_org, u_events)
                _upsert_user_profile(
                    prof,
                    org_id=resolved_org,
                    user_id=uid,
                    pipeline_version=PIPELINE_VERSION,
                    updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
                )
            profiles.append(prof)
        weights = derive_user_weights_from_profiles(profiles)

        ranked: list[dict[str, Any]] = []
        ranked_by_user: dict[str, list[dict[str, Any]]] = {}
        if directions and user_ids:
            hg = build_hypergraph(user_ids=user_ids, insights=insights, cards=cards, directions=directions)
            ranked = rank_directions_for_group(user_ids=user_ids, hg=hg, user_weights=weights, limit=int(limit))
            ranked_by_user = {uid: rank_directions_for_user(user_id=uid, hg=hg, limit=int(limit)) for uid in user_ids}

        # Persist artifacts as project-scoped memcubes for auditability.
        now = utc_now_iso8601()
        for ins in insights:
            memcube = {
                "org_id": resolved_org,
                "memcube_id": _ensure_memcube_id("insight", ins.get("insight_id")),
                "level": "project",
                "entity_id": project_id,
                "context_type": "insight",
                "content": ins,
                "metadata": {"user_id": ins.get("user_id"), "source_event_id": ins.get("source_event_id")},
                "embedding": ins.get("embedding"),
                "created_at": now,
                "updated_at": now,
            }
            store.upsert_memcube(memcube)

        for card in cards:
            memcube = {
                "org_id": resolved_org,
                "memcube_id": _ensure_memcube_id("knowledge_card", card.get("card_id")),
                "level": "project",
                "entity_id": project_id,
                "context_type": "knowledge_card",
                "content": card,
                "metadata": {"supporting_event_ids": card.get("supporting_event_ids") or []},
                "embedding": card.get("embedding"),
                "created_at": now,
                "updated_at": now,
            }
            store.upsert_memcube(memcube)

        memcube = {
            "org_id": resolved_org,
            "memcube_id": f"strategic_directions:project:{project_id}:{PIPELINE_VERSION}",
            "level": "project",
            "entity_id": project_id,
            "context_type": "strategic_directions",
            "content": {"directions": directions, "ranked": ranked, "ranked_by_user": ranked_by_user},
            "metadata": {"scope_type": scope_type, "scope_id": scope_id, "pipeline_version": PIPELINE_VERSION},
            "embedding": None,
            "created_at": now,
            "updated_at": now,
        }
        store.upsert_memcube(memcube)

        store.upsert_decide_cache(
            org_id=resolved_org,
            scope_type=scope_type,
            scope_id=scope_id,
            pipeline_version=PIPELINE_VERSION,
            directions=directions,
            ranked=ranked,
            ranked_by_user=ranked_by_user,
        )

        # Update project coordinator (FSA) with the project-level Decide context.
        prev_proj = store.get_project_fsa_state(org_id=resolved_org, project_id=project_id)
        project_state = apply_project_fsa_event(
            prev_proj,
            org_id=resolved_org,
            project_id=project_id,
            action="decide.ready",
            context={
                "ranked_directions": ranked,
                "ranked_by_user": ranked_by_user,
                "scope_type": scope_type,
                "scope_id": scope_id,
                "pipeline_version": PIPELINE_VERSION,
                "computed_at": now,
            },
            event_id=None,
            user_id=None,
            timestamp=now,
        )
        store.upsert_project_fsa_state(project_state)

        direction_ids = {
            str(d.get("direction_id"))
            for d in (directions or [])
            if isinstance(d, dict) and isinstance(d.get("direction_id"), str) and str(d.get("direction_id") or "").strip()
        }
        vote_tallies, user_votes = _compute_vote_tallies(
            events=project_events,
            direction_ids=direction_ids,
            team_id=str(team_id) if isinstance(team_id, str) and team_id else None,
            project_id=project_id,
            user_id=str(user_id) if isinstance(user_id, str) and user_id else None,
        )

        return {
            "org_id": resolved_org,
            "scope_type": scope_type,
            "scope_id": scope_id,
            "project_id": project_id,
            "pipeline_version": PIPELINE_VERSION,
            "directions": directions,
            "ranked": ranked,
            "ranked_by_user": ranked_by_user,
            "vote_tallies": vote_tallies,
            "user_votes": user_votes,
        }

    @app.get("/intelligence/do/projects/ranked")
    def get_org_project_rankings(
        *,
        user_id: str,
        team_id: str,
        org_id: Optional[str] = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Server-side candidate retrieval + ranking for org projects (Do phase)."""
        resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")
        if not isinstance(team_id, str) or not team_id:
            raise HTTPException(status_code=400, detail="team_id is required.")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required.")

        state = store.get_org_fsa_state(org_id=resolved_org)
        if state is None:
            raise HTTPException(status_code=404, detail="org_fsa_state not found; ingest events first.")

        graph = state.get("project_graph") if isinstance(state.get("project_graph"), dict) else {}
        nodes = graph.get("nodes") if isinstance(graph.get("nodes"), dict) else {}
        edges = graph.get("edges") if isinstance(graph.get("edges"), list) else []

        blocked_by_project: dict[str, list[str]] = {}
        for e in edges:
            if not isinstance(e, dict):
                continue
            if str(e.get("type") or "depends_on") != "depends_on":
                continue
            src = e.get("from")
            dst = e.get("to")
            if isinstance(src, str) and isinstance(dst, str) and src and dst:
                blocked_by_project.setdefault(dst, []).append(src)
        for k, v in blocked_by_project.items():
            blocked_by_project[k] = sorted(set(v))

        state_by_id = {
            str(pid): str(meta.get("state") or "")
            for pid, meta in nodes.items()
            if isinstance(pid, str) and isinstance(meta, dict)
        }
        deps_ready = _ready_map(blocked_by=blocked_by_project, state_by_id=state_by_id, done_states={"completed"})

        now = datetime.now(timezone.utc)
        blocked: dict[str, bool] = {}
        for pid, meta in nodes.items():
            if not isinstance(pid, str) or not isinstance(meta, dict):
                continue
            is_blocked, _info = _block_status(
                meta=meta,
                now=now,
                state_by_id=state_by_id,
                done_states={"completed"},
                wait_key="waiting_for_project_id",
            )
            blocked[pid] = bool(is_blocked)

        # Build lightweight project membership/activity signals from raw events (demo-friendly).
        raw_events = store.list_raw_events(org_id=resolved_org)
        project_members: dict[str, set[str]] = {}
        project_last_ts: dict[str, str] = {}
        for ev in raw_events:
            if not isinstance(ev, dict):
                continue
            ev_pid = ev.get("project_id")
            ev_data = ev.get("event_data") if isinstance(ev.get("event_data"), dict) else {}
            pid = ev_pid if isinstance(ev_pid, str) and ev_pid else ev_data.get("project_id")
            if not isinstance(pid, str) or not pid:
                continue
            uid = ev.get("user_id")
            if isinstance(uid, str) and uid:
                project_members.setdefault(pid, set()).add(uid)
            ts = ev.get("timestamp")
            if isinstance(ts, str) and ts:
                prev_ts = project_last_ts.get(pid)
                if not prev_ts or ts > prev_ts:
                    project_last_ts[pid] = ts

        items: list[dict[str, Any]] = []
        for pid, meta in nodes.items():
            if not isinstance(pid, str) or not pid:
                continue
            meta_dict = meta if isinstance(meta, dict) else {}
            proj_state = str(meta_dict.get("state") or "planned")
            if proj_state in {"completed", "archived"}:
                continue
            if not bool(deps_ready.get(pid, True)) or bool(blocked.get(pid)):
                continue

            members = sorted(project_members.get(pid) or set())
            data: dict[str, Any] = {"members": members}
            deps = blocked_by_project.get(pid) or []
            if deps:
                data["dependencies"] = deps

            md: dict[str, Any] = {}
            for key in ("priority", "created_at", "updated_at", "deadline"):
                if key in meta_dict:
                    md[key] = meta_dict.get(key)
            if pid in project_last_ts:
                md["updated_at"] = project_last_ts[pid]

            items.append({"item_id": pid, "item_type": "project", "data": data, "metadata": md})

        user_profile = store.get_user_profile(org_id=resolved_org, user_id=user_id)
        if user_profile is None:
            events = store.list_user_classified_events(org_id=resolved_org, user_id=user_id)
            user_profile = build_user_profile(user_id, resolved_org, events)
            _upsert_user_profile(
                user_profile,
                org_id=resolved_org,
                user_id=user_id,
                pipeline_version=PIPELINE_VERSION,
                updated_at=user_profile.get("updated_at") if isinstance(user_profile, dict) else None,
            )

        team_profile = store.get_team_profile(org_id=resolved_org, team_id=team_id)
        if team_profile is None:
            team_events = store.list_team_classified_events(org_id=resolved_org, team_id=team_id)
            member_ids = sorted({str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)})
            member_profiles: list[dict[str, Any]] = []
            for uid in member_ids:
                prof = store.get_user_profile(org_id=resolved_org, user_id=uid)
                if prof is None:
                    u_events = store.list_user_classified_events(org_id=resolved_org, user_id=uid)
                    prof = build_user_profile(uid, resolved_org, u_events)
                    _upsert_user_profile(
                        prof,
                        org_id=resolved_org,
                        user_id=uid,
                        pipeline_version=PIPELINE_VERSION,
                        updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
                    )
                member_profiles.append(prof)
            team_profile = build_team_profile(team_id, resolved_org, member_profiles, team_events)
            _upsert_team_profile(
                team_profile,
                org_id=resolved_org,
                team_id=team_id,
                pipeline_version=PIPELINE_VERSION,
            )

        ranked_items = rank_feed(
            feed_type="projects",
            user_id=user_id,
            team_id=team_id,
            project_id=None,
            items=items,
            context={"org_stage": state.get("stage")},
            user_profile=user_profile,
            team_profile=team_profile,
            user_profiles=store.list_user_profiles(org_id=resolved_org),
            team_profiles=store.list_team_profiles(org_id=resolved_org),
        )

        return {
            "org_id": resolved_org,
            "team_id": team_id,
            "user_id": user_id,
            "total": len(items),
            "ranked_items": ranked_items[: int(limit)],
        }

    @app.get("/intelligence/projects/{project_id}/do/tasks/ranked")
    def get_project_task_rankings(
        project_id: str,
        *,
        user_id: str,
        org_id: Optional[str] = None,
        team_id: Optional[str] = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        """Server-side candidate retrieval + ranking for project tasks (Do phase)."""
        resolved_org = org_id or store.resolve_org_id(team_id=team_id, user_id=user_id)
        if not isinstance(resolved_org, str) or not resolved_org:
            raise HTTPException(status_code=400, detail="org_id (or team_id/user_id with known org) is required.")
        if not isinstance(team_id, str) or not team_id:
            raise HTTPException(status_code=400, detail="team_id is required.")
        if not project_id:
            raise HTTPException(status_code=400, detail="project_id is required.")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required.")

        state = store.get_project_fsa_state(org_id=resolved_org, project_id=project_id)
        if state is None:
            raise HTTPException(status_code=404, detail="project_fsa_state not found; ingest project events first.")

        graph = state.get("task_graph") if isinstance(state.get("task_graph"), dict) else {}
        nodes = graph.get("nodes") if isinstance(graph.get("nodes"), dict) else {}
        edges = graph.get("edges") if isinstance(graph.get("edges"), list) else []
        tasks_state = state.get("tasks") if isinstance(state.get("tasks"), dict) else {}

        blockers_by_task: dict[str, list[str]] = {}
        for e in edges:
            if not isinstance(e, dict):
                continue
            src = e.get("from")
            dst = e.get("to")
            if isinstance(src, str) and isinstance(dst, str) and src and dst:
                blockers_by_task.setdefault(dst, []).append(src)
        for k, v in blockers_by_task.items():
            blockers_by_task[k] = sorted(set(v))

        state_by_id = {str(k): str(v) for k, v in tasks_state.items() if isinstance(k, str) and isinstance(v, str)}
        now = datetime.now(timezone.utc)
        done_states = {"completed_pending_approval", "completed_approved"}

        items: list[dict[str, Any]] = []
        for tid, node in nodes.items():
            if not isinstance(tid, str) or not tid:
                continue
            node_dict = node if isinstance(node, dict) else {}
            task_state_local = (
                str(tasks_state.get(tid))
                if isinstance(tasks_state.get(tid), str)
                else str(node_dict.get("state") or "")
            )

            dependencies = blockers_by_task.get(tid) or []
            unsatisfied = [
                dep for dep in dependencies if state_by_id.get(dep) not in {"completed_approved"}
            ]

            is_blocked, block_info = _block_status(
                meta=node_dict,
                now=now,
                state_by_id=state_by_id,
                done_states={"completed_approved"},
                wait_key="waiting_for_task_id",
            )

            # Do not recommend blocked tasks (except completed/review states).
            if task_state_local not in done_states and (unsatisfied or is_blocked):
                continue

            task_type = node_dict.get("task_type")
            assignee = node_dict.get("assignee_id")
            effort = node_dict.get("effort")
            meta: dict[str, Any] = {}
            if isinstance(assignee, str) and assignee:
                meta["assigned_to"] = [assignee]
            if isinstance(effort, str) and effort:
                meta["effort"] = effort
            for key in ("deadline", "priority", "created_at", "updated_at"):
                if key in node_dict:
                    meta[key] = node_dict.get(key)
            data: dict[str, Any] = {}
            if isinstance(task_type, str) and task_type:
                data["task_type"] = task_type
            if dependencies:
                data["dependencies"] = dependencies
            # Explicit blockers list (even if empty) so the factor reflects current block status
            # rather than the total number of historical dependencies.
            data["blockers"] = unsatisfied
            if block_info:
                data["block"] = block_info
            items.append({"item_id": tid, "item_type": "task", "data": data, "metadata": meta})

        # If task_graph isn't populated yet, fallback to the simple task-state map.
        if not items and not nodes:
            tasks = state.get("tasks") if isinstance(state.get("tasks"), dict) else {}
            for tid in tasks.keys():
                if isinstance(tid, str) and tid:
                    items.append({"item_id": tid, "item_type": "task", "data": {}, "metadata": {}})

        user_profile = store.get_user_profile(org_id=resolved_org, user_id=user_id)
        if user_profile is None:
            events = store.list_user_classified_events(org_id=resolved_org, user_id=user_id)
            user_profile = build_user_profile(user_id, resolved_org, events)
            _upsert_user_profile(
                user_profile,
                org_id=resolved_org,
                user_id=user_id,
                pipeline_version=PIPELINE_VERSION,
                updated_at=user_profile.get("updated_at") if isinstance(user_profile, dict) else None,
            )

        team_profile = store.get_team_profile(org_id=resolved_org, team_id=team_id)
        if team_profile is None:
            team_events = store.list_team_classified_events(org_id=resolved_org, team_id=team_id)
            member_ids = sorted({str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)})
            member_profiles: list[dict[str, Any]] = []
            for uid in member_ids:
                prof = store.get_user_profile(org_id=resolved_org, user_id=uid)
                if prof is None:
                    u_events = store.list_user_classified_events(org_id=resolved_org, user_id=uid)
                    prof = build_user_profile(uid, resolved_org, u_events)
                    _upsert_user_profile(
                        prof,
                        org_id=resolved_org,
                        user_id=uid,
                        pipeline_version=PIPELINE_VERSION,
                        updated_at=prof.get("updated_at") if isinstance(prof, dict) else None,
                    )
                member_profiles.append(prof)
            team_profile = build_team_profile(team_id, resolved_org, member_profiles, team_events)
            _upsert_team_profile(
                team_profile,
                org_id=resolved_org,
                team_id=team_id,
                pipeline_version=PIPELINE_VERSION,
            )

        ranked_items = rank_feed(
            feed_type="tasks",
            user_id=user_id,
            team_id=team_id,
            project_id=project_id,
            items=items,
            context={"project_stage": state.get("stage")},
            user_profile=user_profile,
            team_profile=team_profile,
            user_profiles=store.list_user_profiles(org_id=resolved_org),
            team_profiles=store.list_team_profiles(org_id=resolved_org),
        )

        return {
            "org_id": resolved_org,
            "team_id": team_id,
            "project_id": project_id,
            "total": len(items),
            "ranked_items": ranked_items[: int(limit)],
        }

    # -------------------------------------------------------------------------
    # Bandit Policy Endpoints (Learned Controller)
    # -------------------------------------------------------------------------

    @app.get("/intelligence/ux/policy/recommend")
    def bandit_recommend(
        *,
        org_id: str,
        scope_type: str = "team",
        scope_id: str,
        use_bandit: bool = False,
    ) -> dict[str, Any]:
        """Get intervention recommendation using bandit or heuristic policy.

        If use_bandit=True and a trained policy exists, uses the learned policy.
        Otherwise falls back to heuristic recommendations.
        """
        from .ux_control import recommend_ux_interventions

        pv = PIPELINE_VERSION
        user_profiles: list[dict[str, Any]] = []
        team_profile: dict[str, Any] | None = None

        if scope_type == "user":
            p = store.get_user_profile(org_id=org_id, user_id=scope_id)
            if p:
                user_profiles = [p]
        elif scope_type == "team":
            team_profile = store.get_team_profile(org_id=org_id, team_id=scope_id)
            # Get user profiles for team members
            team_events = store.list_team_classified_events(org_id=org_id, team_id=scope_id)
            member_ids = sorted({str(e.get("user_id")) for e in team_events if isinstance(e.get("user_id"), str)})
            for uid in member_ids[:50]:  # Limit to 50 members
                p = store.get_user_profile(org_id=org_id, user_id=uid)
                if p:
                    user_profiles.append(p)

        interventions = store.list_ux_interventions(org_id=org_id, limit=100)

        # Try bandit if requested
        bandit_used = False
        bandit_arm: str | None = None
        bandit_confidence: float | None = None

        if use_bandit:
            try:
                from collectium_intelligence.bandit import (
                    load_policy,
                    recommend_intervention,
                )

                policy_path = f"/tmp/bandit_policy_{org_id}.json"
                policy = load_policy(policy_path)
                if policy is not None:
                    heuristic_result = recommend_ux_interventions(
                        user_profiles=user_profiles,
                        team_profile=team_profile,
                        interventions=interventions,
                    )
                    group_metrics = heuristic_result.get("group_metrics", {})
                    arm_keys = [str(i.get("intervention_key")) for i in interventions if i.get("intervention_key")]

                    if arm_keys and group_metrics:
                        bandit_arm = recommend_intervention(policy, group_metrics, arm_keys)
                        bandit_used = True
                        # Get UCB confidence
                        from collectium_intelligence.bandit import extract_context
                        ctx = extract_context(group_metrics)
                        theta = policy.get_theta(bandit_arm)
                        bandit_confidence = float(ctx @ theta) if theta is not None else None
            except Exception:
                pass

        # Always compute heuristic as fallback/comparison
        result = recommend_ux_interventions(
            user_profiles=user_profiles,
            team_profile=team_profile,
            interventions=interventions,
        )

        # If bandit selected an arm, reorder recommendations
        if bandit_used and bandit_arm:
            recs = result.get("recommendations", [])
            bandit_rec = next((r for r in recs if r.get("intervention_key") == bandit_arm), None)
            if bandit_rec:
                bandit_rec["bandit_selected"] = True
                bandit_rec["bandit_confidence"] = bandit_confidence
                # Move to front
                recs = [bandit_rec] + [r for r in recs if r.get("intervention_key") != bandit_arm]
                result["recommendations"] = recs

        result["policy_type"] = "bandit" if bandit_used else "heuristic"
        result["bandit_arm"] = bandit_arm
        return result

    @app.post("/intelligence/ux/policy/train")
    def train_bandit_policy(*, org_id: str, policy_type: str = "linucb") -> dict[str, Any]:
        """Train bandit policy from historical intervention logs.

        Requires intervention runs with measured effects.
        """
        from collectium_intelligence.bandit import (
            LinUCBPolicy,
            ThompsonSamplingPolicy,
            train_policy_from_logs,
            save_policy,
            evaluate_policy_offline,
        )

        # Gather intervention logs with effects
        runs = store.list_ux_intervention_runs(org_id=org_id, limit=10000)
        logs: list[dict[str, Any]] = []

        for run in runs:
            if not run.get("measured_at"):
                continue
            delta = run.get("delta") if isinstance(run.get("delta"), dict) else {}
            wb_delta = delta.get("wellbeing_proxies") if isinstance(delta.get("wellbeing_proxies"), dict) else {}

            # Compute reward from wellbeing improvement
            improvements = [v for v in wb_delta.values() if isinstance(v, (int, float)) and v > 0]
            reward = sum(improvements) / max(len(improvements), 1) if improvements else 0.0

            # Get pre-state group metrics
            pre_state = run.get("pre_state") if isinstance(run.get("pre_state"), dict) else {}
            psych = pre_state.get("team_profile_psychodynamics") or pre_state.get("user_profile_psychodynamics")
            if not isinstance(psych, dict):
                continue

            group_metrics = {
                "avg_animals": psych.get("animals", {}),
                "avg_kernel_drift": psych.get("kernel_drift", 0.0),
                "te_reciprocity": psych.get("te_reciprocity"),
                "dominance_ratio": psych.get("dominance_ratio", 0.0),
                "avg_mean_certainty": psych.get("mean_certainty", 0.0),
            }

            logs.append({
                "intervention_key": run.get("intervention_key"),
                "group_metrics": group_metrics,
                "reward": float(reward),
            })

        if len(logs) < 10:
            return {"status": "insufficient_data", "n_logs": len(logs), "min_required": 10}

        # Get arm keys
        interventions = store.list_ux_interventions(org_id=org_id, limit=100)
        arm_keys = list({str(i.get("intervention_key")) for i in interventions if i.get("intervention_key")})

        if not arm_keys:
            return {"status": "no_interventions", "n_logs": len(logs)}

        # Train policy
        policy = train_policy_from_logs(logs, arm_keys, policy_type=policy_type)

        # Evaluate offline
        eval_result = evaluate_policy_offline(policy, logs)

        # Save policy
        policy_path = f"/tmp/bandit_policy_{org_id}.json"
        save_policy(policy, policy_path)

        return {
            "status": "trained",
            "policy_type": policy_type,
            "n_logs": len(logs),
            "n_arms": len(arm_keys),
            "arm_keys": arm_keys,
            "evaluation": eval_result,
            "policy_path": policy_path,
        }

    # -------------------------------------------------------------------------
    # Causal Attribution Endpoints
    # -------------------------------------------------------------------------

    @app.get("/intelligence/ux/attribution/report")
    def get_attribution_report(
        *,
        org_id: str,
        intervention_key: str | None = None,
        scope_type: str | None = None,
        scope_id: str | None = None,
        min_runs: int = 5,
    ) -> dict[str, Any]:
        """Generate causal attribution report for interventions."""
        from collectium_intelligence.intervention_effects import aggregate_effects, InterventionEffect
        from datetime import datetime

        runs = store.list_ux_intervention_runs(
            org_id=org_id,
            scope_type=scope_type,
            scope_id=scope_id,
            limit=10000,
        )

        # Filter to measured runs
        measured_runs = [r for r in runs if r.get("measured_at")]
        if intervention_key:
            measured_runs = [r for r in measured_runs if r.get("intervention_key") == intervention_key]

        if len(measured_runs) < min_runs:
            return {
                "status": "insufficient_data",
                "n_runs": len(measured_runs),
                "min_required": min_runs,
            }

        # Build effects list
        effects: list[InterventionEffect] = []
        for run in measured_runs:
            delta = run.get("delta") if isinstance(run.get("delta"), dict) else {}
            wb_delta = delta.get("wellbeing_proxies") if isinstance(delta.get("wellbeing_proxies"), dict) else {}

            pre_state = run.get("pre_state") if isinstance(run.get("pre_state"), dict) else {}
            post_state = run.get("post_state") if isinstance(run.get("post_state"), dict) else {}

            # Extract wellbeing proxies
            def extract_wb(state: dict) -> dict:
                psych = state.get("team_profile_psychodynamics") or state.get("user_profile_psychodynamics")
                if isinstance(psych, dict):
                    return psych.get("wellbeing_proxies", {})
                return {}

            pre_wb = extract_wb(pre_state)
            post_wb = extract_wb(post_state)

            if not pre_wb and not post_wb:
                continue

            # Compute effect size
            common_keys = set(pre_wb.keys()) & set(post_wb.keys())
            if not common_keys:
                continue

            pre_vals = [pre_wb[k] for k in common_keys]
            post_vals = [post_wb[k] for k in common_keys]

            pre_mean = sum(pre_vals) / len(pre_vals)
            post_mean = sum(post_vals) / len(post_vals)
            pooled_std = (sum((x - pre_mean)**2 for x in pre_vals) + sum((x - post_mean)**2 for x in post_vals))
            pooled_std = (pooled_std / max(len(pre_vals) + len(post_vals) - 2, 1)) ** 0.5 if pooled_std > 0 else 1.0
            effect_size = (post_mean - pre_mean) / max(pooled_std, 1e-10)

            effects.append(InterventionEffect(
                run_id=str(run.get("run_id")),
                intervention_key=str(run.get("intervention_key")),
                pre_wellbeing=pre_wb,
                post_wellbeing=post_wb,
                delta=wb_delta,
                effect_size=effect_size,
                confidence_interval=(-1.0, 1.0),  # Simplified
                p_value=0.05 if abs(effect_size) > 0.2 else 0.5,  # Simplified
                significant=abs(effect_size) > 0.2,
                attributed_to=str(run.get("intervention_key")),
                measurement_time=datetime.now(),
                observation_window_seconds=3600,
                sample_size=len(common_keys),
            ))

        # Aggregate
        summary = aggregate_effects(effects)

        # Generate recommendations
        recommendations: list[str] = []
        for key, stats in summary.get("effects_by_intervention", {}).items():
            if stats.get("success_rate", 0) > 0.7:
                recommendations.append(f"Scale up '{key}' - high success rate ({stats['success_rate']:.0%})")
            elif stats.get("success_rate", 0) < 0.3 and stats.get("n_runs", 0) > 10:
                recommendations.append(f"Consider discontinuing '{key}' - low success rate ({stats['success_rate']:.0%})")
            elif stats.get("n_runs", 0) < 10:
                recommendations.append(f"Collect more data for '{key}' - only {stats['n_runs']} runs")

        return {
            "status": "success",
            "org_id": org_id,
            "intervention_key": intervention_key,
            "summary": summary,
            "recommendations": recommendations,
            "n_effects": len(effects),
        }

    @app.post("/intelligence/ux/attribution/did")
    def run_difference_in_differences(
        *,
        org_id: str,
        intervention_key: str,
        treatment_scope_id: str,
        control_scope_ids: list[str],
        scope_type: str = "team",
        pre_periods: int = 7,
        post_periods: int = 7,
    ) -> dict[str, Any]:
        """Run Difference-in-Differences analysis for an intervention."""
        from collectium_intelligence.causal_attribution import (
            difference_in_differences,
            parallel_trends_test,
            UnitTimeSeries,
        )

        # Get wellbeing windows for treatment and control groups
        def get_unit_series(scope_id: str, is_treated: bool) -> UnitTimeSeries | None:
            windows = store.list_wellbeing_windows(
                org_id=org_id,
                scope_type=scope_type,
                scope_id=scope_id,
                limit=pre_periods + post_periods + 10,
            )
            if len(windows) < pre_periods + post_periods:
                return None

            # Extract outcomes (mean wellbeing proxy value per window)
            outcomes = []
            for w in sorted(windows, key=lambda x: x.get("window_end", "")):
                proxies = w.get("proxies") if isinstance(w.get("proxies"), dict) else {}
                if proxies:
                    outcomes.append(sum(proxies.values()) / len(proxies))

            if len(outcomes) < pre_periods + post_periods:
                return None

            treatment_time = pre_periods if is_treated else None

            return UnitTimeSeries(
                unit_id=scope_id,
                outcomes=outcomes[: pre_periods + post_periods],
                treatment_time=treatment_time,
            )

        # Build treatment series
        treatment_series = get_unit_series(treatment_scope_id, is_treated=True)
        if treatment_series is None:
            return {"status": "insufficient_treatment_data", "treatment_scope_id": treatment_scope_id}

        # Build control series
        control_series = []
        for cid in control_scope_ids:
            cs = get_unit_series(cid, is_treated=False)
            if cs:
                control_series.append(cs)

        if not control_series:
            return {"status": "insufficient_control_data", "control_scope_ids": control_scope_ids}

        # Run DiD
        did_result = difference_in_differences(treatment_series, control_series, pre_periods)

        # Run parallel trends test
        pt_result = parallel_trends_test(treatment_series, control_series, pre_periods)

        return {
            "status": "success",
            "intervention_key": intervention_key,
            "treatment_scope_id": treatment_scope_id,
            "control_scope_ids": [cs.unit_id for cs in control_series],
            "did_estimate": did_result.estimate,
            "did_std_error": did_result.std_error,
            "did_t_stat": did_result.t_stat,
            "did_p_value": did_result.p_value,
            "did_significant": did_result.significant,
            "parallel_trends_p_value": pt_result.get("p_value"),
            "parallel_trends_valid": pt_result.get("p_value", 0) > 0.05,
        }

    @app.get("/intelligence/ux/effects/aggregate")
    def get_aggregated_effects(
        *,
        org_id: str,
        intervention_key: str | None = None,
        days: int = 30,
    ) -> dict[str, Any]:
        """Get aggregated intervention effects over time."""
        from collectium_intelligence.intervention_effects import aggregate_effects, InterventionEffect
        from datetime import datetime, timedelta

        runs = store.list_ux_intervention_runs(org_id=org_id, limit=10000)
        cutoff = datetime.now() - timedelta(days=days)

        # Filter by date and measurement status
        recent_runs = []
        for r in runs:
            measured_at = r.get("measured_at")
            if not measured_at:
                continue
            try:
                ts = datetime.fromisoformat(measured_at.replace("Z", "+00:00"))
                if ts >= cutoff:
                    recent_runs.append(r)
            except:
                pass

        if intervention_key:
            recent_runs = [r for r in recent_runs if r.get("intervention_key") == intervention_key]

        # Build simplified effects
        effects = []
        for run in recent_runs:
            delta = run.get("delta", {}).get("wellbeing_proxies", {})
            if not delta:
                continue

            improvements = [v for v in delta.values() if isinstance(v, (int, float))]
            if not improvements:
                continue

            effect_size = sum(improvements) / len(improvements)
            significant = abs(effect_size) > 0.1

            effects.append(InterventionEffect(
                run_id=str(run.get("run_id")),
                intervention_key=str(run.get("intervention_key")),
                pre_wellbeing={},
                post_wellbeing={},
                delta=delta,
                effect_size=effect_size,
                confidence_interval=(-1, 1),
                p_value=0.05 if significant else 0.5,
                significant=significant,
                attributed_to=str(run.get("intervention_key")),
                measurement_time=datetime.now(),
                observation_window_seconds=3600,
                sample_size=len(improvements),
            ))

        summary = aggregate_effects(effects)

        return {
            "org_id": org_id,
            "intervention_key": intervention_key,
            "days": days,
            "summary": summary,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Demo / Seeding Endpoints
    # ─────────────────────────────────────────────────────────────────────────

    @app.post("/intelligence/demo/seed")
    async def seed_demo_data(
        org_id: str = "demo-org",
        n_users: int = 5,
        n_events: int = 100,
    ) -> dict[str, Any]:
        """Seed demo data for testing the full Discourse -> Decide -> Do flow.

        This creates:
        - Sample users with varied psychodynamic profiles
        - Discourse events (messages, discussions)
        - Classified events with animal scores
        - User profiles with Markov kernels
        - Strategic directions and knowledge cards
        - Sample wellbeing windows
        """
        import random

        animals = ["seeking", "directing", "conferring", "revising"]
        event_types = [
            "message.sent", "message.received", "discussion.started",
            "decision.proposed", "decision.voted", "task.created",
            "task.completed", "document.shared", "meeting.scheduled"
        ]
        context_blocks = ["discourse", "decide", "do"]

        users = [f"user-{i}" for i in range(n_users)]
        created_events = []

        # Generate events
        for i in range(n_events):
            user_id = random.choice(users)
            event_type = random.choice(event_types)

            # Assign context block based on event type
            if "message" in event_type or "discussion" in event_type:
                block = "discourse"
            elif "decision" in event_type:
                block = "decide"
            else:
                block = "do"

            # Generate animal scores
            scores = {a: random.random() for a in animals}
            total = sum(scores.values())
            scores = {a: v / total for a, v in scores.items()}
            phase = max(scores, key=scores.get)

            event = {
                "event_id": str(uuid.uuid4()),
                "org_id": org_id,
                "user_id": user_id,
                "event_type": event_type,
                "event_data": {
                    "content": f"Sample {event_type} content #{i}",
                    "thread_id": f"thread-{i % 10}",
                },
                "context": {
                    "collectium_context": {
                        "block": block,
                        "phase": phase,
                    }
                },
                "timestamp": utc_now_iso8601(),
            }

            # Store raw event
            store.upsert_event_raw(event)

            # Store classified event
            classified = {
                "event_id": event["event_id"],
                "org_id": org_id,
                "user_id": user_id,
                "context_block": block,
                "phase": phase,
                "animal_scores": scores,
                "certainty": 0.5 + random.random() * 0.5,
            }
            store.upsert_event_classified(classified)
            created_events.append(event["event_id"])

        # Generate user profiles
        for user_id in users:
            # Random animal distribution
            dist = {a: random.random() for a in animals}
            total = sum(dist.values())
            dist = {a: v / total for a, v in dist.items()}

            # Random Markov kernel (row-stochastic)
            kernel = {}
            for from_animal in animals:
                row = {to: random.random() for to in animals}
                row_total = sum(row.values())
                kernel[from_animal] = {to: v / row_total for to, v in row.items()}

            profile = {
                "user_id": user_id,
                "org_id": org_id,
                "animal_distribution": dist,
                "markov_kernel": kernel,
                "certainty_mean": 0.5 + random.random() * 0.3,
                "certainty_variance": random.random() * 0.1,
                "event_count": random.randint(10, 100),
            }
            store.upsert_user_profile(profile)

        # Generate strategic directions
        directions = [
            {"id": "dir-1", "title": "Improve team communication", "score": 0.85, "evidence_count": 12},
            {"id": "dir-2", "title": "Reduce meeting overhead", "score": 0.72, "evidence_count": 8},
            {"id": "dir-3", "title": "Streamline decision process", "score": 0.68, "evidence_count": 6},
        ]
        for d in directions:
            store.upsert_memcube({
                "id": d["id"],
                "org_id": org_id,
                "context_type": "direction",
                "content": d,
            })

        # Generate knowledge cards
        cards = [
            {"id": "card-1", "title": "Communication patterns", "summary": "Team prefers async communication"},
            {"id": "card-2", "title": "Decision velocity", "summary": "Decisions take avg 2.3 days"},
            {"id": "card-3", "title": "Task completion", "summary": "85% completion rate on weekly tasks"},
        ]
        for c in cards:
            store.upsert_memcube({
                "id": c["id"],
                "org_id": org_id,
                "context_type": "knowledge_card",
                "content": c,
            })

        # Generate wellbeing windows
        for user_id in users:
            window = {
                "org_id": org_id,
                "scope_type": "user",
                "scope_id": user_id,
                "window_start": utc_now_iso8601(),
                "window_end": utc_now_iso8601(),
                "pipeline_version": PIPELINE_VERSION,
                "proxies": {
                    "engagement": 0.5 + random.random() * 0.5,
                    "autonomy": 0.5 + random.random() * 0.5,
                    "mastery": 0.5 + random.random() * 0.5,
                    "purpose": 0.5 + random.random() * 0.5,
                },
                "composite_score": 0.5 + random.random() * 0.4,
            }
            store.upsert_wellbeing_window(window)

        # Generate block matrix
        block_matrix = {
            from_a: {to_a: random.random() for to_a in animals}
            for from_a in animals
        }
        store.upsert_psychodynamic_block_matrix({
            "org_id": org_id,
            "scope_type": "org",
            "scope_id": org_id,
            "context_block": "all",
            "pipeline_version": PIPELINE_VERSION,
            "block_matrix": block_matrix,
        })

        # Generate influence layer (TE matrix)
        influence = {
            u1: {u2: random.random() * 0.5 for u2 in users if u2 != u1}
            for u1 in users
        }
        store.upsert_psychodynamic_influence_layer({
            "org_id": org_id,
            "team_id": "default-team",
            "context_block": "all",
            "pipeline_version": PIPELINE_VERSION,
            "influence_matrix": influence,
        })

        return {
            "status": "seeded",
            "org_id": org_id,
            "users_created": len(users),
            "events_created": len(created_events),
            "directions_created": len(directions),
            "knowledge_cards_created": len(cards),
            "message": f"Demo data ready! Visit /intelligence to explore.",
        }

    @app.get("/intelligence/demo/status")
    async def demo_status(org_id: str = "demo-org") -> dict[str, Any]:
        """Check what demo data exists for an org."""
        events = store.list_raw_events(org_id=org_id, limit=10000)
        profiles = store.list_user_profiles(org_id=org_id)
        memcubes = store.list_memcubes(org_id=org_id)

        directions = [m for m in memcubes if m.get("context_type") == "direction"]
        cards = [m for m in memcubes if m.get("context_type") == "knowledge_card"]

        return {
            "org_id": org_id,
            "has_data": len(events) > 0,
            "event_count": len(events),
            "user_count": len(profiles),
            "direction_count": len(directions),
            "knowledge_card_count": len(cards),
        }

    return app


# Convenience for `uvicorn backend.app:app --reload`
app = create_app()
