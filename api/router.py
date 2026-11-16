"""The verb table served over HTTP: `POST /api/v1/<verb>` for each verb, and `GET /api/v1/tools`, the MCP tool
list (name, description, JSON schema, read or write) with the server instructions.

Every request has passed the token gate. A write verb also needs the token as `Authorization: Bearer`: the
gate's cookie rides along on any same-site request, and every localhost port is the same site. A write verb
that names a project runs only on a project within the agent's reach (`api/reach.py`: the API's own, and the
user's own while User settings allow it); afterwards the project is saved and the write recorded. A verb that
deletes for good runs only as a step of an approved change plan (`api/plans.py`). A handler's outcome comes
back as JSON with status 200, refused or not; 403 and 422 are for a request the dispatcher turns away before
any handler runs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import TypeAdapter, ValidationError

from api.context import ApiContext, open_project, record_write
from api.handlers import API_VERSION, Call
from api.models import Need
from api.reach import write_refusal
from api.verbs import VERBS, Verb
from services.event_log import events
from services.project_state import get_project_state_for
from services.result import ErrorCode, err
from services.server_auth import AUTH_SCOPE_KEY

logger = logging.getLogger(__name__)

INSTRUCTIONS_FILE = Path(__file__).with_name("instructions.md")
# Claude Code keeps this much of a server's instructions and drops the rest without a word.
INSTRUCTIONS_LIMIT = 4096

_NEEDS = TypeAdapter(list[Need])


def instructions() -> str:
    return INSTRUCTIONS_FILE.read_text().strip()


def tool_entry(verb: Verb) -> dict[str, Any]:
    """One MCP tool: the verb's name, description and input schema. Reads carry `readOnlyHint`; the verbs
    that make a project or submit or cancel SLURM work ask the user on every call."""
    entry: dict[str, Any] = {
        "name": verb.name,
        "description": verb.description,
        "inputSchema": verb.model.model_json_schema(),
        "kind": verb.kind,
        "annotations": {"readOnlyHint": verb.kind == "read"},
    }
    if verb.asks:
        entry["_meta"] = {"anthropic/requiresUserInteraction": True}
    return entry


def _answer(result: dict[str, Any], status: int = 200) -> JSONResponse:
    """Every answer carries `needs` and `warnings`, the needs checked against their shape."""
    result.setdefault("needs", [])
    result.setdefault("warnings", [])
    try:
        result["needs"] = [n.model_dump() for n in _NEEDS.validate_python(result["needs"])]
    except ValidationError:
        logger.exception("A result's needs do not have the Need shape: %s", result["needs"])
    return JSONResponse(jsonable_encoder(result), status_code=status)


async def dispatch(ctx: ApiContext, verb: Verb, request: Request) -> JSONResponse:
    if verb.kind == "write" and request.scope.get(AUTH_SCOPE_KEY) != "bearer":
        return _answer(
            err(f"{verb.name} changes a project: send the token as an Authorization: Bearer header, not the cookie."),
            403,
        )
    raw = await request.body()
    try:
        data = json.loads(raw) if raw.strip() else {}
        args = verb.model.model_validate(data)
    except ValidationError as e:
        rejected = {".".join(str(p) for p in x["loc"]) or "body": x["msg"] for x in e.errors()}
        return _answer(err(f"Bad input for {verb.name}: {rejected}", rejected=rejected), 422)
    except ValueError as e:
        return _answer(err(f"Bad input for {verb.name}: the body is not JSON ({e})"), 422)
    return _answer(await run_verb(ctx, verb, args))


async def run_verb(ctx: ApiContext, verb: Verb, args, *, plan_id: str | None = None) -> dict[str, Any]:
    """One verb on validated input, as a single call or a plan's step: open its project, check the gate for a
    write, run the handler, then save the project and record the write."""
    if verb.plan_only and plan_id is None:
        return err(
            f"{verb.name} deletes for good: it runs only as a step of a plan the user approved on its page "
            "(plan_changes, then apply_changes)."
        )
    path = None
    hand_made = False
    if verb.names_project:
        path, why = await open_project(args.project)
        if why is not None:
            return why
        if verb.kind == "write":
            state = get_project_state_for(path)
            refusal = await write_refusal(state, path)
            if refusal is not None:
                return err(refusal, code=ErrorCode.NOT_WRITABLE, ui_url=ctx.ui_url(path))
            hand_made = not state.made_by_api

    try:
        result = await verb.handler(Call(ctx, args, path))
    except Exception as e:
        logger.exception("Agent API verb %s failed", verb.name)
        result = err(f"{verb.name} failed: {e}")
    # A write that went to a background task saves and records when the task ends. A deleted project has
    # nothing left to save into or record in: the console event is its record.
    if verb.kind == "write" and path is not None and not result.get("task_id"):
        if verb.name == "delete_project":
            if result.get("success"):
                events.info("%s · agent: deleted the project (%s)", path.name, path)
        else:
            if result.get("success"):
                await ctx.backend.save_project(path)
            await record_write(path, verb.name, args.model_dump(), result, plan_id=plan_id, hand_made=hand_made)
    return result


def create_api_router(backend, port: int) -> APIRouter:
    ctx = ApiContext(backend=backend, port=port)
    ctx.verbs = VERBS

    async def _run(verb: Verb, args, *, plan_id: str | None = None) -> dict[str, Any]:
        return await run_verb(ctx, verb, args, plan_id=plan_id)

    ctx.run_verb = _run
    router = APIRouter(prefix="/api/v1")

    @router.get("/tools", name="tools")
    async def tools() -> JSONResponse:
        return JSONResponse(
            {
                "api_version": API_VERSION,
                "instructions": instructions(),
                "tools": [tool_entry(v) for v in VERBS.values()],
            }
        )

    def endpoint_for(verb: Verb):
        async def endpoint(request: Request) -> JSONResponse:
            return await dispatch(ctx, verb, request)

        return endpoint

    for verb in VERBS.values():
        router.add_api_route(
            f"/{verb.name}",
            endpoint_for(verb),
            methods=["POST"],
            name=verb.name,
            summary=verb.description,
            openapi_extra={
                "requestBody": {"content": {"application/json": {"schema": verb.model.model_json_schema()}}}
            },
        )
    return router
