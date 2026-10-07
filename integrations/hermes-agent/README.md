# Hermes origin bridge (partial: lazy gateway bootstrap)

Hooks only: familiar `relay_send_message` / `relay_delegate_task` MCP tools remain
unchanged. No extra tools, toolset mutation, prompt modification, transcript
writes, fake human messages, private runner discovery or monkeypatches.

## Verified installed interfaces

Inspected `/home/hermes/.hermes/hermes-agent`, HEAD
`2243a332e1f8b6fa9b7922c061a6ec6f94897aba` (the installed working tree is authoritative):

- `model_tools.py::_emit_post_tool_call_hook` supplies `tool_name`, `args`,
  `result`, `session_id`, correlation IDs, duration, status/error fields and
  middleware trace. **It does not supply gateway/source/session-key objects.**
- `tools/mcp_tool_handlers.py::_render_call_tool_result` gives the hook JSON
  containing `result` (often itself a JSON text string), optionally
  `structuredContent`. The bridge decodes this actual envelope.
- `gateway/session_context.py::get_session_env` exposes caller-local
  `HERMES_SESSION_*`; `hermes_cli/plugins_dispatch.py` copies contextvars into
  bounded hook workers. Hook session ID must agree with caller context and the
  existing routing entry. Delegated-child and cron calls are excluded.
- `pre_gateway_dispatch(event, gateway, session_store)` is supported, asynchronous,
  pre-auth and non-internal. The bridge only retains its gateway/store handles;
  it does not derive route ownership from an unauthenticated incoming message.
- `SessionStore.list_sessions()` gives existing `SessionEntry` objects. Route
  checking is read-only; no new session is created, and reset/compression ID
  changes, suspended sessions, missing origin and destination mismatches fail
  closed. Admission carries both pinned identity metadata fields so Hermes'
  internal-event guard can recheck the same identity.
- `gateway/wake.py::admit_internal_event(adapter, event)` requires the adapter's
  `_gateway_accepted is True` receipt. This means **scheduled/queued**, not model
  completion, human approval, or successful outward delivery. The bridge creates
  actual `MessageEvent(internal=True)` objects with `gateway_session_key` and
  `gateway_session_id`, plus `gateway_session_strict: true` (no creation or
  continuation adoption) and `allow_gateway_control=False`. Pinned metadata
  matches `run_notifications.py::_inject_watch_notification`; strict/no-control
  flags match the existing supported plugin injection path.

Official documentation was checked via web search and a direct HTML fetch:

- https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks
- https://hermes-agent.nousresearch.com/docs/user-guide/features/plugins
- https://hermes-agent.nousresearch.com/docs/developer-guide/plugins

The subagent tool surface did not expose `web.run`; `web_extract` reported that
its configured OpenAI native backend is search-only. `web_search` succeeded via
its Firecrawl fallback and the official hooks page was fetched with urllib.

## Explicit blockers / limitations

**Unattended restart bootstrap is NOT implemented.** The supported gateway
`gateway:startup` HOOK is a different registry from Python plugin hooks.
`gateway/run_startup.py::_start_post_connect_services` emits only
`{"platforms": [...]}`; `gateway/hooks.py` calls `handle(event_type, context)`.
Neither supplies the runner, adapter or session store. `PluginContext` has no
public gateway-start runner callback. The public injection facade cannot supply
this bridge's required adapter admission receipt. No fake startup HOOK or
private global-runner lookup is included. A real non-internal gateway event must
reach `pre_gateway_dispatch` after each process start before polling begins.

**Exactly-once completed model execution is NOT implemented.** Inspected
Hermes' delegation-specific completion ledger, delivery ledger and lifecycle
ledger; none provides a supported generic atomic callback-ID/adapter-admission
transaction. Hermes' own `_inject_watch_notification` explicitly documents
crash replay as at-least-once. This bridge instead persists a local SQLite
admission fence:

1. Commit `reserved` before calling the adapter.
2. On explicit `WakeNotAccepted`, release the reservation and retry.
3. After receipt, commit `accepted`, then POST callback `/complete`.
4. Replay of `accepted` repeats only `/complete`, never injection.
5. A crash or unexpected handler exception during `reserved` is ambiguous:
   **do not inject again or claim delivery**; report retry error requiring
   operator reconciliation. There is intentionally no automatic timeout that
   would erase the fence and duplicate a potentially accepted callback.

This is durable dedupe with a fail-closed ambiguity fence, not lossless recovery.
A process can die after in-memory scheduling but before execution; marking the
callback complete cannot prove its model turn ran. `/complete` here means adapter
admission, consistent with the wake API. Do not present this as end-to-end
completion semantics without an upstream Hermes durable consumer interface.

Only **default-profile push adapters** are currently supported. Non-default
multiplexed profiles fail closed rather than guessing which store/adapter map
owns a route. CLI/TUI/stateless API sessions have no supported bootstrap here;
stateless self-POST is deliberately forbidden. The bridge never silently adopts
a compression continuation or a newly reset session. Observer exceptions/timeouts
are fail-open in Hermes: API failure during binding is logged and does not make
an already successful send fail; there is no durable route-binding outbox.

## Private local API contract

- Fixed base `http://127.0.0.1:7420`; redirects and process HTTP proxy settings
  are disabled so private routes/tokens cannot be exported.
- `AGENT_RELAY_AGENT_ID`: required exact local relay identity (same as the relay
  service's local ID). Unset => auto-binding fails closed.
- Optional `AGENT_RELAY_LOCAL_TOKEN`: `Authorization: Bearer <token>`.
- `POST /v1/local/routes`: direct route body with `thread_id`, `task_id`,
  `owner_agent_id`, `platform`, `chat_id`, `platform_thread_id`,
  `hermes_session_id`, `hermes_session_key`, `profile_name`,
  `reply_policy: "normal"`. Route ownership remains local and must also be
  validated server-side. No route fields are inserted into replicated events.
- `GET /v1/local/callbacks/next?limit=20`: array of
  `{callback_id, route, events: []domain.Event, attempts}`.
- `POST /v1/local/callbacks/{url-escaped-id}/complete`: `{}`.
- `POST /v1/local/callbacks/{url-escaped-id}/retry`: `{"error":"reason"}`.
- Poll every five seconds in the existing gateway event loop. HTTP work runs in
  executor threads with a five-second request timeout. Private ledger lives at
  `<get_hermes_home()>/state/agent-relay-admissions.db`, mode `0600`.

No installation/configuration/deployment was performed. The parent must align
its retry endpoint body with the above contract and provide the exact local ID.

## Tests

Tests were written before implementation, first failed on the missing module,
then an installed-MCP-renderer regression test failed on the nested `result`
envelope before its decoder fix. Tests use the actual installed `SessionSource`,
`SessionEntry`, `MessageEvent`, session context API, MCP renderer,
`model_tools.handle_function_call` and `admit_internal_event`; transport/registry
and adapter acceptance are controlled fakes. No production callback or transcript
was modified. Registration test isolates the ledger in a temporary home.

```sh
/home/hermes/.hermes/hermes-agent/venv/bin/python -m unittest discover -s integrations/hermes-agent/tests -v
```

18 tests passed: direct send/delegate binding, actual dispatch and MCP rendering,
unchanged result/tool surface, missing/mismatched context, exact private owner and
all destination fields, lazy single-worker bootstrap, internal admission receipt,
stale/suspended/stateless rejection, durable replay dedupe, complete failure retry,
ambiguous crash fence, fixed-loopback transport/token and callback API shape.
This is not a deployed end-to-end relay/gateway integration test.
