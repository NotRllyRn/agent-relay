# Conversation return routing: partial implementation

Task: `t_621d5ab4`. This is not deployed and does not satisfy live acceptance.

## Implemented locally

- Local-only immutable destination bindings with local thread/task ownership checks.
- Durable remote-event callbacks, transactional ingestion/backfill, event dedupe,
  stable batch IDs, policy selection, five-second coalescing and progress throttling.
- Loopback-only `/v1/local/routes` and callback next/complete/retry controls on the
  existing MCP listener. Optional `AGENT_RELAY_LOCAL_TOKEN` bearer authentication.
- Task updates include an explicit meaningful-progress flag.
- Hook-only Hermes bridge captures actual MCP creation results and scoped context;
  pins internal events to exact existing session IDs/keys without transcript edits.
- Admission fence dedupes accepted callbacks across acknowledgement retries.

## Verified commands

- `go test ./...` passed.
- `go test -race ./...` passed.
- `go vet ./...` passed.
- Linux amd64 and Darwin arm64 `go build ./cmd/relay` passed.
- Installed Hermes Python: `python -m unittest discover -s integrations/hermes-agent/tests -v` passed (18 tests).
- `git diff --check` passed.

`pytest` is not installed in the Hermes venv; unittest is the actual test framework.
Independent third-agent review could not run: this one-shot runtime enforces a
maximum of two delegated children, already used for the store and bridge slices.

## Supported Hermes integration gap

Official plugin documentation describes `ctx.inject_message(content, role,
session_key=...)`; installed `hermes_cli/plugins.py:603` implements it. It is not
absent. However, its boolean receipt means scheduling, not concrete adapter
admission; it accepts only a session key, not the original session ID or callback
identity. `gateway/run_inbound.py:1861` resolves the current session for that key.
Consequently a stale callback after `/new` cannot safely use it as-is.

Installed `gateway/wake.py:79` does expose `admit_internal_event`, with a real
adapter-admission receipt. The bridge uses this after `pre_gateway_dispatch`
provides the gateway/store handles. Installed lifecycle startup dispatch
(`gateway/run_startup.py:1430`) exposes platform names, not those handles, so this
bridge cannot resume pending callbacks unattended after a cold restart until
another external inbound event supplies the handles.

The SQLite admission fence also has an unavoidable ambiguous window between its
reservation and gateway acceptance. It fails closed instead of duplicating a
turn, requiring reconciliation. A scheduled/accepted event is not durable model
completion or outbound Discord delivery; claiming exactly-once terminal delivery
would be false.

A supported Hermes-side lifecycle/admission integration must expose gateway-ready
handles and original-session/callback identity with durable admission/recovery.
Request explicit scope approval before modifying the separate Hermes source tree
or deploying an incomplete integration. Do not work around this by mutating
transcripts, forging Tim messages, discovering runners via private globals, or
marking scheduling as successful callback admission.

## Still required

- Resolve cold-start and ambiguous-admission integration.
- Model-visible immediate creation receipt / binding failure warning.
- Suppress obsolete weaker updates after stronger task outcomes; configurable
  callback status/policy visibility remains unfinished.
- Independent review and final secret scan; push and exact-head CI readback.
- Backup-first symmetric deployment and binary/process/readiness verification.
- Real original-context Discord test in thread `1553488764075122738`: distinct
  post-turn gateway execution and outbound Wilbur message without human prompting.

No live test was triggered; no Discord receipt exists. Existing services were not
restarted, no binary/config/plugin was installed, and nothing was pushed.

Deployment discovery: local systemd gateway PID 250 and relay PID 288 at inspection;
Maya SSH reachable, Darwin arm64, relay PID 50989 at inspection. Treat these as
historical evidence, never as current process state for rollout.
