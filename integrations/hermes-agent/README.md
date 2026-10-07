# Hermes conversation return bridge

Hooks-only integration: existing relay MCP send/delegate tools are unchanged.
Requires the reviewed Hermes fork callback extension: `gateway_ready`,
`transform_tool_result`, `admit_callback`, and `get_callback_receipt`.

Install this directory under the active Hermes home's `plugins/agent-relay-bridge`,
enable it with `hermes plugins enable agent-relay-bridge`, and set
`AGENT_RELAY_AGENT_ID` to the exact local Relay agent ID in the gateway environment.
Optional `AGENT_RELAY_LOCAL_TOKEN` is secret configuration and must match Relay.
Restart only at a safe turn boundary.

Gateway-ready startup starts recovery after restart. Creation-result transforms
capture caller-local context and persist an immutable local route before returning
a model-visible binding receipt. Routes pin runtime and receiving-bot profiles
independently. Child, cron, stateless and mismatched origins fail closed. Binding
errors warn without undoing successful peer sends. No private runner discovery,
runtime monkeypatching, human impersonation, transcript or toolset mutation.

Local HTTP is fixed to `http://127.0.0.1:7420`, with redirects and proxy inheritance
disabled. Routes never enter peer sync. Local endpoints are `/v1/local/routes`,
`/v1/local/callbacks/next?limit=20` and callback-ID `complete`/`retry` actions.
Polling and request timeouts are five seconds.

The Hermes ledger durably queues trusted internal text, checks original session
and transport before running, and waits for idle turn boundaries without blocking
unrelated lanes. Control commands are disabled. Queued/running receipts are not
completion. Only completed adapter outcomes are acknowledged. Rejected/uncertain
outcomes need reconciliation, not replay. Queued callbacks recover after restart;
interrupted running callbacks are uncertain. Exactly-once execution or delivery
is not claimed.

Callback text labels peer events as untrusted data and requests a summary of at
most 150 words, progress/blocker next steps, and a final result for terminal
outcomes without initiating new work. This bounds the requested response, not
the serialized event input or the model's actual output. Relay suppresses
unbatched weaker task updates after a terminal outcome; already-frozen batches
remain stable for durable admission and retry.

Tests use actual context, MCP rendering/dispatch, registration, message and session
types with controlled transport fakes. Run unittest discovery under
`integrations/hermes-agent/tests` with reviewed Hermes source on `PYTHONPATH`.
See `docs/conversation-return-routing-status.md` for release and live acceptance.