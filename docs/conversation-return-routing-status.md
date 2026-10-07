# Conversation return routing release checkpoint

Task: `t_621d5ab4`. Live acceptance is not yet verified.

## Implemented

- Immutable local-only return routes pin original platform/chat/thread, Hermes session ID/key, runtime profile and receiving-bot profile. No routing identifiers are synchronized to peers.
- Durable remote-event callbacks include deduplication, stable batches, five-second coalescing, progress throttling and suppression of weaker updates after terminal outcomes.
- Loopback local route/callback API; optional bearer authentication.
- Hermes plugin binds actual MCP creation results before returning a model-visible binding receipt. It uses gateway-ready startup and durable admission APIs, not transcript mutation or private runner discovery.
- Narrow Hermes fork extension persists queued callbacks, restores receiving transport under multiplexing, rejects stale/reset routes, waits for idle turn boundaries without blocking unrelated lanes, and records adapter outcome.
- Queued callbacks recover after restart. Running callbacks interrupted by crashes are explicitly uncertain and are not silently replayed; exactly-once model execution or transport delivery is not claimed.

## Release verification

Continuation verified 22 bridge unittest tests and 47 gateway callback, transport, delivery, shutdown and streaming regression tests through the official isolated test runner. Go unit tests and vet pass; previous run verified race tests and Linux amd64 / Darwin arm64 builds. Fresh independent release review found no blocking defects in the corrections (delivery boundary, idle-lane fairness, receiving-bot pinning). Deployment and exact-head remote CI must be recorded separately after readback.

## Outstanding acceptance

Publish reviewed heads; verify exact remote commit and CI. Deploy backup-first on both peers and verify actual process/source/binary identities and readiness. Notify Tim through supported delivery before testing. Send delayed Maya request from a real originating gateway turn in Discord thread `1553488764075122738`, end that turn, and require a separate callback-driven gateway turn plus outbound Wilbur message without Tim prompting. Record admission, distinct turn IDs, original session ID, peer event and outbound Discord message ID. Polling or worker completion notifications are not this acceptance test.
