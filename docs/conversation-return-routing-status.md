# Conversation return routing release checkpoint

Task: `t_621d5ab4`. Live acceptance is not yet verified.

## Implemented

- Immutable local-only return routes pin original platform/chat/thread, Hermes session ID/key, runtime profile and receiving-bot profile. No routing identifiers are synchronized to peers.
- Durable remote-event callbacks include deduplication, stable batches, five-second coalescing and progress throttling. Terminal outcomes suppress unbatched weaker updates for the same task, including late progress; frozen batches are preserved because admission may already have occurred. Message replies and other tasks remain independent.
- Loopback local route/callback API; optional bearer authentication.
- Hermes plugin binds actual MCP creation results before returning a model-visible binding receipt. It uses gateway-ready startup and durable admission APIs, not transcript mutation or private runner discovery.
- Narrow Hermes fork extension persists queued callbacks, restores receiving transport under multiplexing, rejects stale/reset routes, waits for idle turn boundaries without blocking unrelated lanes, and records adapter outcome.
- Queued callbacks recover after restart. Running callbacks interrupted by crashes are explicitly uncertain and are not silently replayed; exactly-once model execution or transport delivery is not claimed.
- Callback prompts request a user-facing summary of at most 150 words, distinguish peer claims from verified results, and report terminal outcomes without starting new work. This is a model instruction, not an enforced output-length limit.

## Release verification

Review corrections bound new frozen batches to 16 events and 512 KiB of stored event JSON (a single larger seed is represented by an explicit bounded payload preview). The aggregate callback response is capped at 1 MiB, below the bridge's unchanged 2 MiB safety limit. Remaining events stay pending; candidates which do not fit are not frozen. Previously frozen oversized batches retain their database membership and callback identity and return an explicit operator-inspection summary rather than silently discarding source data. Full payloads remain in the local store; previews/summaries must not be treated as verified final outcomes.

Operator reconciliation: authenticated loopback GET `/v1/local/callbacks/status` reports pending event count, oldest next-attempt timestamp, and up to 100 failed logical batches with attempts, last error and next-attempt time. Rejected/uncertain gateway receipts appear there after the bridge records retry errors; they require inspection of the gateway admission ledger, not blind manual replay. This endpoint is private, not synchronized and may contain peer-derived error text. `oldest_due_unix_nano` is a due timestamp, not creation age.

Scope disposition: coalescing (5 seconds), progress throttle (300 seconds), retry (30 seconds) and default normal policy remain fixed. Route policy can be changed through the local binding API. Configurable timing/default-enable settings and dedicated routes/callback CLI commands from plan task 8 are deferred, not claimed implemented. The minimal reconciliation endpoint replaces no gateway recovery mechanism.

Continuation verified 22 bridge unittest tests and 47 gateway callback, transport, delivery, shutdown and streaming regression tests through the official isolated test runner. Go unit tests and vet pass; previous run verified race tests and Linux amd64 / Darwin arm64 builds. Fresh independent release review found no blocking defects in the corrections (delivery boundary, idle-lane fairness, receiving-bot pinning). Deployment and exact-head remote CI must be recorded separately after readback.

## Outstanding acceptance

Publish reviewed heads; verify exact remote commit and CI. Deploy backup-first on both peers and verify actual process/source/binary identities and readiness. Notify Tim through supported delivery before testing. Send delayed Maya request from a real originating gateway turn in Discord thread `1553488764075122738`, end that turn, and require a separate callback-driven gateway turn plus outbound Wilbur message without Tim prompting. Record admission, distinct turn IDs, original session ID, peer event and outbound Discord message ID. Polling or worker completion notifications are not this acceptance test.
