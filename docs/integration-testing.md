# Live integration acceptance checks

These checks require two configured Hermes profiles and cannot run in unauthenticated CI.

1. Start both relays and Hermes API servers. Send an acknowledgement-required message with `relay_send_message`; confirm sender history advances through sent, received, delivered, and acknowledged.
2. Stop recipient relay, send another message, restart it, and confirm exactly one message and one Hermes run exist.
3. Stop recipient Hermes only. Confirm `relay ping` reports relay healthy and Hermes unhealthy, then restart Hermes and confirm queued delivery completes.
4. Delegate a task and exercise accepted, in-progress, blocked, resumed, and completed transitions. Confirm Discord receives transition/progress notifications exactly once.
5. With Hindsight enabled, complete collaborative work, open a separate later Hermes session, and query for the outcome. Confirm auto-retain/recall finds it.
6. Keep an active task silent past its configured update interval and verify the deployment's scheduled progress check creates one deterministic Discord heartbeat and one idempotent Hermes progress request per time bucket.

Use test-only peers/channels. Never place credentials in captured logs or fixtures.
