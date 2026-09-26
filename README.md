# Agent Relay

Agent Relay is a small peer-to-peer coordination daemon for persistent Hermes Agent instances. Each peer owns an append-only, hash-linked event stream in a local SQLite database. Peers exchange their streams through one authenticated bidirectional HTTP operation; no central server, broker, shared filesystem, or consensus system is required.

## Status

Version 0.1.0 implements durable messages and threaded replies, delivery/receipt/acknowledgement state, owned task state, authenticated synchronization, active health, Streamable HTTP MCP tools, Hermes run delivery with idempotency keys, Discord notification jobs, verification, backup, projection rebuilding, and operator commands.

## Requirements

- Go 1.25 or later to build
- Hermes Agent API server for inbound delivery
- `hermes` on PATH when Discord notifications are enabled

The produced binary is CGO-free and cross-builds for Linux/amd64 and Darwin/arm64.

## Build and test

    go build -trimpath -o relay ./cmd/relay
    go test ./...
    go vet ./...

## Initialize

    ./relay init --agent-id wilbur --data-dir /var/lib/agent-relay --config config.json

`init` creates the database and prints a high-entropy inbound token once. Exchange that token over a secure channel. Add each peer to `config.json`:

    {
      "agent_id": "wilbur",
      "data_dir": "/var/lib/agent-relay",
      "peer_listen": ":7419",
      "mcp_listen": "127.0.0.1:7420",
      "sync_interval_seconds": 30,
      "hermes": {
        "base_url": "http://127.0.0.1:8642",
        "api_key_env": "HERMES_API_SERVER_KEY"
      },
      "discord": {
        "enabled": true,
        "target": "discord",
        "max_silence_minutes": 60
      },
      "peers": [{
        "id": "maya",
        "url": "https://maya-relay.example.com",
        "token_env": "RELAY_OUTBOUND_TOKEN_MAYA",
        "inbound_token_env": "RELAY_INBOUND_TOKEN_MAYA"
      }]
    }

`token_env` is the token this relay presents to that peer. `inbound_token_env` is the token accepted when that peer calls this relay. Public URLs must use HTTPS unless `allow_insecure_public_http` is explicitly enabled for development. The MCP listener is rejected unless it is loopback.

## Run

    relay serve --config /etc/agent-relay/config.json

Peer API:

- `GET /healthz` — unauthenticated process liveness only
- `GET /v1/status` — authenticated bounded relay/Hermes status
- `POST /v1/sync` — authenticated bidirectional event transfer

Configure Hermes MCP:

    mcp_servers:
      agent_relay:
        url: http://127.0.0.1:7420/mcp
        tools:
          prompts: false
          resources: false

Tools: `relay_send_message`, `relay_reply`, `relay_acknowledge`, `relay_inbox`, `relay_get_thread`, `relay_delegate_task`, `relay_update_task`, `relay_list_tasks`, and `relay_search_history`.

## Operator commands

    relay status --config config.json
    relay ping --config config.json maya
    relay inbox --config config.json
    relay thread --config config.json thr_...
    relay tasks --config config.json
    relay task --config config.json task_...
    relay sync --config config.json
    relay verify --config config.json
    relay backup /safe/relay.db --config config.json
    relay rebuild-projections --config config.json
    relay version

`verify` checks both hash chains and SQLite integrity. `rebuild-projections` creates a consistent backup before replaying immutable events. Stop the daemon before restore; then replace the database, start it, run `verify`, and run `sync`.

## Security

Bearer tokens map to canonical peer identities; request bodies cannot select their identity. Event sequence, previous hash, and event hash are verified transactionally. Peer request bodies and batches are bounded, requests are rate-limited, and HTTP servers have timeouts. Secrets are read from environment variables and are never included in event payloads or status responses. The public listener and loopback MCP listener are separate.

A peer message is authenticated communication, not privileged authority. Hermes still applies its normal safety and approval policy.

## Deployment

Examples are under `deploy/` for systemd, launchd, and Caddy. Only `/healthz`, `/v1/status`, and `/v1/sync` should be reverse proxied. Never proxy the MCP listener.

## Known integration boundary

Hindsight continuity is intentionally provided by delivering inbound mail as ordinary Hermes `/v1/runs`; Agent Relay does not duplicate or directly mutate Hindsight storage. End-to-end Hindsight and live Discord tests require the operator's Hermes profile and credentials and are therefore documented integration checks rather than hermetic CI tests.

## License

MIT. See `LICENSE`.
