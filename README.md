# Glafira for Apache Kafka

A free, open-source desktop client for Apache Kafka on macOS, Windows and Linux, built with Tauri 2, Rust and React.
It also works as an MCP gateway: an LLM assistant can read your cluster's metadata, leave notes and propose changes,
but it cannot change anything in the cluster.

[Русская версия](README.ru.md)

Apache Kafka is a trademark of the Apache Software Foundation; this project is not affiliated with the ASF.

## Install

Installers for macOS (`.dmg`, `.pkg`), Windows (`.msi`) and Linux (`.deb`, `.rpm`, `.AppImage`):
https://github.com/vvnovg/glafira-kafka-releases/releases

macOS builds are signed and notarized. Windows builds are not signed yet: if SmartScreen blocks the installer,
choose "More info", then "Run anyway".

User guide in 12 languages: [English](docs/guide/en/getting-started.md).
For administrators (silent install, policy, update mirror): [`docs/admin.md`](docs/admin.md) (in Russian).

## Features

- **Connections.** PLAINTEXT, SSL, SASL PLAIN and SCRAM-SHA-256/512, mTLS with client certificates in PEM, PKCS#12 or JKS.
  Nested folders, export to a file without passwords and import with a preview. Passwords live in the OS secret store
  (Keychain, Credential Manager, Secret Service).
- **Browse messages** without joining a consumer group. Start from the last N messages, the beginning, an offset,
  a timestamp or the last X minutes. Filter by substring or regex on key, value and headers, by a JSONPath condition
  such as `$.order.status == "FAILED"` and by time range. Live tail. Formats: auto, string, json, hex, base64.
- **Producer.** Key, value, headers, partition, `acks`, compression, idempotence. Templates with placeholders
  (`{{uuid}}`, `{{now}}`, `{{seq}}`, `{{randomInt 1 100}}`, `{{env NAME}}`), batch send from a form or a JSON Lines / CSV file,
  "Resend" of a browsed message byte for byte.
- **Consumer sessions** that join a group, show assignments and rebalances and commit offsets by hand.
- **Topics and consumer groups.** Create topics, edit configs with a before/after review, add partitions, purge, delete.
  Group lag per partition, members with host and `group.instance.id`, offset reset with a mandatory preview.
- **Cluster check.** Built-in rules for partitions and replication, topic and broker configs and consumer groups;
  severity depends on the connection's environment (local, dev, test, stage, prod).
- **12 languages**, built-in guide (F1), a "?" hint on every field and topic config parameter.

## Safety

- **Read-only** connections: every changing operation is rejected in Rust with `READ_ONLY`, and the buttons are hidden.
- **prod** connections: every changing operation asks you to type the connection name; delete and purge also ask
  for the object name.
- Offset reset and group deletion work only on groups without members, and the reset always shows a dry run first.

## LLM assistants (MCP)

Claude Desktop, Claude Code, Cursor or any other MCP client runs `glafira-kafka mcp`, which connects to the running
Glafira through a local socket (a named pipe on Windows). The assistant sees only connections you open for it in
the connection form: "LLM access" set to "Metadata only" or "Metadata and messages". It never gets passwords,
certificates or the bootstrap address.

- **Reads metadata:** brokers, topics, partitions and ISR, topic and broker configs with secrets hidden,
  consumer groups with members and lag, and it can observe a group for up to 50 seconds to diagnose rebalances.
- **Reads messages**, only on connections with "Metadata and messages": messages of a topic for a time window or an
  offset range with the same filters as the Messages screen, a window summary without contents, and a group's commit
  history from `__consumer_offsets`. Values are masked before the answer leaves Glafira: a per-connection list of field
  names plus built-in patterns for emails, card numbers, JWTs and Bearer tokens. The MCP log shows what was read, and
  "Open in Messages" shows you the same messages. An administrator can disable message reading by policy.
- **Leaves notes** that appear next to topics and groups in Glafira, and runs the same cluster check as the button.
- **Proposes changes:** topic configs, a new topic, more partitions or an offset reset. A proposal opens the usual
  Glafira form already filled in; you review the diff and apply it yourself. Deletes and purges cannot be proposed.

Changing the cluster through the gateway is impossible by construction: the MCP layer is a separate crate,
[`crates/glafira-mcp`](crates/glafira-mcp), that depends neither on `rdkafka` nor on the application crate, and its
backend trait has only read operations (a test checks the crate's manifest). Every new client is approved in a dialog,
every call goes to an audit log, and an administrator can disable MCP by policy.

Enable it in Settings → "LLM assistants (MCP)" → "Allow the MCP server"; the same section shows ready-made client config snippets.
Details: [guide chapter](docs/guide/en/llm-mcp.md).

**Catalog build for MCP registries.** Registries such as Glama start a server in a container and ask it for its
tools. The real server lives inside the desktop app, so the crate also ships `glafira-mcp-catalog`: the same server
over stdio with the real tool list, where every call answers `desktop_app_required` with a link to the installers.
Build it with `docker build -f crates/glafira-mcp/Dockerfile .`. You do not need it to use Glafira.

## Development

Requirements:
- Rust stable (`rustup`), cmake, Xcode Command Line Tools on macOS
- Node 22+, pnpm
- Docker for the local Kafka (`confluentinc/cp-kafka:7.6.1`) and integration tests

Run:
```bash
pnpm install
docker compose -f dev/docker-compose.yml up -d   # local Kafka, see "Local cluster"
docker wait ke-kafka-init   # waits for the SCRAM user (prints 0); needed by SCRAM tests
pnpm tauri dev
```

Local cluster (`dev/docker-compose.yml`):
- PLAINTEXT: `localhost:9092`.
- SASL_PLAINTEXT: `localhost:9094` with PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512.
  - PLAIN: `tester` / `tester-secret`, `admin` / `admin-secret`.
  - SCRAM-SHA-256 and SCRAM-SHA-512: `scram-user` / `scram-secret` (created by the one-off `kafka-init` service).
- SSL with client certificates (mTLS): `localhost:9095`, certificates in `dev/certs/out` (see `dev/certs/README.md`).

Tests:
```bash
pnpm test                                              # frontend
cargo test --manifest-path src-tauri/Cargo.toml --lib  # Rust unit
KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 KE_IT_MTLS_BOOTSTRAP=localhost:9095 \
  cargo test --manifest-path src-tauri/Cargo.toml --tests   # Rust integration (needs docker compose)
cargo test --manifest-path crates/glafira-mcp/Cargo.toml   # MCP gateway (no Kafka)
```
An integration test without its `KE_IT_*` variable is skipped. Logs: `RUST_LOG=debug pnpm tauri dev`.

Where the app keeps its data, the MCP files and the full developer notes are described in [README.ru.md](README.ru.md)
(in Russian); design specs are in `docs/superpowers/specs/`.

## License and support

- The code is licensed under [Apache License 2.0](LICENSE). Every feature is free for everyone: no paid tiers, no license keys.
- The Glafira name and icon are not covered by the license, see [`TRADEMARK.md`](TRADEMARK.md).
  Dependency licenses: [`THIRD_PARTY.md`](THIRD_PARTY.md).
- The project lives on donations: [support the project](https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md).
- Contributing: [`CONTRIBUTING.md`](CONTRIBUTING.md). Security reports: [`SECURITY.md`](SECURITY.md).
