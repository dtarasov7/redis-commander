# Changelog

All notable changes to this project will be documented in this file.

## [1.2.0] - 2026-08-09

### Added

- Redis Sentinel profiles with multiple Sentinel endpoints and monitored service names
- Sentinel AUTH/TLS settings, master discovery, replica read preferences, and diagnostics
- Docker Sentinel integration stand with three Redis and three Sentinel processes

### Changed

- Writes are routed to the current Sentinel master; eligible reads may use replicas
- Sentinel topology is refreshed after rejected writes, read failures, and scan failures
- Ambiguous writes after a network failure are not retried automatically

## [1.1.0] - 2026-08-09

### Added

- Startup profile selector, connection diagnostics modal, and explicit disconnect flow
- Dynamic standalone database counts, including DB16 and higher
- `Tab`/`Shift+Tab` navigation across databases, keys, and details
- Docker standalone performance test stand with mixed Redis data types

### Changed

- Key loading now uses cancellable background SCAN workers, batched TYPE pipelines, incremental UI updates, and a global key limit
- Cluster masters are scanned in parallel and per-command connection PING checks were removed
- The main interface shows databases only for the active connection

## [1.0.0] - 2026-03-02

### Added

- Initial public release of Redis Commander TUI
- Terminal UI for Redis Standalone and Redis Cluster
- Multi-profile connection loading from plain JSON, encrypted config, and HashiCorp Vault
- Key browsing, filtering, bulk marking, and delete workflows
- Add and edit dialogs for `string`, `hash`, `list`, `set`, `zset`, `bitmap`, and `stream`
- Built-in Redis console with persistent command history
- TLS/SSL and Unix socket connection support
- Audit logging to `redis_tui_audit.log`
- English and Russian user and developer documentation
- PlantUML architecture diagrams in English and Russian
