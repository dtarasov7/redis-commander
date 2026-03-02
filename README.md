# Redis Commander TUI

[![Python](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Terminal user interface for Redis Standalone and Redis Cluster.

This project provides a keyboard-driven TUI for browsing keys, inspecting values, editing common Redis data types, running Redis commands, and switching between multiple connection profiles.

## Features

- Standalone Redis support with `DB0-DB15`
- Redis Cluster support with cluster-aware key scanning and console commands
- Multiple connection profiles loaded from:
  - plain JSON
  - encrypted config file
  - HashiCorp Vault
- Key browsing with filtering and bulk marking
- Add, edit, and delete workflows for:
  - `string`
  - `hash`
  - `list`
  - `set`
  - `zset`
  - `bitmap`
  - `stream`
- Built-in Redis console with persistent command history
- TLS/SSL support
- Unix socket support
- Audit logging to `redis_tui_audit.log`

## Project Files

- `redis-commander.py` - main application
- `config-encryptor.py` - config encryption utility
- `redis_profiles.json` - sample config
- `UserGuide.md` / `UserGuide-ru.md` - end-user documentation
- `DeveloperGuide.md` / `DeveloperGuide-ru.md` - developer documentation

## Requirements

- Python 3.7+
- `urwid`
- `cryptography` for encrypted configs
- `hvac` for Vault mode
- `simple_redis_client` available in the Python environment

Example dependency install:

```bash
pip install urwid cryptography hvac
```

## Quick Start

Create a minimal config:

```json
{
  "local": {
    "name": "local",
    "host": "127.0.0.1",
    "port": 6379,
    "cluster_mode": false
  }
}
```

Run the application:

```bash
python redis-commander.py
```

Run with an explicit config path:

```bash
python redis-commander.py -c redis_profiles.json
```

## Configuration Modes

### Plain JSON

```bash
python redis-commander.py -c redis_profiles.json
```

### Encrypted Config

Encrypt a config:

```bash
python config-encryptor.py redis_profiles.json redis_profiles.enc
```

Run with the encrypted file:

```bash
python redis-commander.py -e redis_profiles.enc
```

### HashiCorp Vault

```bash
python redis-commander.py \
  --vault-url https://vault.example.com:8200 \
  --vault-path secret/data/redis-commander \
  --vault-user redis-admin
```

## Command Line Options

```text
-c, --config              Path to plaintext config file
-e, --encrypted-config    Path to encrypted config file
--vault-url               Vault URL
--vault-path              Vault secret path
--vault-user              Vault username
--vault-pass              Vault password
```

## Notes

- The UI scans up to `5000` keys per active database or cluster scan.
- In cluster mode, only `DB0` is available.
- `readonly` is currently a UI-level limitation, not a full write-protection mechanism.
- `bitmap` and `stream` can be created, but their value rendering and edit preload support are limited compared to the core Redis types.

## Architecture Diagrams

PlantUML source diagrams are available in `diagramms/`:

- `diagramms/architecture-overview.puml`
- `diagramms/startup-and-profile-loading.puml`
- `diagramms/ui-key-workflow.puml`
- `diagramms/cluster-scan-and-console.puml`
- Russian copies with the `-ru` suffix are provided for each diagram

## Documentation

Use the full guides for detailed usage and implementation notes:

- `UserGuide.md`
- `UserGuide-ru.md`
- `DeveloperGuide.md`
- `DeveloperGuide-ru.md`

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

## Author

**Tarasov Dmitry**
- Email: dtarasov7@gmail.com

## Attribution
Parts of this code were generated with assistance
