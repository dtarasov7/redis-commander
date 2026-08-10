# Redis Commander TUI - User Guide

---

## Contents

1. [Introduction](#introduction)
2. [Installation and Startup](#installation-and-startup)
3. [Connection Configuration](#connection-configuration)
4. [User Interface](#user-interface)
5. [Working with Keys](#working-with-keys)
6. [Redis Console](#redis-console)
7. [Hotkeys](#hotkeys)
8. [Security Modes](#security-modes)
9. [FAQ and Limitations](#faq-and-limitations)
10. [Logs and Diagnostics](#logs-and-diagnostics)

---

## Introduction

**Redis Commander TUI** is a terminal interface for Redis Standalone, Redis Sentinel, and Redis Cluster. It discovers Sentinel master/replicas, switches databases, browses keys, edits values, runs commands, and loads profiles from plain JSON, encrypted files, or HashiCorp Vault.

### What the current version supports

- Connections to **Redis Standalone**, **Redis Sentinel**, and **Redis Cluster**
- Multiple connection profiles in one configuration file
- Switching across all databases configured on a standalone server
- Key browsing with detected Redis types
- Filtering and bulk marking with glob patterns
- Adding and editing keys of type `string`, `hash`, `list`, `set`, `zset`, `bitmap`, `stream`
- Deleting a single key from the details pane and deleting multiple marked keys
- Built-in Redis console with persistent history and cluster-aware command handling
- TLS/SSL, Unix socket, encrypted config, and HashiCorp Vault profile sources
- Audit logging to `redis_tui_audit.log`

### Architecture diagrams

PlantUML source diagrams for the current implementation are available in `diagramms/`:

- `diagramms/architecture-overview.puml`
- `diagramms/startup-and-profile-loading.puml`
- `diagramms/ui-key-workflow.puml`
- `diagramms/cluster-scan-and-console.puml`
- `diagramms/sentinel-connection-and-failover.puml`

Each diagram also has a Russian copy with the `-ru` suffix.

### Important implementation limits

- Key scanning stops after **5000 keys** per active database or cluster scan
- The details pane fully renders values for `string`, `hash`, `list`, `set`, and `zset`
- For `bitmap`, `stream`, and other unsupported types, the details pane only shows generic type information
- For `list`, `set`, and `zset`, the details pane shows only the first **100 items**
- The profile flag `readonly` is not hard write protection: it hides the `Delete Key` button in the details pane, but it does not block writes from add/edit dialogs or the console. Use Redis ACLs or server-side permissions for real protection

---

## Installation and Startup

### Requirements

- Python 3.7+
- `urwid`
- the `simple_redis_client` module used by the application
- `cryptography` for encrypted configs
- `hvac` for HashiCorp Vault mode

Example Python dependency install:

```bash
pip install urwid cryptography hvac
```

### Project files

- Main executable: `redis-commander.py`
- Config encryption script: `config-encryptor.py`
- Default config file: `redis_profiles.json`

### Startup modes

#### Mode 1: plain JSON config

```bash
python redis-commander.py
# or
python redis-commander.py -c redis_profiles.json
```

If the config file is missing or empty, the selector shows that no profiles were
found. Add a profile to the configuration or choose `Exit`.

#### Mode 2: encrypted config

```bash
python redis-commander.py -e redis_profiles.enc
```

The application prompts for the decryption password at startup.

#### Mode 3: HashiCorp Vault

```bash
python redis-commander.py \
  --vault-url https://vault.example.com:8200 \
  --vault-path secret/data/redis-commander \
  --vault-user redis-admin
```

If `--vault-user` or `--vault-pass` is omitted, the application prompts interactively.

### Command line options

| Option | Purpose |
|--------|---------|
| `-c`, `--config` | path to the plain JSON config, default `redis_profiles.json` |
| `-e`, `--encrypted-config` | path to the encrypted config |
| `--vault-url` | HashiCorp Vault URL |
| `--vault-path` | Vault secret path |
| `--vault-user` | Vault username |
| `--vault-pass` | Vault password; passing it on the command line is insecure |

---

## Connection Configuration

### Basic `redis_profiles.json` format

```json
{
  "my-local": {
    "name": "my-local",
    "host": "127.0.0.1",
    "port": 6379,
    "cluster_mode": false
  },
  "my-cluster": {
    "name": "my-cluster",
    "host": "127.0.0.1",
    "port": 7000,
    "cluster_mode": true,
    "cluster_nodes": [
      ["127.0.0.1", 7000],
      ["127.0.0.1", 7001],
      ["127.0.0.1", 7002]
    ]
  }
}
```

### Supported profile fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | display name; if omitted, use a meaningful top-level entry name |
| `host` | string | Redis host |
| `port` | integer | Redis port |
| `password` | string/null | password |
| `username` | string/null | Redis 6+ username |
| `ssl` | boolean | enable TLS/SSL |
| `ssl_ca_certs` | string | path to the CA certificate |
| `ssl_certfile` | string | path to the client certificate |
| `ssl_keyfile` | string | path to the client key |
| `socket_path` | string | Unix socket path |
| `readonly` | boolean | safer browse-only UI mode |
| `cluster_mode` | boolean | connect as Redis Cluster |
| `cluster_nodes` | array | cluster node list |
| `mode` | string | `standalone`, `cluster`, or `sentinel` |
| `sentinel_service_name` | string | name configured by `sentinel monitor` |
| `sentinels` | array | Sentinel endpoints; list, string, and object forms are accepted |
| `read_preference` | string | `master`, `replica_preferred`, or `replica_only` |
| `replica_selector` | string | `random` or `round_robin` |
| `sentinel_username`, `sentinel_password` | string/null | separate Sentinel ACL credentials |
| `sentinel_ssl` | boolean | enable TLS for Sentinel discovery |
| `sentinel_ssl_ca_certs`, `sentinel_ssl_certfile`, `sentinel_ssl_keyfile` | string | Sentinel TLS files |
| `sentinel_ssl_check_hostname`, `sentinel_ssl_verify` | boolean | verify Sentinel hostname/certificates |

### Accepted `cluster_nodes` formats

The application accepts several node formats:

```json
{
  "cluster_nodes": [
    ["redis-1", 7000],
    "redis-2:7001",
    {"host": "redis-3", "port": 7002}
  ]
}
```

If `host` and `port` are omitted, the first node from `cluster_nodes` is used for the initial connection.

### Redis Sentinel example

```json
{
  "production-sentinel": {
    "name": "production-sentinel",
    "mode": "sentinel",
    "sentinel_service_name": "redis-production",
    "sentinels": [
      ["sentinel-1.example.com", 26379],
      ["sentinel-2.example.com", 26379],
      ["sentinel-3.example.com", 26379]
    ],
    "username": "redis-user",
    "password": "redis-password",
    "sentinel_username": "sentinel-user",
    "sentinel_password": "sentinel-password",
    "read_preference": "replica_preferred",
    "replica_selector": "round_robin"
  }
}
```

Writes always use the master reported by Sentinel. Eligible reads follow the
configured preference and may be stale on replicas. On failover, topology is
refreshed after connection, `READONLY`, `MASTERDOWN`, or scan errors. Ambiguous
writes interrupted by the network are not replayed automatically.

### TLS/SSL example

```json
{
  "prod-tls": {
    "name": "prod-tls",
    "host": "redis.example.com",
    "port": 6380,
    "ssl": true,
    "ssl_ca_certs": "/path/to/ca.pem",
    "ssl_certfile": "/path/to/client-cert.pem",
    "ssl_keyfile": "/path/to/client-key.pem"
  }
}
```

### Unix socket example

```json
{
  "local-socket": {
    "name": "local-socket",
    "socket_path": "/var/run/redis/redis.sock",
    "cluster_mode": false
  }
}
```

### Creating an encrypted config

Use the bundled script:

```bash
python config-encryptor.py redis_profiles.json redis_profiles.enc
```

The script:

- validates that the input file is valid JSON
- prompts for the password twice
- stores the result as `salt + encrypted_data`

---

## User Interface

### Screen layout

- Startup screen: configured servers and clusters, plus an explicit `Exit` item
- Top line: application header
- Button bar: `F1`, `F2`, `F3`, `F4`, `F8`, `F9`, `F10`, `F11`
- Left area:
  - databases of the active server or cluster on top
  - key list below it
- Right area: selected key details
- Bottom line: status bar
- With console enabled, an additional bottom pane appears for console history and input

### Left pane

After a connection succeeds, the left side combines two blocks:

1. Databases of the active connection
2. Keys of the selected database

Indicators:

- `▪` - active database
- `▫` - inactive database
- `[St]` - `string`
- `[Hs]` - `hash`
- `[Li]` - `list`
- `[Se]` - `set`
- `[Zs]` - `zset`
- `[Tr]` - `stream`
- `✓` before the type icon - key is marked for a bulk action

### Right pane

For the selected key, the UI shows:

- key name
- type
- TTL
- size/length/member count when supported by the type
- for cluster connections: `Hash Slot` and the node that owns the key
- the value itself or a message explaining the current display limitation

### Console

The console opens in the lower part of the screen and contains:

- command and result history
- the `redis>` input prompt
- its own focus area, selected automatically when the console opens

---

## Working with Keys

### Selecting a connection and database

1. On the startup screen, select a configured profile and press `Enter`
2. If connection fails, review the diagnostic modal and choose `Retry`, `Back`, or `Exit`
3. After connection succeeds, choose any database configured on a standalone server
4. In Redis Cluster mode, only `DB0` is available
5. After database selection, the application rescans keys automatically
6. `F9` disconnects completely and returns to the startup profile list

### Scanning and browsing

- The key list is built with `SCAN`, not blocking `KEYS`
- The application detects the type of each discovered key
- Press `Enter` on a selected key to open its details
- For large datasets, scanning stops after 5000 keys

### Adding a new key

**Hotkey: `F3`**

Available types:

- `string`
- `hash`
- `list`
- `set`
- `zset`
- `bitmap`
- `stream`

Input formats:

**String**

```text
Any text
Multiline input is saved as a string containing \n
```

**Hash**

```text
field1:value1
field2:value2
```

**List**

```text
item1
item2
item3
```

**Set**

```text
member1
member2
member3
```

**ZSet**

```text
member1:1.0
member2:2.5
member3:10
```

**Bitmap**

```text
0:1
1:0
100:1
```

**Stream**

```text
field1:value1
field2:value2
```

You can also set an optional `TTL` in seconds.

### Editing a key

**Hotkey: `F4`**

Behavior:

- for `string`, `hash`, `list`, `set`, and `zset`, the current value is preloaded into the form
- if you change the type, the old key is deleted and recreated
- for `bitmap` and `stream`, the current implementation does not preload existing content, so you must re-enter it manually

Save and cancel:

- `F7` - save
- `Esc` - close the form

### Deleting keys

#### Delete a single key

1. Open the key in the details pane
2. Click the `Delete Key` button
3. Confirm the deletion

#### Bulk delete

1. Mark keys with `Space` or by pattern
2. Press `F8`
3. Confirm the deletion

### Marking and filtering

**List filter**

- hotkey `/`
- supports glob patterns with `*` and `?`
- applies only to the already loaded key list

Examples:

```text
user:*
*session*
temp:?123
```

**Bulk marking**

- `Space` - toggle mark on the current key
- `Ctrl+A` - mark keys by pattern
- `Ctrl+U` - clear all marks

### Redis Cluster specifics

- the database is always `DB0`
- multi-key operations require the same hash slot
- if you intentionally use hash tags, keep keys in the form `{tag}:suffix`

---

## Redis Console

### Opening the console

**Hotkey: `F2`**

After opening:

- the main UI remains visible
- a console pane appears at the bottom
- focus moves to the input prompt

### Basic usage

```text
redis> SET mykey "Hello"
OK

redis> GET mykey
"Hello"

redis> HGETALL user:1001
1) "name" => "John Doe"
2) "email" => "john@example.com"
```

### Command history

- `Page Up` or `Ctrl+P` - previous command
- `Page Down` or `Ctrl+N` - next command
- history is stored in `~/.redis_commander_history`
- the last **100 commands** persist across restarts

### Special console commands

- `exit` or `quit` - close the console
- `clear` - clear the console output

### Auto-refresh after changes

After modifying commands, the key list refreshes automatically. This includes commands such as:

- `SET`, `DEL`, `HSET`, `LPUSH`, `SADD`, `ZADD`
- `EXPIRE`, `PERSIST`, `RENAME`
- `FLUSHDB`, `FLUSHALL`

### Cluster-aware console behavior

In cluster mode, the console applies special handling to several commands:

- `KEYS <pattern>` - gathers keys from all master nodes
- `SCAN ...` - scans all master nodes and merges the result
- `INFO [section]` - can aggregate information across master nodes
- `DBSIZE` - sums key counts across master nodes
- `FLUSHDB`, `FLUSHALL` - runs the command on all master nodes
- `CLUSTER ...` - forwards the command through the cluster client

In cluster mode, these commands are explicitly treated as unsupported:

- `SELECT`
- `MOVE`
- `SWAPDB`
- `MIGRATE`
- `RANDOMKEY`

For `CROSSSLOT` and `MOVED` errors, the UI prints dedicated messages in the console pane.

---

## Hotkeys

### Main

| Key | Action |
|-----|--------|
| `F1` or `?` | show help in the details pane |
| `F2` | open or close the console |
| `F3` | add a key |
| `F4` | edit the selected key |
| `F8` | delete marked keys |
| `F9` | disconnect and return to the profile selector |
| `F10` | quit the application |
| `F11` | rescan keys |
| `F12` | show cluster diagnostic information in the details pane |
| `Tab` | move focus: databases → keys → details |
| `Shift+Tab` | move focus in the reverse direction |
| `q` / `Q` | quit the application when the console is closed |
| `Esc` | close the console or the current dialog |

### Key list

| Key | Action |
|-----|--------|
| `Enter` | open details for the selected key |
| `Space` | toggle mark |
| `/` | open the filter dialog |
| `Ctrl+A` | mark keys by pattern |
| `Ctrl+U` | clear all marks |
| `↑` / `↓` | move through the list |

### Edit dialogs

| Key | Action |
|-----|--------|
| `F7` | save the key |
| `Esc` | close the form |

### Console

| Key | Action |
|-----|--------|
| `Enter` | execute the command |
| `Page Up` / `Ctrl+P` | previous command from history |
| `Page Down` / `Ctrl+N` | next command from history |
| `Esc` | close the console |

---

## Security Modes

### Mode 1: plain JSON

```bash
python redis-commander.py -c redis_profiles.json
```

Good for:

- local development
- test environments
- temporary configs without sensitive credentials

Tradeoff:

- passwords are stored in plain text

### Mode 2: encrypted config

Create:

```bash
python config-encryptor.py redis_profiles.json redis_profiles.enc
```

Use:

```bash
python redis-commander.py -e redis_profiles.enc
```

Details:

- uses `PBKDF2-HMAC-SHA256`
- the first 16 bytes store the salt
- decryption happens at startup after password entry

### Mode 3: HashiCorp Vault

Use:

```bash
python redis-commander.py \
  --vault-url https://vault.company.com:8200 \
  --vault-path secret/data/redis-commander \
  --vault-user redis-admin
```

Expected secret content:

- the secret must contain profile objects in the same format as `redis_profiles.json`
- the application reads the data as KV v2

### `readonly` mode

Profile setting:

```json
{
  "readonly": true
}
```

What it currently does:

- hides the `Delete Key` button in the details pane

What it does not do:

- it does not block `F3` or `F4`
- it does not block write commands in the console

For real read-only access, use Redis ACLs or a dedicated user with restricted permissions.

---

## FAQ and Limitations

### Is there a key count limit?

Yes. The key list is limited to **5000 entries** per scan to keep the UI responsive.

### What happens if the config file is missing?

If the JSON file is missing, the startup selector reports that no profiles were found.

### Can it handle binary values?

Yes, but only partially:

- binary strings and fields are displayed as `<binary data: N bytes>` or `<binary: N bytes>`
- there is no dedicated binary editor in the current version

### How do I switch databases?

- in standalone mode, the UI reads the configured database count and supports DB16 and above
- in cluster mode, only `DB0` is available

### How should I handle multi-key commands in a cluster?

Use keys with the same hash tag:

```text
{user:42}:profile
{user:42}:settings
```

This improves the chances that multi-key operations will not fail with `CROSSSLOT`.

### Are `bitmap` and `stream` fully supported?

No, support is partial:

- you can create and save these keys from the dialog
- editing existing values is less convenient because the form does not preload current content
- the details pane does not render their contents in a decoded form

### Where is command history stored?

In:

```text
~/.redis_commander_history
```

The last **100 commands** are saved after each update.

---

## Logs and Diagnostics

### Audit log

All major actions are written to:

```text
redis_tui_audit.log
```

The file contains:

- connections and disconnects
- executed Redis commands
- scanning, loading, and editing errors

### Cluster diagnostics

Hotkey `F12` shows cluster client diagnostics in the details pane:

- available client methods
- node information
- `CLUSTER INFO` output if it can be collected

---

**Author:** Dmitry Tarasov  
**Code version:** 1.2.0
**License:** MIT
