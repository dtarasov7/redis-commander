# Redis Commander TUI - Developer Guide

---

## Contents

1. [Architecture](#architecture)
2. [Code Structure](#code-structure)
3. [Core Components](#core-components)
4. [Execution Flows and Data Handling](#execution-flows-and-data-handling)
5. [Extending the Application](#extending-the-application)
6. [Testing](#testing)
7. [Performance](#performance)
8. [Known Issues and Technical Debt](#known-issues-and-technical-debt)
9. [Development Practices](#development-practices)
10. [Contributing](#contributing)

---

## Architecture

The current version is implemented mostly inside `redis-commander.py`. It is a monolithic TUI script where these concerns live together:

- connection models
- Redis access
- `urwid` UI
- profile loading
- built-in console
- command history and logging

Simplified startup flow:

```text
main()
  -> argparse
  -> RedisCommanderUI(args)
       -> load_profiles()
       -> validate_profiles()
       -> create_ui()
       -> create_profile_selector_ui()
       -> load_command_history()
       -> urwid.MainLoop(...)
       -> wait for explicit profile selection
```

Current architecture traits:

- main orchestrator: `RedisCommanderUI`
- one event loop per process
- one active `current_connection`; disconnect closes it before profile selection resumes
- UI components communicate through `urwid.connect_signal(...)` and callback methods

External dependencies:

- `urwid`
- `simple_redis_client.RedisClient`
- `cryptography`
- `hvac`

Important: `simple_redis_client` is not present in the current workspace and must be treated as an external runtime dependency.

---

## Code Structure

Files in the project root:

- `redis-commander.py` - main application
- `config-encryptor.py` - JSON config encryption utility
- `redis_profiles.json` - example config
- `UserGuide-ru.md`, `UserGuide.md`
- `DeveloperGuide-ru.md`, `DeveloperGuide.md`

Main classes in `redis-commander.py`:

| Class | Purpose |
|------|---------|
| `ConnectionProfile` | stores connection parameters |
| `RedisConnection` | connects to standalone Redis or Redis Cluster |
| `KeyListItem` | one key list entry |
| `KeyListView` | key list, filtering, bulk marks |
| `CommandPromptWrapper` | history-aware console prompt |
| `AddKeyDialog` | add/edit key dialog |
| `ScrollBar` | visual scroll indicator |
| `RedisCommanderUI` | main application class |

Entry point:

```python
def main():
    parser = argparse.ArgumentParser(...)
    parser.add_argument(...)
    args = parser.parse_args()
    app = RedisCommanderUI(args)
    app.run()
```

Supported CLI modes:

- `--config`
- `--encrypted-config`
- `--vault-url`
- `--vault-path`
- `--vault-user`
- `--vault-pass`

Logging is configured globally and writes to `redis_tui_audit.log`.

---

## Core Components

### `ConnectionProfile`

Stores:

- `name`, `host`, `port`
- `password`, `username`
- `ssl`, `ssl_ca_certs`, `ssl_certfile`, `ssl_keyfile`
- `socket_path`
- `readonly`
- `cluster_mode`, `cluster_nodes`

`cluster_nodes` is normalized in `_parse_profiles()` and may arrive as:

- `["host", 7000]`
- `"host:7000"`
- `{"host": "host", "port": 7000}`

### `RedisConnection`

Responsible for:

- `_connect_standalone()`
- `_connect_cluster()`
- `select_db()` in standalone mode only
- `active_client`
- `disconnect()`

Behavior:

- standalone starts with `db=0`
- cluster works on `DB0` only
- TLS settings are forwarded into `RedisClient`
- Unix socket mode is supported through `socket_path`

### `KeyListItem`

A single key widget:

- stores the original `bytes` key
- shows a type icon
- stores `marked`
- triggers callback on `enter`

Icons exist for:

- `string`, `hash`, `list`, `set`, `zset`, `stream`

`bitmap` currently has no dedicated icon in the list.

### `KeyListView`

Central key list component.

Main fields:

- `all_keys`
- `filtered_keys`
- `marked_keys`
- `key_cache`

Main methods:

- `set_keys()`
- `apply_filter()`
- `refresh_display()`
- `mark_by_pattern()`
- `unmark_all()`
- `toggle_mark_focused()`

Filtering works on the already loaded key set.

### `CommandPromptWrapper`

Wraps `urwid.Edit` and intercepts:

- `ctrl p` / `page up`
- `ctrl n` / `page down`
- `enter`

### `AddKeyDialog`

Used for both key creation and editing.

Supported types:

- `string`
- `hash`
- `list`
- `set`
- `zset`
- `bitmap`
- `stream`

Responsibilities:

- type selection with radio buttons
- dynamic value form
- value preload while editing
- TTL handling
- Redis write path
- unsaved changes confirmation

Full preload currently exists only for:

- `string`
- `hash`
- `list`
- `set`
- `zset`

### `ScrollBar`

This is a text-based position indicator for `ListBox`, not a separate scrolling engine.

### `RedisCommanderUI`

The main class manages:

- profiles and connections
- server/database tree
- key scanning
- details pane
- console
- history
- add/edit/delete/filter dialogs

---

## Execution Flows and Data Handling

### Initialization

Order inside `RedisCommanderUI.__init__()`:

1. Load profiles through `load_profiles()`
2. Validate profiles through `validate_profiles()`
3. Create `KeyListView` and subscribe to `key_selected`
4. Build the main UI with `create_ui()`
5. Build the startup selector with `create_profile_selector_ui()`
6. Load command history
7. Create `urwid.MainLoop` with the profile selector as its initial widget

### Profile loading

`load_profiles()` selects one source:

- `_load_plaintext_config()`
- `_load_encrypted_config()`
- `_load_from_vault()`

`_parse_profiles()`:

- filters supported fields
- normalizes `cluster_nodes`
- normalizes Sentinel endpoints and validates Sentinel service/read policies
- derives `host/port` from the first cluster node if needed
- creates `ConnectionProfile`
- falls back to a minimal profile on parse errors

If no profiles exist, the startup selector displays an empty-configuration message
and keeps the explicit `Exit` action available.

### Connection flow

`connect_to_profile()`:

- creates a new `RedisConnection` after the user presses `Enter` on a profile
- for Sentinel profiles, discovers master and healthy replicas before opening the UI
- for cluster connections, tries to read `CLUSTER INFO`
- after success, switches to the main interface and calls `refresh_keys()`
- after failure, shows secret-free endpoint, TLS/ACL, exception, and troubleshooting details

`disconnect()` closes the connection, clears key/detail state, and restores the
profile selector. `Exit`/`F10` terminates the program from either interface.

Sentinel profiles pass separate Redis and Sentinel AUTH/TLS settings to
`RedisClient`. Writes and background key scans use `main_pool`; eligible value
reads use replica pools according to `read_preference`. Scan failures trigger a
Sentinel topology refresh. The client retries reads and explicitly rejected
writes once, but never replays an ambiguous network-interrupted write.

### Databases

Standalone and Sentinel:

- reads the configured database count with `CONFIG GET databases`
- falls back to the highest DB from `INFO keyspace` and then to 16 databases
- `get_all_db_key_counts()` reads and caches one `INFO keyspace` response

Cluster:

- only `DB0` is available
- key count comes from `DBSIZE`

### Key scanning

`refresh_keys()`:

- starts cancellable background scan workers
- uses node-level `SCAN MATCH * COUNT 1000`
- resolves types with pipelined `TYPE` batches
- appends each completed batch to the UI incrementally
- limits the merged result to `5000` unique keys

Cluster scan:

- scans master nodes in parallel
- holds one pooled connection per worker
- merges and deduplicates batches on the UI thread

### Details pane

`display_key_details()` shows:

- key name
- type
- TTL
- size/length/member count when available
- in cluster mode: `Hash Slot` and node
- value rendering for `string/hash/list/set/zset`

If `readonly == False`, it adds a `Delete Key` button.

### Supported Redis types

- `string` -> `SET` / `GET`
- `hash` -> `HSET` / `HGETALL`
- `list` -> `RPUSH` / `LRANGE`
- `set` -> `SADD` / `SMEMBERS`
- `zset` -> `ZADD` / `ZRANGE ... WITHSCORES`
- `bitmap` -> `SETBIT`
- `stream` -> `XADD`

TTL is applied after save:

```python
if ttl:
    client.expire(key, ttl)
```

### Built-in console

`toggle_console()` rebuilds the layout and adds the bottom console pane.

`execute_console_command()`:

1. reads the prompt
2. saves the command in history
3. handles `exit`, `quit`, `clear`
4. parses `cmd` and `args`
5. executes the command
6. formats the result through `format_redis_result()`
7. calls `refresh_keys()` after mutating commands

Cluster-aware handling exists for:

- `KEYS`
- `SCAN`
- `INFO`
- `DBSIZE`
- `FLUSHALL`
- `FLUSHDB`
- `CLUSTER ...`

Explicitly blocked in cluster mode:

- `SELECT`
- `MOVE`
- `SWAPDB`
- `MIGRATE`
- `RANDOMKEY`

### Command history

- file: `~/.redis_commander_history`
- loaded at startup
- saved after each new command
- effectively capped at 100 entries

### Diagnostics

- audit log: `redis_tui_audit.log`
- `debug_cluster_info()` prints cluster client debug data
- hotkey `F12` shows it in the details pane

---

## Extending the Application

### Adding a new Redis type

Update:

1. icons in `KeyListItem`
2. radio button in `AddKeyDialog`
3. `update_value_widget()`
4. `load_key_data()`
5. `on_save()`
6. `display_key_details()`

Mini checklist:

```text
KeyListItem
AddKeyDialog.__init__()
AddKeyDialog.update_value_widget()
AddKeyDialog.load_key_data()
AddKeyDialog.on_save()
RedisCommanderUI.display_key_details()
```

### Adding a new secret source

The profile loading architecture already supports this:

1. add a new CLI flag in `main()`
2. add a new loader method
3. add a new branch in `load_profiles()`
4. reuse `_parse_profiles()`

### Changing layout

The main UI is assembled in `create_ui()` and the startup screen in
`create_profile_selector_ui()`. If you add a new pane or screen, update:

- `create_ui()`
- `toggle_console()`
- `handle_input()` so `Tab`/`Shift+Tab` focus cycling still works

### Adding hotkeys

- console prompt -> `CommandPromptWrapper.keypress()`
- add/edit dialog -> `AddKeyDialog.keypress()`
- global UI -> `RedisCommanderUI.handle_input()`

---

## Testing

### Current state

Regression tests are in `tests/test_performance_paths.py` and cover connection
pooling, batched scans, DB counts above 16, keyboard focus, and selector/main UI flow.

### Import caveat

The main file is named `redis-commander.py`, so ad hoc tests should use `importlib`.

Example:

```python
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "redis_commander_app",
    Path("redis-commander.py")
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
```

### Minimum smoke tests

1. Plaintext startup
2. Encrypted startup through `config-encryptor.py`
3. Standalone connect -> DB switch -> scan
4. Cluster connect -> cluster scan -> console `INFO`/`DBSIZE`
5. Add/edit/delete for `string/hash/list/set/zset`
6. Creation of `bitmap` and `stream`
7. History persistence
8. Filter and mark-by-pattern

### Highest-value unit test targets

- `_parse_profiles()`
- `validate_profiles()`
- `format_redis_result()`
- `navigate_history()`
- value parsing in `AddKeyDialog.on_save()`

---

## Performance

Current optimizations:

1. **5000-key** limit
2. `SCAN` instead of `KEYS`
3. `COUNT 1000` with pipelined `TYPE`
4. parallel cluster master scanning
5. incremental list updates from background workers
6. periodic pool health checks instead of per-command `PING`
7. cached `INFO keyspace` database counts
8. `KeyListItem` cache in `key_cache`
9. trimming console history in the UI

Remaining limitations:

1. the list is not true virtualization
2. the UI intentionally stops after 5000 unique keys
3. filtering applies to the already loaded key set

Possible future improvements:

1. true paging or a virtualized walker
2. on-demand type loading for visible rows only
3. server-side filtering before filling the 5000-key window

---

## Known Issues and Technical Debt

### 1. The previous developer guide did not match the code

The earlier version contained outdated references, incorrect test examples, and unrelated fragments.

### 2. `hvac` is used without an explicit import

The code contains:

```python
client = hvac.Client(url=vault_url)
```

but there is no explicit `import hvac`. Vault mode may fail with `NameError`.

### 3. `HAS_CRYPTO` is not used as a guard

If `cryptography` is missing, `_load_encrypted_config()` still tries to use `Fernet` and related classes.

### 4. `__VERSION__` typo in `main()`

`main()` prints `__VERSION__`, but the module defines `__version__`.

### 5. Stale `redis.*` type hints

There are annotations like:

```python
self.client: Optional[redis.Redis] = None
self.cluster_client: Optional[redis.RedisCluster] = None
```

but `redis` is commented out and the real client is `RedisClient`.

### 6. `readonly` is only partial protection

Right now `readonly` hides `Delete Key`, but does not block:

- `F3`
- `F4`
- `F8`
- write commands in the console

### 7. Incomplete `bitmap` and `stream` support

- creation exists
- value preload for editing does not
- details pane does not render these values in decoded form

### 8. Unused `F5/F6`

`copy_btn` and `move_btn` are created, but never added to the toolbar and have no handlers.

### 9. Dialog wiring bugs

There are constructs like:

```python
cancel_btn = urwid.Button('Cancel', on_press=self.close_dialog(None))
```

This calls `close_dialog(None)` immediately and leaves `on_press=None`.

### 10. Module name is inconvenient for imports

`redis-commander.py` is convenient for manual execution, but inconvenient for testing and packaging.

---

## Development Practices

Useful rules for the current code:

1. extract logic from the UI layer whenever possible
2. fix known issues first when they intersect with new work
3. do not treat `readonly` as a real security control
4. in cluster code, keep master-only operations explicit
5. handle `bytes` and `str` carefully because the app uses `decode_responses=False`

Preferred refactor targets:

1. console dispatcher
2. profile loading/parsing
3. parsing and serialization in `AddKeyDialog`
4. cluster scanning helpers
5. details pane rendering

Logging:

- `logger.info(...)` for important operations
- `logger.error(..., exc_info=True)` when traceback matters

---

## Contributing

Recommended order of work:

1. verify current behavior in `redis-commander.py`
2. update documentation if user-visible behavior changes
3. add a smoke check or reproducible verification path
4. avoid mixing large refactors with feature work unless necessary

Useful near-term improvements:

1. fix `__VERSION__`
2. add runtime guards for optional dependencies
3. make `readonly` actually read-only in the UI
4. fix dialog callbacks
5. move code into an importable package

---

**Project author:** Dmitry Tarasov  
**Code version:** 1.2.0
**License:** MIT
