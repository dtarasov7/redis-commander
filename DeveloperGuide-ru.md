# Redis Commander TUI - Документация разработчика

---

## Содержание

1. [Архитектура](#архитектура)
2. [Структура кода](#структура-кода)
3. [Основные компоненты](#основные-компоненты)
4. [Потоки выполнения и работа с данными](#потоки-выполнения-и-работа-с-данными)
5. [Расширение функционала](#расширение-функционала)
6. [Тестирование](#тестирование)
7. [Производительность](#производительность)
8. [Известные проблемы и технический долг](#известные-проблемы-и-технический-долг)
9. [Практики разработки](#практики-разработки)
10. [Вклад в проект](#вклад-в-проект)

---

## Архитектура

Текущая версия реализована в основном в одном модуле `redis-commander.py`. Это монолитный TUI-скрипт, где рядом находятся:

- модели подключения
- работа с Redis
- UI на `urwid`
- загрузка профилей
- встроенная консоль
- история команд и логирование

Упрощенный поток запуска:

```text
main()
  -> argparse
  -> RedisCommanderUI(args)
       -> load_profiles()
       -> validate_profiles()
       -> create_ui()
       -> load_command_history()
       -> connect_to_profile(first_profile)
       -> urwid.MainLoop(...)
```

Особенности текущей архитектуры:

- главный orchestrator: `RedisCommanderUI`
- один event loop на процесс
- один `current_connection`, но `self.connections` может держать несколько открытых соединений
- связь между UI-компонентами идет через `urwid.connect_signal(...)` и callback-методы

Внешние зависимости:

- `urwid`
- `simple_redis_client.RedisClient`
- `cryptography`
- `hvac`

Важно: `simple_redis_client` в текущем workspace отсутствует и считается внешней runtime-зависимостью.

---

## Структура кода

Файлы в корне проекта:

- `redis-commander.py` - основное приложение
- `config-encryptor.py` - шифрование JSON-конфига
- `redis_profiles.json` - пример конфигурации
- `UserGuide-ru.md`, `UserGuide.md`
- `DeveloperGuide-ru.md`, `DeveloperGuide.md`

Основные классы в `redis-commander.py`:

| Класс | Назначение |
|------|------------|
| `ConnectionProfile` | хранит параметры подключения |
| `RedisConnection` | подключает standalone Redis или Redis Cluster |
| `KeyListItem` | один элемент списка ключей |
| `KeyListView` | список ключей, фильтр, отметки |
| `CommandPromptWrapper` | history-aware prompt для консоли |
| `AddKeyDialog` | создание и редактирование ключей |
| `ScrollBar` | визуальный индикатор прокрутки |
| `RedisCommanderUI` | основной класс приложения |

Точка входа:

```python
def main():
    parser = argparse.ArgumentParser(...)
    parser.add_argument(...)
    args = parser.parse_args()
    app = RedisCommanderUI(args)
    app.run()
```

Поддерживаемые CLI-режимы:

- `--config`
- `--encrypted-config`
- `--vault-url`
- `--vault-path`
- `--vault-user`
- `--vault-pass`

Логирование настраивается глобально и пишет в `redis_tui_audit.log`.

---

## Основные компоненты

### `ConnectionProfile`

Хранит:

- `name`, `host`, `port`
- `password`, `username`
- `ssl`, `ssl_ca_certs`, `ssl_certfile`, `ssl_keyfile`
- `socket_path`
- `readonly`
- `cluster_mode`, `cluster_nodes`

`cluster_nodes` нормализуется в `_parse_profiles()` и может приходить как:

- `["host", 7000]`
- `"host:7000"`
- `{"host": "host", "port": 7000}`

### `RedisConnection`

Отвечает за:

- `_connect_standalone()`
- `_connect_cluster()`
- `select_db()` только для standalone
- `active_client`
- `disconnect()`

Поведение:

- standalone стартует с `db=0`
- cluster работает только с `DB0`
- TLS-параметры пробрасываются в `RedisClient`
- Unix socket поддерживается через `socket_path`

### `KeyListItem`

Один виджет ключа:

- хранит исходный `bytes` key
- показывает иконку типа
- хранит `marked`
- вызывает callback на `enter`

Иконки есть для:

- `string`, `hash`, `list`, `set`, `zset`, `stream`

Для `bitmap` отдельной иконки в списке сейчас нет.

### `KeyListView`

Центральный компонент списка ключей.

Основные поля:

- `all_keys`
- `filtered_keys`
- `marked_keys`
- `key_cache`

Основные методы:

- `set_keys()`
- `apply_filter()`
- `refresh_display()`
- `mark_by_pattern()`
- `unmark_all()`
- `toggle_mark_focused()`

Фильтрация работает по уже загруженному набору ключей.

### `CommandPromptWrapper`

Оборачивает `urwid.Edit` и перехватывает:

- `ctrl p` / `page up`
- `ctrl n` / `page down`
- `enter`

### `AddKeyDialog`

Используется для создания и редактирования ключей.

Поддерживаемые типы:

- `string`
- `hash`
- `list`
- `set`
- `zset`
- `bitmap`
- `stream`

Ответственность:

- radio buttons выбора типа
- динамическая форма значения
- preload значения при редактировании
- TTL
- сохранение в Redis
- подтверждение потери несохраненных изменений

Полный preload сейчас есть только для:

- `string`
- `hash`
- `list`
- `set`
- `zset`

### `ScrollBar`

Это текстовый индикатор позиции для `ListBox`, а не отдельный scrolling engine.

### `RedisCommanderUI`

Главный класс управляет:

- профилями и подключениями
- деревом серверов/БД
- сканированием ключей
- панелью деталей
- консолью
- history
- диалогами add/edit/delete/filter

---

## Потоки выполнения и работа с данными

### Инициализация

Порядок в `RedisCommanderUI.__init__()`:

1. Загрузка профилей через `load_profiles()`
2. Валидация профилей через `validate_profiles()`
3. Создание `KeyListView` и подписка на `key_selected`
4. Построение UI через `create_ui()`
5. Загрузка истории команд
6. Автоподключение к первому профилю
7. Создание `urwid.MainLoop`

### Загрузка профилей

`load_profiles()` выбирает один из источников:

- `_load_plaintext_config()`
- `_load_encrypted_config()`
- `_load_from_vault()`

`_parse_profiles()`:

- фильтрует поддерживаемые поля
- нормализует `cluster_nodes`
- подставляет `host/port` из первого cluster node, если нужно
- создает `ConnectionProfile`
- при ошибке делает минимальный fallback profile

Если профилей нет, создается:

```python
ConnectionProfile('localhost', 'localhost', 6379)
```

### Подключение

`connect_to_profile()`:

- переиспользует уже открытое соединение, если параметры совпадают
- иначе создает новый `RedisConnection`
- для cluster пытается получить `CLUSTER INFO`
- после успешного подключения вызывает `refresh_keys()`

### Базы данных

Standalone:

- доступны `DB0-DB15`
- `get_all_db_key_counts()` делает `SELECT + DBSIZE` для каждой БД

Cluster:

- доступна только `DB0`
- количество ключей берется через `DBSIZE`

### Сканирование ключей

`refresh_keys()`:

- для standalone использует `scan_iter(match='*', count=100)`
- для cluster использует `_scan_cluster_keys_with_types_iter()`
- ограничивает список `max_keys = 5000`

Cluster scan:

- пытается брать только master nodes
- делает node-level `SCAN`
- для каждого ключа отдельно определяет тип
- объединяет и дедуплицирует результат

### Панель деталей

`display_key_details()` показывает:

- key name
- type
- TTL
- size/length/member count, если это поддержано
- для cluster: `Hash Slot` и ноду
- значение для `string/hash/list/set/zset`

При `readonly == False` добавляется кнопка `Delete Key`.

### Поддержанные типы данных

- `string` -> `SET` / `GET`
- `hash` -> `HSET` / `HGETALL`
- `list` -> `RPUSH` / `LRANGE`
- `set` -> `SADD` / `SMEMBERS`
- `zset` -> `ZADD` / `ZRANGE ... WITHSCORES`
- `bitmap` -> `SETBIT`
- `stream` -> `XADD`

TTL применяется после сохранения:

```python
if ttl:
    client.expire(key, ttl)
```

### Встроенная консоль

`toggle_console()` перестраивает layout и добавляет нижнюю панель консоли.

`execute_console_command()`:

1. читает prompt
2. сохраняет команду в history
3. обрабатывает `exit`, `quit`, `clear`
4. разбирает `cmd` и `args`
5. выполняет команду
6. форматирует результат через `format_redis_result()`
7. после mutating commands вызывает `refresh_keys()`

Cluster-aware обработка есть для:

- `KEYS`
- `SCAN`
- `INFO`
- `DBSIZE`
- `FLUSHALL`
- `FLUSHDB`
- `CLUSTER ...`

В cluster mode явно запрещены:

- `SELECT`
- `MOVE`
- `SWAPDB`
- `MIGRATE`
- `RANDOMKEY`

### История команд

- файл: `~/.redis_commander_history`
- загрузка при старте
- сохранение после каждой новой команды
- фактически хранится не более 100 записей

### Диагностика

- audit log: `redis_tui_audit.log`
- `debug_cluster_info()` выводит debug-данные cluster client
- хоткей `F12` показывает эту информацию в панели деталей

---

## Расширение функционала

### Добавление нового Redis-типа

Нужно изменить:

1. иконки в `KeyListItem`
2. radio button в `AddKeyDialog`
3. `update_value_widget()`
4. `load_key_data()`
5. `on_save()`
6. `display_key_details()`

Мини-чеклист:

```text
KeyListItem
AddKeyDialog.__init__()
AddKeyDialog.update_value_widget()
AddKeyDialog.load_key_data()
AddKeyDialog.on_save()
RedisCommanderUI.display_key_details()
```

### Добавление нового источника секретов

Архитектура загрузки профилей уже позволяет это сделать:

1. новый CLI-флаг в `main()`
2. новый loader-метод
3. новая ветка в `load_profiles()`
4. повторное использование `_parse_profiles()`

### Изменение layout

UI собирается в `create_ui()`. Если добавляется новая панель, нужно обновить:

- `create_ui()`
- `toggle_console()`
- `handle_input()` для корректного `Tab`

### Добавление хоткеев

- prompt консоли -> `CommandPromptWrapper.keypress()`
- add/edit dialog -> `AddKeyDialog.keypress()`
- глобальный UI -> `RedisCommanderUI.handle_input()`

---

## Тестирование

### Текущее состояние

В репозитории нет автоматизированного набора тестов. Старые примеры тестов в документе не соответствовали фактическому имени модуля и состоянию проекта.

### Особенность импорта

Файл называется `redis-commander.py`, поэтому для ad hoc тестов нужен `importlib`.

Пример:

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

### Минимальные smoke tests

1. Plaintext startup
2. Encrypted startup через `config-encryptor.py`
3. Standalone connect -> DB switch -> scan
4. Cluster connect -> cluster scan -> console `INFO`/`DBSIZE`
5. Add/edit/delete для `string/hash/list/set/zset`
6. Создание `bitmap` и `stream`
7. Persistence history
8. Filter и mark-by-pattern

### Приоритетные unit test зоны

- `_parse_profiles()`
- `validate_profiles()`
- `format_redis_result()`
- `navigate_history()`
- парсинг значений в `AddKeyDialog.on_save()`

---

## Производительность

Текущие оптимизации:

1. лимит **5000 ключей**
2. `SCAN` вместо `KEYS`
3. `count=100`
4. кэш `KeyListItem` в `key_cache`
5. подрезка консольной истории на экране

Узкие места:

1. cluster scan идет последовательно
2. для каждого ключа отдельно вызывается `TYPE`
3. `get_all_db_key_counts()` проходит по всем `DB0-DB15`
4. список не является true virtualization

Реалистичные улучшения:

1. параллельный cluster scan
2. lazy type loading
3. кэш `DBSIZE`
4. paging/incremental loading
5. разделение scanning и rendering

---

## Известные проблемы и технический долг

### 1. Старый developer guide не соответствовал коду

Предыдущая версия документа содержала устаревшие ссылки, неверные тестовые примеры и даже посторонние фрагменты, не относящиеся к проекту.

### 2. `hvac` используется без явного импорта

В коде есть:

```python
client = hvac.Client(url=vault_url)
```

но явного `import hvac` нет. Vault mode может упасть с `NameError`.

### 3. `HAS_CRYPTO` не используется как guard

Если `cryptography` не установлена, `_load_encrypted_config()` все равно попытается использовать `Fernet` и связанные классы.

### 4. Опечатка `__VERSION__` в `main()`

В `main()` печатается `__VERSION__`, хотя объявлен `__version__`.

### 5. Stale type hints на `redis.*`

Есть аннотации:

```python
self.client: Optional[redis.Redis] = None
self.cluster_client: Optional[redis.RedisCluster] = None
```

При этом импорт `redis` закомментирован, а фактический клиент - `RedisClient`.

### 6. `readonly` защищает только частично

Сейчас `readonly` скрывает кнопку `Delete Key`, но не блокирует:

- `F3`
- `F4`
- `F8`
- команды записи в консоли

### 7. Неполная поддержка `bitmap` и `stream`

- создание есть
- preload при редактировании нет
- детальный рендеринг в панели деталей нет

### 8. Неиспользуемые `F5/F6`

Создаются `copy_btn` и `move_btn`, но не добавляются в toolbar и не имеют обработчиков.

### 9. Ошибки wiring в диалогах

Есть конструкции вида:

```python
cancel_btn = urwid.Button('Cancel', on_press=self.close_dialog(None))
```

Это вызывает `close_dialog(None)` сразу и оставляет `on_press=None`.

### 10. Имя модуля неудобно для импорта

`redis-commander.py` удобен для запуска, но неудобен для тестирования и пакетной организации.

---

## Практики разработки

Полезные правила для текущего кода:

1. По возможности выносить логику из UI-слоя в чистые функции
2. Сначала исправлять known issues, если они затрагивают новую фичу
3. Не считать `readonly` security control
4. В cluster-коде явно разделять master-only операции
5. Аккуратно работать с `bytes` и `str`, так как проект использует `decode_responses=False`

Предпочтительные зоны рефакторинга:

1. console dispatcher
2. profile loading/parsing
3. parsing/serialization в `AddKeyDialog`
4. cluster scanning helpers
5. details pane rendering

Логирование:

- `logger.info(...)` для важных операций
- `logger.error(..., exc_info=True)` когда нужен traceback

---

## Вклад в проект

Рекомендуемый порядок работы:

1. Проверить текущее поведение в `redis-commander.py`
2. Обновить документацию при изменении пользовательского сценария
3. Добавить smoke-check или воспроизводимый сценарий проверки
4. Не смешивать крупный рефакторинг и новую функциональность без необходимости

Полезные ближайшие улучшения:

1. исправить `__VERSION__`
2. добавить runtime guards для optional dependencies
3. сделать `readonly` реально read-only в UI
4. починить dialog callbacks
5. вынести код в импортируемый пакет

---

**Автор проекта:** Тарасов Дмитрий  
**Версия по коду:** 1.0.0  
**Лицензия:** MIT
