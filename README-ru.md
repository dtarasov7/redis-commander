# Redis Commander TUI

[![Python](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Терминальный пользовательский интерфейс для Redis Standalone и Redis Cluster.

Проект предоставляет TUI, управляемый с клавиатуры, для просмотра ключей, анализа значений, редактирования распространенных типов данных Redis, выполнения Redis-команд и переключения между несколькими профилями подключения.

## Возможности

- Поддержка standalone Redis с `DB0-DB15`
- Поддержка Redis Cluster со cluster-aware сканированием ключей и обработкой команд в консоли
- Несколько профилей подключения, загружаемых из:
  - обычного JSON
  - зашифрованного файла конфигурации
  - HashiCorp Vault
- Просмотр ключей с фильтрацией и массовой отметкой
- Добавление, редактирование и удаление для:
  - `string`
  - `hash`
  - `list`
  - `set`
  - `zset`
  - `bitmap`
  - `stream`
- Встроенная Redis-консоль с сохранением истории команд
- Поддержка TLS/SSL
- Поддержка Unix socket
- Аудит-лог в `redis_tui_audit.log`

## Файлы проекта

- `redis-commander.py` - основное приложение
- `config-encryptor.py` - утилита шифрования конфигурации
- `redis_profiles.json` - пример конфигурации
- `UserGuide.md` / `UserGuide-ru.md` - пользовательская документация
- `DeveloperGuide.md` / `DeveloperGuide-ru.md` - документация разработчика

## Требования

- Python 3.7+
- `urwid`
- `cryptography` для зашифрованных конфигов
- `hvac` для режима Vault
- `simple_redis_client`, доступный в Python environment

Пример установки зависимостей:

```bash
pip install urwid cryptography hvac
```

## Быстрый старт

Создайте минимальный конфиг:

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

Запустите приложение:

```bash
python redis-commander.py
```

Запуск с явным путем к конфигу:

```bash
python redis-commander.py -c redis_profiles.json
```

## Режимы конфигурации

### Обычный JSON

```bash
python redis-commander.py -c redis_profiles.json
```

### Зашифрованный конфиг

Зашифруйте конфиг:

```bash
python config-encryptor.py redis_profiles.json redis_profiles.enc
```

Запустите приложение с зашифрованным файлом:

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

## Параметры командной строки

```text
-c, --config              Путь к plaintext конфигу
-e, --encrypted-config    Путь к зашифрованному конфигу
--vault-url               URL Vault
--vault-path              Путь к секрету в Vault
--vault-user              Пользователь Vault
--vault-pass              Пароль Vault
```

## Примечания

- UI сканирует до `5000` ключей на активную базу данных или cluster scan.
- В cluster mode доступна только `DB0`.
- `readonly` сейчас является ограничением на уровне UI, а не полным механизмом защиты от записи.
- `bitmap` и `stream` можно создавать, но их отображение и preload значений при редактировании поддержаны хуже, чем для основных Redis-типов.

## Архитектурные диаграммы

Исходники PlantUML находятся в каталоге `diagramms/`:

- `diagramms/architecture-overview.puml`
- `diagramms/startup-and-profile-loading.puml`
- `diagramms/ui-key-workflow.puml`
- `diagramms/cluster-scan-and-console.puml`
- для каждой диаграммы есть русская копия с суффиксом `-ru`

## Документация

Подробное описание использования и реализации находится в отдельных руководствах:

- `UserGuide.md`
- `UserGuide-ru.md`
- `DeveloperGuide.md`
- `DeveloperGuide-ru.md`

## Лицензия

Проект распространяется по лицензии MIT. См. [LICENSE](LICENSE).

## Author

**Tarasov Dmitry**
- Email: dtarasov7@gmail.com

## Attribution
Parts of this code were generated with assistance
