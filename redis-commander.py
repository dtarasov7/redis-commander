#!/usr/bin/env python3
"""Redis Commander TUI main application module."""

import urwid
# import redis
from simple_redis_client import RedisClient, RedisError, RedisConnectionError, RedisClusterError
import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from collections import OrderedDict, defaultdict
import logging
import base64
import getpass
from typing import Dict
# Для шифрования (нужен pip install cryptography)
try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.backends import default_backend
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

try:
    import hvac
    HAS_HVAC = True
except ImportError:
    HAS_HVAC = False


__version__ = "1.0.0"
__author__ = "Tarasov Dmitry"

# Настройка логирования
logging.basicConfig(
    filename='redis_tui_audit.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('redis-commander')


class ConnectionProfile:
    """Store Redis connection settings for standalone or cluster mode."""
    def __init__(self, name: str, host: str = 'localhost', port: int = 6379,
             password: Optional[str] = None, username: Optional[str] = None,
             ssl: bool = False, ssl_ca_certs: Optional[str] = None,
             ssl_certfile: Optional[str] = None, ssl_keyfile: Optional[str] = None,
             socket_path: Optional[str] = None, readonly: bool = False,
             cluster_mode: bool = False,
             cluster_nodes: Optional[List[Tuple[str, int]]] = None):
        """Initialize a Redis connection profile.

        Args:
            name (str): Profile display name.
            host (str): Redis host name or IP address.
            port (int): Redis TCP port.
            password (Optional[str]): Redis password, if required.
            username (Optional[str]): Redis ACL username, if required.
            ssl (bool): Enable TLS for the connection.
            ssl_ca_certs (Optional[str]): Path to the CA certificate bundle.
            ssl_certfile (Optional[str]): Path to the client certificate.
            ssl_keyfile (Optional[str]): Path to the client private key.
            socket_path (Optional[str]): Unix socket path for local connections.
            readonly (bool): UI-level read-only flag for the profile.
            cluster_mode (bool): Treat the profile as a Redis Cluster endpoint.
            cluster_nodes (Optional[List[Tuple[str, int]]]): Seed nodes for cluster mode.
        """
        self.name = name
        self.host = host
        self.port = port
        self.password = password
        self.username = username
        self.ssl = ssl
        self.ssl_ca_certs = ssl_ca_certs
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.socket_path = socket_path
        self.readonly = readonly
        self.cluster_mode = cluster_mode
        self.cluster_nodes = cluster_nodes or [(host, port)]

    def to_dict(self) -> Dict:
        """Return a minimal serializable representation of the profile.

        Returns:
            Dict: Basic fields used for display or export.
        """
        return {
            'name': self.name,
            'host': self.host,
            'port': self.port,
            'ssl': self.ssl,
            'readonly': self.readonly
        }


class RedisConnection:
    """Manage a single Redis standalone or cluster connection."""

    def __init__(self, profile: ConnectionProfile):
        self.profile = profile
        self.client: Optional[redis.Redis] = None
        # self.client: RedisClient
        self.cluster_client: Optional[redis.RedisCluster] = None  # Новый клиент
        self.connected = False
        self.current_db = 0
        self.is_cluster = profile.cluster_mode

    def connect(self) -> Tuple[bool, str]:
        """Open the configured Redis connection.

        Returns:
            Tuple[bool, str]: Success flag and status message.
        """
        try:
            if self.profile.cluster_mode:
                return self._connect_cluster()
            else:
                return self._connect_standalone()
        except Exception as e:
            self.connected = False
            logger.error(f"Connection failed: {e}")
            return False, str(e)

    def _connect_standalone(self) -> Tuple[bool, str]:
        """Connect to a standalone Redis server.

        Returns:
            Tuple[bool, str]: Success flag and status message.
        """
        try:
            kwargs = {
                'db': 0,  # Всегда начинаем с DB0
                'socket_timeout': 5,
                'decode_responses': False
            }

            if self.profile.socket_path:
                kwargs['unix_socket_path'] = self.profile.socket_path
            else:
                kwargs['host'] = self.profile.host
                kwargs['port'] = self.profile.port

            if self.profile.password:
                kwargs['password'] = self.profile.password

            if self.profile.username:
                kwargs['username'] = self.profile.username

            if self.profile.ssl:
                kwargs['ssl'] = True
                if self.profile.ssl_ca_certs:
                    kwargs['ssl_ca_certs'] = self.profile.ssl_ca_certs
                if self.profile.ssl_certfile:
                    kwargs['ssl_certfile'] = self.profile.ssl_certfile
                if self.profile.ssl_keyfile:
                    kwargs['ssl_keyfile'] = self.profile.ssl_keyfile

            self.client = RedisClient(**kwargs)
            self.client.ping()
            self.connected = True
            self.current_db = 0
            logger.info(f"Connected to {self.profile.host}:{self.profile.port}")
            return True, "Connected"

        except RedisConnectionError as e:
            logger.error(f"Connection failed: {e}")
            return False, str(e)
        except Exception as e:
            self.connected = False
            logger.error(f"Connection failed: {e}")
            return False, str(e)

    def _connect_cluster(self) -> Tuple[bool, str]:
        """Connect to a Redis Cluster using the configured seed node.

        Returns:
            Tuple[bool, str]: Success flag and status message.
        """
        try:
            # Просто передаем список узлов как есть
            kwargs = {
                'host': self.profile.host,
                'port': self.profile.port,
                'decode_responses': False,
                'socket_timeout': 5,
            }

            if self.profile.password:
                kwargs['password'] = self.profile.password

            if self.profile.ssl:
                kwargs['ssl'] = True
                if self.profile.ssl_ca_certs:
                    kwargs['ssl_ca_certs'] = self.profile.ssl_ca_certs
                if self.profile.ssl_certfile:
                    kwargs['ssl_certfile'] = self.profile.ssl_certfile
                if self.profile.ssl_keyfile:
                    kwargs['ssl_keyfile'] = self.profile.ssl_keyfile

            # Для отладки
            logger.info(f"Connecting to Redis Cluster at {self.profile.host}:{self.profile.port}")

            # RedisCluster автоматически обнаружит другие узлы
            # self.cluster_client = redis.RedisCluster(**kwargs)
            self.cluster_client = RedisClient(**kwargs, is_cluster=True)
            self.cluster_client.ping()
            self.connected = True
            self.is_cluster = True

            # Проверяем, что это действительно кластер
            try:
                cluster_info = self.cluster_client.cluster_info()
                logger.info(f"Cluster state: {cluster_info.get('cluster_state', 'unknown')}")
            except:
                pass

            return True, "Connected to cluster"

        except RedisClusterError as e:
            logger.error(f"Redis Cluster error: {e}")
            return False, f"Cluster error: {str(e)}"
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False, str(e)

    @property
    def active_client(self):
        """Return the active standalone or cluster client.

        Returns:
            Any: Client instance used for Redis operations.
        """
        if self.is_cluster:
            return self.cluster_client
        return self.client

    def select_db(self, db: int) -> Tuple[bool, str]:
        """Switch the standalone connection to another logical database.

        Args:
            db (int): Redis database index to select.

        Returns:
            Tuple[bool, str]: Success flag and status message.
        """
        if self.is_cluster:
            return False, "Redis Cluster supports only DB0"

        if not self.connected:
            return False, "Not connected"

        try:
            self.client.select(db)
            self.current_db = db
            logger.info(f"Switched to DB:{db}")
            return True, f"Switched to DB:{db}"
        except Exception as e:
            logger.error(f"Failed to switch DB: {e}")
            return False, str(e)

    def disconnect(self):
        """Close the active client connection if it exists."""
        if self.client:
            self.client.close()
            self.connected = False
            logger.info(f"Disconnected from {self.profile.host}:{self.profile.port}")


class KeyListItem(urwid.WidgetWrap):
    """Render one selectable key row with type and mark state."""

    def __init__(self, key: bytes, key_type: str, on_select_callback, marked: bool = False):
        """Initialize the key row widget.

        Args:
            key (bytes): Raw Redis key.
            key_type (str): Redis type name for the key.
            on_select_callback: Callback invoked when the item is activated.
            marked (bool): Initial bulk-mark state.
        """
        self.key = key
        self.key_type = key_type
        self.marked = marked
        self.on_select_callback = on_select_callback

        # Декодируем ключ для отображения
        try:
            self.key_str = key.decode('utf-8', errors='replace')
        except:
            self.key_str = str(key)

        # Иконка типа
        type_icons = {
            'string': '[St]',
            'hash': '[Hs]',
            'list': '[Li]',
            'set': '[Se]',
            'zset': '[Zs]',
            'stream': '[Tr]',
            'none': '[No]',
            'unknown': '[?]',
        }
        icon = type_icons.get(key_type, '[?]')

        # Отметка (для массовых операций)
        mark = '✓' if marked else ' '

        # Текст элемента
        text = f"{mark}{icon} {self.key_str}"

        self.text_widget = urwid.SelectableIcon(text, cursor_position=0)

        super().__init__(self.text_widget)

    def toggle_mark(self):
        """Toggle the bulk-mark state for this item."""
        self.marked = not self.marked
        self.update_display()

    def update_display(self):
        """Refresh the rendered text after state changes."""
        type_icons = {
            'string': '[St]',
            'hash': '[Hs]',
            'list': '[Li]',
            'set': '[Se]',
            'zset': '[Zs]',
            'stream': '[Tr]',
            'none': '[No]',
            'unknown': '[?]',
        }
        icon = type_icons.get(self.key_type, '[?]')
        mark = '✓' if self.marked else ' '
        text = f"{mark}{icon} {self.key_str}"
        self.text_widget.set_text(text)

    def selectable(self):
        return True

    def keypress(self, size, key):
        if key == '~':  # Пробел - переключить отметку
            self.toggle_mark()
            return None
        elif key == 'enter':
            # Вызываем callback напрямую
            if self.on_select_callback:
                self.on_select_callback(self.key)
            return None
        return key


class KeyListView(urwid.WidgetWrap):
    """Display, filter, and bulk-mark the loaded key set."""

    signals = ['key_selected']

    def __init__(self):
        self.all_keys = []  # Все ключи: [(key_bytes, type_str), ...]
        self.filtered_keys = []  # Отфильтрованные ключи
        self.filter_pattern = None  # Текущий фильтр
        self.marked_keys = set()  # Отмеченные ключи
        self.key_cache = {}  # Кеш виджетов: {key: KeyListItem}

        # Walker для ListBox
        self.walker = urwid.SimpleFocusListWalker([])
        self.listbox = urwid.ListBox(self.walker)

        # Заголовок с информацией
        self.header = urwid.Text('')
        self.update_header()

        # Компоновка
        pile = urwid.Pile([
            ('pack', urwid.AttrMap(self.header, 'info_label')),
            ('pack', urwid.Divider('─')),
            ('weight', 1, self.listbox),
        ])

        super().__init__(pile)

    def set_keys(self, keys_with_types: List[Tuple[bytes, str]]):
        """Replace the current key collection and refresh the list view.

        Args:
            keys_with_types (List[Tuple[bytes, str]]): Keys paired with detected Redis types.
        """
        logger.info(f"KeyListView.set_keys called with {len(keys_with_types)} keys")
        self.all_keys = keys_with_types
        self.filtered_keys = keys_with_types.copy()
        self.key_cache.clear()  # Очищаем кеш при полном обновлении
        logger.info(f"all_keys: {len(self.all_keys)}, filtered_keys: {len(self.filtered_keys)}")
        self.refresh_display()
        logger.info("KeyListView.set_keys completed")

    def apply_filter(self, pattern: Optional[str]):
        """Apply or clear a glob filter on the loaded keys.

        Args:
            pattern (Optional[str]): Glob pattern or None to clear the filter.
        """
        self.filter_pattern = pattern

        if not pattern:
            self.filtered_keys = self.all_keys.copy()
        else:
            import fnmatch
            self.filtered_keys = []
            pattern_lower = pattern.lower()

            for key, key_type in self.all_keys:
                try:
                    key_str = key.decode('utf-8', errors='replace').lower()
                    # Поддержка wildcard или простого поиска
                    if '*' in pattern or '?' in pattern:
                        if fnmatch.fnmatch(key_str, pattern_lower):
                            self.filtered_keys.append((key, key_type))
                    else:
                        if pattern_lower in key_str:
                            self.filtered_keys.append((key, key_type))
                except:
                    pass

        self.refresh_display()

    def refresh_display(self):
        """Rebuild the visible list from the current filtered key set."""
        logger.info(f"KeyListView.refresh_display called, filtered_keys: {len(self.filtered_keys)}")

        self.walker.clear()
        logger.info("Walker cleared")

        # Создаем виджеты
        added_count = 0
        for idx, (key, key_type) in enumerate(self.filtered_keys):
            try:
                # Проверяем кеш
                if key in self.key_cache:
                    item = self.key_cache[key]
                    item.marked = key in self.marked_keys
                    item.update_display()
                else:
                    # ✅ Создаем виджет с callback напрямую
                    item = KeyListItem(
                        key,
                        key_type,
                        on_select_callback=self.on_key_selected,  # передаем метод
                        marked=key in self.marked_keys
                    )
                    self.key_cache[key] = item

                # Оборачиваем и добавляем
                wrapped = urwid.AttrMap(item, 'key_item', 'key_item_focus')
                self.walker.append(wrapped)
                added_count += 1

                if added_count <= 3:
                    logger.info(f"Added key #{added_count}: {key[:50] if len(key) > 50 else key}")

            except Exception as e:
                logger.error(f"Error creating widget for key {key}: {e}", exc_info=True)

        logger.info(f"Walker now has {len(self.walker)} items (added {added_count})")

        self.update_header()
        logger.info("KeyListView.refresh_display completed")

    def update_header(self):
        """Update the list header with counts and active filter state."""
        total = len(self.all_keys)
        filtered = len(self.filtered_keys)
        marked = len(self.marked_keys)

        if self.filter_pattern:
            text = f" Keys: {filtered}/{total} (filter: {self.filter_pattern})"
        else:
            text = f" Keys: {total}"

        if marked > 0:
            text += f" | Marked: {marked}"

        self.header.set_text(text)

    def on_key_selected(self, key):
        """Forward a selected key through the widget signal.

        Args:
            key: Raw Redis key selected in the list.
        """
        logger.info(f"KeyListView.on_key_selected called")
        logger.info(f"Type of 'key': {type(key)}")
        logger.info(f"Value of 'key': {key}")
        logger.info(f"Number of args received: 1")

        # Передаем дальше через сигнал
        logger.info("Calling urwid.emit_signal...")
        try:
            urwid.emit_signal(self, 'key_selected', key)
            logger.info("emit_signal completed")
        except Exception as e:
            logger.error(f"emit_signal failed: {e}", exc_info=True)

    def get_focused_key(self) -> Optional[bytes]:
        """Return the key currently focused in the list.

        Returns:
            Optional[bytes]: Focused key or None when nothing is selected.
        """
        focus_widget, focus_pos = self.walker.get_focus()
        if focus_widget and isinstance(focus_widget.base_widget, KeyListItem):
            return focus_widget.base_widget.key
        return None

    def get_marked_keys(self) -> List[bytes]:
        """Return all keys marked for a bulk operation.

        Returns:
            List[bytes]: Marked Redis keys.
        """
        return list(self.marked_keys)

    def mark_by_pattern(self, pattern: str):
        """Bulk-mark keys whose names match a glob pattern.

        Args:
            pattern (str): Glob pattern used to match loaded keys.
        """
        import fnmatch
        pattern_lower = pattern.lower()

        for key, _ in self.all_keys:
            try:
                key_str = key.decode('utf-8', errors='replace').lower()
                if fnmatch.fnmatch(key_str, pattern_lower):
                    self.marked_keys.add(key)
            except:
                pass

        self.refresh_display()

    def unmark_all(self):
        """Clear all bulk marks in the current key set."""
        self.marked_keys.clear()
        self.refresh_display()

    def toggle_mark_focused(self):
        """Toggle the mark on the focused item and advance focus."""
        focus_widget, focus_pos = self.walker.get_focus()
        if focus_widget and isinstance(focus_widget.base_widget, KeyListItem):
            item = focus_widget.base_widget
            if item.key in self.marked_keys:
                self.marked_keys.remove(item.key)
            else:
                self.marked_keys.add(item.key)
            item.marked = item.key in self.marked_keys
            item.update_display()
            self.update_header()

            # Переход к следующему элементу
            if focus_pos < len(self.walker) - 1:
                self.walker.set_focus(focus_pos + 1)


class CommandPromptWrapper(urwid.WidgetWrap):
    """Wrap the console prompt to support history navigation and execution."""

    def __init__(self, edit_widget, on_up, on_down, on_enter):
        self.edit_widget = edit_widget
        self.on_up = on_up
        self.on_down = on_down
        self.on_enter = on_enter
        super().__init__(edit_widget)

    def selectable(self):
        """Report that the prompt widget can receive focus."""
        return True

    def keypress(self, size, key):
        """Handle prompt-specific keys before delegating to the edit widget."""
        if key == 'ctrl p' or key == 'page up':
            # ✅ Вызываем callback и возвращаем None (обработано)
            self.on_up()
            return None
        elif key == 'ctrl n' or key == 'page down':
            # ✅ Вызываем callback и возвращаем None (обработано)
            self.on_down()
            return None
        elif key == 'enter':
            # ✅ Вызываем callback и возвращаем None (обработано)
            self.on_enter()
            return None
        else:
            # Остальные клавиши передаем в Edit
            return self.edit_widget.keypress(size, key)


class AddKeyDialog(urwid.WidgetWrap):
    """Collect and validate data for creating or editing a Redis key."""
    signals = ['close']

    def __init__(self, connection: RedisConnection, on_success_callback, edit_key=None):
        self.connection = connection
        self.on_success = on_success_callback
        self.edit_key = edit_key
        self.current_type = 'string'
        self.value_changed = False

        # Поля ввода с темным стилем
        self.key_edit = urwid.AttrMap(
            urwid.Edit(''),  # ✅ Убираем "Key: " отсюда
            'input',
            'input_focus'
        )

        self.type_group = []

        # Создаем кнопки с обработчиком
        self.type_buttons = [
            urwid.RadioButton(self.type_group, 'String', state=True,
                              on_state_change=lambda btn, state: self.on_type_changed(btn, state, 'string')),
            urwid.RadioButton(self.type_group, 'Hash',
                              on_state_change=lambda btn, state: self.on_type_changed(btn, state, 'hash')),
            urwid.RadioButton(self.type_group, 'List',
                              on_state_change=lambda btn, state: self.on_type_changed(btn, state, 'list')),
            urwid.RadioButton(self.type_group, 'Set',
                              on_state_change=lambda btn, state: self.on_type_changed(btn, state, 'set')),
            urwid.RadioButton(self.type_group, 'ZSet',
                              on_state_change=lambda btn, state: self.on_type_changed(btn, state, 'zset')),
            urwid.RadioButton(self.type_group, 'Bitmap',
                              on_state_change=lambda btn, state: self.on_type_changed(btn, state, 'bitmap')),
            urwid.RadioButton(self.type_group, 'Stream',
                              on_state_change=lambda btn, state: self.on_type_changed(btn, state, 'stream')),
        ]

        # Универсальные виджеты
        self.value_edit = None
        self.value_container = urwid.Pile([])
        self.ttl_edit = urwid.AttrMap(
            urwid.Edit(''),  # ✅ Убираем "TTL: " отсюда
            'input',
            'input_focus'
        )
        self.error_text = urwid.Text('')
        self.confirm_dialog = None

        # Кнопки
        save_btn = urwid.Button('Save (F7)', on_press=self.on_save)
        cancel_btn = urwid.Button('Cancel (Esc)', on_press=self.on_cancel)

        # ✅ Подписи с белым на синем
        key_label = urwid.AttrMap(urwid.Text('Key:'), 'info_label')
        type_label = urwid.AttrMap(urwid.Text('Type:'), 'info_label')
        ttl_label = urwid.AttrMap(urwid.Text('TTL (seconds, optional):'), 'info_label')

        # Компоновка
        title = 'Edit Key' if edit_key else 'Add New Key'
        body = [
            self.error_text,
            urwid.Divider(),
            key_label,  # ✅ Подпись отдельно
            self.key_edit,
            urwid.Divider(),
            type_label,  # ✅ Подпись отдельно
        ]
        body.extend(self.type_buttons)
        body.extend([
            urwid.Divider(),
            self.value_container,
            urwid.Divider(),
            ttl_label,  # ✅ Подпись отдельно
            self.ttl_edit,
            urwid.Divider(),
            urwid.Columns([
                urwid.AttrMap(save_btn, 'button', 'button_focus'),
                urwid.AttrMap(cancel_btn, 'button', 'button_focus'),
            ]),
        ])

        self.pile = urwid.Pile(body)
        fill = urwid.Filler(self.pile, valign='top')
        padding = urwid.Padding(fill, left=2, right=2)
        linebox = urwid.LineBox(padding, title=title)

        super().__init__(urwid.AttrMap(linebox, 'dialog'))

        # Если режим редактирования, загружаем данные
        if edit_key:
            self.load_key_data(edit_key)
        else:
            # Инициализируем виджеты для String (по умолчанию)
            self.update_value_widget('string')

    def load_key_data(self, key):
        """Load the current key value into the edit form.

        Args:
            key: Redis key to preload into the dialog.
        """
        try:
            client = self.connection.active_client

            # Устанавливаем имя ключа (в режиме редактирования нельзя менять)
            self.key_edit.base_widget.set_edit_text(key)
            self.key_edit.base_widget.set_edit_pos(len(key))

            # Получаем тип ключа
            key_type = client.type(key)
            if isinstance(key_type, bytes):
                key_type = key_type.decode('utf-8')

            # Устанавливаем тип
            type_index = ['string', 'hash', 'list', 'set', 'zset', 'bitmap', 'stream'].index(key_type)
            self.type_buttons[type_index].set_state(True)
            self.current_type = key_type
            self.update_value_widget(key_type)

            # Загружаем значение в зависимости от типа
            if key_type == 'string':
                value = client.get(key)
                if value:
                    if isinstance(value, bytes):
                        value = value.decode('utf-8', errors='replace')
                    # Разбиваем по \n на строки для многострочного редактора
                    lines = value.split('\\n')
                    self.value_edit.base_widget.set_edit_text('\n'.join(lines))

            elif key_type == 'hash':
                hash_data = client.hgetall(key)
                lines = []
                for field, val in hash_data.items():
                    if isinstance(field, bytes):
                        field = field.decode('utf-8', errors='replace')
                    if isinstance(val, bytes):
                        val = val.decode('utf-8', errors='replace')
                    lines.append(f"{field}:{val}")
                self.value_edit.base_widget.set_edit_text('\n'.join(lines))

            elif key_type == 'list':
                items = client.lrange(key, 0, -1)
                lines = []
                for item in items:
                    if isinstance(item, bytes):
                        item = item.decode('utf-8', errors='replace')
                    lines.append(item)
                self.value_edit.base_widget.set_edit_text('\n'.join(lines))

            elif key_type == 'set':
                members = client.smembers(key)
                lines = []
                for member in members:
                    if isinstance(member, bytes):
                        member = member.decode('utf-8', errors='replace')
                    lines.append(member)
                self.value_edit.base_widget.set_edit_text('\n'.join(lines))

            elif key_type == 'zset':
                items = client.zrange(key, 0, -1, withscores=True)
                lines = []
                for member, score in items:
                    if isinstance(member, bytes):
                        member = member.decode('utf-8', errors='replace')
                    lines.append(f"{member}:{score}")
                self.value_edit.base_widget.set_edit_text('\n'.join(lines))

            # Получаем TTL
            ttl = client.ttl(key)
            if ttl > 0:
                self.ttl_edit.base_widget.set_edit_text(str(ttl))

            # Сбрасываем флаг изменения после загрузки
            self.value_changed = False

        except Exception as e:
            logger.error(f"Failed to load key data: {e}")
            self.show_error(f"Failed to load key: {e}")

    def keypress(self, size, key):
        """Обработка нажатий клавиш"""
        # Если открыт диалог подтверждения, передаем ему управление
        if self.confirm_dialog:
            return super().keypress(size, key)

        if key == 'esc':
            self.request_close()
            return None
        elif key == 'f7':
            self.on_save(None)
            return None

        # Отслеживаем изменения в value_edit
        result = super().keypress(size, key)

        # Проверяем, был ли изменен текст
        if self.value_edit and hasattr(self.value_edit, 'base_widget'):
            current_text = self.value_edit.base_widget.get_edit_text()
            if current_text.strip():
                self.value_changed = True

        return result

    def request_close(self):
        """Close the dialog or confirm when there are unsaved changes."""
        if self.value_changed:
            self.show_confirm_dialog(
                "Discard changes?",
                "You have unsaved changes. Close anyway?",
                on_yes=lambda: self._emit('close'),
                on_no=lambda: None
            )
        else:
            self._emit('close')

    def on_type_changed(self, button, new_state, type_name):
        """Handle Redis type changes and confirm how to treat existing input."""
        if not new_state:
            return

        if type_name == self.current_type:
            return

        # Если есть несохраненные изменения, спрашиваем
        if self.value_changed:
            # Запоминаем новый тип
            self.pending_type = type_name

            self.show_confirm_dialog(
                "Change type?",
                "Keep current value or clear?",
                yes_text="Keep",
                no_text="Clear",
                on_yes=lambda: self.change_type_keep_value(type_name),
                on_no=lambda: self.change_type_clear_value(type_name)
            )
        else:
            logger.debug(f"Type changed from {self.current_type} to {type_name}")
            self.current_type = type_name
            self.update_value_widget(type_name)

    def change_type_keep_value(self, type_name):
        """Switch the form type and keep the current raw input text."""
        old_value = self.get_value_text()
        self.current_type = type_name
        self.update_value_widget(type_name)
        # Пытаемся сохранить старое значение
        if self.value_edit and old_value:
            self.value_edit.base_widget.set_edit_text(old_value)

    def change_type_clear_value(self, type_name):
        """Switch the form type and clear the current raw input text."""
        self.current_type = type_name
        self.update_value_widget(type_name)
        self.value_changed = False

    def show_confirm_dialog(self, title, message, yes_text="Yes", no_text="No", on_yes=None, on_no=None):
        """Show a modal confirmation dialog inside the editor."""
        yes_btn = urwid.Button(yes_text, on_press=lambda btn: self.close_confirm_dialog(on_yes))
        no_btn = urwid.Button(no_text, on_press=lambda btn: self.close_confirm_dialog(on_no))

        pile = urwid.Pile([
            urwid.Text(message),
            urwid.Divider(),
            urwid.Columns([
                urwid.AttrMap(yes_btn, 'button', 'button_focus'),
                urwid.AttrMap(no_btn, 'button', 'button_focus'),
            ]),
        ])

        linebox = urwid.LineBox(urwid.Filler(pile), title=title)
        overlay = urwid.Overlay(
            urwid.AttrMap(linebox, 'dialog'),
            self._w,
            align='center',
            width=('relative', 60),
            valign='middle',
            height=('relative', 30)
        )

        self.confirm_dialog = overlay
        self._w = overlay

    def close_confirm_dialog(self, callback):
        """Close the confirmation overlay and run the selected callback."""
        # Восстанавливаем исходный виджет
        self._w = urwid.AttrMap(
            urwid.LineBox(
                urwid.Padding(
                    urwid.Filler(self.pile, valign='top'),
                    left=2, right=2
                ),
                title='Edit Key' if self.edit_key else 'Add New Key'
            ),
            'dialog'
        )
        self.confirm_dialog = None

        # Выполняем callback
        if callback:
            callback()

    def update_value_widget(self, key_type):
        """Rebuild the value editor for the selected Redis type.

        Args:
            key_type: Redis type currently selected in the dialog.
        """
        self.current_type = key_type

        # Очищаем контейнер
        self.value_container.contents.clear()

        # ✅ Подпись для значения
        value_label = urwid.AttrMap(urwid.Text('Value:'), 'info_label')
        self.value_container.contents.append((value_label, ('pack', None)))

        if key_type == 'string':
            # Многострочное поле для string
            self.value_edit = urwid.AttrMap(
                urwid.Edit('', multiline=True),
                'input',
                'input_focus'
            )
            self.value_container.contents.append(
                (urwid.LineBox(self.value_edit), ('weight', 1))
            )

        elif key_type == 'hash':
            # Для hash - таблица key-value пар
            help_text = urwid.Text('Enter key-value pairs (one per line, format: key=value)')
            self.value_edit = urwid.AttrMap(
                urwid.Edit('', multiline=True),
                'input',
                'input_focus'
            )
            self.value_container.contents.extend([
                (help_text, ('pack', None)),
                (urwid.LineBox(self.value_edit), ('weight', 1))
            ])

        elif key_type == 'list':
            # Для list - одно значение на строку
            help_text = urwid.Text('Enter list items (one per line)')
            self.value_edit = urwid.AttrMap(
                urwid.Edit('', multiline=True),
                'input',
                'input_focus'
            )
            self.value_container.contents.extend([
                (help_text, ('pack', None)),
                (urwid.LineBox(self.value_edit), ('weight', 1))
            ])

        elif key_type == 'set':
            # Для set - одно значение на строку
            help_text = urwid.Text('Enter set members (one per line)')
            self.value_edit = urwid.AttrMap(
                urwid.Edit('', multiline=True),
                'input',
                'input_focus'
            )
            self.value_container.contents.extend([
                (help_text, ('pack', None)),
                (urwid.LineBox(self.value_edit), ('weight', 1))
            ])

        elif key_type == 'zset':
            # Для zset - score:member пары
            help_text = urwid.Text('Enter scored items (format: score:member, one per line)')
            self.value_edit = urwid.AttrMap(
                urwid.Edit('', multiline=True),
                'input',
                'input_focus'
            )
            self.value_container.contents.extend([
                (help_text, ('pack', None)),
                (urwid.LineBox(self.value_edit), ('weight', 1))
            ])

        else:
            # Для остальных типов
            help_text = urwid.Text(f'{key_type} type - use appropriate format')
            self.value_edit = urwid.AttrMap(
                urwid.Edit('', multiline=True),
                'input',
                'input_focus'
            )
            self.value_container.contents.extend([
                (help_text, ('pack', None)),
                (urwid.LineBox(self.value_edit), ('weight', 1))
            ])

    def get_value_text(self) -> str:
        """Return normalized text from the active value editor.

        Returns:
            str: Trimmed editor content or an empty string.
        """
        if self.value_edit:
            return self.value_edit.base_widget.get_edit_text().strip()
        return ''

    def on_save(self, button):
        """Validate the form and write the key to Redis.

        Args:
            button: Urwid callback argument for the save button.
        """
        key = self.key_edit.base_widget.get_edit_text().strip()
        key_type = self.current_type
        ttl_text = self.ttl_edit.base_widget.get_edit_text().strip()

        if not key:
            self.show_error("Key cannot be empty")
            return

        # Проверка для кластера
        if self.connection.is_cluster:
            if '{' in key and '}' in key:
                import re
                if not re.search(r'\{.*?\}', key):
                    self.show_error("Invalid hash tag format. Use {tag} format.")
                    return

        try:
            ttl = int(ttl_text) if ttl_text else None
            client = self.connection.active_client
            value = self.get_value_text()

            # При редактировании удаляем старый ключ если тип изменился
            if self.edit_key:
                old_type = client.type(self.edit_key)
                if isinstance(old_type, bytes):
                    old_type = old_type.decode('utf-8')
                if old_type != key_type:
                    client.delete(self.edit_key)

            if key_type == 'string':
                if not value:
                    self.show_error("Value cannot be empty")
                    return
                # Преобразуем многострочный ввод в одну строку с \n
                final_value = '\\n'.join(value.split('\n'))
                client.set(key, final_value)

            elif key_type == 'hash':
                pairs = {}
                for line in value.split('\n'):
                    line = line.strip()
                    if ':' in line:
                        k, v = line.split(':', 1)
                        pairs[k.strip()] = v.strip()
                if not pairs:
                    self.show_error("At least one field:value pair required")
                    return
                # При редактировании сначала очищаем
                if self.edit_key:
                    client.delete(key)
                client.hset(key, mapping=pairs)

            elif key_type == 'list':
                items = [item.strip() for item in value.split('\n') if item.strip()]
                if not items:
                    self.show_error("At least one element required")
                    return
                # При редактировании сначала очищаем
                if self.edit_key:
                    client.delete(key)
                client.rpush(key, *items)

            elif key_type == 'set':
                items = [item.strip() for item in value.split('\n') if item.strip()]
                if not items:
                    self.show_error("At least one member required")
                    return
                # При редактировании сначала очищаем
                if self.edit_key:
                    client.delete(key)
                client.sadd(key, *items)

            elif key_type == 'zset':
                pairs = {}
                for line in value.split('\n'):
                    line = line.strip()
                    if ':' in line:
                        member, score = line.rsplit(':', 1)
                        try:
                            pairs[member.strip()] = float(score.strip())
                        except ValueError:
                            self.show_error(f"Invalid score in line: {line}")
                            return
                if not pairs:
                    self.show_error("At least one member:score pair required")
                    return
                # При редактировании сначала очищаем
                if self.edit_key:
                    client.delete(key)
                client.zadd(key, pairs)

            elif key_type == 'bitmap':
                client.set(key, '')

                bit_set = False
                for line in value.split('\n'):
                    line = line.strip()
                    if ':' in line:
                        offset_str, bit_str = line.split(':', 1)
                        try:
                            offset = int(offset_str.strip())
                            bit = int(bit_str.strip())
                            if bit not in (0, 1):
                                self.show_error(f"Bit value must be 0 or 1, got: {bit}")
                                client.delete(key)
                                return
                            client.execute_command('SETBIT', key, offset, bit, key=key)
                            bit_set = True
                        except ValueError:
                            self.show_error(f"Invalid offset:bit format in line: {line}")
                            client.delete(key)
                            return

                if not bit_set:
                    self.show_error("At least one offset:bit pair required")
                    client.delete(key)
                    return

            elif key_type == 'stream':
                fields = {}
                for line in value.split('\n'):
                    line = line.strip()
                    if ':' in line:
                        k, v = line.split(':', 1)
                        fields[k.strip()] = v.strip()

                if not fields:
                    self.show_error("At least one field:value pair required for stream entry")
                    return

                args = [key, '*']
                for k, v in fields.items():
                    args.extend([k, v])

                client.execute_command('XADD', *args, key=key)

            # Устанавливаем TTL если указан
            if ttl:
                client.expire(key, ttl)

            action = "Updated" if self.edit_key else "Created"
            logger.info(f"{action} key: {key} (type: {key_type})")
            self.on_success()
            self._emit('close')

        except RedisClusterError as e:
            self.show_error(f"Cluster error: {e}")
            logger.error(f"Cluster error with key: {e}")
        except RedisError as e:
            self.show_error(f"Redis error: {e}")
            logger.error(f"Redis error with key: {e}")
        except Exception as e:
            self.show_error(f"Failed to save key: {e}")
            logger.error(f"Failed to save key: {e}", exc_info=True)

    def show_error(self, message):
        """Display an inline validation or save error message.

        Args:
            message: Error text shown inside the dialog.
        """
        self.error_text.set_text(('error', f"❌ Error: {message}"))

    def on_cancel(self, button):
        """Отмена"""
        self.request_close()


class ScrollBar(urwid.WidgetWrap):
    """Draw a lightweight visual scrollbar for a ListBox-based widget."""

    def __init__(self, widget):
        """Initialize the scrollbar wrapper.

        Args:
            widget: ListBox or a container that includes one.
        """
        self.widget = widget
        self.scrollbar_width = 1

        # Находим ListBox внутри виджета
        self.listbox = self._find_listbox(widget)

        # Создаем текстовый индикатор
        self.scrollbar_text = urwid.Text('│', align='left')

        # Комбинируем основной виджет и скроллбар
        self.columns = urwid.Columns([
            ('weight', 1, widget),
            ('given', self.scrollbar_width, urwid.AttrMap(
                urwid.Filler(self.scrollbar_text, valign='top'),
                'scrollbar'
            )),
        ], dividechars=0)

        super().__init__(self.columns)

    def _find_listbox(self, widget):
        """Recursively locate the first nested ListBox.

        Args:
            widget: Root widget that may contain a ListBox.

        Returns:
            Optional[urwid.ListBox]: Located ListBox or None.
        """
        if isinstance(widget, urwid.ListBox):
            return widget
        elif hasattr(widget, 'original_widget'):
            return self._find_listbox(widget.original_widget)
        elif isinstance(widget, urwid.Pile):
            for item, _ in widget.contents:
                lb = self._find_listbox(item)
                if lb:
                    return lb
        return None

    def render(self, size, focus=False):
        """Render the wrapped widget and refresh the scrollbar position."""
        maxcol, maxrow = size

        if self.listbox is None:
            return self.columns.render(size, focus)

        try:
            walker = self.listbox.body

            if not hasattr(walker, '__len__') or len(walker) == 0:
                # Пустой список
                self.scrollbar_text.set_text(' ' * maxrow)
            else:
                total_items = len(walker)

                # Получаем текущую позицию фокуса
                try:
                    focus_pos = walker.focus
                    if not isinstance(focus_pos, int):
                        focus_pos = 0
                except:
                    focus_pos = 0

                # Создаем визуальный индикатор
                if total_items <= maxrow:
                    # Всё помещается на экран
                    indicator = '█' * maxrow
                else:
                    # Есть прокрутка
                    lines = []

                    # Вычисляем размер и позицию "ползунка"
                    thumb_size = max(1, int(maxrow * maxrow / total_items))
                    scroll_progress = focus_pos / max(1, total_items - 1)
                    thumb_pos = int(scroll_progress * (maxrow - thumb_size))

                    for i in range(maxrow):
                        if i == 0 and focus_pos > 0:
                            lines.append('▲')  # Стрелка вверх - есть данные выше
                        elif i == maxrow - 1 and focus_pos < total_items - 1:
                            lines.append('▼')  # Стрелка вниз - есть данные ниже
                        elif thumb_pos <= i < thumb_pos + thumb_size:
                            lines.append('█')  # Ползунок
                        else:
                            lines.append('│')  # Дорожка

                    indicator = '\n'.join(lines)

                self.scrollbar_text.set_text(indicator)

        except Exception as e:
            logger.debug(f"ScrollBar update error: {e}")
            # Fallback
            self.scrollbar_text.set_text('│' * maxrow)

        return self.columns.render(size, focus)


class RedisCommanderUI:
    """Coordinate profile loading, UI state, Redis actions, and console flows."""

    palette = [
        ('header', 'white', 'dark blue', 'bold'),
        ('header_text', 'white', 'dark blue'),
        ('footer', 'white', 'dark blue'),
        ('sidebar', 'light gray', 'dark blue'),
        ('sidebar_focus', 'white', 'dark blue', 'bold'),
        ('connection', 'light cyan', 'dark blue'),
        ('connection_selected', 'black', 'light cyan', 'bold'),
        ('key_folder', 'white', 'dark blue'),
        ('key_folder_focus', 'black', 'light cyan', 'bold'),
        ('key_item', 'yellow', 'dark blue'),
        ('key_item_focus', 'black', 'light cyan', 'bold'),
        ('button', 'white', 'dark blue'),
        ('button_focus', 'white', 'light blue', 'bold'),
        ('button_danger', 'white', 'dark red'),
        ('info_label', 'light cyan', 'dark blue'),
        ('info_value', 'white', 'dark blue'),
        ('tab_active', 'white', 'light blue', 'bold'),
        ('tab_inactive', 'light gray', 'dark blue'),
        ('error', 'light red', 'dark blue'),
        ('success', 'light green', 'dark blue'),
        ('dialog', 'white', 'dark blue'),
        ('background', 'white', 'dark blue'),
        ('input', 'black', 'dark cyan'),
        ('input_focus', 'black', 'light cyan'),
        ('scrollbar', 'dark gray', 'dark blue'),
    ]

    def __init__(self, args):
        """Initialize the application, load profiles, and build the UI.

        Args:
            args: Parsed command-line arguments from argparse.
        """
        self.connections: Dict[str, RedisConnection] = {}
        self.current_connection: Optional[RedisConnection] = None
        self.args = args

        # Загружаем профили
        self.profiles = self.load_profiles()

        self.validate_profiles()

        self.key_list_view = KeyListView()
        urwid.connect_signal(self.key_list_view, 'key_selected', self.on_key_select)

        self.current_key: Optional[bytes] = None
        self.separator = '№'
        self.console_mode = False
        self.max_db = 15  # Redis поддерживает 0-15 баз по умолчанию
        self.key_count = 0

        # История команд
        self.command_history = []  # Список всех команд
        self.history_index = -1  # Текущая позиция в истории (-1 = новая команда)
        self.current_input = ''  # Временное хранилище для текущего ввода

        # Создаем интерфейс
        self.create_ui()

        # Загружаем историю из файла
        self.load_command_history()

        # Автоматически подключаемся к первому профилю
        if self.profiles:
            first_profile = list(self.profiles.values())[0]
            self.connect_to_profile(first_profile)

        self.loop = urwid.MainLoop(
            self.main_frame,
            palette=self.palette,
            unhandled_input=self.handle_input,
            pop_ups=True
        )

    def load_command_history(self):
        """Load persisted console history from the user history file."""
        history_file = os.path.expanduser('~/.redis_commander_history')
        try:
            if os.path.exists(history_file):
                with open(history_file, 'r', encoding='utf-8') as f:
                    self.command_history = [line.strip() for line in f if line.strip()]
                logger.info(f"Loaded {len(self.command_history)} commands from history")
        except Exception as e:
            logger.error(f"Failed to load command history: {e}")

    def save_command_history(self):
        """Persist the most recent console commands to disk."""
        history_file = os.path.expanduser('~/.redis_commander_history')
        try:
            # Сохраняем последние 1000 команд
            with open(history_file, 'w', encoding='utf-8') as f:
                for cmd in self.command_history[-100:]:
                    f.write(cmd + '\n')
            logger.info(f"Saved {len(self.command_history)} commands to history")
        except Exception as e:
            logger.error(f"Failed to save command history: {e}")

    def add_to_history(self, command: str):
        """Append a command to history unless it is empty or duplicated.

        Args:
            command (str): Command text entered in the console.
        """
        command = command.strip()
        if not command:
            return

        # Не добавляем дубликаты подряд
        if self.command_history and self.command_history[-1] == command:
            return

        self.command_history.append(command)
        self.history_index = -1  # Сбрасываем индекс
        self.current_input = ''

        # Сохраняем историю
        self.save_command_history()

    def navigate_history(self, direction: str):
        """Move backward or forward in console command history.

        Args:
            direction (str): Either ``up`` or ``down``.
        """
        if not self.command_history:
            return

        # Сохраняем текущий ввод если мы в режиме новой команды
        if self.history_index == -1:
            self.current_input = self.command_prompt.get_edit_text()

        if direction == 'up':
            # Идем назад в истории
            if self.history_index == -1:
                # Первое нажатие - переходим к последней команде
                self.history_index = len(self.command_history) - 1
            elif self.history_index > 0:
                self.history_index -= 1

        elif direction == 'down':
            # Идем вперед в истории
            if self.history_index == -1:
                # Уже в режиме новой команды
                return
            elif self.history_index < len(self.command_history) - 1:
                self.history_index += 1
            else:
                # Дошли до конца - возвращаемся к текущему вводу
                self.history_index = -1
                self.command_prompt.set_edit_text(self.current_input)
                self.command_prompt.set_edit_pos(len(self.current_input))
                return

        # Устанавливаем текст из истории
        if self.history_index >= 0:
            command = self.command_history[self.history_index]
            self.command_prompt.set_edit_text(command)
            self.command_prompt.set_edit_pos(len(command))

    def get_key_type_icon(self, key: str) -> str:
        """Resolve an ASCII icon for the current Redis key type.

        Args:
            key (str): Redis key name as text.

        Returns:
            str: Type icon used by the UI.
        """

        # Альтернатива: ASCII символы
        ascii_icons = {
            'string': '[St] ',
            'hash': '[Hs] ',
            'list': '[Li] ',
            'set': '[Se] ',
            'zset': '[Zs] ',
            'stream': '[Tr] ',
            'none': '[No] ',
            'unknown': '[?] ',
        }

        # Используем ascii_icons для максимальной совместимости
        icon_set = ascii_icons

        if not self.current_connection or not self.current_connection.connected:
            return icon_set['unknown']

        try:
            client = self.current_connection.active_client
            key_type = client.type(key.encode('utf-8', errors='replace'))

            if isinstance(key_type, bytes):
                key_type = key_type.decode('utf-8').lower()
            else:
                key_type = str(key_type).lower()

            return icon_set.get(key_type, icon_set['unknown'])
        except Exception as e:
            logger.error(f"Error getting key type for {key}: {e}")
            return icon_set['unknown']

    def load_profiles(self) -> Dict[str, ConnectionProfile]:
        """Load connection profiles from the selected configuration source.

        Returns:
            Dict[str, ConnectionProfile]: Parsed connection profiles keyed by name.
        """

        # Определяем режим работы
        if self.args.vault_url:
            # Режим 3: Vault
            profiles_data = self._load_from_vault()
        elif self.args.encrypted_config:
            # Режим 2: Зашифрованный файл
            profiles_data = self._load_encrypted_config(self.args.encrypted_config)
        else:
            # Режим 1: Обычный файл
            profiles_data = self._load_plaintext_config(self.args.config)

        # Парсим загруженные данные в профили
        return self._parse_profiles(profiles_data)

    def _load_plaintext_config(self, config_path: str) -> dict:
        """Load profiles from a plain JSON file.

        Args:
            config_path (str): Path to the JSON config file.

        Returns:
            dict: Raw profile data or an empty dict on failure.
        """
        if not os.path.exists(config_path):
            logger.warning(f"Config file not found: {config_path}")
            return {}

        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file: {e}")
            return {}
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {}

    def _load_encrypted_config(self, config_path: str) -> dict:
        """Load and decrypt profiles from an encrypted config file.

        Args:
            config_path (str): Path to the encrypted config file.

        Returns:
            dict: Raw decrypted profile data.
        """
        if not os.path.exists(config_path):
            self._exit_error(f"Encrypted config file not found: {config_path}")

        try:
            with open(config_path, 'rb') as f:
                file_data = f.read()

            # Первые 16 байт - это salt
            if len(file_data) < 16:
                self._exit_error("Encrypted file is corrupted (too short)")

            salt = file_data[:16]
            encrypted_data = file_data[16:]

            # Запрашиваем пароль
            password = getpass.getpass("Enter decryption password: ")

            # Генерируем ключ (параметры ДОЛЖНЫ совпадать с encryptor.py)
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
                backend=default_backend()
            )
            key = base64.urlsafe_b64encode(kdf.derive(password.encode('utf-8')))

            f = Fernet(key)

            # Расшифровываем
            try:
                decrypted_data = f.decrypt(encrypted_data)
                return json.loads(decrypted_data.decode('utf-8'))
            except Exception:
                self._exit_error("Decryption failed! Invalid password or corrupted file.")

        except Exception as e:
            self._exit_error(f"Error reading encrypted config: {e}")

    def _load_from_vault(self) -> dict:
        """Load profiles from HashiCorp Vault.

        Returns:
            dict: Raw profile data returned by Vault.
        """
        if not HAS_HVAC:
            self._exit_error("Vault mode requires the 'hvac' package to be installed")

        vault_url = self.args.vault_url
        vault_path = self.args.vault_path

        if not vault_path:
            self._exit_error("--vault-path is required when using Vault mode")

        # Получаем учетные данные
        username = self.args.vault_user
        if not username:
            username = input("Vault username: ")

        password = self.args.vault_pass
        if not password:
            password = getpass.getpass("Vault password: ")

        try:
            # Подключаемся к Vault
            client = hvac.Client(url=vault_url)

            # Аутентификация (userpass метод)
            client.auth.userpass.login(
                username=username,
                password=password
            )

            if not client.is_authenticated():
                self._exit_error("Vault authentication failed")

            # Читаем секрет из KV v2
            # Для KV v2 путь должен содержать /data/
            secret_response = client.secrets.kv.v2.read_secret_version(
                path=vault_path.replace('secret/data/', '').replace('secret/', '')
            )

            # Извлекаем данные
            profiles_data = secret_response['data']['data']
            return profiles_data

        except Exception as e:
            self._exit_error(f"Failed to load from Vault: {e}")

    def _parse_profiles(self, data: dict) -> Dict[str, ConnectionProfile]:
        """Normalize raw profile data into ConnectionProfile objects.

        Args:
            data (dict): Raw profile mapping loaded from the selected source.

        Returns:
            Dict[str, ConnectionProfile]: Parsed profile objects keyed by name.
        """
        profiles = {}

        for name, config in data.items():
            # Подготавливаем конфиг для ConnectionProfile
            clean_config = {}

            # Базовые параметры
            base_params = ['name', 'host', 'port', 'password', 'username',
                           'ssl', 'ssl_verify', 'socket_path', 'readonly', 'ssl_ca_certs',
                           'ssl_certfile', 'ssl_keyfile']
            for param in base_params:
                if param in config:
                    clean_config[param] = config[param]

            # Параметры кластера
            if 'cluster_mode' in config:
                clean_config['cluster_mode'] = bool(config['cluster_mode'])

            # Обработка cluster_nodes
            if 'cluster_nodes' in config:
                nodes = config['cluster_nodes']
                if isinstance(nodes, list):
                    formatted_nodes = []
                    for node in nodes:
                        if isinstance(node, str):
                            # Формат "host:port"
                            if ':' in node:
                                host, port_str = node.split(':', 1)
                                try:
                                    port = int(port_str)
                                    formatted_nodes.append((host, port))
                                except ValueError:
                                    logger.warning(f"Invalid port in node {node}")
                        elif isinstance(node, dict):
                            # Формат {"host": "...", "port": ...}
                            host = node.get('host', 'localhost')
                            port = node.get('port', 6379)
                            try:
                                port = int(port)
                                formatted_nodes.append((host, port))
                            except ValueError:
                                logger.warning(f"Invalid port in node {node}")
                        elif isinstance(node, list) and len(node) == 2:
                            # Формат ["host", port]
                            host, port = node[0], node[1]
                            try:
                                port = int(port)
                                formatted_nodes.append((str(host), port))
                            except ValueError:
                                logger.warning(f"Invalid port in node {node}")
                    if formatted_nodes:
                        clean_config['cluster_nodes'] = formatted_nodes

            # Создаем профиль
            try:
                # Убедимся, что есть host и port
                if 'host' not in clean_config:
                    if 'cluster_nodes' in clean_config and clean_config['cluster_nodes']:
                        clean_config['host'] = clean_config['cluster_nodes'][0][0]
                        clean_config['port'] = clean_config['cluster_nodes'][0][1]
                    else:
                        clean_config['host'] = 'localhost'
                        clean_config['port'] = 6379

                profiles[name] = ConnectionProfile(**clean_config)
            except TypeError as e:
                logger.error(f"Invalid config for profile '{name}': {e}")
                # Пробуем создать профиль с минимальными параметрами
                minimal_config = {
                    'name': name,
                    'host': config.get('host', 'localhost'),
                    'port': config.get('port', 6379),
                    'cluster_mode': config.get('cluster_mode', False)
                }
                if 'password' in config:
                    minimal_config['password'] = config['password']
                profiles[name] = ConnectionProfile(**minimal_config)

        # Профиль по умолчанию, если данных нет
        if not profiles:
            profiles['localhost'] = ConnectionProfile('localhost', 'localhost', 6379)

        return profiles

    def _exit_error(self, message: str):
        """Print a fatal error message, log it, and terminate the process."""
        print(f"ERROR: {message}", file=sys.stderr)
        logger.error(message)
        sys.exit(1)

    def validate_profiles(self):
        """Validate loaded profiles and downgrade invalid cluster entries."""
        for name, profile in self.profiles.items():
            if profile.cluster_mode:
                # Проверка узлов кластера
                if not hasattr(profile, 'cluster_nodes') or not profile.cluster_nodes:
                    logger.warning(f"Profile '{name}' has cluster_mode=True but no cluster_nodes defined")
                    profile.cluster_mode = False  # Отключаем режим кластера

                # Проверка формата узлов
                for i, node in enumerate(profile.cluster_nodes):
                    if not isinstance(node, (list, tuple)) or len(node) != 2:
                        logger.error(f"Profile '{name}': Invalid node format at index {i}: {node}")
                    elif not isinstance(node[0], str) or not isinstance(node[1], int):
                        logger.error(f"Profile '{name}': Node {i} should be (host:str, port:int)")

            # Проверка обязательных полей для standalone
            elif not profile.socket_path and (not profile.host or not profile.port):
                logger.error(f"Profile '{name}' missing host/port for standalone connection")

        logger.info(f"Validated {len(self.profiles)} profiles")

    def create_toolbar(self):
        """Build the top toolbar row with the main action buttons."""
        return urwid.Columns([
            ('weight', 20, urwid.AttrMap(self.help_btn, 'button', 'button_focus')),
            ('pack', urwid.Text(' ')),
            ('weight', 20, urwid.AttrMap(self.commands_btn, 'button', 'button_focus')),
            ('pack', urwid.Text(' ')),
            ('weight', 20, urwid.AttrMap(self.add_key_btn, 'button', 'button_focus')),
            ('pack', urwid.Text(' ')),
            ('weight', 20, urwid.AttrMap(self.edit_btn, 'button', 'button_focus')),
            ('weight', 1, urwid.Text('')),
            ('weight', 20, urwid.AttrMap(self.delete_btn, 'button', 'button_focus')),
            ('weight', 1, urwid.Text('')),
            ('weight', 20, urwid.AttrMap(self.disconnect_btn, 'button', 'button_focus')),
            ('pack', urwid.Text(' ')),
            ('weight', 20, urwid.AttrMap(self.exit_btn, 'button', 'button_focus')),
            ('pack', urwid.Text(' ')),
            ('weight', 20, urwid.AttrMap(self.refresh_btn, 'button', 'button_focus')),
        ], dividechars=0)

    def create_ui(self):
        """Construct the main TUI layout, panels, and console widgets."""
        # Header
        header_text = urwid.Text([
            ('header_text', ' ● '),
            ('header_text', 'Redis Commander TUI')
        ], align='left')

        self.header = urwid.AttrMap(
            urwid.Padding(header_text, left=1, right=1),
            'header'
        )

        # Toolbar
        self.copy_btn = urwid.Button('F5: Copy')
        self.move_btn = urwid.Button('F6: Move')
        self.refresh_btn = urwid.Button('F11: Refresh')
        self.delete_btn = urwid.Button('F8: Delete')
        self.commands_btn = urwid.Button('F2: Commands')
        self.add_key_btn = urwid.Button('F3: Add New Key')
        self.edit_btn = urwid.Button('F4: Edit')
        self.disconnect_btn = urwid.Button('F9: Disconnect')
        self.help_btn = urwid.Button('F1: Help')
        self.exit_btn = urwid.Button('F10: Exit')

        urwid.connect_signal(self.refresh_btn, 'click', lambda b: self.refresh_keys())
        urwid.connect_signal(self.delete_btn, 'click', lambda b: self.delete_marked_keys())
        urwid.connect_signal(self.commands_btn, 'click', lambda b: self.toggle_console())
        urwid.connect_signal(self.edit_btn, 'click', lambda b: self.edit_key_wrapper())
        urwid.connect_signal(self.add_key_btn, 'click', lambda b: self.show_add_key_dialog())
        urwid.connect_signal(self.disconnect_btn, 'click', lambda b: self.disconnect())
        urwid.connect_signal(self.exit_btn, 'click', lambda b: self.exit_main())
        urwid.connect_signal(self.help_btn, 'click', lambda b: self.show_help())

        # Используем метод для создания toolbar
        toolbar = self.create_toolbar()

        # Левая панель: КОМБИНИРОВАННАЯ (серверы/DB вверху + ключи внизу)
        # Дерево подключений и баз данных
        self.connection_tree = urwid.SimpleListWalker([])
        self.connection_listbox = urwid.ListBox(self.connection_tree)

        # Объединяем: дерево подключений + список ключей, КАЖДЫЙ со своим scrollbar
        self.left_panel_pile = urwid.Pile([
            ('weight', 1, ScrollBar(urwid.AttrMap(self.connection_listbox, 'sidebar'))),  # ✅ Scrollbar для дерева
            ('pack', urwid.Divider('─')),
            ('weight', 4, ScrollBar(self.key_list_view.listbox)),  # ✅ Scrollbar для ключей
        ])

        # НЕ оборачиваем Pile в ScrollBar, т.к. scrollbar'ы уже внутри
        self.left_panel = urwid.LineBox(
            self.left_panel_pile,
            title='Connections & Keys'
        )

        # Правая панель
        self.detail_walker = urwid.SimpleListWalker([])
        self.detail_listbox = urwid.ListBox(self.detail_walker)

        self.right_panel = urwid.LineBox(
            ScrollBar(urwid.AttrMap(self.detail_listbox, 'background')),  # ✅ Оборачиваем
            title='Details'
        )

        # Основная область
        content = urwid.Columns([
            ('weight', 1, self.left_panel),
            ('weight', 1, self.right_panel),
        ], dividechars=1)

        # Tabs
        self.tab_text = urwid.Text('')

        # Статус
        self.status_text = urwid.Text('Ready')

        # Консоль - история команд и результатов
        self.console_history = urwid.SimpleListWalker([])
        self.console_listbox = urwid.ListBox(self.console_history)

        # Промпт для ввода команды
        self.command_prompt = urwid.Edit('redis> ')

        # Оборачиваем промпт для перехвата клавиш
        self.command_prompt_wrapper = CommandPromptWrapper(
            self.command_prompt,
            on_up=lambda: self.navigate_history('up'),
            on_down=lambda: self.navigate_history('down'),
            on_enter=self.execute_console_command
        )
        # Панель консоли
        console_pile = urwid.Pile([
            ('weight', 1, ScrollBar(urwid.AttrMap(self.console_listbox, 'background'))),  # ✅ Оборачиваем
            ('pack', urwid.Divider('─')),
            ('pack', urwid.AttrMap(self.command_prompt_wrapper, 'footer')),
        ])

        console_pile.focus_position = 2  # Фокус на промпт по умолчанию

        self.console_panel = urwid.LineBox(
            console_pile,
            title='Redis Console'
        )

        # Основное тело с возможностью переключения
        self.main_content = urwid.Pile([
            ('pack', urwid.AttrMap(urwid.Padding(toolbar, left=1, right=1), 'background')),
            ('weight', 1, urwid.AttrMap(content, 'background')),
        ])

        # Footer pile
        self.footer_pile = urwid.Pile([
            urwid.AttrMap(self.tab_text, 'footer'),
            urwid.AttrMap(self.status_text, 'footer'),
        ])

        self.main_frame = urwid.Frame(
            header=self.header,
            body=self.main_content,
            footer=self.footer_pile
        )

        # Устанавливаем начальный фокус на body (не на header с кнопками)
        self.main_frame.focus_position = 'body'

        # Устанавливаем фокус на content_area (пропускаем toolbar)
        self.main_content.focus_position = 1  # 0=toolbar, 1=content_area

        self.update_connection_tree()

    def toggle_console(self):
        """Open or close the bottom console pane."""
        if self.console_mode:
            # Выключаем консоль - показываем основной контент
            self.main_content.contents = [
                (urwid.AttrMap(urwid.Padding(
                    self.create_toolbar(),  # ✅ Используем метод
                    left=1, right=1), 'background'), ('pack', None)),
                (urwid.AttrMap(urwid.Columns([
                    ('weight', 1, self.left_panel),  # ✅ Используем left_panel с деревом+ключами
                    ('weight', 1, self.right_panel),  # ✅ Используем right_panel
                ], dividechars=1), 'background'), ('weight', 1)),
            ]
            self.console_mode = False
            self.set_status("Console closed")
        else:
            # Включаем консоль - разделяем экран
            content_area = urwid.Columns([
                ('weight', 1, self.left_panel),  # ✅ Используем left_panel с деревом+ключами
                ('weight', 1, self.right_panel),  # ✅ Используем right_panel
            ], dividechars=1)

            self.main_content.contents = [
                (urwid.AttrMap(urwid.Padding(
                    self.create_toolbar(),  # ✅ Используем метод
                    left=1, right=1), 'background'), ('pack', None)),
                (urwid.AttrMap(content_area, 'background'), ('weight', 2)),
                (urwid.AttrMap(self.console_panel, 'background'), ('weight', 1)),
            ]
            self.console_mode = True

            # Добавляем приветственное сообщение если консоль пустая
            if len(self.console_history) == 0:
                self.console_history.append(
                    urwid.Text(('success',
                                'Redis Console - Type commands and press Enter. Type "exit" or press Esc to close.'))
                )
                self.console_history.append(urwid.Divider())

            # Устанавливаем фокус на консольную панель
            self.main_content.focus_position = 2
            # Фокус на промпт внутри консольной панели
            self.console_panel.original_widget.focus_position = 2
            self.set_status("Console opened (press F2 to close)")

    def execute_console_command(self):
        """Execute the current console command, including cluster-specific logic."""
        if not self.current_connection or not self.current_connection.connected:
            self.console_history.append(urwid.Text(('error', '✗ Not connected to Redis')))
            self.console_history.append(urwid.Divider())
            if len(self.console_history) > 0:
                self.console_listbox.set_focus(len(self.console_history) - 1)
            return

        command = self.command_prompt.get_edit_text().strip()
        if not command:
            return

        # Добавляем команду в историю СРАЗУ
        self.add_to_history(command)

        # Проверяем на команду выхода
        if command.lower() in ['exit', 'quit']:
            self.toggle_console()
            return

        # Команда очистки консоли
        if command.lower() == 'clear':
            self.console_history.clear()
            self.command_prompt.set_edit_text('')
            return

        # Добавляем команду в визуальную историю консоли
        self.console_history.append(urwid.Text(('info_label', f'redis> {command}')))

        try:
            client = self.current_connection.active_client
            is_cluster = self.current_connection.is_cluster

            # Разбираем команду
            parts = command.split()
            cmd = parts[0].upper()
            args = parts[1:]

            # Специальная обработка для кластера
            if is_cluster:
                # Команды, которые не поддерживаются в кластере
                unsupported_commands = ['SELECT', 'MOVE', 'SWAPDB', 'MIGRATE', 'RANDOMKEY']
                if cmd in unsupported_commands:
                    self.console_history.append(
                        urwid.Text(('error', f"(error) Command '{cmd}' not supported in Redis Cluster"))
                    )
                    self.console_history.append(urwid.Divider())
                    self.command_prompt.set_edit_text('')
                    # Прокручиваем вниз
                    if len(self.console_history) > 0:
                        self.console_listbox.set_focus(len(self.console_history) - 1)
                    return

                # Команды кластера (начинающиеся с CLUSTER)
                if cmd.startswith('CLUSTER'):
                    result = client.execute_command(cmd, *args)

                # KEYS - требует сканирования всех узлов
                elif cmd == 'KEYS':
                    pattern = args[0] if args else '*'
                    keys = []

                    # проверяем наличие метода get_nodes
                    if hasattr(client, 'get_nodes'):
                        for node in client.get_nodes():
                            # Проверяем атрибут server_type безопасно
                            if hasattr(node, 'server_type') and node.server_type == 'master':
                                try:
                                    node_keys = node.keys(pattern)
                                    keys.extend(node_keys)
                                except Exception as e:
                                    logger.warning(f"Error getting keys from node: {e}")
                    else:
                        # Fallback для других реализаций кластера
                        keys = list(client.scan_iter(match=pattern, count=100))

                    result = keys

                # SCAN - требует обработки всех узлов
                elif cmd == 'SCAN':
                    cursor = int(args[0]) if args and args[0].isdigit() else 0
                    pattern = args[2] if len(args) > 2 and args[1].upper() == 'MATCH' else '*'
                    count = int(args[4]) if len(args) > 4 and args[3].upper() == 'COUNT' else 10

                    all_keys = []

                    if hasattr(client, 'get_nodes'):
                        for node in client.get_nodes():
                            if hasattr(node, 'server_type') and node.server_type == 'master':
                                try:
                                    node_cursor = 0
                                    while True:
                                        node_cursor, batch = node.scan(node_cursor, match=pattern, count=count)
                                        all_keys.extend(batch)
                                        if node_cursor == 0:
                                            break
                                except Exception as e:
                                    logger.warning(f"Error scanning node: {e}")
                    else:
                        # Fallback
                        for key in client.scan_iter(match=pattern, count=count):
                            all_keys.append(key)
                            if len(all_keys) >= count:
                                break

                    result = [0, all_keys[:count]] if all_keys else [0, []]

                # INFO - собираем информацию со всех узлов
                elif cmd == 'INFO':
                    if args and args[0].lower() in ['cluster', 'keyspace', 'memory', 'cpu', 'stats', 'replication']:
                        section = args[0].lower()
                        info_results = []

                        if hasattr(client, 'get_nodes'):
                            for node in client.get_nodes():
                                if hasattr(node, 'server_type') and node.server_type == 'master':
                                    try:
                                        node_info = node.info(section)
                                        node_host = getattr(node, 'host', 'unknown')
                                        node_port = getattr(node, 'port', 'unknown')
                                        info_results.append(f"--- {node_host}:{node_port} ---")

                                        # Форматируем словарь
                                        if isinstance(node_info, dict):
                                            for k, v in node_info.items():
                                                info_results.append(f"{k}: {v}")
                                        else:
                                            info_results.append(str(node_info))
                                    except Exception as e:
                                        info_results.append(f"--- ERROR: {e} ---")
                            result = "\n".join(info_results)
                        else:
                            result = client.info(section)
                    else:
                        result = client.info()

                # DBSIZE - суммируем по всем узлам
                elif cmd == 'DBSIZE':
                    total = 0

                    if hasattr(client, 'get_nodes'):
                        for node in client.get_nodes():
                            if hasattr(node, 'server_type') and node.server_type == 'master':
                                try:
                                    total += node.dbsize()
                                except Exception as e:
                                    logger.warning(f"Error getting dbsize from node: {e}")
                    else:
                        total = client.dbsize()

                    result = total

                # FLUSHALL/FLUSHDB - выполняем на всех узлах
                elif cmd in ['FLUSHALL', 'FLUSHDB']:
                    results = []

                    if hasattr(client, 'get_nodes'):
                        for node in client.get_nodes():
                            if hasattr(node, 'server_type') and node.server_type == 'master':
                                try:
                                    node_result = node.execute_command(cmd, *args)
                                    node_host = getattr(node, 'host', 'unknown')
                                    node_port = getattr(node, 'port', 'unknown')
                                    results.append(f"{node_host}:{node_port}: {node_result}")
                                except Exception as e:
                                    results.append(f"ERROR: {e}")
                        result = "\n".join(results) if results else "OK"
                    else:
                        result = client.execute_command(cmd, *args)

                # Обычные команды - выполняем через клиент кластера
                else:
                    result = client.execute_command(cmd, *args)

            else:
                # Standalone режим
                result = client.execute_command(cmd, *args)

            # Форматируем результат
            result_str = self.format_redis_result(cmd, result)

            # Добавляем результат в историю
            if '\n' in result_str:
                for line in result_str.split('\n'):
                    self.console_history.append(urwid.Text(('success', line)))
            else:
                self.console_history.append(urwid.Text(('success', result_str)))

            logger.info(f"Executed: {command} => {result_str[:100]}")

            # ✅ Очищаем промпт
            self.command_prompt.set_edit_text('')

            # Обновляем если команда могла изменить данные
            modifying_commands = [
                'SET', 'DEL', 'HSET', 'HDEL', 'LPUSH', 'RPUSH', 'LPOP', 'RPOP',
                'SADD', 'SREM', 'ZADD', 'ZREM', 'FLUSHDB', 'FLUSHALL', 'RENAME',
                'RENAMENX', 'EXPIRE', 'EXPIREAT', 'PERSIST', 'APPEND', 'DECR',
                'INCR', 'LSET', 'LTRIM', 'MSET', 'PEXPIRE', 'PEXPIREAT', 'PSETEX',
                'SETEX', 'SETNX', 'HSETNX', 'HMSET', 'LPUSHX', 'RPUSHX', 'SINTERSTORE',
                'SUNIONSTORE', 'SDIFFSTORE', 'ZINTERSTORE', 'ZUNIONSTORE', 'ZREMRANGEBYLEX',
                'ZREMRANGEBYRANK', 'ZREMRANGEBYSCORE', 'GETSET', 'UNLINK'
            ]

            if cmd in modifying_commands:
                # ✅ Обновляем асинхронно чтобы не блокировать консоль
                try:
                    self.refresh_keys()
                except Exception as e:
                    logger.error(f"Failed to refresh keys: {e}")

        except RedisClusterError as e:
            # Cluster-специфичные ошибки
            error_msg = str(e)
            if 'CROSSSLOT' in error_msg:
                self.console_history.append(urwid.Text(('error',
                                                        f'(error) CROSSSLOT: Keys must be in same hash slot for multi-key operations')))
            elif 'MOVED' in error_msg:
                self.console_history.append(urwid.Text(('error',
                                                        f'(error) MOVED: Key moved to another node')))
            else:
                self.console_history.append(urwid.Text(('error', f'(cluster error) {error_msg}')))
            logger.error(f"Redis cluster error: {e}")

        except RedisConnectionError as e:
            # Ошибки подключения
            self.console_history.append(urwid.Text(('error', f'(connection error) {str(e)}')))
            logger.error(f"Redis connection error: {e}")

        except RedisError as e:
            # Общие Redis ошибки (включая ResponseError)
            self.console_history.append(urwid.Text(('error', f'(error) {str(e)}')))
            logger.error(f"Redis error: {e}")

        except Exception as e:
            # Все остальные ошибки
            self.console_history.append(urwid.Text(('error', f'(error) {str(e)}')))
            logger.error(f"Command failed: {e}", exc_info=True)

        # Разделитель
        self.console_history.append(urwid.Divider())

        # Прокручиваем вниз к последней команде
        if len(self.console_history) > 0:
            try:
                self.console_listbox.set_focus(len(self.console_history) - 1)
            except:
                pass

        # Ограничиваем историю (визуальную в консоли)
        if len(self.console_history) > 200:
            self.console_history[:] = self.console_history[-150:]

    def format_redis_result(self, cmd: str, result) -> str:
        """Форматирование результата Redis команды"""

        if cmd == 'PING':
            if result is True or result == b'PONG' or result == 'PONG':
                return 'PONG'
            elif isinstance(result, bytes):
                return result.decode('utf-8', errors='replace')
            elif isinstance(result, str):
                return result
            else:
                return 'PONG'

        elif result is None:
            return '(nil)'

        elif isinstance(result, bool):
            return 'OK' if result else '(error)'

        elif isinstance(result, int):
            return f'(integer) {result}'

        elif isinstance(result, bytes):
            try:
                decoded = result.decode('utf-8', errors='replace')
                return f'"{decoded}"'
            except:
                return f'<bytes: {len(result)}>'

        elif isinstance(result, str):
            return f'"{result}"'

        elif isinstance(result, list):
            if len(result) == 0:
                return '(empty list or set)'
            else:
                result_lines = []
                for i, item in enumerate(result[:50]):  # Ограничиваем 50 элементами
                    if isinstance(item, bytes):
                        try:
                            decoded = item.decode("utf-8", errors="replace")
                            result_lines.append(f'{i + 1}) "{decoded}"')
                        except:
                            result_lines.append(f'{i + 1}) <bytes: {len(item)}>')
                    elif isinstance(item, str):
                        result_lines.append(f'{i + 1}) "{item}"')
                    elif isinstance(item, list):
                        # Вложенные списки (например, SCAN)
                        if i == 0 and len(item) == 2 and isinstance(item[0], int):
                            # Это результат SCAN
                            result_lines.append(f'{i + 1}) 1) "{item[0]}"')
                            result_lines.append(f'   2) {self.format_redis_result("SCAN", item[1])}')
                        else:
                            result_lines.append(f'{i + 1}) {item}')
                    else:
                        result_lines.append(f'{i + 1}) {str(item)}')

                if len(result) > 50:
                    result_lines.append(f'... and {len(result) - 50} more items')

                return '\n'.join(result_lines)

        elif isinstance(result, dict):
            if len(result) == 0:
                return '(empty hash)'
            else:
                result_lines = []
                for i, (key, value) in enumerate(list(result.items())[:50]):
                    if isinstance(key, bytes):
                        try:
                            key = key.decode("utf-8", errors="replace")
                        except:
                            key = f'<bytes: {len(key)}>'

                    if isinstance(value, bytes):
                        try:
                            value = value.decode("utf-8", errors="replace")
                        except:
                            value = f'<bytes: {len(value)}>'

                    result_lines.append(f'{i + 1}) "{key}" => "{value}"')

                if len(result) > 50:
                    result_lines.append(f'... and {len(result) - 50} more fields')

                return '\n'.join(result_lines)

        else:
            return str(result)

    def update_connection_tree(self):
        """Обновление дерева подключений (БЕЗ ключей - они в key_list_view)"""
        self.connection_tree[:] = []

        for name, profile in self.profiles.items():
            conn = self.connections.get(name)

            if conn and conn.connected:
                icon = '● '
                if conn.is_cluster:
                    server_label = f"{icon}{profile.name} [CLUSTER]"
                else:
                    server_label = f"{icon}{profile.name} ({profile.host}:{profile.port})"
                style = 'connection'
            else:
                icon = '○ '
                server_label = f"{icon}{profile.name} ({profile.host}:{profile.port})"
                style = 'connection'

            # Заголовок сервера
            server_btn = urwid.Button(server_label, on_press=self.on_server_select, user_data=name)

            if conn and conn.connected:
                self.connection_tree.append(urwid.AttrMap(server_btn, style, 'connection_selected'))
            else:
                self.connection_tree.append(urwid.AttrMap(server_btn, style, 'sidebar_focus'))

            # Показываем базы данных для подключенного сервера
            if conn and conn.connected:
                if conn.is_cluster:
                    # Кластер - только DB0
                    try:
                        is_current = (self.current_connection == conn)
                        db_icon = '  ▪ ' if is_current else '  ▫ '
                        db_label = f"{db_icon}DB0 ({self.key_count} keys)"

                        db_btn = urwid.Button(db_label, on_press=self.on_db_select,
                                              user_data=(name, 0))

                        if is_current:
                            item = urwid.AttrMap(db_btn, 'connection_selected', 'connection_selected')
                        else:
                            item = urwid.AttrMap(db_btn, 'key_folder', 'key_folder_focus')

                        self.connection_tree.append(item)
                    except Exception as e:
                        logger.error(f"Error displaying cluster DB: {e}")
                else:
                    # Standalone
                    db_key_counts = self.get_all_db_key_counts(conn)

                    for db_num in range(self.max_db + 1):
                        is_current = (self.current_connection == conn and
                                      conn.current_db == db_num)

                        db_icon = '  ▪ ' if is_current else '  ▫ '
                        key_count = db_key_counts.get(db_num, 0)

                        if key_count > 0:
                            db_label = f"{db_icon}DB{db_num} ({key_count} keys)"
                        else:
                            db_label = f"{db_icon}DB{db_num}"

                        db_btn = urwid.Button(db_label, on_press=self.on_db_select,
                                              user_data=(name, db_num))

                        if is_current:
                            item = urwid.AttrMap(db_btn, 'connection_selected', 'connection_selected')
                        else:
                            item = urwid.AttrMap(db_btn, 'key_folder', 'key_folder_focus')

                        self.connection_tree.append(item)

        self.update_tabs()

    def get_all_db_key_counts(self, conn: RedisConnection) -> Dict[int, int]:
        """Получить количество ключей во всех базах данных"""
        db_counts = {}

        if not conn or not conn.connected:
            return db_counts

        # Для кластера возвращаем только DB0
        if conn.is_cluster:
            try:
                # В кластере можно получить общее количество ключей через DBSIZE
                total_keys = conn.active_client.dbsize()

                if total_keys > 0:
                    db_counts[0] = total_keys
            except Exception as e:
                logger.error(f"Failed to get cluster key count: {e}")
            return db_counts

        # Сохраняем текущую БД
        current_db = conn.current_db

        try:
            # Проходим по всем БД и считаем ключи
            for db_num in range(self.max_db + 1):
                try:
                    conn.client.select(db_num)
                    count = conn.client.dbsize()
                    if count > 0:
                        db_counts[db_num] = count
                except Exception as e:
                    logger.error(f"Failed to get key count for DB{db_num}: {e}")
                    continue

            # Возвращаемся к текущей БД
            conn.client.select(current_db)

        except Exception as e:
            logger.error(f"Failed to get all DB key counts: {e}")
            # Если что-то пошло не так, возвращаемся к текущей БД
            try:
                conn.client.select(current_db)
            except:
                pass

        return db_counts

    def on_server_select(self, button, profile_name: str):
        """Выбор сервера (подключение/переподключение)"""
        profile = self.profiles.get(profile_name)
        if not profile:
            return

        # Если уже подключены к этому серверу
        if profile_name in self.connections and self.connections[profile_name].connected:
            self.current_connection = self.connections[profile_name]
            self.set_status(f"Already connected to {profile.name}", 'success')
        else:
            self.connect_to_profile(profile)

        self.refresh_keys()
        self.update_connection_tree()

    def parse_cluster_info(self, info_string: str) -> dict:
        """Parse ``CLUSTER INFO`` output into a dictionary.

        Args:
            info_string (str): Raw command output as text or bytes.

        Returns:
            dict: Parsed key-value pairs from the cluster info payload.
        """
        result = {}

        if isinstance(info_string, bytes):
            info_string = info_string.decode('utf-8')

        for line in info_string.strip().split('\n'):
            line = line.strip()
            if ':' in line:
                key, value = line.split(':', 1)
                result[key] = value

        return result

    def connect_to_profile(self, profile: ConnectionProfile):
        """Connect to a profile or reuse an existing compatible connection.

        Args:
            profile (ConnectionProfile): Target profile to activate.
        """
        if profile.cluster_mode:
            # Для кластера используем первый узел для идентификации
            if profile.cluster_nodes:
                first_node = profile.cluster_nodes[0]
                server_key = f"cluster:{first_node[0]}:{first_node[1]}"
            else:
                server_key = f"cluster:{profile.host}:{profile.port}"
        else:
            # Для standalone
            if profile.socket_path:
                server_key = f"unix:{profile.socket_path}"
            else:
                server_key = f"{profile.host}:{profile.port}"

        # Проверяем существующие подключения
        for existing_name, conn in self.connections.items():
            if conn.connected:
                # Сравниваем параметры подключения
                if (conn.is_cluster == profile.cluster_mode and
                        conn.profile.host == profile.host and
                        conn.profile.port == profile.port):

                    # Для кластера дополнительно проверяем узлы
                    if profile.cluster_mode:
                        if (hasattr(conn.profile, 'cluster_nodes') and
                                conn.profile.cluster_nodes == profile.cluster_nodes):
                            self.current_connection = conn
                            self.set_status(f"Already connected to cluster {server_key}", 'success')
                            self.refresh_keys()
                            return
                    else:
                        self.current_connection = conn
                        self.set_status(f"Already connected to {server_key}", 'success')
                        self.refresh_keys()
                        return

        # Создаем новое подключение
        conn = RedisConnection(profile)
        success, message = conn.connect()

        if success:
            self.connections[profile.name] = conn
            self.current_connection = conn

            if profile.cluster_mode:
                try:
                    # Получаем информацию о кластере
                    cluster_info_str = conn.active_client.execute_command('CLUSTER', 'INFO')

                    # парсим строку в словарь
                    cluster_info = self.parse_cluster_info(cluster_info_str)

                    state = cluster_info.get('cluster_state', 'unknown')
                    node_count = cluster_info.get('cluster_known_nodes', '?')

                    self.set_status(
                        f"Connected to Redis Cluster (state: {state}, nodes: {node_count})",
                        'success'
                    )
                except Exception as e:
                    logger.error(f"Could not get cluster info: {e}")
                    # Fallback - просто показываем что подключились к кластеру
                    node_count = len(profile.cluster_nodes) if hasattr(profile, 'cluster_nodes') else 1
                    self.set_status(f"Connected to Redis Cluster ({node_count} nodes)", 'success')
            else:
                self.set_status(f"Connected to {profile.host}:{profile.port}", 'success')

            self.refresh_keys()
        else:
            self.set_status(f"Connection failed: {message}", 'error')

    def on_db_select(self, button, data: Tuple[str, int]):
        """Handle database selection from the connection tree.

        Args:
            button: Urwid callback argument for the pressed button.
            data (Tuple[str, int]): Profile name and selected database index.
        """
        profile_name, db_num = data

        conn = self.connections.get(profile_name)
        if not conn or not conn.connected:
            return

        # Для кластера разрешаем только DB0
        if conn.is_cluster:
            if db_num != 0:
                self.set_status("Redis Cluster supports only DB0", 'warning')
                return
            # В кластере нет команды SELECT, просто обновляем интерфейс
            self.current_connection = conn
            self.set_status("Redis Cluster (DB0)", 'success')
            self.refresh_keys()
            self.update_connection_tree()
        else:
            # Standalone Redis
            self.current_connection = conn
            success, message = conn.select_db(db_num)

            if success:
                self.set_status(f"Switched to DB{db_num}", 'success')
                self.refresh_keys()
                self.update_connection_tree()
            else:
                self.set_status(f"Failed to switch DB: {message}", 'error')

    def disconnect(self):
        """Disconnect the current profile and clear active UI state."""
        if self.current_connection:
            name = self.current_connection.profile.name
            self.current_connection.disconnect()
            del self.connections[name]
            self.current_connection = None
            self.set_status(f"Disconnected from {name}", 'success')
            self.keys_list = []   # self.key_tree_root = KeyTreeNode('root')
            self.update_connection_tree()

    def exit_main(self):
        raise urwid.ExitMainLoop()

    def get_cluster_master_nodes(self, client):
        """Return master nodes from a cluster client using several fallbacks.

        Args:
            client: Cluster-capable Redis client.

        Returns:
            list: Master node handles that can be scanned directly.
        """
        try:
            # Способ 1: совместимый с redis-py-cluster >= 2.1.0
            if hasattr(client, 'get_primaries'):
                return client.get_primaries()

            # Способ 2: для более старых версий
            elif hasattr(client, 'get_nodes'):
                nodes = client.get_nodes()
                master_nodes = []
                for node in nodes:
                    # Проверяем разными способами
                    if hasattr(node, 'server_type'):
                        if node.server_type == 'master':
                            master_nodes.append(node)
                    elif hasattr(node, 'redis_connection'):
                        # Попробуем получить информацию через команду
                        try:
                            info = node.execute_command('ROLE')
                            if info and isinstance(info, list) and info[0] == 'master':
                                master_nodes.append(node)
                        except:
                            continue

                return master_nodes

            # Способ 3: через CLUSTER NODES
            else:
                try:
                    nodes_info = client.execute_command('CLUSTER NODES')
                    if isinstance(nodes_info, bytes):
                        nodes_info = nodes_info.decode('utf-8')

                    master_nodes = []
                    for line in nodes_info.split('\n'):
                        if line.strip() and 'master' in line and 'myself' not in line:
                            # Парсим адрес ноды
                            parts = line.split()
                            if len(parts) > 1:
                                addr = parts[1]
                                if ':' in addr:
                                    host, port = addr.split(':')
                                    # Создаем подключение к ноде
                                    node_client = redis.Redis(
                                        host=host,
                                        port=int(port),
                                        decode_responses=False,
                                        socket_timeout=2
                                    )
                                    master_nodes.append(node_client)

                    return master_nodes
                except:
                    return []

        except Exception as e:
            logger.error(f"Failed to get master nodes: {e}")
            return []

    def refresh_keys(self):
        """Scan the active database or cluster and refresh the key list."""
        if not self.current_connection or not self.current_connection.connected:
            self.key_list_view.set_keys([])
            return

        try:
            client = self.current_connection.active_client
            logger.info(f"Client type: {type(client)}")
            logger.info(f"Is cluster: {self.current_connection.is_cluster}")

            self.set_status("Scanning keys...")

            if self.current_connection.is_cluster:
                logger.info("Starting cluster scan...")
                keys_with_types = self._scan_cluster_keys_with_types_iter(client)
                logger.info(f"Cluster scan returned {len(keys_with_types)} keys")
            else:
                # Standalone - сканируем с типами
                logger.info("Starting standalone scan...")
                keys_with_types = []
                scan_count = 0
                max_keys = 5000

                logger.info("Using scan_iter...")
                for key in client.scan_iter(match='*', count=100):
                    if scan_count >= max_keys:
                        break

                    try:
                        key_type = client.type(key)
                        if isinstance(key_type, bytes):
                            key_type = key_type.decode('utf-8')
                        keys_with_types.append((key, key_type))
                    except:
                        keys_with_types.append((key, 'unknown'))

                    scan_count += 1

                    if scan_count % 100 == 0:
                        self.set_status(f"Scanning... {scan_count} keys")
                logger.info(f"Standalone scan complete: {len(keys_with_types)} keys")

            # Показываем первые несколько для проверки
            if keys_with_types:
                logger.info(f"First 3 keys: {keys_with_types[:3]}")
            else:
                logger.warning("No keys found!")

            logger.info("Setting keys to key_list_view...")
            self.key_list_view.set_keys(keys_with_types)
            logger.info("Keys set successfully")

            self.key_count = len(keys_with_types)
            # Обновляем дерево подключений (чтобы показать количество ключей)
            self.update_connection_tree()

            self.set_status(f"Loaded {len(keys_with_types)} keys", 'success')

            if self.current_key:
                self.display_key_details(self.current_key)
            logger.info("=== refresh_keys completed ===")

        except Exception as e:
            self.set_status(f"Error loading keys: {e}", 'error')
            logger.error(f"Error loading keys: {e}")

    def on_key_select(self, key: bytes):
        """Store the selected key and update the details pane.

        Args:
            key (bytes): Selected Redis key.
        """
        logger.info(f"UI: Key selected: {key}")

        if not self.current_connection or not self.current_connection.connected:
            return

        self.current_key = key
        self.display_key_details(self.current_key)

    def get_selected_key(self) -> Optional[bytes]:
        """Return the currently focused key from the key list.

        Returns:
            Optional[bytes]: Selected key or None.
        """
        return self.key_list_view.get_focused_key()

    def _scan_cluster_keys_with_types_iter(self, client) -> List[Tuple[bytes, str]]:
        """Scan cluster master nodes and resolve key types.

        Args:
            client: Cluster-capable Redis client.

        Returns:
            List[Tuple[bytes, str]]: Unique keys paired with detected Redis types.
        """
        keys_with_types = []
        self.key_count = 0

        try:
            max_keys = 5000  # Ограничение для производительности
            scan_count = 0

            # Проверяем, является ли это кластером
            if client.is_cluster and client.cluster_nodes:

                # Фильтруем только мастера из cluster_nodes_info
                master_nodes = {
                    node_id: pool
                    for node_id, pool in client.cluster_nodes.items()
                    if node_id in client.cluster_nodes_info
                       and client.cluster_nodes_info[node_id].role == 'master'
                }
                # Сканируем КАЖДЫЙ мастер узел в кластере
                logger.info(f"Scanning {len(master_nodes)} master nodes...")

                # logger.info(f"Scanning {len(client.cluster_nodes)} cluster nodes...")
                for node_id, node_conn in master_nodes.items():
                    logger.info(f"Scanning master node: {node_id}")
                # for node_id, node_conn in client.cluster_nodes.items():
                #    logger.info(f"Scanning node: {node_id}")

                    try:
                        # SCAN на конкретном узле
                        cursor = 0
                        node_keys_count = 0

                        while True:
                            # Выполняем SCAN напрямую на узле
                            cursor, keys = self._scan_node(node_conn, cursor, match='*', count=100)

                            for key in keys:
                                if scan_count >= max_keys:
                                    break

                                try:
                                    # Получаем тип ключа (через правильный узел!)
                                    key_type = client.type(key)
                                    if isinstance(key_type, bytes):
                                        key_type = key_type.decode('utf-8')
                                    keys_with_types.append((key, key_type))
                                except Exception as e:
                                    logger.info(f"Error getting type for key {key}: {e}")
                                    keys_with_types.append((key, 'unknown'))

                                scan_count += 1
                                node_keys_count += 1
                                self.key_count += 1

                                # Обновляем статус каждые 100 ключей
                                if scan_count % 100 == 0:
                                    self.set_status(f"Scanning... Found {scan_count} keys")

                            if cursor == 0 or scan_count >= max_keys:
                                break

                        logger.info(f"Node {node_id}: found {node_keys_count} keys")

                        if scan_count >= max_keys:
                            self.set_status(f"Stopped at {max_keys} keys (limit reached)")
                            break

                    except Exception as e:
                        logger.error(f"Error scanning node {node_id}: {e}")
                        continue

            else:
                # ✅ Standalone режим - обычный scan_iter
                if hasattr(client, 'scan_iter'):
                    for key in client.scan_iter(match='*', count=100):
                        if scan_count >= max_keys:
                            self.set_status(f"Stopped at {max_keys} keys (limit reached)")
                            break

                        try:
                            key_type = client.type(key)
                            if isinstance(key_type, bytes):
                                key_type = key_type.decode('utf-8')
                            keys_with_types.append((key, key_type))
                        except:
                            keys_with_types.append((key, 'unknown'))

                        scan_count += 1
                        self.key_count += 1

                        if scan_count % 100 == 0:
                            self.set_status(f"Scanning... Found {scan_count} keys")
                else:
                    # Fallback для старых версий
                    cursor = 0
                    while True:
                        cursor, keys = client.scan(cursor, match='*', count=100)
                        for key in keys:
                            if scan_count >= max_keys:
                                break
                            try:
                                key_type = client.type(key)
                                if isinstance(key_type, bytes):
                                    key_type = key_type.decode('utf-8')
                                keys_with_types.append((key, key_type))
                            except:
                                keys_with_types.append((key, 'unknown'))
                            scan_count += 1
                            self.key_count += 1

                        if cursor == 0 or scan_count >= max_keys:
                            break

        except Exception as e:
            logger.error(f"Error processing keys: {e}", exc_info=True)

        # Убираем дубликаты
        seen = set()
        unique_keys = []
        for key, key_type in keys_with_types:
            if key not in seen:
                seen.add(key)
                unique_keys.append((key, key_type))

        logger.info(f"Scanned {len(unique_keys)} unique keys with types")
        return unique_keys

    def _scan_node(self, node_conn, cursor: int, match: str, count: int):
        """Execute a SCAN command directly against one cluster node.

        Args:
            node_conn: Node-level Redis connection.
            cursor (int): Scan cursor to continue from.
            match (str): Match pattern for SCAN.
            count (int): Requested batch size.

        Returns:
            Tuple[int, list]: Next cursor and keys returned by the node.
        """
        args = ['SCAN', cursor]

        if match:
            args.extend(['MATCH', match])
        if count:
            args.extend(['COUNT', count])

        response = node_conn.execute_command(*args)

        new_cursor = int(response[0])
        keys = response[1]

        return new_cursor, keys

    def display_key_details(self, key: bytes):
        """Render metadata and value details for the selected key.

        Args:
            key (bytes): Redis key to inspect.
        """
        if not self.current_connection or not self.current_connection.connected:
            return

        try:
            client = self.current_connection.active_client
            key_str = key.decode('utf-8', errors='replace')

            # Получаем основную информацию о ключе
            try:
                key_type = client.type(key)
                if isinstance(key_type, bytes):
                    key_type = key_type.decode('utf-8')
                ttl = client.ttl(key)
            except Exception as e:
                self.set_status(f"Error getting key info: {e}", 'error')
                return

            # Начинаем формировать вывод
            self.detail_walker[:] = []

            self.detail_walker.append(urwid.Text([
                ('info_label', 'Key: '),
                ('info_value', key_str)
            ]))
            self.detail_walker.append(urwid.Divider())

            # Основная информация (для кластера и standalone)
            info_items = [
                ('Type', key_type),
                ('TTL', f"{ttl}s" if ttl > 0 else 'No expiration' if ttl == -1 else 'N/A'),
            ]

            # Дополнительная информация для кластера
            if self.current_connection.is_cluster:
                try:
                    slot = client.keyslot(key_str)
                    node = client.get_node_from_key(key_str)
                    info_items.extend([
                        ('Hash Slot', str(slot)),
                        ('Node', f"{node.host}:{node.port}"),
                    ])
                except Exception as e:
                    logger.warning(f"Could not get cluster info for key: {e}")

            # Добавляем информацию о размере в зависимости от типа
            try:
                if key_type == 'string':
                    size = client.strlen(key)
                    info_items.append(('Size', f"{size} bytes"))
                elif key_type == 'list':
                    size = client.llen(key)
                    info_items.append(('Length', str(size)))
                elif key_type == 'set':
                    size = client.scard(key)
                    info_items.append(('Members', str(size)))
                elif key_type == 'zset':
                    size = client.zcard(key)
                    info_items.append(('Members', str(size)))
                elif key_type == 'hash':
                    size = client.hlen(key)
                    info_items.append(('Fields', str(size)))
            except Exception as e:
                logger.warning(f"Could not get size info for key: {e}")

            # Отображаем всю информацию
            for label, value in info_items:
                row = urwid.Columns([
                    ('weight', 1, urwid.AttrMap(urwid.Text(label), 'info_label')),
                    ('weight', 2, urwid.AttrMap(urwid.Text(str(value)), 'info_value')),
                ])
                self.detail_walker.append(row)

            self.detail_walker.append(urwid.Divider())
            self.detail_walker.append(urwid.Text(('info_label', 'Value:')))
            self.detail_walker.append(urwid.Divider())

            # Отображение значения в зависимости от типа ключа
            # Этот код общий для cluster и standalone
            try:
                if key_type == 'string':
                    value = client.get(key)
                    if value is None:
                        self.detail_walker.append(urwid.Text(('error', 'Key not found')))
                    else:
                        try:
                            value_str = value.decode('utf-8', errors='replace')
                            self.detail_walker.append(urwid.Text(value_str))
                        except:
                            self.detail_walker.append(
                                urwid.Text(f'<binary data: {len(value)} bytes>')
                            )

                elif key_type == 'hash':
                    hash_data = client.hgetall(key)
                    if not hash_data:
                        self.detail_walker.append(urwid.Text('(empty hash)'))
                    else:
                        for field, val in hash_data.items():
                            try:
                                field_str = field.decode('utf-8', errors='replace')
                                val_str = val.decode('utf-8', errors='replace')
                                row = urwid.Columns([
                                    ('weight', 1, urwid.Text(('key_folder', field_str))),
                                    ('weight', 2, urwid.Text(val_str)),
                                ])
                                self.detail_walker.append(row)
                            except:
                                # Для бинарных данных
                                row = urwid.Columns([
                                    ('weight', 1, urwid.Text(('key_folder', '<binary field>'))),
                                    ('weight', 2, urwid.Text(f'<binary: {len(val)} bytes>')),
                                ])
                                self.detail_walker.append(row)

                elif key_type == 'list':
                    # Получаем только первые 100 элементов
                    list_data = client.lrange(key, 0, 99)
                    if not list_data:
                        self.detail_walker.append(urwid.Text('(empty list)'))
                    else:
                        for idx, item in enumerate(list_data):
                            try:
                                item_str = item.decode('utf-8', errors='replace')
                                self.detail_walker.append(urwid.Text(f"[{idx}] {item_str}"))
                            except:
                                self.detail_walker.append(
                                    urwid.Text(f"[{idx}] <binary: {len(item)} bytes>")
                                )

                elif key_type == 'set':
                    # Получаем только первые 100 элементов
                    set_data = list(client.smembers(key))[:100]
                    if not set_data:
                        self.detail_walker.append(urwid.Text('(empty set)'))
                    else:
                        for item in set_data:
                            try:
                                item_str = item.decode('utf-8', errors='replace')
                                self.detail_walker.append(urwid.Text(f"• {item_str}"))
                            except:
                                self.detail_walker.append(
                                    urwid.Text(f"• <binary: {len(item)} bytes>")
                                )

                elif key_type == 'zset':
                    # Получаем только первые 100 элементов
                    zset_data = client.zrange(key, 0, 99, withscores=True)
                    if not zset_data:
                        self.detail_walker.append(urwid.Text('(empty zset)'))
                    else:
                        for member, score in zset_data:
                            try:
                                member_str = member.decode('utf-8', errors='replace')
                                row = urwid.Columns([
                                    ('weight', 2, urwid.Text(member_str)),
                                    ('weight', 1, urwid.Text(('info_value', str(score)))),
                                ])
                                self.detail_walker.append(row)
                            except:
                                row = urwid.Columns([
                                    ('weight', 2, urwid.Text(f'<binary: {len(member)} bytes>')),
                                    ('weight', 1, urwid.Text(('info_value', str(score)))),
                                ])
                                self.detail_walker.append(row)

                else:
                    self.detail_walker.append(urwid.Text(f'Unknown key type: {key_type}'))

            except Exception as e:
                self.detail_walker.append(
                    urwid.Text(('error', f'Error retrieving value: {str(e)}'))
                )
                logger.error(f"Error retrieving value for key {key_str}: {e}")

            # Кнопки действий (удаление и т.д.)
            if not self.current_connection.profile.readonly:
                self.detail_walker.append(urwid.Divider())
                delete_btn = urwid.Button('Delete Key', on_press=lambda b: self.delete_key(key))
                self.detail_walker.append(
                    urwid.AttrMap(delete_btn, 'button_danger', 'button_focus')
                )

            # Если это кластер, добавляем информацию о миграции
            if self.current_connection.is_cluster:
                try:
                    # Проверяем, не мигрируется ли ключ
                    cluster_info = client.cluster('KEYSLOT', key_str)
                    if isinstance(cluster_info, bytes):
                        cluster_info = cluster_info.decode('utf-8')
                    self.detail_walker.append(urwid.Divider())
                    self.detail_walker.append(
                        urwid.Text(('info_label', f'Cluster Slot: {cluster_info}'))
                    )
                except:
                    pass

        except Exception as e:
            self.set_status(f"Error displaying key: {e}", 'error')
            logger.error(f"Error displaying key {key}: {e}")
            # Показываем ошибку в деталях
            self.detail_walker[:] = [
                urwid.Text(('error', f'Error: {str(e)}'))
            ]

    def debug_cluster_info(self):
        """Collect diagnostic information about the active cluster client."""
        if not self.current_connection or not self.current_connection.is_cluster:
            return

        try:
            client = self.current_connection.active_client

            # Выводим всю доступную информацию
            info = {}

            # 1. Проверяем методы клиента
            info['client_methods'] = [m for m in dir(client) if not m.startswith('_')]

            # 2. Пробуем получить информацию о нодах
            try:
                if hasattr(client, 'get_nodes'):
                    nodes = client.get_nodes()
                    info['nodes_count'] = len(nodes)
                    info['nodes_info'] = []
                    for i, node in enumerate(nodes[:3]):  # Первые 3 ноды
                        node_info = {
                            'index': i,
                            'type': str(type(node)),
                            'attrs': [a for a in dir(node) if not a.startswith('_')]
                        }
                        # Пробуем получить адрес
                        if hasattr(node, 'host') and hasattr(node, 'port'):
                            node_info['address'] = f"{node.host}:{node.port}"
                        info['nodes_info'].append(node_info)
            except Exception as e:
                info['nodes_error'] = str(e)

            # 3. Пробуем CLUSTER INFO
            try:
                cluster_info = client.cluster_info()
                info['cluster_info'] = cluster_info
            except Exception as e:
                info['cluster_info_error'] = str(e)

            # 4. Пробуем ROLE
            try:
                role = client.execute_command('ROLE')
                info['role'] = role
            except Exception as e:
                info['role_error'] = str(e)

            # Записываем в лог
            logger.info(f"Cluster debug info: {json.dumps(info, default=str, indent=2)}")

            # Показываем в UI
            self.detail_walker[:] = []
            self.detail_walker.append(urwid.Text(('header_text', 'Cluster Debug Info:')))
            self.detail_walker.append(urwid.Divider())

            for key, value in info.items():
                if isinstance(value, (list, dict)):
                    value_str = json.dumps(value, default=str, indent=2)
                else:
                    value_str = str(value)

                self.detail_walker.append(urwid.Text(f"{key}: {value_str}"))
                self.detail_walker.append(urwid.Divider())

        except Exception as e:
            logger.error(f"Debug failed: {e}")

    def delete_key(self, key: bytes):
        """Delete a single Redis key and refresh the UI.

        Args:
            key (bytes): Redis key to delete.
        """
        if not self.current_connection:
            return

        try:
            client = self.current_connection.active_client
            deleted = client.delete(key)

            if deleted:
                self.set_status(f"Key deleted", 'success')
            else:
                self.set_status(f"Key not found", 'warning')

            self.refresh_keys()
            self.detail_walker[:] = []

        except Exception as e:
            self.set_status(f"Delete failed: {e}", 'error')
            logger.error(f"Delete failed for key {key}: {e}")

    def update_tabs(self):
        """Refresh footer tabs with the current connection summary."""
        tabs = []

        if self.current_connection and self.current_connection.connected:
            name = self.current_connection.profile.name

            if self.current_connection.is_cluster:
                # Для кластера показываем специальную иконку
                try:
                    # Получаем информацию о кластере
                    cluster_info_str = conn.active_client.execute_command('CLUSTER', 'INFO')
                    cluster_info = self.parse_cluster_info(cluster_info_str)
                    state = cluster_info.get('cluster_state', 'unknown')
 
                    if state == 'ok':
                        icon = '🟢'
                        style = 'success'
                    else:
                        icon = '🔴'
                        style = 'error'

                    label = f" {icon} {name} [Cluster] "
                    tabs.append((style, label))

                except Exception as e:
                    label = f" ⚠ {name} [Cluster] "
                    tabs.append(('warning', label))
            else:
                db = self.current_connection.current_db
                label = f" ● {name} [DB{db}] "
                tabs.append(('tab_active', label))

        self.tab_text.set_text(tabs if tabs else '')

    def _find_listbox_in_ui(self):
        """Locate the primary ListBox inside the current UI tree."""
        try:
            # Проверяем main_frame
            if hasattr(self, 'main_frame'):
                body = self.main_frame.get_body()
                listbox = self._find_listbox_recursive(body)
                if listbox:
                    return listbox

            # Проверяем columns (двухпанельный интерфейс)
            if hasattr(self, 'columns'):
                for widget, options in self.columns.contents:
                    listbox = self._find_listbox_recursive(widget)
                    if listbox:
                        return listbox

            # Проверяем content_frame (если есть)
            if hasattr(self, 'content_frame'):
                body = self.content_frame.get_body()
                listbox = self._find_listbox_recursive(body)
                if listbox:
                    return listbox

            # Проверяем key_view (если есть)
            if hasattr(self, 'key_view'):
                listbox = self._find_listbox_recursive(self.key_view)
                if listbox:
                    return listbox

            return None

        except Exception as e:
            logger.error(f"Error finding listbox: {e}")
            return None

    def _find_listbox_recursive(self, widget, depth=0):
        """Recursively search for a nested ListBox in a widget tree."""
        if depth > 10:
            return None

        if widget is None:
            return None

        # Нашли ListBox
        if isinstance(widget, urwid.ListBox):
            return widget

        # Разворачиваем обертки
        if hasattr(widget, 'original_widget'):
            return self._find_listbox_recursive(widget.original_widget, depth + 1)

        if hasattr(widget, 'base_widget'):
            return self._find_listbox_recursive(widget.base_widget, depth + 1)

        # Frame - проверяем body
        if isinstance(widget, urwid.Frame):
            body = widget.get_body()
            if body:
                result = self._find_listbox_recursive(body, depth + 1)
                if result:
                    return result

        # Columns/Pile - проверяем contents
        if hasattr(widget, 'contents'):
            for item in widget.contents:
                if isinstance(item, tuple) and len(item) > 0:
                    result = self._find_listbox_recursive(item[0], depth + 1)
                    if result:
                        return result

        return None

    def show_edit_key_dialog(self, key):
        """Open the edit dialog for an existing key.

        Args:
            key: Redis key name to edit.
        """

        def on_success():
            self.refresh_keys()

        dialog = AddKeyDialog(
            self.current_connection,
            on_success,
            edit_key=key  # Передаем ключ для редактирования
        )

        urwid.connect_signal(dialog, 'close', lambda w: self.close_dialog(None))

        overlay = urwid.Overlay(
            dialog,
            self.main_frame,
            align='center',
            width=('relative', 80),
            valign='middle',
            height=('relative', 80)
        )

        self.dialog = overlay
        self.loop.widget = overlay

    def show_add_key_dialog(self):
        """Open the dialog for creating a new key."""
        if not self.current_connection or not self.current_connection.connected:
            self.set_status("Not connected", 'error')
            return

        dialog = AddKeyDialog(self.current_connection, self.on_key_added)
        urwid.connect_signal(dialog, 'close', self.close_dialog)

        overlay = urwid.Overlay(
            dialog,
            self.main_frame,
            align='center',
            width=('relative', 60),
            valign='middle',
            height=('relative', 70)
        )

        self.loop.widget = overlay

    def show_mark_pattern_dialog(self):
        """Open the dialog used to bulk-mark keys by pattern."""
        edit = urwid.Edit('Pattern (* and ? wildcards): ')

        def on_mark(button):
            pattern = edit.get_edit_text().strip()
            if pattern:
                self.key_list_view.mark_by_pattern(pattern)
                marked_count = len(self.key_list_view.get_marked_keys())
                self.set_status(f"Marked {marked_count} keys by pattern: {pattern}", 'success')
            self.close_dialog(None)

        def on_cancel(button):
            self.close_dialog(None)

        mark_btn = urwid.Button('Mark', on_press=on_mark)
        cancel_btn = urwid.Button('Cancel', on_press=on_cancel)

        pile = urwid.Pile([
            urwid.Text('Enter pattern to mark keys:'),
            urwid.Text('Examples: user:*, session:?123, *temp*'),
            urwid.Divider(),
            edit,
            urwid.Divider(),
            urwid.Columns([
                urwid.AttrMap(mark_btn, 'button', 'button_focus'),
                urwid.AttrMap(cancel_btn, 'button', 'button_focus'),
            ]),
        ])

        dialog = urwid.LineBox(urwid.Filler(pile), title='Mark Keys by Pattern')

        overlay = urwid.Overlay(
            urwid.AttrMap(dialog, 'dialog'),
            self.main_frame,
            align='center',
            width=('relative', 50),
            valign='middle',
            height=('relative', 30)
        )

        self.loop.widget = overlay

    def show_filter_dialog(self):
        """Open the dialog used to filter the loaded key list."""
        edit = urwid.Edit('Filter pattern (* for wildcard): ')

        def on_apply(button):
            pattern = edit.get_edit_text().strip()
            if pattern:
                self.key_list_view.apply_filter(pattern)
                self.set_status(f"Filter applied: {pattern}")
            else:
                self.key_list_view.apply_filter(None)
                self.set_status("Filter cleared")
            self.close_dialog(None)

        def on_clear(button):
            self.key_list_view.apply_filter(None)
            self.set_status("Filter cleared")
            self.close_dialog(None)

        apply_btn = urwid.Button('Apply', on_press=on_apply)
        clear_btn = urwid.Button('Clear', on_press=on_clear)
        cancel_btn = urwid.Button('Cancel', on_press=lambda button: self.close_dialog(button))

        pile = urwid.Pile([
            edit,
            urwid.Divider(),
            urwid.Columns([
                urwid.AttrMap(apply_btn, 'button', 'button_focus'),
                urwid.AttrMap(clear_btn, 'button', 'button_focus'),
                urwid.AttrMap(cancel_btn, 'button', 'button_focus'),
            ]),
        ])

        dialog = urwid.LineBox(urwid.Filler(pile), title='Filter Keys')

        overlay = urwid.Overlay(
            urwid.AttrMap(dialog, 'dialog'),
            self.main_frame,
            align='center',
            width=('relative', 50),
            valign='middle',
            height=('relative', 20)
        )

        self.loop.widget = overlay

    def delete_marked_keys(self):
        """Delete all currently marked keys after confirmation."""
        marked = self.key_list_view.get_marked_keys()

        if not marked:
            self.set_status("No keys marked", 'warning')
            return

        # Подтверждение
        def on_confirm(button):
            try:
                client = self.current_connection.active_client
                deleted = 0
                for key in marked:
                    try:
                        if client.delete(key):
                            deleted += 1
                    except:
                        pass

                self.set_status(f"Deleted {deleted}/{len(marked)} keys", 'success')
                self.key_list_view.unmark_all()
                self.refresh_keys()
                self.close_dialog(None)
            except Exception as e:
                self.set_status(f"Delete failed: {e}", 'error')

        text = urwid.Text(f"Delete {len(marked)} marked keys?")
        yes_btn = urwid.Button('Yes', on_press=on_confirm)
        no_btn = urwid.Button('No', on_press=lambda button: self.close_dialog(button))

        pile = urwid.Pile([
            text,
            urwid.Divider(),
            urwid.Columns([
                urwid.AttrMap(yes_btn, 'button_danger', 'button_focus'),
                urwid.AttrMap(no_btn, 'button', 'button_focus'),
            ]),
        ])

        dialog = urwid.LineBox(urwid.Filler(pile), title='Confirm Delete')

        overlay = urwid.Overlay(
            urwid.AttrMap(dialog, 'dialog'),
            self.main_frame,
            align='center',
            width=('relative', 40),
            valign='middle',
            height=('relative', 20)
        )

        self.loop.widget = overlay

    def on_key_added(self):
        """Handle a successful add-key workflow and refresh the UI."""
        self.set_status("Key added successfully", 'success')
        self.refresh_keys()

    def close_dialog(self, button):
        """Close an overlay dialog and restore the main frame.

        Args:
            button: Urwid callback argument or None.
        """
        self.loop.widget = self.main_frame

    def set_status(self, message: str, style: str = 'footer'):
        """Update the footer status line.

        Args:
            message (str): Status text shown to the user.
            style (str): Palette entry used to render the status line.
        """
        self.status_text.set_text((style, f" {message}"))

    def edit_key_wrapper(self):
        if not self.console_mode:
            selected_key = self.get_selected_key()
            if selected_key:
                # Декодируем если bytes
                if isinstance(selected_key, bytes):
                    selected_key = selected_key.decode('utf-8', errors='replace')
                self.show_edit_key_dialog(selected_key)
            else:
                self.set_status("No key selected")

    def handle_input(self, key):
        """Dispatch global keyboard shortcuts for the main UI."""
        # ✅ Tab - переключение между панелями
        if key == 'tab':
            try:
                # Получаем текущую content_area (это Columns)
                content_widget = self.main_content.contents[1][0].original_widget

                if isinstance(content_widget, urwid.Columns):
                    current_focus = content_widget.focus_position
                    num_columns = len(content_widget.contents)

                    # Переключаем на следующую колонку по кругу
                    next_focus = (current_focus + 1) % num_columns
                    content_widget.focus_position = next_focus

                    # Обновляем статус
                    panel_names = ['Connections & Keys', 'Details', 'Console']
                    if next_focus < len(panel_names):
                        self.set_status(f"Focus: {panel_names[next_focus]}")

                    return True
            except Exception as e:
                logger.error(f"Failed to switch panel: {e}")

        if key == 'f10':
            raise urwid.ExitMainLoop()
        elif key in ('q', 'Q'):
            if not self.console_mode:
                raise urwid.ExitMainLoop()
        elif key == 'f11':
            if not self.console_mode:
                self.refresh_keys()
        elif key == 'f1' or key == '?':
            self.show_help()
        elif key == 'f2':
            self.toggle_console()
        elif key == 'esc':
            if self.console_mode:
                self.toggle_console()
        elif key == 'f3':
            if not self.console_mode:
                self.show_add_key_dialog()
        elif key == 'f9':
            if not self.console_mode:
                self.disconnect()
        elif key == 'f4':
            self.edit_key_wrapper()
        elif key == 'f12':
            if not self.console_mode:
                self.debug_cluster_info()
        elif key == 'enter':
            if self.console_mode:
                # Проверяем, находимся ли мы в промпте
                try:
                    if self.main_content.focus_position == 2:  # Консольная панель
                        console_pile = self.console_panel.original_widget
                        if console_pile.focus_position == 2:  # Промпт
                            self.execute_console_command()
                except:
                    pass

        if key == ' ':  # Пробел - отметить/снять отметку
            self.key_list_view.toggle_mark_focused()
            return

        elif key == 'ctrl a':  # Ctrl+A - отметить все
            pattern = self.show_mark_pattern_dialog()
            if pattern:
                self.key_list_view.mark_by_pattern(pattern)
            return

        elif key == 'ctrl u':  # Ctrl+U - снять все отметки
            self.key_list_view.unmark_all()
            self.set_status("All marks cleared")
            return

        elif key == '/':  # Фильтр
            self.show_filter_dialog()
            return

        elif key == 'f8':  # Массовое удаление отмеченных
            self.delete_marked_keys()
            return

    def show_help(self):
        """Render the built-in help text in the details pane."""
        help_text = """
Redis Commander TUI - Help

Hotkeys:
  q, Q       - Quit (when not in console mode)
  ?, F1      - This help
  F2         - Toggle console mode
  F3         - New key
  F4         - Edit key
  F8         - Delete Marked key
  F9         - Disconnect
  F10        - Quit
  F11        - Refresh keys

  /          - Key Filter
  Ctrl+a     - mark keys
  Ctrl+u     - unmark keys 
  Enter      - View key
             

Console Mode:
  Type Redis commands and press Enter
  Examples: GET key, SET key value, KEYS *

  Page up   - prev command from history
  Page down - next command from history
"""
        self.detail_walker[:] = []
        for line in help_text.split('\n'):
            self.detail_walker.append(urwid.Text(line))

    def run(self):
        """Start the Urwid main loop."""
        self.loop.run()


def main():
    """Parse CLI arguments, create the app, and start the UI loop."""
    parser = argparse.ArgumentParser(description=f'REDIS Commander - TUI Manager for REDIS ; Version {__version__}')

    # Режим 1: Обычный файл (по умолчанию)
    parser.add_argument('-c', '--config', default='redis_profiles.json', help='Path to plaintext config file (default: redis_profiles.json)')
    # Режим 2: Зашифрованный файл
    parser.add_argument('-e', '--encrypted-config',  help='Path to encrypted config file (Mode 2). Requires password.')
    # Режим 3: Vault
    parser.add_argument('--vault-url', help='Vault URL (e.g. http://127.0.0.1:8200). Activates Mode 3.')
    parser.add_argument('--vault-path', help='Path to secret in Vault (e.g. secret/data/redis-commander)')
    parser.add_argument('--vault-user', help='Vault username (will prompt if missing)')
    parser.add_argument('--vault-pass', help='Vault password (will prompt if missing - INSECURE in history)')

    args = parser.parse_args()
    print(f'REDIS Commander - TUI Manager for REDIS ; Version {__version__}')

    try:
        app = RedisCommanderUI(args)
        app.run()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        logger.exception("Fatal error")
        sys.exit(1)


if __name__ == '__main__':
    main()
