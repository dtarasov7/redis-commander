"""
Simple Redis Client 

"""

__version__ = "2.3.0"
__author__ = "Dmitry Tarasov"

__all__ = [
    'RedisClient',
    'RedisConnection', 
    'RedisPipeline',
    'RedisError',
    'RedisConnectionError',
    'RedisClusterError',
    'ClusterNode',
    'ConnectionPool'
]

import socket
import ssl as ssl_module
import time
import random
import logging
import hashlib
from typing import Optional, List, Dict, Any, Tuple, Union, Iterator, Callable
from collections import defaultdict
from threading import Lock

# Настройка логирования для библиотеки (best practice)
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# ============ Exceptions ============

class RedisError(Exception):
    """Базовая ошибка Redis"""
    pass


class RedisConnectionError(RedisError):
    """Ошибка подключения"""
    pass


class RedisClusterError(RedisError):
    """Ошибка кластера"""
    pass


# ============ Buffered Socket ============

class BufferedSocket:
    """Буферизованное чтение из сокета для ускорения I/O"""

    def __init__(self, sock: socket.socket, buffer_size: int = 8192):
        self._sock = sock
        self._buffer = b''
        self._buffer_size = buffer_size

    def recv(self, n: int) -> bytes:
        """Прочитать ровно n байт из буфера"""
        while len(self._buffer) < n:
            chunk = self._sock.recv(self._buffer_size)
            if not chunk:
                raise RedisConnectionError("Connection closed")
            self._buffer += chunk

        result = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return result

    def recv_line(self) -> bytes:
        """Прочитать строку до \r\n"""
        while b'\r\n' not in self._buffer:
            chunk = self._sock.recv(self._buffer_size)
            if not chunk:
                raise RedisConnectionError("Connection closed")
            self._buffer += chunk

        idx = self._buffer.index(b'\r\n')
        line = self._buffer[:idx]
        self._buffer = self._buffer[idx + 2:]
        return line

    def sendall(self, data: bytes):
        """Отправить данные"""
        return self._sock.sendall(data)

    def close(self):
        """Закрыть сокет"""
        try:
            self._sock.close()
        except:
            pass


# ============ RESP Parser ============

class RESPParser:
    """Парсер RESP протокола с буферизацией"""

    @staticmethod
    def encode_command(*args) -> bytes:
        """Кодирование команды в RESP формат"""
        parts = [f'*{len(args)}\r\n'.encode()]
        for arg in args:
            if isinstance(arg, bytes):
                data = arg
            elif isinstance(arg, str):
                data = arg.encode('utf-8')
            elif isinstance(arg, (int, float)):
                data = str(arg).encode('utf-8')
            else:
                data = str(arg).encode('utf-8')

            parts.append(f'${len(data)}\r\n'.encode())
            parts.append(data)
            parts.append(b'\r\n')

        return b''.join(parts)

    @staticmethod
    def encode_commands(commands: List[Tuple]) -> bytes:
        """Кодирование нескольких команд в один буфер (для pipeline)"""
        return b''.join(RESPParser.encode_command(*cmd) for cmd in commands)

    @staticmethod
    def decode_response(sock: BufferedSocket) -> Any:
        """Декодирование ответа RESP"""
        line = sock.recv_line()
        if not line:
            raise RedisConnectionError("Connection closed")

        prefix = chr(line[0])
        data = line[1:]

        if prefix == '+':
            return data.decode('utf-8', errors='replace')
        elif prefix == '-':
            error_msg = data.decode('utf-8', errors='replace')
            raise RedisError(error_msg)
        elif prefix == ':':
            return int(data)
        elif prefix == '$':
            length = int(data)
            if length == -1:
                return None
            bulk_data = sock.recv(length)
            sock.recv(2)  # \r\n
            return bulk_data
        elif prefix == '*':
            count = int(data)
            if count == -1:
                return None
            return [RESPParser.decode_response(sock) for _ in range(count)]
        else:
            raise RedisError(f"Unknown RESP prefix: {prefix}")


# ============ Connection ============

class RedisConnection:
    """Подключение к одному Redis узлу с auto-reconnect"""

    def __init__(self, host: str = 'localhost', port: int = 6379,
                 password: Optional[str] = None, username: Optional[str] = None,
                 db: int = 0, socket_timeout: int = 5,
                 ssl: bool = False, ssl_ca_certs: Optional[str] = None,
                 ssl_certfile: Optional[str] = None, ssl_keyfile: Optional[str] = None,
                 ssl_check_hostname: bool = True, ssl_verify: bool = True,
                 max_reconnect_attempts: int = 3, reconnect_backoff_base: float = 0.1,
                 reconnect_backoff_max: float = 5.0,
                 on_connect: Optional[Callable] = None,
                 on_disconnect: Optional[Callable] = None,
                 on_reconnect: Optional[Callable] = None):

        self.host = host
        self.port = port
        self.password = password
        self.username = username
        self.db = db
        self.socket_timeout = socket_timeout
        self.ssl = ssl
        self.ssl_ca_certs = ssl_ca_certs
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.ssl_check_hostname = ssl_check_hostname
        self.ssl_verify = ssl_verify

        # Reconnect параметры
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_backoff_base = reconnect_backoff_base
        self.reconnect_backoff_max = reconnect_backoff_max

        # Callbacks
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.on_reconnect = on_reconnect

        self.sock: Optional[BufferedSocket] = None
        self._is_connected = False
        self._connect()

    @property
    def is_connected(self) -> bool:
        """Return whether the underlying socket is currently usable."""
        return self._is_connected and self.sock is not None

    def _connect(self):
        """Установка соединения"""
        try:
            raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_sock.settimeout(self.socket_timeout)
            raw_sock.connect((self.host, self.port))

            if self.ssl:
                context = ssl_module.create_default_context()

                if self.ssl_ca_certs:
                    context.load_verify_locations(cafile=self.ssl_ca_certs)

                if self.ssl_certfile and self.ssl_keyfile:
                    context.load_cert_chain(certfile=self.ssl_certfile, keyfile=self.ssl_keyfile)

                if not self.ssl_verify:
                    context.check_hostname = False
                    context.verify_mode = ssl_module.CERT_NONE
                elif not self.ssl_check_hostname:
                    context.check_hostname = False

                raw_sock = context.wrap_socket(raw_sock, server_hostname=self.host)

            self.sock = BufferedSocket(raw_sock)

            # Аутентификация
            if self.password:
                if self.username:
                    self._execute_command_internal('AUTH', self.username, self.password)
                else:
                    self._execute_command_internal('AUTH', self.password)

            # Выбор БД
            if self.db != 0:
                self._execute_command_internal('SELECT', self.db)

            self._is_connected = True
            logger.info(f"Connected to {self.host}:{self.port} (db={self.db})")

            if self.on_connect:
                self.on_connect(self)

        except Exception as e:
            if self.sock:
                self.sock.close()
                self.sock = None
            self._is_connected = False
            raise RedisConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")

    def _reconnect(self) -> bool:
        """Попытка переподключения с экспоненциальным backoff"""
        for attempt in range(self.max_reconnect_attempts):
            try:
                backoff = min(
                    self.reconnect_backoff_base * (2 ** attempt),
                    self.reconnect_backoff_max
                )

                if attempt > 0:
                    logger.info(f"Reconnect attempt {attempt + 1}/{self.max_reconnect_attempts} "
                              f"to {self.host}:{self.port} after {backoff:.2f}s")
                    time.sleep(backoff)

                self._connect()

                if self.on_reconnect:
                    self.on_reconnect(self, attempt + 1)

                return True

            except Exception as e:
                logger.warning(f"Reconnect attempt {attempt + 1} failed: {e}")

        return False

    def _execute_command_internal(self, *args) -> Any:
        """Внутреннее выполнение команды без retry"""
        if not self.sock:
            raise RedisConnectionError("Not connected")

        command = RESPParser.encode_command(*args)
        self.sock.sendall(command)
        return RESPParser.decode_response(self.sock)

    def execute_command(self, *args, retry: bool = True) -> Any:
        """Выполнение команды с auto-reconnect"""
        if not self._is_connected:
            raise RedisConnectionError("Not connected")

        try:
            return self._execute_command_internal(*args)

        except RedisError:
            raise

        except Exception as e:
            logger.error(f"Command failed: {e}")
            self._is_connected = False

            if self.on_disconnect:
                self.on_disconnect(self)

            if self.sock:
                self.sock.close()
                self.sock = None

            if retry:
                if self._reconnect():
                    return self.execute_command(*args, retry=False)

            raise RedisConnectionError(f"Command failed: {e}")

    def execute_pipeline(self, commands: List[Tuple]) -> List[Any]:
        """Выполнение пакета команд через один сокет"""
        if not self._is_connected or not self.sock:
            raise RedisConnectionError("Not connected")

        try:
            # Отправляем все команды одним sendall
            encoded = RESPParser.encode_commands(commands)
            self.sock.sendall(encoded)

            # Читаем все ответы
            results = []
            for _ in commands:
                try:
                    results.append(RESPParser.decode_response(self.sock))
                except RedisError as e:
                    results.append(e)

            return results

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self._is_connected = False

            if self.on_disconnect:
                self.on_disconnect(self)

            if self.sock:
                self.sock.close()
                self.sock = None

            raise RedisConnectionError(f"Pipeline failed: {e}")

    def is_healthy(self) -> bool:
        """Health check соединения"""
        try:
            return self.ping()
        except:
            return False

    def ping(self) -> bool:
        """PING проверка"""
        try:
            response = self.execute_command('PING', retry=False)
            return response in ('PONG', b'PONG')
        except:
            return False

    def close(self):
        """Закрытие соединения"""
        if self.sock:
            self._is_connected = False

            if self.on_disconnect:
                try:
                    self.on_disconnect(self)
                except:
                    pass

            self.sock.close()
            self.sock = None


# ============ Connection Pool ============

class ConnectionPool:
    """Пул переиспользуемых соединений"""

    def __init__(self, host: str = 'localhost', port: int = 6379,
                 password: Optional[str] = None, username: Optional[str] = None,
                 db: int = 0, socket_timeout: int = 5,
                 ssl: bool = False, ssl_ca_certs: Optional[str] = None,
                 ssl_certfile: Optional[str] = None, ssl_keyfile: Optional[str] = None,
                 ssl_check_hostname: bool = True, ssl_verify: bool = True,
                 max_connections: int = 50, min_idle_connections: int = 1,
                 health_check_interval: int = 30):

        self.connection_kwargs = {
            'host': host,
            'port': port,
            'password': password,
            'username': username,
            'db': db,
            'socket_timeout': socket_timeout,
            'ssl': ssl,
            'ssl_ca_certs': ssl_ca_certs,
            'ssl_certfile': ssl_certfile,
            'ssl_keyfile': ssl_keyfile,
            'ssl_check_hostname': ssl_check_hostname,
            'ssl_verify': ssl_verify
        }

        self.max_connections = max_connections
        self.min_idle_connections = min_idle_connections
        self.health_check_interval = health_check_interval

        self._pool: List[RedisConnection] = []
        self._in_use: Dict[int, RedisConnection] = {}
        self._lock = Lock()
        self._created_connections = 0
        self._last_health_check = 0

    def get_connection(self) -> RedisConnection:
        """Получить соединение из пула"""
        with self._lock:
            # Health check периодически
            current_time = time.time()
            if current_time - self._last_health_check > self.health_check_interval:
                self._health_check()
                self._last_health_check = current_time

            # Ищем готовое соединение
            while self._pool:
                conn = self._pool.pop()
                if not conn.is_connected:
                    conn.close()
                    self._created_connections -= 1
                    continue

                target_db = self.connection_kwargs['db']
                if conn.db != target_db:
                    try:
                        conn.execute_command('SELECT', target_db)
                        conn.db = target_db
                    except Exception:
                        conn.close()
                        self._created_connections -= 1
                        continue

                self._in_use[id(conn)] = conn
                return conn

            # Создаём новое
            if self._created_connections < self.max_connections:
                conn = RedisConnection(**self.connection_kwargs)
                self._created_connections += 1
                self._in_use[id(conn)] = conn
                return conn

            raise RedisConnectionError("Connection pool exhausted")

    def release(self, conn: RedisConnection):
        """Вернуть соединение в пул"""
        with self._lock:
            conn_id = id(conn)
            if conn_id in self._in_use:
                self._in_use.pop(conn_id)

                if conn.is_connected:
                    self._pool.append(conn)
                else:
                    conn.close()
                    self._created_connections -= 1

    def set_db(self, db: int):
        """Set the logical database expected for pooled connections."""
        with self._lock:
            self.connection_kwargs['db'] = db

    def _health_check(self):
        """Проверка здоровья соединений в пуле"""
        healthy = []
        for conn in self._pool:
            if conn.is_healthy():
                healthy.append(conn)
            else:
                conn.close()
                self._created_connections -= 1

        self._pool = healthy

    def close_all(self):
        """Закрыть все соединения"""
        with self._lock:
            for conn in self._pool:
                conn.close()
            for conn in self._in_use.values():
                conn.close()
            self._pool.clear()
            self._in_use.clear()
            self._created_connections = 0

    def execute_command(self, *args, **kwargs) -> Any:
        """Обратная совместимость с v1.0.0: выполнить команду через пул

        Автоматически получает соединение из пула, выполняет команду и возвращает соединение.
        Это позволяет использовать ConnectionPool как RedisConnection из v1.0.0.

        Example:
            pool = client.cluster_nodes['172.22.0.6:7004']
            response = pool.execute_command('SCAN', 0, 'MATCH', 'user:*')
        """
        conn = self.get_connection()
        try:
            return conn.execute_command(*args, **kwargs)
        finally:
            self.release(conn)

# ============ Cluster Node ============

class ClusterNode:
    """Информация об узле кластера"""

    def __init__(self, host: str, port: int, node_id: str, slots: List[int],
                 role: str = 'master', replicas: Optional[List['ClusterNode']] = None):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.slots = slots
        self.role = role
        self.replicas = replicas or []

    def __repr__(self):
        return f"ClusterNode(host={self.host}, port={self.port}, role={self.role}, slots={len(self.slots)})"

    def __str__(self):
        return f"{self.host}:{self.port}"


# ============ Redis Client ============

class RedisClient:
    """Production-ready Redis клиент с полной поддержкой Cluster"""

    def __init__(self, host: str = 'localhost', port: int = 6379,
                 password: Optional[str] = None, username: Optional[str] = None,
                 db: int = 0, socket_timeout: int = 5,
                 ssl: bool = False, ssl_ca_certs: Optional[str] = None,
                 ssl_certfile: Optional[str] = None, ssl_keyfile: Optional[str] = None,
                 ssl_check_hostname: bool = True, ssl_verify: bool = True,
                 decode_responses: bool = False, is_cluster: bool = False,
                 max_connections: int = 50,
                 read_from_replicas: bool = False,
                 replica_selector: str = 'random',
                 auto_refresh_topology: bool = True,
                 topology_refresh_interval: int = 300,
                 sentinel_service_name: Optional[str] = None,
                 sentinels: Optional[List[Tuple[str, int]]] = None,
                 read_preference: str = 'master',
                 sentinel_username: Optional[str] = None,
                 sentinel_password: Optional[str] = None,
                 sentinel_ssl: bool = False,
                 sentinel_ssl_ca_certs: Optional[str] = None,
                 sentinel_ssl_certfile: Optional[str] = None,
                 sentinel_ssl_keyfile: Optional[str] = None,
                 sentinel_ssl_check_hostname: bool = True,
                 sentinel_ssl_verify: bool = True):

        self.host = host
        self.port = port
        self.password = password
        self.username = username
        self.db = db
        self.socket_timeout = socket_timeout
        self.ssl = ssl
        self.ssl_ca_certs = ssl_ca_certs
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.ssl_check_hostname = ssl_check_hostname
        self.ssl_verify = ssl_verify
        self.decode_responses = decode_responses
        self.is_cluster = is_cluster
        self.max_connections = max_connections

        # Replica support
        self.read_from_replicas = read_from_replicas
        self.replica_selector = replica_selector

        # Topology refresh
        self.auto_refresh_topology = auto_refresh_topology
        self.topology_refresh_interval = topology_refresh_interval
        self._last_topology_refresh = 0

        # Sentinel support
        self.sentinel_service_name = sentinel_service_name
        self.sentinels = sentinels
        self.is_sentinel = bool(sentinels and sentinel_service_name)
        self.read_preference = (
            'replica_preferred'
            if self.is_sentinel and read_from_replicas and read_preference == 'master'
            else read_preference
        )
        self.sentinel_username = sentinel_username
        self.sentinel_password = sentinel_password
        self.sentinel_ssl = sentinel_ssl
        self.sentinel_ssl_ca_certs = sentinel_ssl_ca_certs
        self.sentinel_ssl_certfile = sentinel_ssl_certfile
        self.sentinel_ssl_keyfile = sentinel_ssl_keyfile
        self.sentinel_ssl_check_hostname = sentinel_ssl_check_hostname
        self.sentinel_ssl_verify = sentinel_ssl_verify
        self.sentinel_master: Optional[Tuple[str, int]] = None
        self.sentinel_replicas: List[Tuple[str, int]] = []
        self.sentinel_replica_pools: Dict[str, ConnectionPool] = {}
        self._sentinel_lock = Lock()
        self._sentinel_round_robin_index = 0

        valid_read_preferences = {'master', 'replica_preferred', 'replica_only'}
        if self.read_preference not in valid_read_preferences:
            raise ValueError(
                f"read_preference must be one of {sorted(valid_read_preferences)}"
            )
        if self.is_sentinel and replica_selector not in {'random', 'round_robin'}:
            raise ValueError("replica_selector must be 'random' or 'round_robin'")

        # Cluster state
        self.cluster_pools: Dict[str, ConnectionPool] = {}
        self.cluster_slots: Dict[int, str] = {}
        self.cluster_nodes_info: Dict[str, ClusterNode] = {}
        self._pool_lock = Lock()

        # Lua script cache (SHA1 хеши)
        self._script_cache: Dict[str, str] = {}

        # Main connection pool
        if self.is_sentinel:
            master_host, master_port = self._discover_master_from_sentinel()
            self.host = master_host
            self.port = master_port
            self.sentinel_master = (master_host, master_port)
            self.sentinel_replicas = self._discover_replicas_from_sentinel(
                self.sentinel_master
            )

        self.main_pool = ConnectionPool(
            host=self.host, port=self.port,
            password=password, username=username,
            db=db, socket_timeout=socket_timeout,
            ssl=ssl, ssl_ca_certs=ssl_ca_certs,
            ssl_certfile=ssl_certfile, ssl_keyfile=ssl_keyfile,
            ssl_check_hostname=ssl_check_hostname, ssl_verify=ssl_verify,
            max_connections=max_connections
        )

        if self.is_sentinel:
            self.sentinel_replica_pools = {
                f'{replica_host}:{replica_port}': self._create_data_pool(
                    replica_host, replica_port
                )
                for replica_host, replica_port in self.sentinel_replicas
            }

        if self.is_cluster:
            self._load_cluster_topology()

    # ============ Backward Compatibility Properties ============

    @property
    def cluster_nodes(self) -> Dict[str, ConnectionPool]:
        """Backward compatibility: cluster_nodes теперь указывает на cluster_pools

        В v1.0.0: cluster_nodes был Dict[str, RedisConnection]
        В v2.1.0+: cluster_pools - Dict[str, ConnectionPool]

        Для совместимости возвращаем cluster_pools.
        Чтобы получить соединение: pool.get_connection()
        """
        return self.cluster_pools

    def _create_data_pool(self, host: str, port: int) -> ConnectionPool:
        """Create a Redis data pool using the client's data-node settings."""
        return ConnectionPool(
            host=host,
            port=port,
            password=self.password,
            username=self.username,
            db=self.db,
            socket_timeout=self.socket_timeout,
            ssl=self.ssl,
            ssl_ca_certs=self.ssl_ca_certs,
            ssl_certfile=self.ssl_certfile,
            ssl_keyfile=self.ssl_keyfile,
            ssl_check_hostname=self.ssl_check_hostname,
            ssl_verify=self.ssl_verify,
            max_connections=self.max_connections
        )

    def _execute_sentinel_command(self, *args) -> Any:
        """Execute one command against the first reachable Sentinel."""
        if not self.sentinels or not self.sentinel_service_name:
            raise RedisError("Sentinels not configured")

        last_error = None
        for sentinel_host, sentinel_port in self.sentinels:
            sentinel_conn = None
            try:
                sentinel_conn = RedisConnection(
                    host=sentinel_host,
                    port=sentinel_port,
                    username=self.sentinel_username,
                    password=self.sentinel_password,
                    socket_timeout=self.socket_timeout,
                    ssl=self.sentinel_ssl,
                    ssl_ca_certs=self.sentinel_ssl_ca_certs,
                    ssl_certfile=self.sentinel_ssl_certfile,
                    ssl_keyfile=self.sentinel_ssl_keyfile,
                    ssl_check_hostname=self.sentinel_ssl_check_hostname,
                    ssl_verify=self.sentinel_ssl_verify
                )
                return sentinel_conn.execute_command('SENTINEL', *args)
            except Exception as e:
                last_error = e
                logger.warning(f"Failed to contact sentinel {sentinel_host}:{sentinel_port}: {e}")
            finally:
                if sentinel_conn:
                    sentinel_conn.close()

        raise RedisConnectionError(
            f"Failed to query all Sentinels: {last_error or 'no endpoints'}"
        )

    def _discover_master_from_sentinel(self) -> Tuple[str, int]:
        """Return the master endpoint reported by Sentinel."""
        response = self._execute_sentinel_command(
            'get-master-addr-by-name', self.sentinel_service_name
        )
        if response and len(response) == 2:
            master_host = (
                response[0].decode('utf-8')
                if isinstance(response[0], bytes)
                else response[0]
            )
            master_port = int(response[1])
            logger.info(f"Discovered master from sentinel: {master_host}:{master_port}")
            return master_host, master_port

        raise RedisConnectionError(
            f"Sentinel service '{self.sentinel_service_name}' has no master"
        )

    @staticmethod
    def _parse_sentinel_record(record: List[Any]) -> Dict[str, str]:
        """Convert a flat Sentinel key/value response into a string dictionary."""
        result = {}
        for index in range(0, len(record) - 1, 2):
            key = record[index]
            value = record[index + 1]
            if isinstance(key, bytes):
                key = key.decode('utf-8', errors='replace')
            if isinstance(value, bytes):
                value = value.decode('utf-8', errors='replace')
            result[str(key)] = str(value)
        return result

    def _discover_replicas_from_sentinel(
            self, master: Optional[Tuple[str, int]] = None
    ) -> List[Tuple[str, int]]:
        """Return healthy replica endpoints reported by Sentinel."""
        response = self._execute_sentinel_command(
            'replicas', self.sentinel_service_name
        )
        replicas = []
        for raw_record in response or []:
            record = self._parse_sentinel_record(raw_record)
            flags = set(record.get('flags', '').split(','))
            if flags.intersection({'s_down', 'o_down', 'disconnected', 'master'}):
                continue
            if record.get('master-link-status', 'ok') != 'ok':
                continue

            host = record.get('ip') or record.get('name')
            port = record.get('port')
            if not host or not port:
                continue
            endpoint = (host, int(port))
            if endpoint != (master or self.sentinel_master) and endpoint not in replicas:
                replicas.append(endpoint)
        return replicas

    def refresh_sentinel_topology(self) -> bool:
        """Rediscover Sentinel master/replicas and atomically replace changed pools."""
        if not self.is_sentinel:
            return False

        master = self._discover_master_from_sentinel()
        replicas = self._discover_replicas_from_sentinel(master)
        pools_to_close = []

        with self._sentinel_lock:
            changed = master != self.sentinel_master
            if changed:
                old_main_pool = self.main_pool
                self.main_pool = self._create_data_pool(*master)
                pools_to_close.append(old_main_pool)
                self.host, self.port = master
                self.sentinel_master = master

            old_replica_pools = self.sentinel_replica_pools
            new_replica_pools = {}
            for host, port in replicas:
                node_id = f'{host}:{port}'
                pool = old_replica_pools.get(node_id)
                if pool is None:
                    pool = self._create_data_pool(host, port)
                new_replica_pools[node_id] = pool

            for node_id, pool in old_replica_pools.items():
                if node_id not in new_replica_pools:
                    pools_to_close.append(pool)

            if replicas != self.sentinel_replicas:
                changed = True
            self.sentinel_replicas = replicas
            self.sentinel_replica_pools = new_replica_pools

        for pool in pools_to_close:
            pool.close_all()

        return changed

    def _get_sentinel_read_pool(self) -> ConnectionPool:
        """Select a Sentinel replica pool according to the configured policy."""
        if self.read_preference == 'master':
            return self.main_pool

        with self._sentinel_lock:
            pools = list(self.sentinel_replica_pools.values())

        if not pools:
            if self.read_preference == 'replica_only':
                raise RedisConnectionError('No healthy Sentinel replicas available')
            return self.main_pool

        if self.replica_selector == 'round_robin':
            with self._sentinel_lock:
                index = self._sentinel_round_robin_index % len(pools)
                self._sentinel_round_robin_index += 1
            return pools[index]
        return random.choice(pools)

    @staticmethod
    def _is_sentinel_topology_error(error: RedisError) -> bool:
        """Return whether an error should trigger Sentinel rediscovery."""
        if isinstance(error, RedisConnectionError):
            return True
        message = str(error).upper()
        return message.startswith('READONLY') or message.startswith('MASTERDOWN')

    def _execute_sentinel_data_command(self, *args,
                                       for_read: bool = False) -> Any:
        """Execute a Sentinel-routed command with one safe failover retry."""
        for attempt in range(2):
            pool = self._get_sentinel_read_pool() if for_read else self.main_pool
            conn = None
            try:
                conn = pool.get_connection()
                response = conn.execute_command(*args, retry=for_read)
                if self.decode_responses:
                    response = self._decode_response(response)
                return response
            except RedisError as e:
                topology_error = self._is_sentinel_topology_error(e)
                if topology_error:
                    try:
                        self.refresh_sentinel_topology()
                    except RedisError as refresh_error:
                        logger.warning(f"Sentinel topology refresh failed: {refresh_error}")

                rejected_write = str(e).upper().startswith(
                    ('READONLY', 'MASTERDOWN')
                )
                if attempt == 0 and topology_error and (for_read or rejected_write):
                    continue
                raise
            finally:
                if conn is not None:
                    pool.release(conn)


    def _load_cluster_topology(self, force: bool = False):
        """Загрузка топологии кластера"""
        current_time = time.time()

        if not force and current_time - self._last_topology_refresh < self.topology_refresh_interval:
            return

        try:
            conn = self.main_pool.get_connection()
            try:
                slots_info = conn.execute_command('CLUSTER', 'SLOTS')
            finally:
                self.main_pool.release(conn)

            new_slots = {}
            new_nodes = {}

            for slot_range in slots_info:
                start_slot = slot_range[0]
                end_slot = slot_range[1]
                master_info = slot_range[2]

                master_host = master_info[0].decode('utf-8') if isinstance(master_info[0], bytes) else master_info[0]
                master_port = master_info[1]
                master_id = f"{master_host}:{master_port}"

                # Реплики
                replicas = []
                for replica_info in slot_range[3:]:
                    replica_host = replica_info[0].decode('utf-8') if isinstance(replica_info[0], bytes) else replica_info[0]
                    replica_port = replica_info[1]
                    replica_id = f"{replica_host}:{replica_port}"

                    replica_node = ClusterNode(
                        host=replica_host,
                        port=replica_port,
                        node_id=replica_id,
                        slots=[],
                        role='replica'
                    )
                    replicas.append(replica_node)

                    # Создаём пул для реплики
                    if replica_id not in self.cluster_pools:
                        self.cluster_pools[replica_id] = ConnectionPool(
                            host=replica_host, port=replica_port,
                            password=self.password, username=self.username,
                            socket_timeout=self.socket_timeout,
                            ssl=self.ssl, ssl_ca_certs=self.ssl_ca_certs,
                            ssl_certfile=self.ssl_certfile, ssl_keyfile=self.ssl_keyfile,
                            ssl_check_hostname=self.ssl_check_hostname, ssl_verify=self.ssl_verify,
                            max_connections=self.max_connections
                        )

                # Создаём или обновляем узел мастера
                slots_for_master = list(range(start_slot, end_slot + 1))

                if master_id in new_nodes:
                    new_nodes[master_id].slots.extend(slots_for_master)
                    new_nodes[master_id].replicas.extend(replicas)
                else:
                    new_nodes[master_id] = ClusterNode(
                        host=master_host,
                        port=master_port,
                        node_id=master_id,
                        slots=slots_for_master,
                        role='master',
                        replicas=replicas
                    )

                # Маппинг слотов
                for slot in slots_for_master:
                    new_slots[slot] = master_id

                # Создаём пул для мастера
                if master_id not in self.cluster_pools:
                    self.cluster_pools[master_id] = ConnectionPool(
                        host=master_host, port=master_port,
                        password=self.password, username=self.username,
                        socket_timeout=self.socket_timeout,
                        ssl=self.ssl, ssl_ca_certs=self.ssl_ca_certs,
                        ssl_certfile=self.ssl_certfile, ssl_keyfile=self.ssl_keyfile,
                        ssl_check_hostname=self.ssl_check_hostname, ssl_verify=self.ssl_verify,
                        max_connections=self.max_connections
                    )

            self.cluster_slots = new_slots
            self.cluster_nodes_info = new_nodes
            self._last_topology_refresh = current_time

            logger.info(f"Loaded cluster topology: {len(new_nodes)} nodes, {len(new_slots)} slots")

        except Exception as e:
            logger.error(f"Failed to load cluster topology: {e}")

    def _get_slot(self, key: Union[str, bytes]) -> int:
        """CRC16 hash slot calculation"""
        if isinstance(key, str):
            key = key.encode('utf-8')

        # Hash tag support
        start = key.find(b'{')
        if start != -1:
            end = key.find(b'}', start + 1)
            if end != -1 and end > start + 1:
                key = key[start + 1:end]

        crc = 0
        for byte in key:
            crc ^= byte << 8
            for _ in range(8):
                if crc & 0x8000:
                    crc = (crc << 1) ^ 0x1021
                else:
                    crc = crc << 1
            crc &= 0xFFFF

        return crc % 16384

    def _get_pool_for_key(self, key: Union[str, bytes], for_read: bool = False) -> ConnectionPool:
        """Получить пул соединений для ключа"""
        if self.is_sentinel:
            return self._get_sentinel_read_pool() if for_read else self.main_pool

        if not self.is_cluster:
            return self.main_pool

        # Автообновление топологии
        if self.auto_refresh_topology:
            self._load_cluster_topology()

        slot = self._get_slot(key)
        node_id = self.cluster_slots.get(slot)

        if not node_id:
            return self.main_pool

        # Если читаем и включено чтение с реплик
        if for_read and self.read_from_replicas:
            node_info = self.cluster_nodes_info.get(node_id)
            if node_info and node_info.replicas:
                if self.replica_selector == 'random':
                    replica = random.choice(node_info.replicas)
                    replica_pool = self.cluster_pools.get(replica.node_id)
                    if replica_pool:
                        return replica_pool
                elif self.replica_selector == 'round_robin':
                    # Простая реализация round-robin
                    replica = node_info.replicas[hash(key) % len(node_info.replicas)]
                    replica_pool = self.cluster_pools.get(replica.node_id)
                    if replica_pool:
                        return replica_pool

        pool = self.cluster_pools.get(node_id)
        return pool if pool else self.main_pool

    def execute_command(self, *args, key: Optional[Union[str, bytes]] = None,
                       for_read: bool = False) -> Any:
        """Выполнение команды с поддержкой кластера и редиректов"""
        if self.is_sentinel:
            return self._execute_sentinel_data_command(*args, for_read=for_read)

        max_redirects = 5
        redirect_count = 0

        if key is None and len(args) > 1:
            key = args[1]

        pool = self._get_pool_for_key(key, for_read=for_read) if key else self.main_pool

        while redirect_count < max_redirects:
            conn = pool.get_connection()
            try:
                response = conn.execute_command(*args)

                if self.decode_responses:
                    response = self._decode_response(response)

                return response

            except RedisError as e:
                error_msg = str(e)

                if error_msg.startswith('MOVED'):
                    # Обновляем топологию
                    parts = error_msg.split()
                    if len(parts) >= 3:
                        slot = int(parts[1])
                        node_addr = parts[2]
                        host, port_str = node_addr.split(':')
                        port = int(port_str)
                        node_id = f"{host}:{port}"

                        # Создаём новый пул если нужно
                        if node_id not in self.cluster_pools:
                            self.cluster_pools[node_id] = ConnectionPool(
                                host=host, port=port,
                                password=self.password, username=self.username,
                                socket_timeout=self.socket_timeout,
                                ssl=self.ssl, ssl_ca_certs=self.ssl_ca_certs,
                                ssl_certfile=self.ssl_certfile, ssl_keyfile=self.ssl_keyfile,
                                ssl_check_hostname=self.ssl_check_hostname, ssl_verify=self.ssl_verify,
                                max_connections=self.max_connections
                            )

                        self.cluster_slots[slot] = node_id
                        pool = self.cluster_pools[node_id]
                        redirect_count += 1

                        # Форсируем обновление топологии
                        if self.auto_refresh_topology:
                            self._load_cluster_topology(force=True)

                        continue

                elif error_msg.startswith('ASK'):
                    parts = error_msg.split()
                    if len(parts) >= 3:
                        node_addr = parts[2]
                        host, port_str = node_addr.split(':')

                        temp_conn = RedisConnection(
                            host=host, port=int(port_str),
                            password=self.password, username=self.username,
                            socket_timeout=self.socket_timeout,
                            ssl=self.ssl, ssl_ca_certs=self.ssl_ca_certs,
                            ssl_certfile=self.ssl_certfile, ssl_keyfile=self.ssl_keyfile,
                            ssl_check_hostname=self.ssl_check_hostname, ssl_verify=self.ssl_verify
                        )

                        try:
                            temp_conn.execute_command('ASKING')
                            response = temp_conn.execute_command(*args)

                            if self.decode_responses:
                                response = self._decode_response(response)

                            return response
                        finally:
                            temp_conn.close()

                raise

            finally:
                pool.release(conn)

        raise RedisClusterError("Too many redirects")

    def _decode_response(self, response):
        """Декодирование ответа"""
        if isinstance(response, bytes):
            return response.decode('utf-8', errors='replace')
        elif isinstance(response, list):
            return [self._decode_response(item) for item in response]
        elif isinstance(response, dict):
            return {self._decode_response(k): self._decode_response(v) 
                   for k, v in response.items()}
        return response

    # ============ Multi-key команды ============

    def mget(self, *keys) -> List[Optional[bytes]]:
        """MGET с группировкой по слотам"""
        if not self.is_cluster:
            return self.execute_command('MGET', *keys)

        # Группировка ключей по слотам
        slot_groups = defaultdict(list)
        key_order = {}

        for idx, key in enumerate(keys):
            slot = self._get_slot(key)
            slot_groups[slot].append(key)
            key_order[key] = idx

        # Если все ключи в одном слоте
        if len(slot_groups) == 1:
            return self.execute_command('MGET', *keys, key=keys[0], for_read=True)

        # Выполняем GET для каждого ключа
        results = [None] * len(keys)
        for slot, slot_keys in slot_groups.items():
            for key in slot_keys:
                value = self.execute_command('GET', key, key=key, for_read=True)
                results[key_order[key]] = value

        return results

    def mset(self, mapping: Dict[Union[str, bytes], Union[str, bytes]]) -> str:
        """MSET с группировкой по слотам"""
        if not self.is_cluster:
            args = ['MSET']
            for k, v in mapping.items():
                args.extend([k, v])
            return self.execute_command(*args)

        # Группировка по слотам
        slot_groups = defaultdict(dict)
        for key, value in mapping.items():
            slot = self._get_slot(key)
            slot_groups[slot][key] = value

        # Если все в одном слоте
        if len(slot_groups) == 1:
            args = ['MSET']
            for k, v in mapping.items():
                args.extend([k, v])
            first_key = next(iter(mapping.keys()))
            return self.execute_command(*args, key=first_key)

        # Выполняем SET для каждого ключа
        for slot, slot_mapping in slot_groups.items():
            for key, value in slot_mapping.items():
                self.execute_command('SET', key, value, key=key)

        return 'OK'

    def delete(self, *keys) -> int:
        """DEL с группировкой по слотам"""
        if not self.is_cluster:
            return self.execute_command('DEL', *keys, key=keys[0] if keys else None)

        # Группировка по слотам
        slot_groups = defaultdict(list)
        for key in keys:
            slot = self._get_slot(key)
            slot_groups[slot].append(key)

        # Если все в одном слоте
        if len(slot_groups) == 1:
            return self.execute_command('DEL', *keys, key=keys[0])

        # Удаляем по группам
        total = 0
        for slot, slot_keys in slot_groups.items():
            total += self.execute_command('DEL', *slot_keys, key=slot_keys[0])

        return total

    # ============ Lua Scripts ============

    def _script_sha1(self, script: str) -> str:
        """Вычислить SHA1 хеш скрипта"""
        return hashlib.sha1(script.encode('utf-8')).hexdigest()

    def eval(self, script: str, numkeys: int, *keys_and_args) -> Any:
        """EVAL - выполнить Lua скрипт

        Args:
            script: Lua скрипт
            numkeys: Количество ключей
            *keys_and_args: Сначала ключи, потом аргументы

        Example:
            client.eval("return redis.call('GET', KEYS[1])", 1, 'mykey')
        """
        # В кластере определяем узел по первому ключу
        key = keys_and_args[0] if numkeys > 0 and keys_and_args else None

        # Проверяем что все ключи в одном слоте (кластер)
        if self.is_cluster and numkeys > 1:
            first_slot = self._get_slot(keys_and_args[0])
            for i in range(1, numkeys):
                if self._get_slot(keys_and_args[i]) != first_slot:
                    raise RedisClusterError(
                        f"All keys must be in the same slot for EVAL. "
                        f"Use hash tags like {{tag}} to ensure same slot."
                    )

        return self.execute_command('EVAL', script, numkeys, *keys_and_args, key=key)

    def evalsha(self, sha: str, numkeys: int, *keys_and_args) -> Any:
        """EVALSHA - выполнить скрипт по SHA1

        Args:
            sha: SHA1 хеш скрипта
            numkeys: Количество ключей
            *keys_and_args: Сначала ключи, потом аргументы
        """
        key = keys_and_args[0] if numkeys > 0 and keys_and_args else None

        # Проверка слотов как в eval
        if self.is_cluster and numkeys > 1:
            first_slot = self._get_slot(keys_and_args[0])
            for i in range(1, numkeys):
                if self._get_slot(keys_and_args[i]) != first_slot:
                    raise RedisClusterError(
                        f"All keys must be in the same slot for EVALSHA. "
                        f"Use hash tags like {{tag}} to ensure same slot."
                    )

        return self.execute_command('EVALSHA', sha, numkeys, *keys_and_args, key=key)

    def script_load(self, script: str, key: Optional[Union[str, bytes]] = None) -> str:
        """SCRIPT LOAD - загрузить скрипт на сервер

        Args:
            script: Lua скрипт
            key: Опциональный ключ для определения узла в кластере

        Returns:
            SHA1 хеш скрипта
        """
        sha = self._script_sha1(script)

        if self.is_cluster:
            # В кластере загружаем на все узлы
            for node_id, pool in self.cluster_pools.items():
                conn = pool.get_connection()
                try:
                    conn.execute_command('SCRIPT', 'LOAD', script)
                except Exception as e:
                    logger.warning(f"Failed to load script on {node_id}: {e}")
                finally:
                    pool.release(conn)
        else:
            self.execute_command('SCRIPT', 'LOAD', script, key=key)

        self._script_cache[script] = sha
        return sha

    def script_exists(self, *shas, key: Optional[Union[str, bytes]] = None) -> List[int]:
        """SCRIPT EXISTS - проверить существование скриптов

        Returns:
            Список 1/0 для каждого SHA
        """
        return self.execute_command('SCRIPT', 'EXISTS', *shas, key=key)

    def script_flush(self, key: Optional[Union[str, bytes]] = None) -> str:
        """SCRIPT FLUSH - удалить все скрипты"""
        if self.is_cluster:
            # В кластере флашим все узлы
            for node_id, pool in self.cluster_pools.items():
                conn = pool.get_connection()
                try:
                    conn.execute_command('SCRIPT', 'FLUSH')
                except Exception as e:
                    logger.warning(f"Failed to flush scripts on {node_id}: {e}")
                finally:
                    pool.release(conn)
            result = 'OK'
        else:
            result = self.execute_command('SCRIPT', 'FLUSH', key=key)

        self._script_cache.clear()
        return result

    def register_script(self, script: str) -> 'Script':
        """Зарегистрировать скрипт для удобного использования

        Returns:
            Объект Script с методом __call__
        """
        return Script(self, script)

    # ============ Basic Commands ============

    def ping(self) -> bool:
        """PING"""
        try:
            response = self.execute_command('PING')
            return response in ('PONG', b'PONG')
        except:
            return False

    def get(self, key: Union[str, bytes]) -> Optional[bytes]:
        """GET"""
        return self.execute_command('GET', key, key=key, for_read=True)

    def set(self, key: Union[str, bytes], value: Union[str, bytes, int, float],
            ex: Optional[int] = None, px: Optional[int] = None,
            nx: bool = False, xx: bool = False) -> Optional[str]:
        """SET"""
        args = ['SET', key, value]

        if ex is not None:
            args.extend(['EX', ex])
        if px is not None:
            args.extend(['PX', px])
        if nx:
            args.append('NX')
        if xx:
            args.append('XX')

        return self.execute_command(*args, key=key)

    def incr(self, key: Union[str, bytes]) -> int:
        """INCR"""
        return self.execute_command('INCR', key, key=key)

    def decr(self, key: Union[str, bytes]) -> int:
        """DECR"""
        return self.execute_command('DECR', key, key=key)

    def exists(self, *keys) -> int:
        """EXISTS"""
        if not keys:
            return 0
        return self.execute_command('EXISTS', *keys, key=keys[0], for_read=True)

    def ttl(self, key: Union[str, bytes]) -> int:
        """TTL"""
        return self.execute_command('TTL', key, key=key, for_read=True)

    def expire(self, key: Union[str, bytes], seconds: int) -> int:
        """EXPIRE"""
        return self.execute_command('EXPIRE', key, seconds, key=key)

    def type(self, key: Union[str, bytes]) -> bytes:
        """TYPE - получить тип ключа"""
        return self.execute_command('TYPE', key, key=key, for_read=True)

    def rename(self, old_key: Union[str, bytes], new_key: Union[str, bytes]) -> str:
        """RENAME - переименовать ключ"""
        return self.execute_command('RENAME', old_key, new_key, key=old_key)

    def strlen(self, key: Union[str, bytes]) -> int:
        """STRLEN - длина строкового значения"""
        return self.execute_command('STRLEN', key, key=key, for_read=True)

    def incrby(self, key: Union[str, bytes], amount: int) -> int:
        """INCRBY - увеличить на значение"""
        return self.execute_command('INCRBY', key, amount, key=key)

    def decrby(self, key: Union[str, bytes], amount: int) -> int:
        """DECRBY - уменьшить на значение"""
        return self.execute_command('DECRBY', key, amount, key=key)

    def persist(self, key: Union[str, bytes]) -> int:
        """PERSIST"""
        return self.execute_command('PERSIST', key, key=key)

    # ============ Hash Commands ============

    def hget(self, key: Union[str, bytes], field: Union[str, bytes]) -> Optional[bytes]:
        """HGET"""
        return self.execute_command('HGET', key, field, key=key, for_read=True)

    def hset(self, key: Union[str, bytes], field: Optional[Union[str, bytes]] = None,
             value: Optional[Union[str, bytes]] = None, mapping: Optional[Dict] = None) -> int:
        """HSET"""
        args = ['HSET', key]

        if mapping:
            for k, v in mapping.items():
                args.extend([k, v])
        elif field is not None and value is not None:
            args.extend([field, value])
        else:
            raise ValueError("field+value or mapping required")

        return self.execute_command(*args, key=key)

    def hgetall(self, key: Union[str, bytes]) -> Dict[bytes, bytes]:
        """HGETALL"""
        response = self.execute_command('HGETALL', key, key=key, for_read=True)
        result = {}
        for i in range(0, len(response), 2):
            result[response[i]] = response[i + 1]
        return result

    def hlen(self, key: Union[str, bytes]) -> int:
        """HLEN"""
        return self.execute_command('HLEN', key, key=key, for_read=True)

    def hdel(self, key: Union[str, bytes], *fields) -> int:
        """HDEL"""
        return self.execute_command('HDEL', key, *fields, key=key)

    # ============ List Commands ============

    def lpush(self, key: Union[str, bytes], *values) -> int:
        """LPUSH"""
        return self.execute_command('LPUSH', key, *values, key=key)

    def rpush(self, key: Union[str, bytes], *values) -> int:
        """RPUSH"""
        return self.execute_command('RPUSH', key, *values, key=key)

    def lpop(self, key: Union[str, bytes]) -> Optional[bytes]:
        """LPOP"""
        return self.execute_command('LPOP', key, key=key)

    def rpop(self, key: Union[str, bytes]) -> Optional[bytes]:
        """RPOP"""
        return self.execute_command('RPOP', key, key=key)

    def llen(self, key: Union[str, bytes]) -> int:
        """LLEN"""
        return self.execute_command('LLEN', key, key=key, for_read=True)

    def lrange(self, key: Union[str, bytes], start: int, stop: int) -> List[bytes]:
        """LRANGE"""
        return self.execute_command('LRANGE', key, start, stop, key=key, for_read=True)

    # ============ Set Commands ============

    def sadd(self, key: Union[str, bytes], *members) -> int:
        """SADD"""
        return self.execute_command('SADD', key, *members, key=key)

    def smembers(self, key: Union[str, bytes]) -> set:
        """SMEMBERS"""
        result = self.execute_command('SMEMBERS', key, key=key, for_read=True)
        return set(result) if result else set()

    def scard(self, key: Union[str, bytes]) -> int:
        """SCARD"""
        return self.execute_command('SCARD', key, key=key, for_read=True)

    def sismember(self, key: Union[str, bytes], member: Union[str, bytes]) -> int:
        """SISMEMBER"""
        return self.execute_command('SISMEMBER', key, member, key=key, for_read=True)

    def srem(self, key: Union[str, bytes], *members) -> int:
        """SREM"""
        return self.execute_command('SREM', key, *members, key=key)

    # ============ Sorted Set Commands ============

    def zadd(self, key: Union[str, bytes], mapping: Dict[Union[str, bytes], float],
             nx: bool = False, xx: bool = False, gt: bool = False, lt: bool = False) -> int:
        """ZADD"""
        args = ['ZADD', key]

        if nx:
            args.append('NX')
        if xx:
            args.append('XX')
        if gt:
            args.append('GT')
        if lt:
            args.append('LT')

        for member, score in mapping.items():
            args.extend([score, member])

        return self.execute_command(*args, key=key)

    def zcard(self, key: Union[str, bytes]) -> int:
        """ZCARD"""
        return self.execute_command('ZCARD', key, key=key, for_read=True)

    def zrange(self, key: Union[str, bytes], start: int, stop: int,
               withscores: bool = False) -> List:
        """ZRANGE"""
        args = ['ZRANGE', key, start, stop]

        if withscores:
            args.append('WITHSCORES')

        response = self.execute_command(*args, key=key, for_read=True)

        if withscores and response:
            result = []
            for i in range(0, len(response), 2):
                result.append((response[i], float(response[i + 1])))
            return result

        return response

    def zrem(self, key: Union[str, bytes], *members) -> int:
        """ZREM"""
        return self.execute_command('ZREM', key, *members, key=key)

    def select(self, db: int):
        """SELECT - выбрать базу данных (не работает в кластере)"""
        if self.is_cluster:
            raise RedisError('SELECT not allowed in cluster mode')

        # Выполняем SELECT через main_pool
        conn = self.main_pool.get_connection()
        try:
            conn.execute_command('SELECT', db)
            conn.db = db
            self.main_pool.set_db(db)
            for replica_pool in self.sentinel_replica_pools.values():
                replica_pool.set_db(db)
            self.db = db
        finally:
            self.main_pool.release(conn)

    # ============ Scan ============

    def scan(self, cursor: int = 0, match: Optional[str] = None,
             count: Optional[int] = None) -> Tuple[int, List[bytes]]:
        """SCAN"""
        args = ['SCAN', cursor]

        if match:
            args.extend(['MATCH', match])
        if count:
            args.extend(['COUNT', count])

        response = self.execute_command(*args, for_read=not self.is_sentinel)
        return int(response[0]), response[1]

    def scan_iter(self, match: Optional[str] = None, count: int = 100) -> Iterator[bytes]:
        """Итератор SCAN"""
        cursor = 0
        while True:
            cursor, keys = self.scan(cursor, match=match, count=count)
            for key in keys:
                yield key
            if cursor == 0:
                break

    def scan_all_nodes_iter(self, match: Optional[str] = None, count: int = 100) -> Iterator[bytes]:
        """Итератор по ВСЕМ ключам кластера (все узлы)

        Совместимость с v1.0.0: работает с cluster_pools (ConnectionPool)
        """
        if not self.is_cluster:
            yield from self.scan_iter(match=match, count=count)
            return

        seen_keys = set()
        for node_id, pool in self.cluster_pools.items():
            logger.debug(f"Scanning node {node_id}")
            conn = pool.get_connection()
            try:
                cursor = 0
                while True:
                    args = ['SCAN', cursor]
                    if match:
                        args.extend(['MATCH', match])
                    if count:
                        args.extend(['COUNT', count])

                    try:
                        response = conn.execute_command(*args)
                        cursor = int(response[0])
                        keys = response[1]

                        for key in keys:
                            if key not in seen_keys:
                                seen_keys.add(key)
                                yield key

                        if cursor == 0:
                            break

                    except Exception as e:
                        logger.error(f"Error scanning node {node_id}: {e}")
                        break
            finally:
                pool.release(conn)

    # ============ Cluster Utilities ============

    def keyslot(self, key: Union[str, bytes]) -> int:
        """Вычисляет hash slot для ключа"""
        return self._get_slot(key)

    def get_cluster_nodes(self) -> List[ClusterNode]:
        """Список всех узлов кластера"""
        if not self.is_cluster:
            return [ClusterNode(
                host=self.host,
                port=self.port,
                node_id=f"{self.host}:{self.port}",
                slots=[],
                role='master'
            )]

        return list(self.cluster_nodes_info.values())

    def refresh_cluster_topology(self):
        """Принудительное обновление топологии кластера"""
        if self.is_cluster:
            self._load_cluster_topology(force=True)

    # ============ Pipeline ============

    def pipeline(self, transaction: bool = False) -> 'RedisPipeline':
        """Создать pipeline"""
        return RedisPipeline(self, transaction=transaction)

    # ============ Close ============

    def close(self):
        """Закрыть все соединения"""
        self.main_pool.close_all()

        for pool in self.sentinel_replica_pools.values():
            pool.close_all()
        self.sentinel_replica_pools.clear()

        for pool in self.cluster_pools.values():
            pool.close_all()

        self.cluster_pools.clear()


    # ============ Недостающие методы из v1.0.0 ============

    def info(self, section: Optional[str] = None) -> Dict[str, Any]:
        """INFO - информация о сервере"""
        if section:
            response = self.execute_command('INFO', section, for_read=True)
        else:
            response = self.execute_command('INFO', for_read=True)

        if isinstance(response, bytes):
            response = response.decode('utf-8')

        result = {}
        current_section = None
        for line in response.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                if line.startswith('#'):
                    current_section = line[1:].strip().lower()
                    result[current_section] = {}
                continue

            if ':' in line:
                key, value = line.split(':', 1)
                try:
                    value = float(value) if '.' in value else int(value)
                except:
                    pass

                if current_section:
                    result[current_section][key] = value
                else:
                    result[key] = value

        return result

    def dbsize(self) -> int:
        """DBSIZE - количество ключей в текущей БД"""
        if self.is_cluster:
            logger.warning("DBSIZE in cluster mode returns approximate count")
        return self.execute_command('DBSIZE', for_read=True)

    def flushdb(self, asynchronous: bool = False) -> str:
        """FLUSHDB - очистить текущую БД"""
        if self.is_cluster:
            raise RedisError('FLUSHDB requires cluster-wide operation')

        if asynchronous:
            return self.execute_command('FLUSHDB', 'ASYNC')
        return self.execute_command('FLUSHDB')

    def flushall(self, asynchronous: bool = False) -> str:
        """FLUSHALL - очистить все БД"""
        if asynchronous:
            return self.execute_command('FLUSHALL', 'ASYNC')
        return self.execute_command('FLUSHALL')

    def keys(self, pattern: str = '*') -> List[bytes]:
        """KEYS - получить все ключи по паттерну (осторожно на продакшене!)"""
        return self.execute_command('KEYS', pattern, for_read=True)

    def hscan(self, key: Union[str, bytes], cursor: int = 0,
              match: Optional[str] = None, count: Optional[int] = None) -> Tuple[int, Dict]:
        """HSCAN - итеративное сканирование hash"""
        args = ['HSCAN', key, cursor]
        if match:
            args.extend(['MATCH', match])
        if count:
            args.extend(['COUNT', count])

        response = self.execute_command(
            *args, key=key, for_read=not self.is_sentinel
        )
        new_cursor = int(response[0])
        items = response[1]
        result = {}
        for i in range(0, len(items), 2):
            result[items[i]] = items[i + 1]
        return new_cursor, result

    def hscan_iter(self, key: Union[str, bytes], match: Optional[str] = None,
                   count: int = 100) -> Iterator[Tuple[bytes, bytes]]:
        """HSCAN итератор"""
        cursor = 0
        while True:
            cursor, data = self.hscan(key, cursor, match=match, count=count)
            for field, value in data.items():
                yield field, value
            if cursor == 0:
                break

    def sscan(self, key: Union[str, bytes], cursor: int = 0,
              match: Optional[str] = None, count: Optional[int] = None) -> Tuple[int, List[bytes]]:
        """SSCAN - итеративное сканирование set"""
        args = ['SSCAN', key, cursor]
        if match:
            args.extend(['MATCH', match])
        if count:
            args.extend(['COUNT', count])

        response = self.execute_command(
            *args, key=key, for_read=not self.is_sentinel
        )
        return int(response[0]), response[1]

    def sscan_iter(self, key: Union[str, bytes], match: Optional[str] = None,
                   count: int = 100) -> Iterator[bytes]:
        """SSCAN итератор"""
        cursor = 0
        while True:
            cursor, members = self.sscan(key, cursor, match=match, count=count)
            for member in members:
                yield member
            if cursor == 0:
                break

    def zscan(self, key: Union[str, bytes], cursor: int = 0,
              match: Optional[str] = None, count: Optional[int] = None) -> Tuple[int, List]:
        """ZSCAN - итеративное сканирование sorted set"""
        args = ['ZSCAN', key, cursor]
        if match:
            args.extend(['MATCH', match])
        if count:
            args.extend(['COUNT', count])

        response = self.execute_command(
            *args, key=key, for_read=not self.is_sentinel
        )
        new_cursor = int(response[0])
        items = response[1]
        result = []
        for i in range(0, len(items), 2):
            result.append((items[i], float(items[i + 1])))
        return new_cursor, result

    def zscan_iter(self, key: Union[str, bytes], match: Optional[str] = None,
                   count: int = 100) -> Iterator[Tuple[bytes, float]]:
        """ZSCAN итератор"""
        cursor = 0
        while True:
            cursor, items = self.zscan(key, cursor, match=match, count=count)
            for member, score in items:
                yield member, score
            if cursor == 0:
                break

    def config_get(self, parameter: str) -> Dict:
        """CONFIG GET - получить параметр конфигурации"""
        response = self.execute_command('CONFIG', 'GET', parameter)
        result = {}
        for i in range(0, len(response), 2):
            key = response[i]
            if isinstance(key, bytes):
                key = key.decode('utf-8')
            value = response[i + 1]
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            result[key] = value
        return result

    def client_list(self) -> str:
        """CLIENT LIST - список подключённых клиентов"""
        response = self.execute_command('CLIENT', 'LIST')
        if isinstance(response, bytes):
            return response.decode('utf-8')
        return response

    def cluster_keyslot(self, key: Union[str, bytes]) -> int:
        """CLUSTER KEYSLOT - вычислить slot для ключа"""
        if not self.is_cluster:
            return self._get_slot(key)

        try:
            return self.execute_command('CLUSTER', 'KEYSLOT', key)
        except:
            return self._get_slot(key)

    def get_node_from_key(self, key: Union[str, bytes]) -> ClusterNode:
        """Получить узел кластера для ключа"""
        if not self.is_cluster:
            return ClusterNode(
                host=self.host,
                port=self.port,
                node_id=f"{self.host}:{self.port}",
                slots=[],
                role='master'
            )

        slot = self._get_slot(key)
        node_id = self.cluster_slots.get(slot)

        if node_id and node_id in self.cluster_nodes_info:
            node_info = self.cluster_nodes_info[node_id]
            return node_info

        return ClusterNode(
            host=self.host,
            port=self.port,
            node_id=f"{self.host}:{self.port}",
            slots=[slot],
            role='master'
        )

    def cluster_info(self) -> str:
        """CLUSTER INFO - информация о кластере"""
        response = self.execute_command('CLUSTER', 'INFO')
        if isinstance(response, bytes):
            return response.decode('utf-8')
        return response

    # def cluster_nodes(self) -> str:
    #     """CLUSTER NODES - информация об узлах кластера"""
    #     response = self.execute_command('CLUSTER', 'NODES')
    #     if isinstance(response, bytes):
    #         return response.decode('utf-8')
    #     return response


# ============ Script Helper ============

class Script:
    """Обёртка для удобной работы с Lua скриптами"""

    def __init__(self, client: RedisClient, script: str):
        self.client = client
        self.script = script
        self.sha = client._script_sha1(script)
        self._loaded = False

    def __call__(self, keys: List = None, args: List = None, client: Optional[RedisClient] = None):
        """Выполнить скрипт

        Args:
            keys: Список ключей (KEYS в Lua)
            args: Список аргументов (ARGV в Lua)
            client: Опциональный другой клиент
        """
        if client is None:
            client = self.client

        keys = keys or []
        args = args or []

        try:
            # Пробуем EVALSHA
            return client.evalsha(self.sha, len(keys), *(keys + args))
        except RedisError as e:
            if 'NOSCRIPT' in str(e):
                # Скрипт не загружен, загружаем и выполняем через EVAL
                return client.eval(self.script, len(keys), *(keys + args))
            raise


# ============ Pipeline ============

class RedisPipeline:
    """Настоящий Pipeline с batch отправкой через один сокет"""

    def __init__(self, client: RedisClient, transaction: bool = False):
        self.client = client
        self.transaction = transaction
        self.commands: List[Tuple] = []
        self.command_keys: List[Optional[Union[str, bytes]]] = []

    def execute(self) -> List[Any]:
        """Выполнить все команды"""
        if not self.commands:
            return []

        # В режиме кластера группируем по узлам
        if self.client.is_cluster:
            return self._execute_cluster()
        else:
            return self._execute_single()

    def _execute_single(self) -> List[Any]:
        """Выполнение на одном узле"""
        pool = self.client.main_pool
        conn = None

        try:
            conn = pool.get_connection()
            if self.transaction:
                # MULTI/EXEC транзакция
                commands_with_multi = [('MULTI',)] + self.commands + [('EXEC',)]
                results = conn.execute_pipeline(commands_with_multi)
                # Возвращаем только результаты EXEC
                final_results = results[-1] if results[-1] else []
                self._refresh_sentinel_after_errors(final_results)
                return final_results
            else:
                # Простой pipeline
                results = conn.execute_pipeline(self.commands)
                self._refresh_sentinel_after_errors(results)

                if self.client.decode_responses:
                    results = [self.client._decode_response(r) if not isinstance(r, Exception) else r 
                             for r in results]

                return results

        except RedisError as e:
            if self.client.is_sentinel and self.client._is_sentinel_topology_error(e):
                try:
                    self.client.refresh_sentinel_topology()
                except RedisError as refresh_error:
                    logger.warning(f"Sentinel topology refresh failed: {refresh_error}")
            raise

        finally:
            if conn is not None:
                pool.release(conn)
            self.commands.clear()
            self.command_keys.clear()

    def _refresh_sentinel_after_errors(self, results: List[Any]):
        """Refresh Sentinel topology after rejected pipeline commands."""
        if not self.client.is_sentinel or not isinstance(results, list):
            return
        if any(
                isinstance(result, RedisError)
                and self.client._is_sentinel_topology_error(result)
                for result in results
        ):
            try:
                self.client.refresh_sentinel_topology()
            except RedisError as refresh_error:
                logger.warning(f"Sentinel topology refresh failed: {refresh_error}")

    def _execute_cluster(self) -> List[Any]:
        """Выполнение в кластере с группировкой по узлам"""
        # Группируем команды по узлам
        node_commands = defaultdict(list)
        command_order = []

        for idx, (cmd, key) in enumerate(zip(self.commands, self.command_keys)):
            if key:
                pool = self.client._get_pool_for_key(key)
            else:
                pool = self.client.main_pool

            node_id = id(pool)
            node_commands[node_id].append((idx, cmd, pool))
            command_order.append((node_id, len(node_commands[node_id]) - 1))

        # Выполняем на каждом узле
        node_results = {}
        for node_id, commands_list in node_commands.items():
            pool = commands_list[0][2]
            commands = [cmd for _, cmd, _ in commands_list]

            conn = pool.get_connection()
            try:
                results = conn.execute_pipeline(commands)
                node_results[node_id] = results
            finally:
                pool.release(conn)

        # Восстанавливаем порядок
        final_results = []
        for node_id, idx_in_node in command_order:
            result = node_results[node_id][idx_in_node]

            if self.client.decode_responses and not isinstance(result, Exception):
                result = self.client._decode_response(result)

            final_results.append(result)

        self.commands.clear()
        self.command_keys.clear()

        return final_results

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.execute()
        return False

    def __getattr__(self, name):
        """Перехват методов клиента"""
        def method(*args, **kwargs):
            # Определяем ключ из аргументов
            key = kwargs.get('key')
            if key is None and len(args) > 0:
                key = args[0]

            # Формируем команду
            cmd_name = name.upper()

            # Особые случаи
            if cmd_name == 'SET':
                cmd_args = ['SET', args[0], args[1]]
                if len(args) > 2:
                    cmd_args.extend(args[2:])
            elif cmd_name == 'HSET':
                cmd_args = ['HSET', args[0]]
                if 'mapping' in kwargs and kwargs['mapping']:
                    for k, v in kwargs['mapping'].items():
                        cmd_args.extend([k, v])
                elif len(args) >= 3:
                    cmd_args.extend([args[1], args[2]])
            else:
                cmd_args = [cmd_name] + list(args)

            self.commands.append(tuple(cmd_args))
            self.command_keys.append(key)

            return self

        return method
