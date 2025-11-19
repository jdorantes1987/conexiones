from typing import Protocol, runtime_checkable, Any, Optional, List
from sqlalchemy.engine import Engine


class CursorProtocol(Protocol):
    """Interfaz mínima que esperamos de un cursor de base de datos."""

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any: ...

    def executemany(self, query: str, param_list: List[Any]) -> Any: ...

    def fetchone(self) -> Any: ...

    def fetchall(self) -> Any: ...

    def close(self) -> None: ...


class ConnectionProtocol(Protocol):
    """Interfaz mínima que esperamos de un objeto de conexión.

    Esto cubre los métodos que usa `DatabaseConnector` y los conectores.
    """

    def commit(self) -> None: ...

    def autocommit(self, value: bool) -> None: ...

    def cursor(self) -> CursorProtocol: ...

    def close(self) -> None: ...


@runtime_checkable
class DBConnectionProtocol(Protocol):
    """Protocolo que debe implementar cualquier conector concreto.

    - `connection` puede ser None si no se ha establecido la conexión aún.
    - `get_cursor()` devuelve un cursor compatible con `CursorProtocol`.
    """

    # Usamos Any aquí para aceptar las distintas implementaciones concretas
    # (pymysql, pyodbc, etc.) sin provocar errores de invariancia de tipo.
    connection: Optional[Any]

    def connect(self) -> None: ...

    def get_cursor(self) -> Any: ...

    def close_connection(self) -> None: ...

    def conn_engine(self) -> Engine: ...
