from typing import Any

from conn.connection_protocolo import DBConnectionProtocol, CursorProtocol


class DBCursor(CursorProtocol):
    """Pequeño wrapper que normaliza la API del cursor entre distintos drivers.

    Implementa execute, fetchone, fetchall y close, y soporta el protocolo de
    context manager para usarse con `with`.
    """

    def __init__(self, raw_cursor: Any):
        if raw_cursor is None:
            raise RuntimeError("Cursor subyacente no puede ser None")
        self._cursor = raw_cursor

    def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        return self._cursor.execute(query, *args, **kwargs)

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> Any:
        return self._cursor.fetchall()

    def lastrowid(self) -> Any:
        return self._cursor.lastrowid

    def close(self) -> None:
        try:
            self._cursor.close()
        except Exception:
            # No queremos que un fallo al cerrar el cursor rompa el flujo
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class DatabaseConnector:
    """
    Clase fachada que utiliza cualquier conector que implemente DBConnectionProtocol.
    """

    def __init__(self, connector: DBConnectionProtocol):
        if not isinstance(connector, DBConnectionProtocol):
            raise TypeError("El conector debe implementar DBConnectionProtocol.")
        self._connector = connector

    def get_cursor(self) -> CursorProtocol:
        raw = self._connector.get_cursor()
        # Devolvemos un wrapper que expone execute/fetchone/fetchall/close
        return DBCursor(raw)

    def close_connection(self):
        self._connector.close_connection()

    def conn_engine(self):
        return self._connector.conn_engine()

    def commit(self):
        if self._connector.connection is None:
            raise RuntimeError("No active connection to commit.")
        return self._connector.connection.commit()

    def autocommit(self, value: bool):
        if self._connector.connection is None:
            raise RuntimeError("No active connection to set autocommit.")
        # Algunos objetos de conexión (pyodbc) no exponen `autocommit` como método
        # pero permiten cambiar la propiedad o usan otro mecanismo. Intentamos usar
        # el método si existe, de lo contrario asignamos el atributo si está presente.
        conn = self._connector.connection
        if hasattr(conn, "autocommit") and callable(getattr(conn, "autocommit")):
            conn.autocommit(value)
        else:
            try:
                setattr(conn, "autocommit", value)
            except Exception:
                raise RuntimeError("El objeto de conexión no soporta autocommit")

    @property
    def connection(self):
        return self._connector.connection
