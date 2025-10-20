from conn.connection_protocolo import DBConnectionProtocol


class DatabaseConnector:
    """
    Clase fachada que utiliza cualquier conector que implemente DBConnectionProtocol.
    """

    def __init__(self, connector: DBConnectionProtocol):
        if not isinstance(connector, DBConnectionProtocol):
            raise TypeError("El conector debe implementar DBConnectionProtocol.")
        self._connector = connector

    def get_cursor(self):
        return self._connector.get_cursor()

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
