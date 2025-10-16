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
        return self._connector.connection.commit()

    def autocommit(self, value: bool):
        self._connector.connection.autocommit(value)

    @property
    def connection(self):
        return self._connector.connection
