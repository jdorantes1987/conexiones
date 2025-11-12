from typing import Any, List, Tuple, Dict, Union

from conn.connection_protocolo import DBConnectionProtocol, CursorProtocol


class DBCursor(CursorProtocol):
    """Pequeño wrapper que normaliza la API del cursor entre distintos drivers."""

    def __init__(self, raw_cursor: Any):
        if raw_cursor is None:
            raise RuntimeError("Cursor subyacente no puede ser None")
        self._cursor = raw_cursor

    # --- Nuevo: Atributo 'description' del cursor
    @property
    def description(self):
        """Permite acceder a la descripción de las columnas, crucial para _row_to_dict."""
        return getattr(self._cursor, "description", None)

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
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class DatabaseConnector:
    """
    Clase fachada que utiliza cualquier conector que implemente DBConnectionProtocol.
    Ahora incluye la detección y exposición del paramstyle del driver.
    """

    def __init__(self, connector: DBConnectionProtocol):
        if not isinstance(connector, DBConnectionProtocol):
            raise TypeError("El conector debe implementar DBConnectionProtocol.")
        self._connector = connector

        # --- Nuevo: Detección y almacenamiento de paramstyle ---
        # 1. Intentamos obtenerlo del conector que envuelve (que debería exponerlo).
        self._paramstyle = getattr(connector, "paramstyle", None)

        # 2. Si no lo tiene, intentamos obtenerlo de la conexión subyacente.
        if self._paramstyle is None and self._connector.connection is not None:
            self._paramstyle = getattr(self._connector.connection, "paramstyle", None)

        # 3. Usamos 'qmark' (o 'format') como valor predeterminado si no se detecta.
        # 'qmark' (e.g., SQLite, pyodbc) es muy común, pero 'format' (e.g., MySQLdb)
        # también se usa. Usaremos 'format' como fallback seguro si el driver lo necesita.
        if self._paramstyle is None:
            self._paramstyle = "format"

    def _format_query(
        self, sql_template: str, params: List[Any]
    ) -> Tuple[str, Union[Tuple[Any, ...], Dict[str, Any]]]:
        """
        sql_template usa '{}' como marcador por cada parámetro.
        Devuelve (sql_final, params_final) donde params_final es tuple o dict
        según el paramstyle detectado.
        """
        if params is None:
            params = []

        paramstyle = self.paramstyle  # garantizado como str
        parts = sql_template.split("{}")
        if len(parts) - 1 != len(params):
            raise ValueError(
                "Número de placeholders '{}' no coincide con la cantidad de params"
            )

        placeholders: List[str] = []
        named_params: Dict[str, Any] = {}
        for i, val in enumerate(params, start=1):
            if paramstyle == "qmark":
                ph = "?"
                placeholders.append(ph)
            elif paramstyle == "format":
                ph = "%s"
                placeholders.append(ph)
            elif paramstyle == "numeric":
                ph = f":{i}"
                placeholders.append(ph)
            elif paramstyle == "named":
                name = f"p{i}"
                ph = f":{name}"
                placeholders.append(ph)
                named_params[name] = val
            elif paramstyle == "pyformat":
                name = f"p{i}"
                ph = f"%({name})s"
                placeholders.append(ph)
                named_params[name] = val
            else:
                ph = "%s"
                placeholders.append(ph)

        sql_final = "".join(p + ph for p, ph in zip(parts, placeholders + [""]))

        if paramstyle in ("named", "pyformat"):
            return sql_final, named_params
        else:
            return sql_final, tuple(params)

    def execute(self, sql: str, params: List[Any]):
        """
        Ejecuta la consulta formateando placeholders '{}' según el paramstyle detectado.
        Devuelve el DBCursor ya posicionado (no lo cierra).
        """
        cursor = self.get_cursor()
        sql_final, params_final = self._format_query(sql, params or [])

        # Algunos drivers aceptan execute(sql) cuando no hay params, otros requieren tuple/dict.
        try:
            if isinstance(params_final, dict):
                cursor.execute(sql_final, params_final)
            else:
                # params_final es tuple
                if len(params_final) == 0:
                    cursor.execute(sql_final)
                else:
                    cursor.execute(sql_final, params_final)
        except TypeError:
            # fallback: pasar parámetros como *args si el driver los espera así
            if isinstance(params_final, tuple):
                cursor.execute(sql_final, *params_final)
            else:
                cursor.execute(sql_final, params_final)
        return cursor

    def get_cursor(self) -> CursorProtocol:
        raw = self._connector.get_cursor()
        return DBCursor(raw)

    def get_paramstyle(self):
        return self._paramstyle

    def close_connection(self):
        self._connector.close_connection()

    def conn_engine(self):
        return self._connector.conn_engine()

    def commit(self):
        if self._connector.connection is None:
            raise RuntimeError("No active connection to commit.")
        return self._connector.connection.commit()

    def rollback(self):
        if self._connector.connection is None:
            raise RuntimeError("No active connection to rollback.")
        return self._connector.connection.rollback()

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

    # def row_to_dict(self, cur, row):
    #     """
    #     Normaliza una fila retornada por cursor.fetchone()/fetchall() a dict.
    #     Maneja objetos tipo mapping, tuplas/listas y construye nombres de columnas
    #     a partir de cursor.description cuando sea necesario.
    #     """
    #     if row is None:
    #         return None

    #     # Si ya se comporta como mapping (p. ej. dict cursor)
    #     if hasattr(row, "keys"):
    #         try:
    #             return dict(row)
    #         except Exception:
    #             pass

    #     cols = [desc[0] for desc in cur._cursor.description]

    #     try:
    #         return dict(zip(cols, row))
    #     except Exception:
    #         return None

    def rows_to_dict(self, cur, row):
        """
        Normaliza una fila retornada por cursor.fetchone()/fetchall() a dict.
        Maneja objetos tipo mapping, tuplas/listas y construye nombres de columnas
        a partir de cursor.description cuando sea necesario.
        """
        if row is None:
            return None

        # Si ya se comporta como mapping (p. ej. dict cursor)
        if hasattr(row, "keys"):
            try:
                return dict(row)
            except Exception:
                pass

        # si es una lista o tupla
        if isinstance(row, (list, tuple)):
            if isinstance(row, list):
                # lista de filas
                return [self.rows_to_dict(cur, r) for r in row]
            else:
                # tupla única
                return self.rows_to_dict(cur, row)

        cols = [desc[0] for desc in cur._cursor.description]

        try:
            return dict(zip(cols, row))
        except Exception:
            return None

    @property
    def connection(self):
        return self._connector.connection

        # --- Nuevo: Propiedad para exponer el paramstyle ---

    @property
    def paramstyle(self):
        """Retorna el estilo de parámetros esperado por el driver de la BD subyacente."""
        return self._paramstyle
