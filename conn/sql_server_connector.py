import pyodbc
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine


class SQLServerConnector:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def connect(self) -> None:
        try:
            # En tu método connect() y conn_engine()
            self.connection = pyodbc.connect(
                f"DRIVER={{FreeTDS}};"
                f"SERVER={self.host};"
                f"PORT=1433;"
                f"DATABASE={self.database};"
                f"UID={self.user};"
                f"PWD={self.password};"
                f"TDS_Version=7.4;"  # Versión para SQL Server 2008 en adelante
                f"Mars_Connection=Yes;"
            )
            print("Conectado a SQL Server")
        except pyodbc.Error as e:
            print(f"Error de conexión a SQL Server: {e}")
            self.connection = None
            raise
        except Exception as e:
            print(f"Error inesperado al conectar: {e}")
            self.connection = None
            raise

    # Nota: no definimos un método llamado `connection` porque eso
    # colisionaría con el atributo `self.connection` usado para almacenar
    # el objeto de conexión. Para obtener el cursor o la conexión, use
    # `get_cursor()` o acceda directamente a `self.connection`.

    def get_cursor(self):
        if self.connection is None:
            raise Exception("No hay conexión activa.")
        try:
            return self.connection.cursor()
        except pyodbc.Error as e:
            print(f"Error al obtener el cursor: {e}")
            raise
        except Exception as e:
            print(f"Error inesperado al obtener el cursor: {e}")
            raise

    def close_connection(self) -> None:
        if self.connection:
            try:
                self.connection.close()
                print("Conexión cerrada")
            except pyodbc.Error as e:
                print(f"Error al cerrar la conexión: {e}")
            except Exception as e:
                print(f"Error inesperado al cerrar la conexión: {e}")

    def conn_engine(self) -> Engine:
        url_sqlserver = (
            f"DRIVER={{FreeTDS}};"
            f"SERVER={self.host};"
            f"PORT=1433;"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            f"TDS_Version=7.4;"  # Versión para SQL Server 2008 en adelante
            f"Mars_Connection=Yes;"
        )

        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": url_sqlserver}
        )
        return create_engine(connection_url)

    @property
    def paramstyle(self) -> str:
        """
        Expone el paramstyle utilizado por el driver pyodbc (qmark).
        Esto permite que DatabaseConnector lo detecte.
        """
        # Si usas pyodbc, generalmente es 'qmark'
        return getattr(pyodbc, "paramstyle", "qmark")


# Ejemplo de uso
if __name__ == "__main__":
    import os
    import sys
    from dotenv import load_dotenv

    sys.path.append("../conexiones")

    from conn.database_connector import DatabaseConnector

    env_path = os.path.join("../conexiones", ".env")
    load_dotenv(
        dotenv_path=env_path,
        override=True,
    )  # Recarga las variables de entorno desde el a

    load_dotenv(override=True)

    # Para SQL Server
    db = None
    try:
        sqlserver_connector = SQLServerConnector(
            host=os.environ["HOST_PRODUCCION_PROFIT"],
            database=os.environ["DB_NAME_DERECHA_PROFIT"],
            user=os.environ["DB_USER_PROFIT"],
            password=os.environ["DB_PASSWORD_PROFIT"],
        )
        sqlserver_connector.connect()
        db = DatabaseConnector(sqlserver_connector)
        cursor = db.get_cursor()
        cursor.execute("SELECT 1")
        print(cursor.fetchone())
    except Exception as e:
        print(f"Ocurrió un error en la conexión o consulta: {e}")
    finally:
        try:
            if db is not None:
                db.close_connection()
        except Exception:
            pass
