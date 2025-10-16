import pyodbc
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
            self.connection = pyodbc.connect(
                f"DRIVER={{SQL Server}};"
                f"SERVER={self.host};DATABASE={self.database};UID={self.user};PWD={self.password}"
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

    def connection(self) -> pyodbc.Connection:
        if self.connection is None:
            raise Exception("No hay conexión activa.")
        return self.connection

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
        url_sqlserver = f"DRIVER={{SQL Server}}; SERVER={self.host};DATABASE={self.database};UID={self.user};PWD={self.password}"
        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": url_sqlserver}
        )
        return create_engine(connection_url)


# Ejemplo de uso
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    from conn.database_connector import DatabaseConnector

    load_dotenv(override=True)

    # Para SQL Server
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
            db.close_connection()
        except Exception:
            pass
