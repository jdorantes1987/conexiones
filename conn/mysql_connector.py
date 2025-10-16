import pymysql
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class MySQLConnector:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def connect(self) -> None:
        try:
            self.connection = pymysql.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                cursorclass=pymysql.cursors.DictCursor,  # <-- Esta línea cambia el tipo de cursor
            )
            print("Conectado a MySQL")
        except pymysql.MySQLError as e:
            print(f"Error de conexión a MySQL: {e}")
            self.connection = None
            raise
        except Exception as e:
            print(f"Error inesperado al conectar: {e}")
            self.connection = None
            raise

    def get_cursor(self):
        if self.connection is None:
            raise Exception("No hay conexión activa.")
        try:
            return self.connection.cursor()
        except pymysql.MySQLError as e:
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
            except pymysql.MySQLError as e:
                print(f"Error al cerrar la conexión: {e}")
            except Exception as e:
                print(f"Error inesperado al cerrar la conexión: {e}")

    def conn_engine(self) -> Engine:
        connection_url = (
            f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/"
            f"{self.database}?auth_plugin=mysql_native_password"
        )
        return create_engine(connection_url)


# Ejemplo de uso
if __name__ == "__main__":
    import os

    from dotenv import load_dotenv

    from conn.database_connector import DatabaseConnector

    load_dotenv(override=True)

    try:
        mysql_connector = MySQLConnector(
            host=os.environ["HOST_PRODUCCION_MKWSP"],
            database=os.environ["DB_NAME_MKWSP"],
            user=os.environ["DB_USER_MKWSP"],
            password=os.environ["DB_PASSWORD_MKWSP"],
        )
        mysql_connector.connect()
        db = DatabaseConnector(mysql_connector)
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
