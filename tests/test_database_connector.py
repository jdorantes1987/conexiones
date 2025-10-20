from conn.database_connector import DBCursor, DatabaseConnector


class FakeConnection:
    def __init__(self):
        self.committed = False
        self.autocommit_flag = None

    def commit(self):
        self.committed = True

    def autocommit(self, value: bool):
        self.autocommit_flag = value

    def cursor(self):
        class C:
            def execute(self, q, *a, **k):
                return None

            def fetchone(self):
                return (1,)

            def fetchall(self):
                return [(1,), (2,)]

            def close(self):
                pass

        return C()

    def close(self):
        pass


class FakeConnector:
    def __init__(self):
        self.connection = FakeConnection()

    def connect(self):
        pass

    def get_cursor(self):
        return self.connection.cursor()

    def close_connection(self):
        self.connection.close()

    def conn_engine(self):
        return None


def test_database_connector_commit_autocommit_and_cursor():
    c = FakeConnector()
    db = DatabaseConnector(c)

    # commit
    db.commit()
    assert c.connection.committed is True

    # autocommit
    db.autocommit(True)
    assert c.connection.autocommit_flag is True

    # cursor wrapper
    cursor = db.get_cursor()
    assert isinstance(cursor, DBCursor)
    cursor.execute("SELECT 1")
    assert cursor.fetchone() == (1,)
    assert len(cursor.fetchall()) == 2

    db.close_connection()
