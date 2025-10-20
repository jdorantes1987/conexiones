from conn.database_connector import DBCursor


class FakeRawCursor:
    def __init__(self):
        self.queries = []
        self.closed = False

    def execute(self, query, *args, **kwargs):
        self.queries.append((query, args, kwargs))
        return None

    def fetchone(self):
        return {"id": 1}

    def fetchall(self):
        return [{"id": 1}, {"id": 2}]

    def close(self):
        self.closed = True


def test_dbcursor_basic():
    raw = FakeRawCursor()
    cursor = DBCursor(raw)

    cursor.execute("SELECT 1")
    assert raw.queries and raw.queries[0][0] == "SELECT 1"

    row = cursor.fetchone()
    assert row == {"id": 1}

    rows = cursor.fetchall()
    assert isinstance(rows, list) and len(rows) == 2

    cursor.close()
    assert raw.closed is True


def test_dbcursor_context_manager():
    raw = FakeRawCursor()
    with DBCursor(raw) as c:
        c.execute("SELECT 2")
    assert raw.closed is True
