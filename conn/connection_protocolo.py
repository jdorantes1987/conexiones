from typing import Protocol, runtime_checkable, Any
from sqlalchemy.engine import Engine


@runtime_checkable
class DBConnectionProtocol(Protocol):
    def connect(self) -> None:
        pass

    def get_cursor(self) -> Any:
        pass

    def close_connection(self) -> None:
        pass

    def conn_engine(self) -> Engine:
        pass
