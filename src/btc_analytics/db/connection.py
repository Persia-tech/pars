from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from typing import Any

try:
    import psycopg
except ModuleNotFoundError:  # pragma: no cover - allows running unit tests without db deps
    psycopg = None


def get_connection(database_url: str) -> Any:
    if psycopg is None:
        raise ModuleNotFoundError("psycopg is required for database connectivity")
    return psycopg.connect(database_url)


@contextmanager
def transaction(conn: Any) -> Iterator[Any]:
    with conn.transaction():
        yield conn


def run_sql_file(conn: psycopg.Connection, file_path: str) -> None:
    sql = Path(file_path).read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
