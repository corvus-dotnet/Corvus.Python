import re
import sys
import threading

import pandera.polars as pa
import polars as pl
import pyodbc
import pytest

from corvus_python.repositories import TableDefinition
from corvus_python.sql.repositories import (
    SqlViewCreationError,
    create_sql_views_from_tables,
    sync_sql_views_from_tables,
)

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup

SCHEMA = pa.DataFrameSchema({"code": pa.Column(pl.String, title="Code"), "value": pa.Column(pl.Int64)})

_VIEW_NAME_PATTERN = re.compile(r"CREATE OR ALTER VIEW \[[^\]]+\]\.\[([^\]]+)\]")


def _path(table_name: str) -> str:
    return f"abfss://container@account.dfs.core.windows.net/silver/{table_name}"


def _tables(*names: str) -> list[TableDefinition]:
    return [TableDefinition(name, SCHEMA) for name in names]


class FakeCursor:
    def __init__(self, conn: "FakeConnection"):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, *params):
        self._conn.execute(sql, params)

    def fetchall(self):
        return [(name,) for name in self._conn.existing_views]


class FakeConnection:
    """Stands in for a pyodbc connection, recording the statements run on it and the threads that used it."""

    def __init__(self, existing_views=(), failing_views=(), on_create=None):
        self.existing_views = list(existing_views)
        self.failing_views = set(failing_views)
        self.on_create = on_create
        self.statements: list[str] = []
        self.threads: set[int] = set()
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass

    def close(self):
        self.closed = True

    def execute(self, sql, params):
        assert not self.closed, "statement executed on a closed connection"
        self.threads.add(threading.get_ident())
        self.statements.append(sql)
        match = _VIEW_NAME_PATTERN.match(sql)
        if match:
            if self.on_create is not None:
                self.on_create()
            if match.group(1) in self.failing_views:
                raise pyodbc.ProgrammingError("42000", f"Cannot bind view {match.group(1)}")

    @property
    def created_views(self) -> list[str]:
        return [m.group(1) for m in (_VIEW_NAME_PATTERN.match(s) for s in self.statements) if m]

    @property
    def dropped_views(self) -> list[str]:
        return [s for s in self.statements if s.startswith("DROP VIEW")]


class FakeConnectionFactory:
    def __init__(self, **connection_kwargs):
        self._connection_kwargs = connection_kwargs
        self._lock = threading.Lock()
        self.connections: list[FakeConnection] = []

    def __call__(self) -> FakeConnection:
        conn = FakeConnection(**self._connection_kwargs)
        with self._lock:
            self.connections.append(conn)
        return conn

    @property
    def created_views(self) -> list[str]:
        return [view for conn in self.connections for view in conn.created_views]


class TestCreateSqlViewsFromTablesSequentially:
    def test_creates_every_view_in_order_on_the_given_connection(self):
        conn = FakeConnection()
        tables = [TableDefinition("medals", SCHEMA, title="Medals"), TableDefinition("athletes", SCHEMA)]

        create_sql_views_from_tables(conn, "dbo", tables, _path)

        assert conn.created_views == ["Medals", "athletes"]
        assert f"BULK '{_path('medals')}'" in conn.statements[0]
        assert "[code] AS [Code]" in conn.statements[0]
        assert not conn.closed

    def test_raises_the_first_error_unwrapped(self):
        conn = FakeConnection(failing_views={"b"})

        with pytest.raises(pyodbc.ProgrammingError):
            create_sql_views_from_tables(conn, "dbo", _tables("a", "b", "c"), _path)

        assert conn.created_views == ["a", "b"]

    def test_raises_for_a_table_without_a_schema_before_creating_any_view(self):
        conn = FakeConnection()
        tables = _tables("a") + [TableDefinition("no_schema", None)]

        with pytest.raises(ValueError, match="no_schema does not have a schema"):
            create_sql_views_from_tables(conn, "dbo", tables, _path)

        assert conn.statements == []


class TestCreateSqlViewsFromTablesInParallel:
    def test_creates_a_view_for_every_table(self):
        factory = FakeConnectionFactory()
        names = [f"table_{i}" for i in range(20)]

        create_sql_views_from_tables(None, "dbo", _tables(*names), _path, max_workers=4, connection_factory=factory)

        assert sorted(factory.created_views) == sorted(names)

    def test_does_not_use_the_given_connection(self):
        conn = FakeConnection()
        factory = FakeConnectionFactory()

        create_sql_views_from_tables(conn, "dbo", _tables("a", "b"), _path, max_workers=2, connection_factory=factory)

        assert conn.statements == []

    def test_opens_one_connection_per_worker_and_closes_them(self):
        factory = FakeConnectionFactory()

        create_sql_views_from_tables(
            None, "dbo", _tables(*[f"t{i}" for i in range(20)]), _path, max_workers=4, connection_factory=factory
        )

        assert len(factory.connections) == 4
        assert all(conn.closed for conn in factory.connections)
        for conn in factory.connections:
            assert len(conn.threads) <= 1, "a connection was shared between threads"

    def test_opens_no_more_connections_than_tables(self):
        factory = FakeConnectionFactory()

        create_sql_views_from_tables(None, "dbo", _tables("a", "b"), _path, max_workers=8, connection_factory=factory)

        assert len(factory.connections) == 2

    def test_opens_no_connections_when_there_are_no_tables(self):
        factory = FakeConnectionFactory()

        create_sql_views_from_tables(None, "dbo", [], _path, max_workers=8, connection_factory=factory)

        assert factory.connections == []

    def test_creates_views_concurrently(self):
        # Each worker's first create waits until all of them are creating at once, or times out.
        barrier = threading.Barrier(3, timeout=10)
        waited: set[int] = set()

        def wait_for_other_workers():
            if threading.get_ident() not in waited:
                waited.add(threading.get_ident())
                barrier.wait()

        factory = FakeConnectionFactory(on_create=wait_for_other_workers)

        create_sql_views_from_tables(
            None, "dbo", _tables(*[f"t{i}" for i in range(9)]), _path, max_workers=3, connection_factory=factory
        )

        assert not barrier.broken
        assert len(factory.created_views) == 9

    def test_attempts_every_view_and_raises_all_failures_together(self):
        factory = FakeConnectionFactory(failing_views={"b", "d"})
        names = ["a", "b", "c", "d", "e"]

        with pytest.raises(ExceptionGroup) as exc_info:
            create_sql_views_from_tables(None, "dbo", _tables(*names), _path, max_workers=2, connection_factory=factory)

        assert sorted(factory.created_views) == names
        group = exc_info.value
        assert "2 of 5 views" in str(group)
        errors = group.exceptions
        assert all(isinstance(error, SqlViewCreationError) for error in errors)
        assert [error.view_name for error in errors] == ["b", "d"]
        assert "[dbo].[b]" in str(errors[0])
        assert "Cannot bind view b" in str(errors[0])
        assert isinstance(errors[0].__cause__, pyodbc.ProgrammingError)
        assert all(conn.closed for conn in factory.connections)

    def test_reports_every_view_when_no_connection_can_be_opened(self):
        def failing_factory():
            raise pyodbc.OperationalError("08001", "Login timeout expired")

        with pytest.raises(ExceptionGroup) as exc_info:
            create_sql_views_from_tables(
                None, "dbo", _tables("a", "b", "c"), _path, max_workers=2, connection_factory=failing_factory
            )

        errors = exc_info.value.exceptions
        assert [error.view_name for error in errors] == ["a", "b", "c"]
        assert "Login timeout expired" in str(errors[0])
        assert isinstance(errors[0].__cause__, pyodbc.OperationalError)

    def test_remaining_workers_create_the_views_when_one_cannot_connect(self):
        healthy = FakeConnectionFactory()
        calls = []
        lock = threading.Lock()

        def flaky_factory():
            with lock:
                calls.append(None)
                first = len(calls) == 1
            if first:
                raise pyodbc.OperationalError("08001", "Login timeout expired")
            return healthy()

        names = [f"t{i}" for i in range(6)]
        create_sql_views_from_tables(
            None, "dbo", _tables(*names), _path, max_workers=3, connection_factory=flaky_factory
        )

        assert sorted(healthy.created_views) == sorted(names)

    def test_requires_a_connection_factory(self):
        with pytest.raises(ValueError, match="connection_factory must be provided"):
            create_sql_views_from_tables(FakeConnection(), "dbo", _tables("a"), _path, max_workers=2)

    def test_rejects_fewer_than_one_worker(self):
        with pytest.raises(ValueError, match="max_workers must be at least 1"):
            create_sql_views_from_tables(FakeConnection(), "dbo", _tables("a"), _path, max_workers=0)

    def test_rejects_tables_producing_the_same_view_name(self):
        factory = FakeConnectionFactory()
        tables = [TableDefinition("a", SCHEMA, title="Results"), TableDefinition("results", SCHEMA)]

        with pytest.raises(ValueError, match="duplicate view names"):
            create_sql_views_from_tables(None, "dbo", tables, _path, max_workers=2, connection_factory=factory)

        assert factory.connections == []


class TestSyncSqlViewsFromTables:
    def test_drops_only_views_no_table_produces_then_creates_every_view(self):
        conn = FakeConnection(existing_views=["MEDALS", "Old Results", "athletes", "renamed_from"])
        tables = [
            TableDefinition("medals", SCHEMA),
            TableDefinition("athletes_v2", SCHEMA, title="Athletes"),
            TableDefinition("new_table", SCHEMA),
        ]

        sync_sql_views_from_tables(conn, "dbo", tables, _path)

        assert conn.dropped_views == ["DROP VIEW [dbo].[Old Results]", "DROP VIEW [dbo].[renamed_from]"]
        assert conn.created_views == ["medals", "Athletes", "new_table"]

    def test_drops_before_creating(self):
        conn = FakeConnection(existing_views=["stale"])

        sync_sql_views_from_tables(conn, "dbo", _tables("a"), _path)

        drop_index = conn.statements.index("DROP VIEW [dbo].[stale]")
        create_index = next(i for i, s in enumerate(conn.statements) if s.startswith("CREATE OR ALTER VIEW"))
        assert drop_index < create_index

    def test_drops_on_the_given_connection_and_creates_in_parallel(self):
        conn = FakeConnection(existing_views=["a", "stale"])
        factory = FakeConnectionFactory()
        names = [f"t{i}" for i in range(10)] + ["a"]

        sync_sql_views_from_tables(conn, "dbo", _tables(*names), _path, max_workers=4, connection_factory=factory)

        assert conn.dropped_views == ["DROP VIEW [dbo].[stale]"]
        assert conn.created_views == []
        assert sorted(factory.created_views) == sorted(names)
        assert len(factory.connections) == 4

    def test_raises_all_creation_failures_together_after_dropping(self):
        conn = FakeConnection(existing_views=["stale"])
        factory = FakeConnectionFactory(failing_views={"b", "c"})

        with pytest.raises(ExceptionGroup) as exc_info:
            sync_sql_views_from_tables(
                conn, "dbo", _tables("a", "b", "c"), _path, max_workers=2, connection_factory=factory
            )

        assert conn.dropped_views == ["DROP VIEW [dbo].[stale]"]
        assert [error.view_name for error in exc_info.value.exceptions] == ["b", "c"]

    def test_validates_tables_before_dropping_any_view(self):
        conn = FakeConnection(existing_views=["stale"])
        tables = _tables("a") + [TableDefinition("no_schema", None)]

        with pytest.raises(ValueError, match="no_schema does not have a schema"):
            sync_sql_views_from_tables(conn, "dbo", tables, _path)

        assert conn.statements == []

    def test_validates_concurrency_arguments_before_dropping_any_view(self):
        conn = FakeConnection(existing_views=["stale"])

        with pytest.raises(ValueError, match="connection_factory must be provided"):
            sync_sql_views_from_tables(conn, "dbo", _tables("a"), _path, max_workers=4)

        assert conn.statements == []
