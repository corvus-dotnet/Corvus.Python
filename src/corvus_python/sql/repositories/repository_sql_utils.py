import queue
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable

import pyodbc

from ...repositories import TableDefinition
from ...sql import (
    SelectColumn,
    create_or_alter_view_over_delta_table,
    drop_views_in_schema,
)

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup


class SqlViewCreationError(RuntimeError):
    """Raised (inside an ExceptionGroup) for each view that could not be created or altered.

    Attributes
    ----------
    schema_name:
        The schema of the view that failed.
    view_name:
        The name of the view that failed.
    """

    def __init__(self, schema_name: str, view_name: str, message: str):
        super().__init__(f"Failed to create or alter view [{schema_name}].[{view_name}]: {message}")
        self.schema_name = schema_name
        self.view_name = view_name


@dataclass
class _ViewSpec:
    view_name: str
    delta_table_path: str
    select_columns: list[SelectColumn]


def _get_view_name(table: TableDefinition) -> str:
    return table.title if table.title is not None else table.name


def _build_view_spec(table: TableDefinition, build_delta_table_path_func: Callable[[str], str]) -> _ViewSpec:
    if table.schema is None:
        raise ValueError(f"Table {table.name} does not have a schema defined.")

    select_cols: list[SelectColumn] = []
    for name in table.schema.columns:
        title = table.schema.columns[name].title
        select_cols.append(SelectColumn(name, title))

    return _ViewSpec(
        view_name=_get_view_name(table),
        delta_table_path=build_delta_table_path_func(table.name),
        select_columns=select_cols,
    )


def _create_view(conn: pyodbc.Connection, schema_name: str, spec: _ViewSpec) -> None:
    create_or_alter_view_over_delta_table(
        conn,
        schema_name,
        view_name=spec.view_name,
        delta_table_path=spec.delta_table_path,
        infer_types=True,
        select_columns=spec.select_columns,
    )


def _validate_concurrency_args(
    max_workers: int,
    connection_factory: Callable[[], pyodbc.Connection] | None,
) -> None:
    if max_workers < 1:
        raise ValueError(f"max_workers must be at least 1, but was {max_workers}.")
    if max_workers > 1 and connection_factory is None:
        raise ValueError("connection_factory must be provided when max_workers is greater than 1.")


def _create_views_in_parallel(
    schema_name: str,
    specs: list[_ViewSpec],
    max_workers: int,
    connection_factory: Callable[[], pyodbc.Connection],
) -> None:
    seen_names: dict[str, str] = {}
    for spec in specs:
        key = spec.view_name.casefold()
        if key in seen_names:
            raise ValueError(
                f"Tables produce duplicate view names '{seen_names[key]}' and '{spec.view_name}' in schema "
                f"'{schema_name}'. View names must be unique (case-insensitively) when creating views in parallel."
            )
        seen_names[key] = spec.view_name

    if not specs:
        return

    # Workers pull from a shared queue (rather than fixed chunks) so slow views don't leave other workers idle.
    pending: queue.Queue[tuple[int, _ViewSpec]] = queue.Queue()
    for index, spec in enumerate(specs):
        pending.put((index, spec))

    def worker() -> tuple[list[tuple[int, SqlViewCreationError]], BaseException | None]:
        failures: list[tuple[int, SqlViewCreationError]] = []
        try:
            conn = connection_factory()
        except Exception as e:
            return failures, e

        try:
            while True:
                try:
                    index, spec = pending.get_nowait()
                except queue.Empty:
                    break
                try:
                    _create_view(conn, schema_name, spec)
                except Exception as e:
                    error = SqlViewCreationError(schema_name, spec.view_name, str(e))
                    error.__cause__ = e
                    failures.append((index, error))
        finally:
            try:
                conn.close()
            except Exception:
                # A failure to close must not hide the outcome of the views this worker processed.
                pass

        return failures, None

    worker_count = min(max_workers, len(specs))
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="sql-view-worker") as executor:
        futures = [executor.submit(worker) for _ in range(worker_count)]
        results = [future.result() for future in futures]

    all_failures: list[tuple[int, SqlViewCreationError]] = []
    connection_errors: list[BaseException] = []
    for failures, connection_error in results:
        all_failures.extend(failures)
        if connection_error is not None:
            connection_errors.append(connection_error)

    # Views left in the queue were never attempted because every worker failed to open a connection.
    while True:
        try:
            index, spec = pending.get_nowait()
        except queue.Empty:
            break
        cause = connection_errors[0] if connection_errors else None
        error = SqlViewCreationError(
            schema_name, spec.view_name, f"no worker could open a connection to create it ({cause})"
        )
        error.__cause__ = cause
        all_failures.append((index, error))

    if all_failures:
        all_failures.sort(key=lambda failure: failure[0])
        raise ExceptionGroup(
            f"Failed to create or alter {len(all_failures)} of {len(specs)} views in schema [{schema_name}]",
            [error for _, error in all_failures],
        )


def create_sql_views_from_tables(
    conn: pyodbc.Connection | None,
    schema_name: str,
    tables: list[TableDefinition],
    build_delta_table_path_func: Callable[[str], str],
    max_workers: int = 1,
    connection_factory: Callable[[], pyodbc.Connection] | None = None,
) -> None:
    """
    Create or alter a SQL view over the Delta table of each table definition.

    Each view is named after the table's title, or its name if it has no title, and selects the columns of the
    table's schema (aliased to their titles, where set) with Synapse inferring their types.

    Parameters
    ----------
    conn:
        An open pyodbc connection to the Synapse workspace. Used to create every view when max_workers is 1. Not used
        when max_workers is greater than 1, so may be None in that case.
    schema_name:
        The schema for the views, e.g. "dbo".
    tables:
        The table definitions to create views over. Every table must have a schema.
    build_delta_table_path_func:
        Function mapping a table name to the path of its Delta table, e.g.
        "abfss://container@storageaccount.dfs.core.windows.net/path/to/table".
    max_workers:
        The number of views to create concurrently. Defaults to 1, which creates the views one at a time on conn and
        raises the first error encountered. When greater than 1, each worker opens its own connection from
        connection_factory (pyodbc connections must not be shared across threads), uses it for all the views it
        creates and closes it when done. Every view is attempted, and any failures are raised together at the end.
    connection_factory:
        Function that opens a new pyodbc connection to the same database as conn. Required when max_workers is
        greater than 1, and called once per worker. To acquire an AAD token once and reuse it across all workers,
        close over a pre-acquired token, e.g.
        ``lambda: get_synapse_sql_pyodbc_connection_with_token(workspace_name, database, token)``.

    Raises
    ------
    ValueError
        If a table has no schema, max_workers is less than 1, max_workers is greater than 1 without a
        connection_factory, or (when max_workers is greater than 1) two tables produce the same view name. Raised
        before any view is created.
    ExceptionGroup
        When max_workers is greater than 1 and any view could not be created. Contains one SqlViewCreationError per
        failed view, naming the view and chaining the underlying error.
    """
    _validate_concurrency_args(max_workers, connection_factory)

    specs = [_build_view_spec(table, build_delta_table_path_func) for table in tables]

    if max_workers == 1:
        if conn is None:
            raise ValueError("conn must be provided when max_workers is 1.")
        for spec in specs:
            _create_view(conn, schema_name, spec)
        return

    assert connection_factory is not None
    _create_views_in_parallel(schema_name, specs, max_workers, connection_factory)


def sync_sql_views_from_tables(
    conn: pyodbc.Connection,
    schema_name: str,
    tables: list[TableDefinition],
    build_delta_table_path_func: Callable[[str], str],
    max_workers: int = 1,
    connection_factory: Callable[[], pyodbc.Connection] | None = None,
) -> None:
    """
    Make the views in a schema match a set of table definitions.

    Drops the views in the schema that none of the tables produce (e.g. for tables that have been removed or
    renamed), then creates or alters a view for each table as create_sql_views_from_tables does. Views that are
    still wanted are altered in place rather than dropped and recreated. View names are compared
    case-insensitively.

    This replaces calling drop_views_in_schema followed by create_sql_views_from_tables.

    Parameters
    ----------
    conn:
        An open pyodbc connection to the Synapse workspace. Always used to drop stale views (sequentially), and to
        create the views when max_workers is 1.
    schema_name:
        The schema for the views, e.g. "dbo".
    tables:
        The table definitions to create views over. Every table must have a schema.
    build_delta_table_path_func:
        Function mapping a table name to the path of its Delta table, e.g.
        "abfss://container@storageaccount.dfs.core.windows.net/path/to/table".
    max_workers:
        The number of views to create concurrently. See create_sql_views_from_tables.
    connection_factory:
        Function that opens a new pyodbc connection to the same database as conn. Required when max_workers is
        greater than 1, and called once per worker. To acquire an AAD token once and reuse it across all workers,
        close over a pre-acquired token, e.g.
        ``lambda: get_synapse_sql_pyodbc_connection_with_token(workspace_name, database, token)``.

    Raises
    ------
    ValueError
        If a table has no schema or the concurrency arguments are invalid. Raised before any view is dropped.
    ExceptionGroup
        When max_workers is greater than 1 and any view could not be created. See create_sql_views_from_tables.
    """
    _validate_concurrency_args(max_workers, connection_factory)
    for table in tables:
        if table.schema is None:
            raise ValueError(f"Table {table.name} does not have a schema defined.")

    drop_views_in_schema(conn, schema_name=schema_name, exclude=[_get_view_name(table) for table in tables])

    create_sql_views_from_tables(
        conn,
        schema_name,
        tables,
        build_delta_table_path_func,
        max_workers=max_workers,
        connection_factory=connection_factory,
    )
