import pytest

from corvus_python.sql.sql_utils import (
    SelectColumn,
    WithColumn,
    drop_views_in_schema,
    generate_create_or_alter_view_over_delta_table_ddl,
)

DELTA_PATH = "abfss://container@storageaccount.dfs.core.windows.net/path/to/table"


class TestGenerateCreateOrAlterViewOverDeltaTableDdl:
    def test_infer_types_single_column_no_title(self):
        result = generate_create_or_alter_view_over_delta_table_ddl(
            schema_name="dbo",
            view_name="vw_test",
            delta_table_path=DELTA_PATH,
            infer_types=True,
            select_columns=[SelectColumn(name="col1")],
        )

        expected = (
            "CREATE OR ALTER VIEW [dbo].[vw_test]\n"
            "AS\n"
            "SELECT\n"
            "    [col1]\n"
            "FROM OPENROWSET(\n"
            f"        BULK '{DELTA_PATH}',\n"
            "FORMAT = 'DELTA'\n"
            "    ) AS [result]"
        )
        assert result == expected

    def test_infer_types_single_column_with_title(self):
        result = generate_create_or_alter_view_over_delta_table_ddl(
            schema_name="dbo",
            view_name="vw_test",
            delta_table_path=DELTA_PATH,
            infer_types=True,
            select_columns=[SelectColumn(name="col1", title="Column One")],
        )

        assert "[col1] AS [Column One]" in result

    def test_infer_types_multiple_columns_mixed_titles(self):
        result = generate_create_or_alter_view_over_delta_table_ddl(
            schema_name="myschema",
            view_name="vw_multi",
            delta_table_path=DELTA_PATH,
            infer_types=True,
            select_columns=[
                SelectColumn(name="id"),
                SelectColumn(name="first_name", title="First Name"),
                SelectColumn(name="last_name"),
            ],
        )

        assert "CREATE OR ALTER VIEW [myschema].[vw_multi]" in result
        assert "[id]" in result
        assert "[first_name] AS [First Name]" in result
        assert "[last_name]" in result
        # Columns should be comma-separated
        assert ",\n        " in result

    def test_infer_types_raises_when_select_columns_missing(self):
        with pytest.raises(ValueError, match="select_columns must be provided"):
            generate_create_or_alter_view_over_delta_table_ddl(
                schema_name="dbo",
                view_name="vw_test",
                delta_table_path=DELTA_PATH,
                infer_types=True,
                select_columns=None,
            )

    def test_infer_types_raises_when_select_columns_empty(self):
        with pytest.raises(ValueError, match="select_columns must be provided"):
            generate_create_or_alter_view_over_delta_table_ddl(
                schema_name="dbo",
                view_name="vw_test",
                delta_table_path=DELTA_PATH,
                infer_types=True,
                select_columns=[],
            )

    def test_no_infer_types_single_with_column_no_title(self):
        result = generate_create_or_alter_view_over_delta_table_ddl(
            schema_name="dbo",
            view_name="vw_typed",
            delta_table_path=DELTA_PATH,
            infer_types=False,
            with_columns=[WithColumn(name="col1", type="INT")],
        )

        expected = (
            "CREATE OR ALTER VIEW [dbo].[vw_typed]\n"
            "AS\n"
            "SELECT\n"
            "    *\n"
            "FROM OPENROWSET(\n"
            f"        BULK '{DELTA_PATH}',\n"
            "FORMAT = 'DELTA'\n"
            "    )\n"
            "    WITH (\n"
            "        [col1] INT '$.col1'\n"
            "    ) AS [result]"
        )
        assert result == expected

    def test_no_infer_types_with_column_with_title(self):
        result = generate_create_or_alter_view_over_delta_table_ddl(
            schema_name="dbo",
            view_name="vw_typed",
            delta_table_path=DELTA_PATH,
            infer_types=False,
            with_columns=[WithColumn(name="col1", type="VARCHAR(100)", title="Column One")],
        )

        assert "[Column One] VARCHAR(100) '$.col1'" in result

    def test_no_infer_types_multiple_with_columns(self):
        result = generate_create_or_alter_view_over_delta_table_ddl(
            schema_name="dbo",
            view_name="vw_typed",
            delta_table_path=DELTA_PATH,
            infer_types=False,
            with_columns=[
                WithColumn(name="id", type="INT"),
                WithColumn(name="name", type="VARCHAR(200)", title="Full Name"),
                WithColumn(name="created_at", type="DATETIME2"),
            ],
        )

        assert "SELECT\n    *" in result
        assert "[id] INT '$.id'" in result
        assert "[Full Name] VARCHAR(200) '$.name'" in result
        assert "[created_at] DATETIME2 '$.created_at'" in result
        assert "WITH (" in result

    def test_no_infer_types_raises_when_with_columns_missing(self):
        with pytest.raises(ValueError, match="with_columns must be provided"):
            generate_create_or_alter_view_over_delta_table_ddl(
                schema_name="dbo",
                view_name="vw_test",
                delta_table_path=DELTA_PATH,
                infer_types=False,
                with_columns=None,
            )

    def test_no_infer_types_raises_when_with_columns_empty(self):
        with pytest.raises(ValueError, match="with_columns must be provided"):
            generate_create_or_alter_view_over_delta_table_ddl(
                schema_name="dbo",
                view_name="vw_test",
                delta_table_path=DELTA_PATH,
                infer_types=False,
                with_columns=[],
            )

    def test_delta_table_path_appears_in_bulk_clause(self):
        custom_path = "abfss://mycontainer@myaccount.dfs.core.windows.net/my/table"
        result = generate_create_or_alter_view_over_delta_table_ddl(
            schema_name="dbo",
            view_name="vw_test",
            delta_table_path=custom_path,
            infer_types=True,
            select_columns=[SelectColumn(name="x")],
        )

        assert f"BULK '{custom_path}'" in result
        assert "FORMAT = 'DELTA'" in result


class _FakeCursor:
    def __init__(self, conn: "_FakeConnection"):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, *params):
        self._conn.executed.append((sql, params))

    def fetchall(self):
        return [(name,) for name in self._conn.existing_views]


class _FakeConnection:
    def __init__(self, existing_views: list[str]):
        self.existing_views = existing_views
        self.executed: list[tuple[str, tuple]] = []

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        pass

    @property
    def drop_statements(self) -> list[str]:
        return [sql for sql, _ in self.executed if sql.startswith("DROP VIEW")]


class TestDropViewsInSchema:
    def test_drops_every_view_by_default(self):
        conn = _FakeConnection(["vw_a", "vw_b"])

        drop_views_in_schema(conn, schema_name="reporting")

        assert conn.drop_statements == ["DROP VIEW [reporting].[vw_a]", "DROP VIEW [reporting].[vw_b]"]

    def test_queries_views_with_schema_name_as_a_parameter(self):
        conn = _FakeConnection([])

        drop_views_in_schema(conn, schema_name="O'Brien")

        query, params = conn.executed[0]
        assert "TABLE_SCHEMA = ?" in query
        assert "O'Brien" not in query
        assert params == ("O'Brien",)

    def test_keeps_excluded_views(self):
        conn = _FakeConnection(["keep_me", "drop_me", "also_keep"])

        drop_views_in_schema(conn, schema_name="dbo", exclude=["keep_me", "also_keep"])

        assert conn.drop_statements == ["DROP VIEW [dbo].[drop_me]"]

    def test_matches_excluded_views_case_insensitively(self):
        conn = _FakeConnection(["Medals", "ATHLETES", "Stale"])

        drop_views_in_schema(conn, schema_name="dbo", exclude=["medals", "Athletes"])

        assert conn.drop_statements == ["DROP VIEW [dbo].[Stale]"]

    def test_accepts_any_iterable_for_exclude(self):
        conn = _FakeConnection(["a", "b", "c"])

        drop_views_in_schema(conn, schema_name="dbo", exclude=(name for name in ["a", "c"]))

        assert conn.drop_statements == ["DROP VIEW [dbo].[b]"]

    def test_empty_exclude_drops_every_view(self):
        conn = _FakeConnection(["a", "b"])

        drop_views_in_schema(conn, schema_name="dbo", exclude=[])

        assert conn.drop_statements == ["DROP VIEW [dbo].[a]", "DROP VIEW [dbo].[b]"]
