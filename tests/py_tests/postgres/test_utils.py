"""postgres/utils.py のテスト。"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ddbj_search_converter.postgres.utils import ALLOWED_POSTGRES_SCHEMES, format_date, parse_postgres_url, postgres_connection


class TestFormatDate:
    """format_date 関数のテスト。"""

    def test_none_returns_none(self) -> None:
        assert format_date(None) is None

    def test_utc_datetime(self) -> None:
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = format_date(dt)
        assert result == "2024-01-15T10:30:00Z"

    def test_datetime_with_microseconds(self) -> None:
        dt = datetime(2024, 6, 20, 12, 0, 0, 123456, tzinfo=timezone.utc)
        result = format_date(dt)
        assert result == "2024-06-20T12:00:00Z"


class TestParsePostgresUrl:
    """parse_postgres_url 関数のテスト。"""

    def test_full_url(self) -> None:
        url = "postgresql://user:pass@localhost:5432"
        host, port, user, password = parse_postgres_url(url)
        assert host == "localhost"
        assert port == 5432
        assert user == "user"
        assert password == "pass"

    def test_url_with_dbname(self) -> None:
        url = "postgresql://admin:secret@dbhost:5433/mydb"
        host, port, user, password = parse_postgres_url(url)
        assert host == "dbhost"
        assert port == 5433
        assert user == "admin"
        assert password == "secret"

    def test_url_with_default_port(self) -> None:
        url = "postgresql://user:pass@localhost"
        host, port, user, password = parse_postgres_url(url)
        assert host == "localhost"
        assert port == 5432
        assert user == "user"
        assert password == "pass"

    def test_url_without_credentials(self) -> None:
        url = "postgresql://localhost:5432"
        host, port, user, password = parse_postgres_url(url)
        assert host == "localhost"
        assert port == 5432
        assert user == ""
        assert password == ""


class TestParsePostgresUrlSchemeValidation:
    """``parse_postgres_url`` must reject schemes that aren't in
    ``ALLOWED_POSTGRES_SCHEMES``. The point is to catch typos and
    cross-DB URLs at startup instead of silently falling back to localhost
    (which would surface only as a misleading connection refused later).
    """

    @pytest.mark.parametrize(
        "scheme",
        ["postgresql", "postgresql+psycopg", "postgres"],
    )
    def test_allowed_schemes_pass(self, scheme: str) -> None:
        host, port, user, password = parse_postgres_url(
            f"{scheme}://user:pass@db.example.com:5432/dbname"
        )
        assert host == "db.example.com"
        assert port == 5432
        assert user == "user"
        assert password == "pass"

    @pytest.mark.parametrize(
        "scheme",
        ["mysql", "sqlite", "http", "redis", "postgresql+asyncpg"],
    )
    def test_other_schemes_raise(self, scheme: str) -> None:
        with pytest.raises(ValueError, match="unsupported PostgreSQL URL scheme"):
            parse_postgres_url(f"{scheme}://user@host:5432")

    def test_uppercase_scheme_passes(self) -> None:
        # urlparse は scheme を lowercase に正規化するので、入力の大文字小文字は
        # 区別しない (RFC 3986 互換)。"POSTGRESQL://..." も "postgresql" として
        # accept されることを pin する。
        host, _, _, _ = parse_postgres_url("POSTGRESQL://user:pass@host:5432")
        assert host == "host"

    def test_empty_url_raises(self) -> None:
        with pytest.raises(ValueError, match="unsupported PostgreSQL URL scheme"):
            parse_postgres_url("")

    def test_no_scheme_raises(self) -> None:
        # urlparse("host:5432") returns scheme="" — must fail early.
        with pytest.raises(ValueError, match="unsupported PostgreSQL URL scheme"):
            parse_postgres_url("host:5432")

    def test_allowed_schemes_constant_shape(self) -> None:
        # SSOT pin: docs/data-architecture.md と一致させる。
        assert ALLOWED_POSTGRES_SCHEMES == frozenset(
            {"postgresql", "postgresql+psycopg", "postgres"}
        )


class TestPostgresConnection:
    """postgres_connection コンテキストマネージャのテスト。"""

    @patch("ddbj_search_converter.postgres.utils.psycopg2.connect")
    def test_connection_opens_and_closes(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with postgres_connection("postgresql://user:pass@localhost:5432", "testdb") as conn:
            assert conn is mock_conn

        mock_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            user="user",
            password="pass",
            dbname="testdb",
        )
        mock_conn.close.assert_called_once()

    @patch("ddbj_search_converter.postgres.utils.psycopg2.connect")
    def test_connection_closes_on_exception(self, mock_connect: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with pytest.raises(ValueError), postgres_connection("postgresql://user:pass@localhost:5432", "testdb"):
            raise ValueError("test error")

        mock_conn.close.assert_called_once()
