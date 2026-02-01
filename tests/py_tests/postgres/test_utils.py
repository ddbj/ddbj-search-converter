"""postgres/utils.py のテスト。"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ddbj_search_converter.postgres.utils import (format_date,
                                                  parse_postgres_url,
                                                  postgres_connection)


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

        with pytest.raises(ValueError):
            with postgres_connection("postgresql://user:pass@localhost:5432", "testdb"):
                raise ValueError("test error")

        mock_conn.close.assert_called_once()
