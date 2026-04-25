"""Integration: PostgreSQL connectivity smoke for TRAD and XSM.

Verifies that staging PostgreSQL is reachable on the configured URLs and
that ``SELECT 1`` succeeds against every database name the converter touches.
A failure here means the converter would crash at startup on the same env;
catching it before deploy is the value of this smoke.

XSM URL is a single host:port. TRAD URL is host-only; each TRAD DB lives on
a separate port (see ``dblink/insdc.TRAD_DBS``).
"""

import psycopg2

from ddbj_search_converter.dblink.insdc import TRAD_DBS
from ddbj_search_converter.postgres.utils import parse_postgres_url, postgres_connection


def test_xsm_postgres_accepts_select_for_each_dbname(
    integration_xsm_postgres_url: str,
) -> None:
    """IT-PG-01: XSM PostgreSQL (bioproject / biosample dbname) で SELECT 1 が通る."""
    for dbname in ("bioproject", "biosample"):
        with (
            postgres_connection(integration_xsm_postgres_url, dbname) as conn,
            conn.cursor() as cur,
        ):
            cur.execute("SELECT 1")
            row = cur.fetchone()
            assert row == (1,), f"XSM PostgreSQL [{dbname}] did not return 1"


def test_trad_postgres_accepts_select_for_each_db(
    integration_trad_postgres_url: str,
) -> None:
    """IT-PG-02: TRAD PostgreSQL の (dbname, port) 全件で SELECT 1 が通る."""
    host, _port, user, password = parse_postgres_url(integration_trad_postgres_url)

    for dbname, port in TRAD_DBS:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                assert row == (1,), f"TRAD PostgreSQL [{dbname}@{port}] did not return 1"
        finally:
            conn.close()
