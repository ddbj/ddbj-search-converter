"""
PostgreSQL から BioProject/BioSample の日付情報を取得し、DuckDB キャッシュを
構築するモジュール。

日次実行では前回の取り込み以降に更新された行だけを取得して既存キャッシュへ
upsert する (窓ビルド)。``--full`` 指定時と、窓ビルドの前提が満たせないときは
全件を取得して作り直す。

PostgreSQL からの取得と DuckDB への投入は中間 TSV を挟んで分離する。取得結果を
書き切ってから接続を閉じ、その後で DuckDB にロードするため、投入に時間がかかっても
PostgreSQL の接続保持時間には影響しない。
"""

import datetime
from collections.abc import Iterator
from typing import NamedTuple

from ddbj_search_converter.config import DEFAULT_MARGIN_DAYS, TODAY, Config
from ddbj_search_converter.date_cache.db import (
    CacheMeta,
    DateRow,
    DateTable,
    cache_meta_is_usable,
    finalize_date_cache_db,
    init_date_cache_db,
    load_dates,
    read_cache_meta,
    set_cache_meta,
    write_dates_tsv,
)
from ddbj_search_converter.logging.logger import log_info
from ddbj_search_converter.postgres.utils import connect_with_retry, format_date, parse_postgres_url

CURSOR_ITERSIZE = 50000
WATERMARK_DATE_FORMAT = "%Y-%m-%d"

BP_POSTGRES_DB_NAME = "bioproject"
BS_POSTGRES_DB_NAME = "biosample"

BP_QUERY = """
    SELECT s.accession, p.create_date, p.modified_date, p.release_date
    FROM mass.bioproject_summary s
    INNER JOIN mass.project p ON s.submission_id = p.submission_id
    WHERE s.accession IS NOT NULL
"""

# accession と mass.sample の行は smp_id で 1:1 に対応する。submission_id で
# 結合すると 1 submission あたり数十件の sample が畳み込まれ、その中の 1 行の
# 日付が同じ submission の全 accession に配られてしまう。
BS_QUERY = """
    SELECT s.accession_id, p.create_date, p.modified_date, p.release_date
    FROM mass.biosample_summary s
    INNER JOIN mass.sample p ON s.smp_id = p.smp_id::text
    WHERE s.accession_id IS NOT NULL
"""

SINCE_CONDITION = "      AND p.modified_date >= %s\n"


class DateSource(NamedTuple):
    table: DateTable
    postgres_db_name: str
    cursor_name: str
    base_query: str


DATE_SOURCES: tuple[DateSource, ...] = (
    DateSource("bp_date", BP_POSTGRES_DB_NAME, "bp_date_cursor", BP_QUERY),
    DateSource("bs_date", BS_POSTGRES_DB_NAME, "bs_date_cursor", BS_QUERY),
)


def build_query(base_query: str, since: str | None) -> str:
    """窓ビルド用に modified_date の下限条件を足したクエリを返す。"""
    if since is None:
        return base_query

    return base_query + SINCE_CONDITION


def window_start(watermark: str, margin_days: int = DEFAULT_MARGIN_DAYS) -> str:
    """watermark から margin を引いた取得開始日を返す。

    窓の境界は日単位で扱う。秒精度で切ると、取得クエリの実行中に更新された行が
    今回の窓にも次回の窓にも入らず取りこぼされるため。
    """
    watermark_date = datetime.datetime.strptime(watermark, WATERMARK_DATE_FORMAT).date()
    return (watermark_date - datetime.timedelta(days=margin_days)).strftime(WATERMARK_DATE_FORMAT)


def _fetch_dates(
    postgres_url: str,
    source: DateSource,
    since: str | None,
) -> Iterator[DateRow]:
    host, port, user, password = parse_postgres_url(postgres_url)
    conn = connect_with_retry(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=source.postgres_db_name,
        keepalives=1,
        keepalives_idle=60,
        keepalives_interval=10,
        keepalives_count=5,
    )
    try:
        with conn.cursor(name=source.cursor_name) as cur:
            cur.itersize = CURSOR_ITERSIZE
            cur.execute(build_query(source.base_query, since), (since,) if since is not None else None)
            for row in cur:
                accession, create_date, modified_date, release_date = row
                yield (
                    accession,
                    format_date(create_date),
                    format_date(modified_date),
                    format_date(release_date),
                )
    finally:
        conn.close()


def build_date_cache(
    config: Config,
    *,
    full: bool = False,
    today: datetime.date | None = None,
) -> None:
    watermark = (today if today is not None else TODAY).strftime(WATERMARK_DATE_FORMAT)

    meta = read_cache_meta(config)
    use_full = full or not cache_meta_is_usable(meta)
    if use_full and not full:
        log_info("no reusable date cache found, falling back to full build")
    log_info(f"building date cache from postgresql ({'full' if use_full else 'window'} mode)")

    init_date_cache_db(config, seed_from_final=not use_full)

    for source in DATE_SOURCES:
        since = None if use_full else _since_for(meta, source.table)
        if since is not None:
            log_info(f"fetching {source.table} rows modified since {since}")
        else:
            log_info(f"fetching all {source.table} rows")

        written = write_dates_tsv(config, source.table, _fetch_dates(config.xsm_postgres_url, source, since))
        # ここで PostgreSQL 接続は閉じている。以降の DuckDB への投入がどれだけ
        # かかっても、接続を保持し続けることはない。
        inserted = load_dates(config, source.table, written, replace_existing=not use_full)
        log_info(f"inserted {inserted} {source.table} rows")

        set_cache_meta(
            config,
            source.table,
            full_built_at=watermark if use_full else None,
            watermark=watermark,
        )

    log_info("finalizing date cache db")
    finalize_date_cache_db(config)
    log_info("date cache build completed")


def _since_for(meta: dict[str, CacheMeta] | None, table: DateTable) -> str:
    # cache_meta_is_usable が真のときだけ呼ばれるので、watermark は必ず存在する。
    assert meta is not None
    watermark = meta[table].watermark
    assert watermark is not None

    return window_start(watermark)
