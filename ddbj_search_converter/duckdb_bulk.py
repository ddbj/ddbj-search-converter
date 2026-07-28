"""
DuckDB へ大量の行を投入するための共通処理。

取得元 (PostgreSQL や外部ファイル) から読んだ行を一度 TSV に書き切り、
その後 ``read_csv`` で一括ロードする。取得と投入を分けることで、DuckDB へ
書き込んでいる間も取得元の接続を保持し続ける、という構造を避けられる。

TSV の encoding は ``None`` と空文字列を quote の有無で区別する。quote されて
いないフィールドだけが NULL になるので、TAB / 改行 / ``"`` が値に含まれていても
区切りが壊れず、``\\N`` のような sentinel と実データが衝突する曖昧さも生じない。
"""

import re
from collections.abc import Iterable, Sequence
from pathlib import Path

import duckdb

_IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_identifier(name: str) -> str:
    """SQL に文字列として埋め込む識別子を検証する。"""
    if not _IDENTIFIER_PATTERN.fullmatch(name):
        raise ValueError(f"invalid SQL identifier: {name!r}")
    return name


def encode_tsv_field(value: str | None) -> str:
    """TSV の 1 フィールドを encode する。

    ``None`` は空文字列 (quote なし) に、それ以外は必ず quote した文字列にする。
    quote の有無そのものが NULL フラグになるので、値が空文字列でも ``None`` と
    区別できる。
    """
    if value is None:
        return ""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def write_rows_to_tsv(tsv_path: Path, rows: Iterable[Sequence[str | None]]) -> int:
    """rows を TSV に書き出し、書いた行数を返す。

    行を消費し切ってから返るので、rows が外部接続に紐づく generator の場合、
    この関数を抜けた時点で接続を閉じられる。
    """
    tsv_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with tsv_path.open("w", encoding="utf-8", newline="") as f:
        for row in rows:
            f.write("\t".join(encode_tsv_field(value) for value in row))
            f.write("\n")
            written += 1

    return written


def load_tsv_into_table(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    columns: Sequence[str],
    tsv_path: Path,
    expected_rows: int,
    *,
    conflict_key: str | None = None,
) -> int:
    """TSV を read_csv で table に流し込み、投入した行数を返す。

    ``conflict_key`` を指定すると、そのキーで衝突した既存行を TSV の値で更新する
    (窓ビルドの upsert 用)。指定しない場合は衝突で例外になる。

    投入行数が ``expected_rows`` と一致しない場合は例外を送出する。encode/decode
    のずれで行が分割・結合されると、黙って件数が変わる形で現れるため。
    """
    _validate_identifier(table_name)
    column_spec = ", ".join(f"'{_validate_identifier(column)}': 'TEXT'" for column in columns)

    # 衝突解決は `INSERT OR REPLACE` ではなく conflict target を明示した
    # `ON CONFLICT` で書く。DuckDB 1.4.4 の `INSERT OR REPLACE` は、対象 table に
    # 非 UNIQUE な secondary index が同居していると既存行を更新しないまま
    # 「挿入した」と報告するため、件数照合をすり抜けて古い値が残る。
    conflict_clause = ""
    if conflict_key is not None:
        _validate_identifier(conflict_key)
        assignments = ", ".join(
            f"{column} = excluded.{column}" for column in columns if column != conflict_key
        )
        conflict_clause = f"ON CONFLICT ({conflict_key}) DO UPDATE SET {assignments}"

    # auto_detect=false で dialect の推定を止める。columns を渡している以上
    # 推定は不要な上に、空ファイルや 1 列に見える内容で sniffer が失敗する。
    # 窓ビルドで対象が 0 件の日は普通に起きるので、そこで落ちては困る。
    rows = conn.execute(
        f"""
        INSERT INTO {table_name}
        SELECT * FROM read_csv(
            ?,
            auto_detect=false,
            header=false,
            new_line='\\n',
            delim=chr(9),
            quote=chr(34),
            escape=chr(34),
            nullstr='',
            allow_quoted_nulls=false,
            columns={{{column_spec}}}
        )
        {conflict_clause}
        """,
        (str(tsv_path),),
    ).fetchall()

    inserted = int(rows[0][0]) if rows else 0
    if inserted != expected_rows:
        raise RuntimeError(
            f"row count mismatch loading {tsv_path} into {table_name}: "
            f"wrote {expected_rows} rows but inserted {inserted}"
        )

    return inserted
