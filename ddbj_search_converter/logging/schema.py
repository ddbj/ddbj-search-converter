from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field

# この run が「何の処理か」を表す名前
# 基本は CLI スクリプト名だが、書き捨て・緊急対応などのための adhoc を用意する
RunName = Literal[
    # DBLink 関連
    "init_dblink_db",
    "create_dblink_assembly_and_master_relations",

    "build_sra_dra_accessions_db",

    # "create_dblink_assembly_and_master_relations",

    # "create_es_index",

    # "create_bp_date_db",
    # "create_bs_date_db",
    # "create_dra_date_db",

    # "create_bp_relation_ids",
    # "create_bs_relation_ids",
    # "create_dra_relation_ids",
    # "create_jga_relation_ids",

    # "bp_xml_to_jsonl",
    # "bp_bulk_insert",

    # "bs_xml_to_jsonl",
    # "bs_bulk_insert",

    # "dra_generate_jsonl",
    # "dra_bulk_insert",

    # "jga_generate_jsonl",
    # "jga_bulk_insert",
    # "jga_delete_indexes",

    "adhoc"
]


Event = Literal[
    # run / operation lifecycle
    "start",            # run / 処理開始
    "end",              # 正常終了
    "failed",           # 致命的失敗 (下流の処理に影響する) で終了

    # progress / info
    "progress",         # 途中経過・節目
    # "debug",            # 設定・内部状態ダンプ

    # item-level results
    "warning",          # 警告 (処理は継続)



    # "skipped",          # 意図的にスキップ（正常系）
    # "skipped_warning",  # 問題があったためスキップ
    # "error",            # 失敗したが処理は継続
    # "error",
]

# error,
# warning,


class Target(BaseModel):
    file: Optional[str] = None        # XML / JSONL / CSV など
    accession: Optional[str] = None   # BP / BS / DRA / JGA accession
    index: Optional[str] = None       # Elasticsearch index
    table: Optional[str] = None       # SQLite / DuckDB table
    row: Optional[int] = None         # CSV / TSV の行番号など


class ErrorInfo(BaseModel):
    type: str                         # 例: ValueError
    message: str                      # str(e)
    traceback: Optional[str] = None   # format_exc() など


class LogRecord(BaseModel):
    # 発生時刻 (UTC 前提)
    timestamp: datetime

    # run 識別子
    run_date: date
    run_id: str
    run_name: str

    # ログ発生元 (module path)
    source: str = Field(
        ...,
        description="e.g., ddbj_search_converter.bioproject.bp_xml_to_jsonl",
    )

    # event: Event
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    # ERROR の分別 (例えば)
    # - accession が入ったか入らないか
    # 致命的が CRITICAL (そこで止まった)
    # 入らなかったものを ERROR とする
    # WARNING はなんか問題があるが、index され入った

    message: Optional[str] = None  # 人間が書く、log message
    target: Optional[Target] = None  # dict or 構造体
    error: Optional[ErrorInfo] = None  # python が吐く traceback

    # schema 化できない特別な情報
    extra: Dict[str, Any] = Field(default_factory=dict)
