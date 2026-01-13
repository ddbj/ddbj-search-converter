from datetime import date, datetime
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ddbj_search_converter.config import Config

# この run が「何の処理か」を表す名前
# 基本は CLI スクリプト名だが、書き捨て・緊急対応などのための adhoc を用意する
# run_name は自動推測されるため、ここでの定義は参照用 (既知の run_name 一覧)
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


# log_level の使い分け:
# - DEBUG: 詳細なデバッグ情報（変数の値、内部状態）
# - INFO: 通常の進捗、正常終了
# - WARNING: 問題あるが処理成功
# - ERROR: 失敗したが処理継続（例: 1レコードの変換失敗）
# - CRITICAL: 致命的、処理停止
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# lifecycle を extra フィールドで表現する:
# - lifecycle="start": run 開始
# - lifecycle="end": run 正常終了
# - lifecycle="failed": run 失敗終了
Lifecycle = Literal["start", "end", "failed"]


class Extra(BaseModel):
    """
    予約フィールド + 任意フィールド。
    extra="allow" により、定義外のフィールドも受け入れる。
    """
    model_config = ConfigDict(extra="allow")

    lifecycle: Optional[Lifecycle] = None
    file: Optional[str] = None
    accession: Optional[str] = None
    index: Optional[str] = None
    table: Optional[str] = None
    row: Optional[int] = None


class LoggerContext(BaseModel):
    """Logger の実行時コンテキスト"""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_name: str
    run_id: str
    run_date: date
    log_file: Path
    config: Config


class ErrorInfo(BaseModel):
    type: str                         # e.g., ValueError
    message: str                      # str(e)
    traceback: Optional[str] = None   # format_exc() etc.


class LogRecord(BaseModel):
    # timestamp (UTC)
    timestamp: datetime

    # run identifiers
    run_date: date
    run_id: str
    run_name: str

    # log source (module path)
    source: str = Field(
        ...,
        description="e.g., ddbj_search_converter.bioproject.bp_xml_to_jsonl",
    )

    log_level: LogLevel

    message: Optional[str] = None
    error: Optional[ErrorInfo] = None
    extra: Extra = Field(default_factory=Extra)
