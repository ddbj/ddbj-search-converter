"""ES の status フィールドを SSOT と突き合わせて同期するモジュール。

SPEC: docs/cli-pipeline.md §status の同期

差分更新は日付列に乗っているが status 変更では日付が動かないため、status だけが
変わったエントリーは JSONL に現れず ES に古い値が残る。api は ES の ``status`` を
可視性の判断に使う (ddbj-search-api の docs/api-spec.md §データ可視性) ので、
ずれたままだと非公開にしたエントリーが検索結果に出続ける。
"""

from typing import Any

import duckdb
from pydantic import BaseModel

from ddbj_search_converter.config import (
    DRA_DB_FILE_NAME,
    STATUS_CACHE_DB_FILE_NAME,
    Config,
)
from ddbj_search_converter.es._error_utils import sanitize_error_info
from ddbj_search_converter.es.client import get_es_client
from ddbj_search_converter.es.index import make_physical_index_name
from ddbj_search_converter.logging.logger import log_info
from ddbj_search_converter.sra_accessions_tab import normalize_status, status_strength
from elasticsearch import helpers

# 同期対象は DDBJ 由来のみ。NCBI 側は Accessions.tab の Updated が status 変更でも動くので
# 通常の差分更新で反映されるうえ、non-live が 1,700 万件あって突合コストが釣り合わない。
# ES 側を引くときもこの prefix で絞り、NCBI 分を巻き込まないようにする。
DDBJ_PREFIX_BY_INDEX: dict[str, str] = {
    "bioproject": "PRJDB",
    "biosample": "SAMD",
    "sra-submission": "DRA",
    "sra-study": "DRP",
    "sra-experiment": "DRX",
    "sra-run": "DRR",
    "sra-sample": "DRS",
    "sra-analysis": "DRZ",
}

# SRA は Accessions.tab の Type 列で index が分かれる
SRA_INDEX_TO_TYPE: dict[str, str] = {
    "sra-submission": "SUBMISSION",
    "sra-study": "STUDY",
    "sra-experiment": "EXPERIMENT",
    "sra-run": "RUN",
    "sra-sample": "SAMPLE",
    "sra-analysis": "ANALYSIS",
}

STATUS_CACHE_TABLE_BY_INDEX: dict[str, str] = {
    "bioproject": "bp_status",
    "biosample": "bs_status",
}

INDEX_GROUPS: dict[str, list[str]] = {
    "bioproject": ["bioproject"],
    "biosample": ["biosample"],
    "sra": list(SRA_INDEX_TO_TYPE),
    "all": ["bioproject", "biosample", *SRA_INDEX_TO_TYPE],
}

ES_PAGE_SIZE = 10000
MGET_CHUNK_SIZE = 1000


class StatusSyncResult(BaseModel):
    """1 index 分の同期結果。"""

    index: str
    checked: int
    updated: int
    missing: int
    error_count: int
    errors: list[dict[str, Any]]


def resolve_indexes(index: str) -> list[str]:
    """``--index`` の指定を実際の index 名のリストに展開する。"""
    if index in INDEX_GROUPS:
        return INDEX_GROUPS[index]
    if index in DDBJ_PREFIX_BY_INDEX:
        return [index]

    raise ValueError(f"unsupported index for status sync: {index!r}")


def resolve_physical_index(index: str, target_suffix: str | None) -> str:
    """更新先の物理 index 名。Blue-Green では alias ではなく日付付き index を直接叩く。"""
    if target_suffix is None:
        return index

    return make_physical_index_name(index, target_suffix)  # type: ignore[arg-type]


def load_ssot_statuses(config: Config, index: str) -> tuple[dict[str, str], set[str]]:
    """SSOT の status を読む。

    BP/BS は Status Cache の値をそのまま、SRA は Accessions.tab の値を
    ``normalize_status`` に通す (JSONL 生成と同じ経路)。

    Returns:
        (non-public な accession -> status, SSOT に存在する全 accession)。
        全集合を併せて返すのは、ES 側が non-public な accession について
        「SSOT では public に戻っている」のか「SSOT に無い」のかを区別するため。
    """
    table = STATUS_CACHE_TABLE_BY_INDEX.get(index)
    if table is not None:
        db_path = config.result_dir / STATUS_CACHE_DB_FILE_NAME
        if not db_path.exists():
            raise FileNotFoundError(f"status cache not found: {db_path}")
        with duckdb.connect(str(db_path), read_only=True) as conn:
            rows = conn.execute(f"SELECT accession, status FROM {table}").fetchall()

        non_public = {accession: status for accession, status in rows if status and status != "public"}

        return non_public, {accession for accession, _ in rows}

    sra_type = SRA_INDEX_TO_TYPE[index]
    db_path = config.const_dir / "sra" / DRA_DB_FILE_NAME
    if not db_path.exists():
        raise FileNotFoundError(f"DRA accessions db not found: {db_path}")

    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            "SELECT Accession, Status FROM accessions WHERE Type = ? AND Accession IS NOT NULL",
            [sra_type],
        ).fetchall()

    # 同一 accession が複数 status で現れることがある。JSONL 生成と同じ priority で 1 つに決める。
    strongest: dict[str, str | None] = {}
    for accession, status in rows:
        if accession not in strongest or status_strength(status) < status_strength(strongest[accession]):
            strongest[accession] = status

    non_public = {
        accession: normalized
        for accession, status in strongest.items()
        if (normalized := normalize_status(status)) != "public"
    }

    return non_public, set(strongest)


def fetch_es_non_public(config: Config, index: str, target_suffix: str | None = None) -> dict[str, str]:
    """ES から non-public な doc の status を取る (DDBJ prefix に限定)。

    SSOT 側の non-public だけを見ると ``suppressed`` から ``public`` に戻ったケースを
    取りこぼすため、ES 側からも引いて和集合を突合対象にする。
    """
    es_client = get_es_client(config)
    physical_index = resolve_physical_index(index, target_suffix)
    prefix = DDBJ_PREFIX_BY_INDEX[index]

    result: dict[str, str] = {}
    search_after: list[Any] | None = None

    while True:
        body: dict[str, Any] = {
            "size": ES_PAGE_SIZE,
            "query": {
                "bool": {
                    "filter": [{"prefix": {"identifier": prefix}}],
                    "must_not": [{"term": {"status": "public"}}],
                }
            },
            "sort": [{"identifier": "asc"}],
            "_source": ["status"],
        }
        if search_after is not None:
            body["search_after"] = search_after

        response = es_client.search(index=physical_index, **body)
        hits = response["hits"]["hits"]
        if not hits:
            break

        for hit in hits:
            status = (hit.get("_source") or {}).get("status")
            if isinstance(status, str):
                result[hit["_id"]] = status
        search_after = hits[-1]["sort"]

    return result


def _fetch_es_statuses(
    config: Config,
    physical_index: str,
    accessions: list[str],
) -> tuple[dict[str, str], set[str]]:
    """mget で現在の status を引く。戻り値は (status map, ES に存在しない accession)。"""
    es_client = get_es_client(config)
    found: dict[str, str] = {}
    missing: set[str] = set()

    for i in range(0, len(accessions), MGET_CHUNK_SIZE):
        batch = accessions[i : i + MGET_CHUNK_SIZE]
        response = es_client.mget(index=physical_index, ids=batch, source=["status"])
        for doc in response["docs"]:
            if not doc.get("found"):
                missing.add(doc["_id"])
                continue
            status = (doc.get("_source") or {}).get("status")
            # status を持たない doc は SSOT の値と一致しない扱いにして更新対象に落とす
            found[doc["_id"]] = status if isinstance(status, str) else ""

    return found, missing


def sync_index_status(
    config: Config,
    index: str,
    target_suffix: str | None = None,
    dry_run: bool = False,
) -> StatusSyncResult:
    """1 index 分の status を SSOT に合わせる。

    SSOT に存在しない accession は触らない (ES から消すか private にするかは
    可視性の設計判断であって、status のずれを直す話とは別)。
    ES に doc が無い accession は skip する (doc を作るには XML が要る)。
    """
    physical_index = resolve_physical_index(index, target_suffix)

    ssot_non_public, ssot_all = load_ssot_statuses(config, index)
    es_non_public = fetch_es_non_public(config, index, target_suffix)
    log_info(
        "loaded status sources",
        index=index,
        ssot_non_public=len(ssot_non_public),
        ssot_total=len(ssot_all),
        es_non_public=len(es_non_public),
    )

    # SSOT が non-public のもの (public -> suppressed 等) と、ES が non-public のもの
    # (suppressed -> public に戻った場合) の和集合。SSOT に無い accession は触らない。
    targets = sorted(set(ssot_non_public) | (set(es_non_public) & ssot_all))
    expected = {accession: ssot_non_public.get(accession, "public") for accession in targets}

    # ES 側の走査で status が判明している分は mget を省ける
    known = {accession: status for accession, status in es_non_public.items() if accession in expected}
    to_fetch = [accession for accession in targets if accession not in known]
    fetched, missing = _fetch_es_statuses(config, physical_index, to_fetch)
    current = {**known, **fetched}

    actions = [
        {
            "_op_type": "update",
            "_index": physical_index,
            "_id": accession,
            "doc": {"status": expected[accession]},
        }
        for accession in targets
        if accession in current and current[accession] != expected[accession]
    ]

    if dry_run:
        log_info(
            "dry-run: status differences detected",
            index=index,
            diff=len(actions),
            missing=len(missing),
        )
        return StatusSyncResult(
            index=index,
            checked=len(targets),
            updated=len(actions),
            missing=len(missing),
            error_count=0,
            errors=[],
        )

    errors: list[dict[str, Any]] = []
    updated = 0
    if actions:
        es_client = get_es_client(config).options(request_timeout=600)
        success, failed = helpers.bulk(es_client, actions, stats_only=False, raise_on_error=False)
        updated = success
        if isinstance(failed, list):
            errors = [sanitize_error_info(err) for err in failed]

    log_info(
        "status sync completed",
        index=index,
        checked=len(targets),
        updated=updated,
        missing=len(missing),
        errors=len(errors),
    )

    return StatusSyncResult(
        index=index,
        checked=len(targets),
        updated=updated,
        missing=len(missing),
        error_count=len(errors),
        errors=errors[:100],
    )


def sync_status(
    config: Config,
    index: str,
    target_suffix: str | None = None,
    dry_run: bool = False,
) -> list[StatusSyncResult]:
    """``--index`` の指定を展開して順に同期する。"""
    return [sync_index_status(config, i, target_suffix, dry_run) for i in resolve_indexes(index)]
