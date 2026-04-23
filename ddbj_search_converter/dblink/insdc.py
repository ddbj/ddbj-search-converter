"""
INSDC 配列 accession と BioProject/BioSample の関連を抽出し、
DBLink データベースに挿入する。

入力:
- Preserved TSV ({const_dir}/dblink/)
    - insdc_bp_preserved.tsv: 手動キュレーションされた INSDC-BioProject 関連
    - insdc_bs_preserved.tsv: 手動キュレーションされた INSDC-BioSample 関連
- TRAD PostgreSQL (g-actual:54308, e-actual:54309, w-actual:54310)
    - accession テーブルと project テーブルを JOIN
    - project_id が PRJ% で始まる行 → insdc-bioproject 関連
    - project_id が SAM% で始まる行 → insdc-biosample 関連

出力:
- dblink.tmp.duckdb (raw_edges テーブル) に挿入
"""

import time
from pathlib import Path

import psycopg2

from ddbj_search_converter.config import (
    INSDC_BP_PRESERVED_REL_PATH,
    INSDC_BS_PRESERVED_REL_PATH,
    Config,
    get_config,
)
from ddbj_search_converter.dblink.db import (
    AccessionType,
    IdPairs,
    get_tmp_dir,
    load_edges_from_tsv,
    load_to_db,
    normalize_edge,
)
from ddbj_search_converter.dblink.utils import filter_pairs_by_blacklist, load_blacklist
from ddbj_search_converter.id_patterns import is_valid_accession
from ddbj_search_converter.logging.logger import log_debug, log_info, log_warn, run_logger
from ddbj_search_converter.logging.schema import DebugCategory
from ddbj_search_converter.postgres.utils import parse_postgres_url

TRAD_DBS = [
    ("g-actual", 54308),
    ("e-actual", 54309),
    ("w-actual", 54310),
]
CURSOR_ITERSIZE = 50_000
MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 30

INSDC_TO_BP_QUERY = """
    SELECT translate(acc.accession, ' ', ''), project.project_id
    FROM accession AS acc
    JOIN link_pr_ac USING(ac_id)
    JOIN project ON(project.pr_id = link_pr_ac.pr_id)
    WHERE project.project_id LIKE 'PRJ%%'
      AND acc.accession IS NOT NULL
"""

INSDC_TO_BS_QUERY = """
    SELECT translate(acc.accession, ' ', ''), project.project_id
    FROM accession AS acc
    JOIN link_pr_ac USING(ac_id)
    JOIN project ON(project.pr_id = link_pr_ac.pr_id)
    WHERE project.project_id LIKE 'SAM%%'
      AND acc.accession IS NOT NULL
"""


def _fetch_from_db(
    host: str,
    port: int,
    user: str,
    password: str,
    dbname: str,
    dst_type: AccessionType,
    query: str,
    blacklist: set[str],
    tsv_path: Path,
) -> int:
    """Fetch INSDC relations from a single TRAD PostgreSQL database."""
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    try:
        count = 0
        with conn.cursor(name=f"insdc_{dbname}") as cur:
            cur.itersize = CURSOR_ITERSIZE
            cur.execute(query)

            with tsv_path.open("w", encoding="utf-8") as f:
                for accession, target_id in cur:
                    if target_id in blacklist:
                        continue
                    normalized = normalize_edge("insdc", accession, dst_type, target_id)
                    f.write("\t".join(normalized) + "\n")
                    count += 1
    finally:
        conn.close()
    return count


def _write_insdc_relations(
    config: Config,
    dst_type: AccessionType,
    query: str,
    blacklist: set[str],
) -> None:
    host, _port, user, password = parse_postgres_url(config.trad_postgres_url)
    tmp_dir = get_tmp_dir(config)

    for dbname, port in TRAD_DBS:
        tsv_path = tmp_dir.joinpath(f"insdc_to_{dst_type}_{dbname}.tsv")

        for attempt in range(1, MAX_RETRIES + 1):
            log_info(f"connecting to {dbname}:{port} for insdc-{dst_type} (attempt {attempt}/{MAX_RETRIES})")
            try:
                count = _fetch_from_db(host, port, user, password, dbname, dst_type, query, blacklist, tsv_path)
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                if attempt == MAX_RETRIES:
                    raise
                log_warn(f"{dbname}:{port} failed: {e}, retrying in {RETRY_WAIT_SECONDS}s")
                time.sleep(RETRY_WAIT_SECONDS)

        if count > 0:
            load_edges_from_tsv(config, tsv_path)
        log_info(f"loaded {count} insdc-{dst_type} from {dbname}")


def _load_insdc_preserved_file(
    config: Config,
    preserved_rel_path: str,
    dst_type: AccessionType,
) -> IdPairs:
    """Preserved TSV から INSDC -> BioProject/BioSample 関連を読み込む。

    TSV format: insdc_accession\ttarget_accession (ヘッダなし)
    ターゲット側 (bioproject/biosample) のみバリデーション。
    INSDC 側は id_patterns.py にパターンがないためスキップ。

    Raises:
        FileNotFoundError: ファイルが存在しない場合。
    """
    preserved_path = config.const_dir.joinpath(preserved_rel_path)
    if not preserved_path.exists():
        raise FileNotFoundError(f"preserved file not found: {preserved_path}")

    pairs: IdPairs = set()
    log_info(f"processing preserved file: {preserved_path}", file=str(preserved_path))

    with preserved_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) >= 2:
                insdc_acc, target_acc = parts[0], parts[1]
                if not is_valid_accession(target_acc, dst_type):
                    log_debug(
                        f"skipping invalid {dst_type}: {target_acc}",
                        accession=target_acc,
                        file=str(preserved_path),
                        debug_category=DebugCategory.INVALID_ACCESSION_ID,
                        source="insdc-preserved",
                    )
                    continue
                pairs.add((insdc_acc, target_acc))

    log_info(
        f"loaded {len(pairs)} insdc-{dst_type} from preserved file",
        file=str(preserved_path),
    )

    return pairs


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        bp_blacklist, bs_blacklist = load_blacklist(config)

        # 1. Preserved file から関連を読み込み (trad_postgres_url に依存しない)
        insdc_to_bp = _load_insdc_preserved_file(config, INSDC_BP_PRESERVED_REL_PATH, "bioproject")
        insdc_to_bs = _load_insdc_preserved_file(config, INSDC_BS_PRESERVED_REL_PATH, "biosample")

        # 2. Blacklist 適用
        insdc_to_bp = filter_pairs_by_blacklist(insdc_to_bp, bp_blacklist, "right")
        insdc_to_bs = filter_pairs_by_blacklist(insdc_to_bs, bs_blacklist, "right")

        # 3. DB にロード
        if insdc_to_bp:
            load_to_db(config, insdc_to_bp, "insdc", "bioproject")
        if insdc_to_bs:
            load_to_db(config, insdc_to_bs, "insdc", "biosample")

        # 4. TRAD PostgreSQL から関連を抽出 (URL が設定されている場合のみ)
        if not config.trad_postgres_url:
            log_warn("trad_postgres_url is not set, skipping insdc TRAD relations")
            return

        _write_insdc_relations(config, "bioproject", INSDC_TO_BP_QUERY, bp_blacklist)
        _write_insdc_relations(config, "biosample", INSDC_TO_BS_QUERY, bs_blacklist)


if __name__ == "__main__":
    main()
