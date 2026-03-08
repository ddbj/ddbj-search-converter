"""
INSDC 配列 accession と BioProject/BioSample の関連を TRAD PostgreSQL から抽出し、
DBLink データベースに挿入する。

入力:
- TRAD PostgreSQL (g-actual:54308, e-actual:54309, w-actual:54310)
    - accession テーブルと project テーブルを JOIN
    - project_id が PRJ% で始まる行 → insdc-bioproject 関連
    - project_id が SAM% で始まる行 → insdc-biosample 関連

出力:
- dblink.tmp.duckdb (relation テーブル) に挿入
"""

import psycopg2

from ddbj_search_converter.config import Config, get_config
from ddbj_search_converter.dblink.db import (
    AccessionType,
    get_tmp_dir,
    load_relations_from_tsv,
    normalize_edge,
)
from ddbj_search_converter.dblink.utils import load_blacklist
from ddbj_search_converter.logging.logger import log_info, log_warn, run_logger
from ddbj_search_converter.postgres.utils import parse_postgres_url

TRAD_DBS = [
    ("g-actual", 54308),
    ("e-actual", 54309),
    ("w-actual", 54310),
]
CURSOR_ITERSIZE = 50_000

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


def _write_insdc_relations(
    config: Config,
    dst_type: AccessionType,
    query: str,
    blacklist: set[str],
) -> None:
    host, _port, user, password = parse_postgres_url(config.trad_postgres_url)
    tmp_dir = get_tmp_dir(config)

    for dbname, port in TRAD_DBS:
        log_info(f"connecting to {dbname}:{port} for insdc-{dst_type}")

        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
        )
        try:
            tsv_path = tmp_dir.joinpath(f"insdc_to_{dst_type}_{dbname}.tsv")
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

        if count > 0:
            load_relations_from_tsv(config, tsv_path)
        log_info(f"loaded {count} insdc-{dst_type} from {dbname}")


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        bp_blacklist, bs_blacklist = load_blacklist(config)

        if not config.trad_postgres_url:
            log_warn("trad_postgres_url is not set, skipping insdc relations")
            return

        _write_insdc_relations(config, "bioproject", INSDC_TO_BP_QUERY, bp_blacklist)
        _write_insdc_relations(config, "biosample", INSDC_TO_BS_QUERY, bs_blacklist)


if __name__ == "__main__":
    main()
