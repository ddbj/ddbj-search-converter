"""
GEA (Gene Expression Archive) の IDF/SDRF ファイルから関連を抽出し、DBLink DB に挿入する。

入力:
- GEA_BASE_PATH 配下の E-GEAD-* ディレクトリ
    - {E-GEAD-NNN}/{E-GEAD-NNN}.idf.txt
    - {E-GEAD-NNN}/{E-GEAD-NNN}.sdrf.txt

出力 (dblink.duckdb への relation 投入):
- gea -> bioproject        (IDF の Comment[BioProject] から)
- gea -> biosample         (SDRF の Comment[BioSample] から)
- gea -> sra-run           (SDRF の Comment[SRA_RUN] から)
- gea -> sra-experiment    (SDRF の Comment[SRA_EXPERIMENT] から)
- gea -> jga-study         (IDF の Comment[Related study] の JGA:JGAS* から)
- gea -> humandbs          (IDF の Comment[Related study] の NBDC:hum* から)

Metabolonote / RPMM / Metabolights / 不明 prefix は silent skip + log_debug で観測可能化。
"""

from collections.abc import Iterator
from pathlib import Path

from ddbj_search_converter.config import GEA_BASE_PATH, get_config
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.dblink.idf_sdrf import _classify_related_study, process_idf_sdrf_dir
from ddbj_search_converter.dblink.utils import filter_pairs_by_blacklist, load_blacklist
from ddbj_search_converter.id_patterns import is_valid_accession
from ddbj_search_converter.logging.logger import log_debug, log_info, run_logger
from ddbj_search_converter.logging.schema import DebugCategory


def iterate_gea_dirs(base_path: Path) -> Iterator[Path]:
    """
    GEA 実験ディレクトリを iterate する。
    E-GEAD-000/, E-GEAD-1000/ などのサブディレクトリ配下の E-GEAD-NNN/ を走査する。
    """
    if not base_path.exists():
        return

    for prefix_dir in sorted(base_path.iterdir()):
        if not prefix_dir.is_dir() or not prefix_dir.name.startswith("E-GEAD-"):
            continue
        for gea_dir in sorted(prefix_dir.iterdir()):
            if gea_dir.is_dir() and gea_dir.name.startswith("E-GEAD-"):
                yield gea_dir


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        bp_blacklist, bs_blacklist = load_blacklist(config)

        gea_to_bp: IdPairs = set()
        gea_to_bs: IdPairs = set()
        gea_to_sra_run: IdPairs = set()
        gea_to_sra_experiment: IdPairs = set()
        gea_to_jga_study: IdPairs = set()
        gea_to_humandbs: IdPairs = set()

        dir_count = 0
        for gea_dir in iterate_gea_dirs(GEA_BASE_PATH):
            result = process_idf_sdrf_dir(gea_dir)
            dir_count += 1

            if result.bioproject:
                if is_valid_accession(result.bioproject, "bioproject"):
                    gea_to_bp.add((result.entry_id, result.bioproject))
                else:
                    log_debug(
                        f"skipping invalid bioproject: {result.bioproject}",
                        accession=result.bioproject,
                        file=str(gea_dir),
                        debug_category=DebugCategory.INVALID_ACCESSION_ID,
                        source="gea",
                    )

            for bs_id in result.biosamples:
                if is_valid_accession(bs_id, "biosample"):
                    gea_to_bs.add((result.entry_id, bs_id))
                else:
                    log_debug(
                        f"skipping invalid biosample: {bs_id}",
                        accession=bs_id,
                        file=str(gea_dir),
                        debug_category=DebugCategory.INVALID_ACCESSION_ID,
                        source="gea",
                    )

            for run_id in result.sra_runs:
                if is_valid_accession(run_id, "sra-run"):
                    gea_to_sra_run.add((result.entry_id, run_id))
                else:
                    log_debug(
                        f"skipping invalid sra-run: {run_id}",
                        accession=run_id,
                        file=str(gea_dir),
                        debug_category=DebugCategory.INVALID_ACCESSION_ID,
                        source="gea",
                    )

            for exp_id in result.sra_experiments:
                if is_valid_accession(exp_id, "sra-experiment"):
                    gea_to_sra_experiment.add((result.entry_id, exp_id))
                else:
                    log_debug(
                        f"skipping invalid sra-experiment: {exp_id}",
                        accession=exp_id,
                        file=str(gea_dir),
                        debug_category=DebugCategory.INVALID_ACCESSION_ID,
                        source="gea",
                    )

            for raw in result.related_studies:
                classified = _classify_related_study(raw)
                if classified is None:
                    log_debug(
                        f"skipping related study with non-target prefix: {raw}",
                        file=str(gea_dir),
                        source="gea",
                    )
                    continue
                xref_type, acc = classified
                if not is_valid_accession(acc, xref_type):
                    log_debug(
                        f"skipping invalid {xref_type}: {acc}",
                        accession=acc,
                        file=str(gea_dir),
                        debug_category=DebugCategory.INVALID_ACCESSION_ID,
                        source="gea",
                    )
                    continue
                if xref_type == "jga-study":
                    gea_to_jga_study.add((result.entry_id, acc))
                else:
                    gea_to_humandbs.add((result.entry_id, acc))

        log_info(f"processed {dir_count} GEA directories")
        log_info(f"extracted {len(gea_to_bp)} GEA -> BioProject relations")
        log_info(f"extracted {len(gea_to_bs)} GEA -> BioSample relations")
        log_info(f"extracted {len(gea_to_sra_run)} GEA -> SRA-Run relations")
        log_info(f"extracted {len(gea_to_sra_experiment)} GEA -> SRA-Experiment relations")
        log_info(f"extracted {len(gea_to_jga_study)} GEA -> JGA-Study relations")
        log_info(f"extracted {len(gea_to_humandbs)} GEA -> humandbs relations")

        # Blacklist 適用 (BP/BS のみ、新 relation には適用しない: BP/BS と一貫した右側フィルタ方針)
        gea_to_bp = filter_pairs_by_blacklist(gea_to_bp, bp_blacklist, "right")
        gea_to_bs = filter_pairs_by_blacklist(gea_to_bs, bs_blacklist, "right")

        if gea_to_bp:
            load_to_db(config, gea_to_bp, "gea", "bioproject")
        if gea_to_bs:
            load_to_db(config, gea_to_bs, "gea", "biosample")
        if gea_to_sra_run:
            load_to_db(config, gea_to_sra_run, "gea", "sra-run")
        if gea_to_sra_experiment:
            load_to_db(config, gea_to_sra_experiment, "gea", "sra-experiment")
        if gea_to_jga_study:
            load_to_db(config, gea_to_jga_study, "gea", "jga-study")
        if gea_to_humandbs:
            load_to_db(config, gea_to_humandbs, "gea", "humandbs")


if __name__ == "__main__":
    main()
