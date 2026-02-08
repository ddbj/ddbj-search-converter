"""
SRA 内部関連および BioProject/BioSample <-> SRA 関連を抽出し、DBLink データベースに挿入する。

入力:
- SRA/DRA accessions DuckDB (sra_accessions.duckdb, dra_accessions.duckdb)
    - sra_accessions_tab モジュールで事前に構築する
- bp_id_to_accession.tsv / bs_id_to_accession.tsv
    - bp_bs モジュール (create_dblink_bp_bs_relations) で事前に生成する

SRA/DRA accessions DB から以下の関連を抽出する:

SRA 内部関連:
- Submission <-> Study
- Study <-> Experiment
- Study <-> Analysis
- Submission <-> Analysis
- Experiment <-> Run
- Experiment <-> Sample
- Run <-> Sample

BioProject <-> SRA:
- BioProject <-> Study
- BioProject <-> Experiment
- BioProject <-> Run
- BioProject <-> Analysis

BioSample <-> SRA:
- BioSample <-> Sample
- BioSample <-> Experiment
- BioSample <-> Run
- BioSample <-> Analysis

出力:
- dblink.tmp.duckdb (relation テーブル) に挿入
"""
from typing import Set

from ddbj_search_converter.config import (BP_ID_TO_ACCESSION_FILE_NAME,
                                          BS_ID_TO_ACCESSION_FILE_NAME,
                                          Config, get_config)
from ddbj_search_converter.dblink.bp_bs import IdMapping, load_id_mapping_tsv
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.dblink.utils import (convert_id_if_needed,
                                                filter_pairs_by_blacklist,
                                                filter_sra_pairs_by_blacklist,
                                                load_blacklist,
                                                load_sra_blacklist)
from ddbj_search_converter.id_patterns import is_valid_accession
from ddbj_search_converter.logging.logger import (log_debug, log_info,
                                                  run_logger)
from ddbj_search_converter.logging.schema import DebugCategory
from ddbj_search_converter.sra_accessions_tab import (
    SourceKind, iter_bp_analysis_relations, iter_bp_experiment_relations,
    iter_bp_run_relations, iter_bp_study_relations,
    iter_bs_analysis_relations, iter_bs_experiment_relations,
    iter_bs_run_relations, iter_bs_sample_relations,
    iter_experiment_run_relations, iter_experiment_sample_relations,
    iter_run_sample_relations, iter_study_analysis_relations,
    iter_study_experiment_relations, iter_submission_analysis_relations,
    iter_submission_study_relations)


def process_sra_internal_relations(
    config: Config,
    *,
    source: SourceKind,
    sra_blacklist: Set[str],
    bp_blacklist: Set[str],
    bs_blacklist: Set[str],
    bp_id_to_accession: IdMapping,
    bs_id_to_accession: IdMapping,
) -> None:
    """
    SRA/DRA 内部関連および BioProject/BioSample <-> SRA 関連を抽出して DBLink DB に登録する。

    Args:
        config: 設定
        source: "sra" または "dra"
        sra_blacklist: SRA blacklist (Submission, Study, Experiment, Run, Sample, Analysis の accession)
        bp_blacklist: BioProject blacklist
        bs_blacklist: BioSample blacklist
        bp_id_to_accession: BioProject 数値 ID -> accession マッピング
        bs_id_to_accession: BioSample 数値 ID -> accession マッピング
    """
    source_label = source.upper()

    # === SRA 内部関連 ===

    # Submission <-> Study
    submission_study: IdPairs = set()
    for submission, study in iter_submission_study_relations(config, source=source):
        if not submission or not study:
            continue
        if not is_valid_accession(submission, "sra-submission"):
            log_debug(f"skipping invalid sra-submission: {submission}", accession=submission,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        if not is_valid_accession(study, "sra-study"):
            log_debug(f"skipping invalid sra-study: {study}", accession=study,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        submission_study.add((submission, study))
    log_info(f"extracted {len(submission_study)} {source_label} Submission <-> Study relations")
    submission_study = filter_sra_pairs_by_blacklist(submission_study, sra_blacklist)
    if submission_study:
        load_to_db(config, submission_study, "sra-submission", "sra-study")

    # Study <-> Experiment
    study_experiment: IdPairs = set()
    for study, experiment in iter_study_experiment_relations(config, source=source):
        if not study or not experiment:
            continue
        if not is_valid_accession(study, "sra-study"):
            log_debug(f"skipping invalid sra-study: {study}", accession=study,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        if not is_valid_accession(experiment, "sra-experiment"):
            log_debug(f"skipping invalid sra-experiment: {experiment}", accession=experiment,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        study_experiment.add((study, experiment))
    log_info(f"extracted {len(study_experiment)} {source_label} Study <-> Experiment relations")
    study_experiment = filter_sra_pairs_by_blacklist(study_experiment, sra_blacklist)
    if study_experiment:
        load_to_db(config, study_experiment, "sra-study", "sra-experiment")

    # Study <-> Analysis
    study_analysis: IdPairs = set()
    for study, analysis in iter_study_analysis_relations(config, source=source):
        if not study or not analysis:
            continue
        if not is_valid_accession(study, "sra-study"):
            log_debug(f"skipping invalid sra-study: {study}", accession=study,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        if not is_valid_accession(analysis, "sra-analysis"):
            log_debug(f"skipping invalid sra-analysis: {analysis}", accession=analysis,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        study_analysis.add((study, analysis))
    log_info(f"extracted {len(study_analysis)} {source_label} Study <-> Analysis relations")
    study_analysis = filter_sra_pairs_by_blacklist(study_analysis, sra_blacklist)
    if study_analysis:
        load_to_db(config, study_analysis, "sra-study", "sra-analysis")

    # Submission <-> Analysis (Study が空の analysis 用)
    submission_analysis: IdPairs = set()
    for submission, analysis in iter_submission_analysis_relations(config, source=source):
        if not submission or not analysis:
            continue
        if not is_valid_accession(submission, "sra-submission"):
            log_debug(f"skipping invalid sra-submission: {submission}", accession=submission,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        if not is_valid_accession(analysis, "sra-analysis"):
            log_debug(f"skipping invalid sra-analysis: {analysis}", accession=analysis,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        submission_analysis.add((submission, analysis))
    log_info(f"extracted {len(submission_analysis)} {source_label} Submission <-> Analysis relations")
    submission_analysis = filter_sra_pairs_by_blacklist(submission_analysis, sra_blacklist)
    if submission_analysis:
        load_to_db(config, submission_analysis, "sra-submission", "sra-analysis")

    # Experiment <-> Run
    experiment_run: IdPairs = set()
    for experiment, run in iter_experiment_run_relations(config, source=source):
        if not experiment or not run:
            continue
        if not is_valid_accession(experiment, "sra-experiment"):
            log_debug(f"skipping invalid sra-experiment: {experiment}", accession=experiment,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        if not is_valid_accession(run, "sra-run"):
            log_debug(f"skipping invalid sra-run: {run}", accession=run,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        experiment_run.add((experiment, run))
    log_info(f"extracted {len(experiment_run)} {source_label} Experiment <-> Run relations")
    experiment_run = filter_sra_pairs_by_blacklist(experiment_run, sra_blacklist)
    if experiment_run:
        load_to_db(config, experiment_run, "sra-experiment", "sra-run")

    # Experiment <-> Sample
    experiment_sample: IdPairs = set()
    for experiment, sample in iter_experiment_sample_relations(config, source=source):
        if not experiment or not sample:
            continue
        if not is_valid_accession(experiment, "sra-experiment"):
            log_debug(f"skipping invalid sra-experiment: {experiment}", accession=experiment,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        if not is_valid_accession(sample, "sra-sample"):
            log_debug(f"skipping invalid sra-sample: {sample}", accession=sample,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        experiment_sample.add((experiment, sample))
    log_info(f"extracted {len(experiment_sample)} {source_label} Experiment <-> Sample relations")
    experiment_sample = filter_sra_pairs_by_blacklist(experiment_sample, sra_blacklist)
    if experiment_sample:
        load_to_db(config, experiment_sample, "sra-experiment", "sra-sample")

    # Run <-> Sample
    run_sample: IdPairs = set()
    for run, sample in iter_run_sample_relations(config, source=source):
        if not run or not sample:
            continue
        if not is_valid_accession(run, "sra-run"):
            log_debug(f"skipping invalid sra-run: {run}", accession=run,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        if not is_valid_accession(sample, "sra-sample"):
            log_debug(f"skipping invalid sra-sample: {sample}", accession=sample,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        run_sample.add((run, sample))
    log_info(f"extracted {len(run_sample)} {source_label} Run <-> Sample relations")
    run_sample = filter_sra_pairs_by_blacklist(run_sample, sra_blacklist)
    if run_sample:
        load_to_db(config, run_sample, "sra-run", "sra-sample")

    # === BioProject <-> SRA ===

    # BioProject <-> Study
    bp_study: IdPairs = set()
    for raw_bp, study in iter_bp_study_relations(config, source=source):
        if not raw_bp or not study:
            continue
        converted_bp = convert_id_if_needed(raw_bp, "bioproject", bp_id_to_accession, "accessions_db", source)
        if not converted_bp:
            continue
        if not is_valid_accession(study, "sra-study"):
            log_debug(f"skipping invalid sra-study: {study}", accession=study,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        bp_study.add((converted_bp, study))
    log_info(f"extracted {len(bp_study)} {source_label} BioProject <-> Study relations")
    bp_study = filter_sra_pairs_by_blacklist(bp_study, sra_blacklist)
    bp_study = filter_pairs_by_blacklist(bp_study, bp_blacklist, "left")
    if bp_study:
        load_to_db(config, bp_study, "bioproject", "sra-study")

    # BioProject <-> Experiment
    bp_experiment: IdPairs = set()
    for raw_bp, experiment in iter_bp_experiment_relations(config, source=source):
        if not raw_bp or not experiment:
            continue
        converted_bp = convert_id_if_needed(raw_bp, "bioproject", bp_id_to_accession, "accessions_db", source)
        if not converted_bp:
            continue
        if not is_valid_accession(experiment, "sra-experiment"):
            log_debug(f"skipping invalid sra-experiment: {experiment}", accession=experiment,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        bp_experiment.add((converted_bp, experiment))
    log_info(f"extracted {len(bp_experiment)} {source_label} BioProject <-> Experiment relations")
    bp_experiment = filter_sra_pairs_by_blacklist(bp_experiment, sra_blacklist)
    bp_experiment = filter_pairs_by_blacklist(bp_experiment, bp_blacklist, "left")
    if bp_experiment:
        load_to_db(config, bp_experiment, "bioproject", "sra-experiment")

    # BioProject <-> Run
    bp_run: IdPairs = set()
    for raw_bp, run in iter_bp_run_relations(config, source=source):
        if not raw_bp or not run:
            continue
        converted_bp = convert_id_if_needed(raw_bp, "bioproject", bp_id_to_accession, "accessions_db", source)
        if not converted_bp:
            continue
        if not is_valid_accession(run, "sra-run"):
            log_debug(f"skipping invalid sra-run: {run}", accession=run,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        bp_run.add((converted_bp, run))
    log_info(f"extracted {len(bp_run)} {source_label} BioProject <-> Run relations")
    bp_run = filter_sra_pairs_by_blacklist(bp_run, sra_blacklist)
    bp_run = filter_pairs_by_blacklist(bp_run, bp_blacklist, "left")
    if bp_run:
        load_to_db(config, bp_run, "bioproject", "sra-run")

    # BioProject <-> Analysis
    bp_analysis: IdPairs = set()
    for raw_bp, analysis in iter_bp_analysis_relations(config, source=source):
        if not raw_bp or not analysis:
            continue
        converted_bp = convert_id_if_needed(raw_bp, "bioproject", bp_id_to_accession, "accessions_db", source)
        if not converted_bp:
            continue
        if not is_valid_accession(analysis, "sra-analysis"):
            log_debug(f"skipping invalid sra-analysis: {analysis}", accession=analysis,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        bp_analysis.add((converted_bp, analysis))
    log_info(f"extracted {len(bp_analysis)} {source_label} BioProject <-> Analysis relations")
    bp_analysis = filter_sra_pairs_by_blacklist(bp_analysis, sra_blacklist)
    bp_analysis = filter_pairs_by_blacklist(bp_analysis, bp_blacklist, "left")
    if bp_analysis:
        load_to_db(config, bp_analysis, "bioproject", "sra-analysis")

    # === BioSample <-> SRA ===

    # BioSample <-> Sample
    bs_sample: IdPairs = set()
    for raw_bs, sample in iter_bs_sample_relations(config, source=source):
        if not raw_bs or not sample:
            continue
        converted_bs = convert_id_if_needed(raw_bs, "biosample", bs_id_to_accession, "accessions_db", source)
        if not converted_bs:
            continue
        if not is_valid_accession(sample, "sra-sample"):
            log_debug(f"skipping invalid sra-sample: {sample}", accession=sample,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        bs_sample.add((converted_bs, sample))
    log_info(f"extracted {len(bs_sample)} {source_label} BioSample <-> Sample relations")
    bs_sample = filter_sra_pairs_by_blacklist(bs_sample, sra_blacklist)
    bs_sample = filter_pairs_by_blacklist(bs_sample, bs_blacklist, "left")
    if bs_sample:
        load_to_db(config, bs_sample, "biosample", "sra-sample")

    # BioSample <-> Experiment
    bs_experiment: IdPairs = set()
    for raw_bs, experiment in iter_bs_experiment_relations(config, source=source):
        if not raw_bs or not experiment:
            continue
        converted_bs = convert_id_if_needed(raw_bs, "biosample", bs_id_to_accession, "accessions_db", source)
        if not converted_bs:
            continue
        if not is_valid_accession(experiment, "sra-experiment"):
            log_debug(f"skipping invalid sra-experiment: {experiment}", accession=experiment,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        bs_experiment.add((converted_bs, experiment))
    log_info(f"extracted {len(bs_experiment)} {source_label} BioSample <-> Experiment relations")
    bs_experiment = filter_sra_pairs_by_blacklist(bs_experiment, sra_blacklist)
    bs_experiment = filter_pairs_by_blacklist(bs_experiment, bs_blacklist, "left")
    if bs_experiment:
        load_to_db(config, bs_experiment, "biosample", "sra-experiment")

    # BioSample <-> Run
    bs_run: IdPairs = set()
    for raw_bs, run in iter_bs_run_relations(config, source=source):
        if not raw_bs or not run:
            continue
        converted_bs = convert_id_if_needed(raw_bs, "biosample", bs_id_to_accession, "accessions_db", source)
        if not converted_bs:
            continue
        if not is_valid_accession(run, "sra-run"):
            log_debug(f"skipping invalid sra-run: {run}", accession=run,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        bs_run.add((converted_bs, run))
    log_info(f"extracted {len(bs_run)} {source_label} BioSample <-> Run relations")
    bs_run = filter_sra_pairs_by_blacklist(bs_run, sra_blacklist)
    bs_run = filter_pairs_by_blacklist(bs_run, bs_blacklist, "left")
    if bs_run:
        load_to_db(config, bs_run, "biosample", "sra-run")

    # BioSample <-> Analysis
    bs_analysis: IdPairs = set()
    for raw_bs, analysis in iter_bs_analysis_relations(config, source=source):
        if not raw_bs or not analysis:
            continue
        converted_bs = convert_id_if_needed(raw_bs, "biosample", bs_id_to_accession, "accessions_db", source)
        if not converted_bs:
            continue
        if not is_valid_accession(analysis, "sra-analysis"):
            log_debug(f"skipping invalid sra-analysis: {analysis}", accession=analysis,
                      debug_category=DebugCategory.INVALID_ACCESSION_ID, source=source)
            continue
        bs_analysis.add((converted_bs, analysis))
    log_info(f"extracted {len(bs_analysis)} {source_label} BioSample <-> Analysis relations")
    bs_analysis = filter_sra_pairs_by_blacklist(bs_analysis, sra_blacklist)
    bs_analysis = filter_pairs_by_blacklist(bs_analysis, bs_blacklist, "left")
    if bs_analysis:
        load_to_db(config, bs_analysis, "biosample", "sra-analysis")


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        sra_blacklist = load_sra_blacklist(config)
        bp_blacklist, bs_blacklist = load_blacklist(config)
        bp_id_to_accession = load_id_mapping_tsv(config.result_dir / BP_ID_TO_ACCESSION_FILE_NAME)
        bs_id_to_accession = load_id_mapping_tsv(config.result_dir / BS_ID_TO_ACCESSION_FILE_NAME)

        for source_label in ("sra", "dra"):
            log_info(f"processing {source_label.upper()} internal relations")
            process_sra_internal_relations(
                config,
                source=source_label,  # type: ignore[arg-type]
                sra_blacklist=sra_blacklist,
                bp_blacklist=bp_blacklist,
                bs_blacklist=bs_blacklist,
                bp_id_to_accession=bp_id_to_accession,
                bs_id_to_accession=bs_id_to_accession,
            )


if __name__ == "__main__":
    main()
