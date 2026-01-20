"""
SRA 内部関連を抽出し、DBLink データベースに挿入する。

入力:
- SRA/DRA accessions DuckDB (sra_accessions.duckdb, dra_accessions.duckdb)
    - sra_accessions_tab モジュールで事前に構築する

SRA/DRA accessions DB から以下の内部関連を抽出する:
- Submission <-> Study
- Study <-> Experiment
- Study <-> Analysis
- Experiment <-> Run
- Experiment <-> Sample
- Run <-> Sample

出力:
- dblink.tmp.duckdb (relation テーブル) に挿入
"""
from typing import Set

from ddbj_search_converter.config import Config, get_config
from ddbj_search_converter.dblink.db import IdPairs, load_to_db
from ddbj_search_converter.dblink.utils import (filter_sra_pairs_by_blacklist,
                                                load_sra_blacklist)
from ddbj_search_converter.logging.logger import log_info, run_logger
from ddbj_search_converter.sra_accessions_tab import (
    SourceKind, iter_experiment_run_relations,
    iter_experiment_sample_relations, iter_run_sample_relations,
    iter_study_analysis_relations, iter_study_experiment_relations,
    iter_submission_study_relations)


def process_sra_internal_relations(
    config: Config,
    *,
    source: SourceKind,
    sra_blacklist: Set[str],
) -> None:
    """
    SRA/DRA 内部関連を抽出して DBLink DB に登録する。

    Args:
        config: 設定
        source: "sra" または "dra"
        sra_blacklist: SRA blacklist (Submission, Study, Experiment, Run, Sample, Analysis の accession)
    """
    source_label = source.upper()

    # Submission <-> Study
    submission_study: IdPairs = set()
    for submission, study in iter_submission_study_relations(config, source=source):
        if submission and study:
            submission_study.add((submission, study))
    log_info(f"extracted {len(submission_study)} {source_label} Submission <-> Study relations")
    submission_study = filter_sra_pairs_by_blacklist(submission_study, sra_blacklist)
    if submission_study:
        load_to_db(config, submission_study, "sra-submission", "sra-study")

    # Study <-> Experiment
    study_experiment: IdPairs = set()
    for study, experiment in iter_study_experiment_relations(config, source=source):
        if study and experiment:
            study_experiment.add((study, experiment))
    log_info(f"extracted {len(study_experiment)} {source_label} Study <-> Experiment relations")
    study_experiment = filter_sra_pairs_by_blacklist(study_experiment, sra_blacklist)
    if study_experiment:
        load_to_db(config, study_experiment, "sra-study", "sra-experiment")

    # Study <-> Analysis
    study_analysis: IdPairs = set()
    for study, analysis in iter_study_analysis_relations(config, source=source):
        if study and analysis:
            study_analysis.add((study, analysis))
    log_info(f"extracted {len(study_analysis)} {source_label} Study <-> Analysis relations")
    study_analysis = filter_sra_pairs_by_blacklist(study_analysis, sra_blacklist)
    if study_analysis:
        load_to_db(config, study_analysis, "sra-study", "sra-analysis")

    # Experiment <-> Run
    experiment_run: IdPairs = set()
    for experiment, run in iter_experiment_run_relations(config, source=source):
        if experiment and run:
            experiment_run.add((experiment, run))
    log_info(f"extracted {len(experiment_run)} {source_label} Experiment <-> Run relations")
    experiment_run = filter_sra_pairs_by_blacklist(experiment_run, sra_blacklist)
    if experiment_run:
        load_to_db(config, experiment_run, "sra-experiment", "sra-run")

    # Experiment <-> Sample
    experiment_sample: IdPairs = set()
    for experiment, sample in iter_experiment_sample_relations(config, source=source):
        if experiment and sample:
            experiment_sample.add((experiment, sample))
    log_info(f"extracted {len(experiment_sample)} {source_label} Experiment <-> Sample relations")
    experiment_sample = filter_sra_pairs_by_blacklist(experiment_sample, sra_blacklist)
    if experiment_sample:
        load_to_db(config, experiment_sample, "sra-experiment", "sra-sample")

    # Run <-> Sample
    run_sample: IdPairs = set()
    for run, sample in iter_run_sample_relations(config, source=source):
        if run and sample:
            run_sample.add((run, sample))
    log_info(f"extracted {len(run_sample)} {source_label} Run <-> Sample relations")
    run_sample = filter_sra_pairs_by_blacklist(run_sample, sra_blacklist)
    if run_sample:
        load_to_db(config, run_sample, "sra-run", "sra-sample")


def main() -> None:
    config = get_config()
    with run_logger(config=config):
        sra_blacklist = load_sra_blacklist(config)

        log_info("processing SRA internal relations")
        process_sra_internal_relations(config, source="sra", sra_blacklist=sra_blacklist)

        log_info("processing DRA internal relations")
        process_sra_internal_relations(config, source="dra", sra_blacklist=sra_blacklist)


if __name__ == "__main__":
    main()
