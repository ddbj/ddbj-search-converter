from pathlib import Path

from ddbj_search_converter.config import (
    BP_BLACKLIST_REL_PATH,
    BP_BS_PRESERVED_REL_PATH,
    BS_BLACKLIST_REL_PATH,
    DDBJ_BIOPROJECT_XML,
    DDBJ_BIOSAMPLE_XML,
    DRA_BASE_PATH,
    JGA_ANALYSIS_STUDY_CSV,
    JGA_BLACKLIST_REL_PATH,
    JGA_DATA_EXPERIMENT_CSV,
    JGA_DATASET_ANALYSIS_CSV,
    JGA_DATASET_DATA_CSV,
    JGA_DATASET_POLICY_CSV,
    JGA_EXPERIMENT_STUDY_CSV,
    JGA_POLICY_DAC_CSV,
    JGA_STUDY_XML,
    MTB_BP_PRESERVED_REL_PATH,
    MTB_BS_PRESERVED_REL_PATH,
    NCBI_BIOPROJECT_XML,
    NCBI_BIOSAMPLE_XML,
    SRA_BLACKLIST_REL_PATH,
    TRAD_TLS_ORGANISM_LIST,
    TRAD_TPA_TLS_ORGANISM_LIST,
    TRAD_TPA_TSA_ORGANISM_LIST,
    TRAD_TPA_WGS_ORGANISM_LIST,
    TRAD_TSA_ORGANISM_LIST,
    TRAD_WGS_ORGANISM_LIST,
    get_config,
)
from ddbj_search_converter.logging.logger import log_info, log_warn, run_logger
from ddbj_search_converter.sra_accessions_tab import (
    find_latest_dra_accessions_tab_file,
    find_latest_sra_accessions_tab_file,
)


def get_required_files(const_dir: Path) -> list[tuple[str, Path | None]]:
    files: list[tuple[str, Path | None]] = [
        # Blacklist files
        ("BP Blacklist", const_dir.joinpath(BP_BLACKLIST_REL_PATH)),
        ("BS Blacklist", const_dir.joinpath(BS_BLACKLIST_REL_PATH)),
        ("SRA Blacklist", const_dir.joinpath(SRA_BLACKLIST_REL_PATH)),
        ("JGA Blacklist", const_dir.joinpath(JGA_BLACKLIST_REL_PATH)),
        # Preserved relations
        ("BP-BS Preserved", const_dir.joinpath(BP_BS_PRESERVED_REL_PATH)),
        ("MetaboBank-BP Preserved", const_dir.joinpath(MTB_BP_PRESERVED_REL_PATH)),
        ("MetaboBank-BS Preserved", const_dir.joinpath(MTB_BS_PRESERVED_REL_PATH)),
        # BioSample XML
        ("NCBI BioSample XML", NCBI_BIOSAMPLE_XML),
        ("DDBJ BioSample XML", DDBJ_BIOSAMPLE_XML),
        # BioProject XML
        ("NCBI BioProject XML", NCBI_BIOPROJECT_XML),
        ("DDBJ BioProject XML", DDBJ_BIOPROJECT_XML),
        # JGA files
        ("JGA Study XML", JGA_STUDY_XML),
        ("JGA Dataset-Analysis CSV", JGA_DATASET_ANALYSIS_CSV),
        ("JGA Analysis-Study CSV", JGA_ANALYSIS_STUDY_CSV),
        ("JGA Dataset-Data CSV", JGA_DATASET_DATA_CSV),
        ("JGA Data-Experiment CSV", JGA_DATA_EXPERIMENT_CSV),
        ("JGA Experiment-Study CSV", JGA_EXPERIMENT_STUDY_CSV),
        ("JGA Dataset-Policy CSV", JGA_DATASET_POLICY_CSV),
        ("JGA Policy-DAC CSV", JGA_POLICY_DAC_CSV),
        # TRAD files
        ("TRAD WGS", TRAD_WGS_ORGANISM_LIST),
        ("TRAD TLS", TRAD_TLS_ORGANISM_LIST),
        ("TRAD TSA", TRAD_TSA_ORGANISM_LIST),
        ("TRAD TPA-WGS", TRAD_TPA_WGS_ORGANISM_LIST),
        ("TRAD TPA-TSA", TRAD_TPA_TSA_ORGANISM_LIST),
        ("TRAD TPA-TLS", TRAD_TPA_TLS_ORGANISM_LIST),
    ]

    # SRA/DRA Accessions.tab files
    sra_path = find_latest_sra_accessions_tab_file()
    files.append(("SRA_Accessions.tab", sra_path))
    dra_path = find_latest_dra_accessions_tab_file()
    files.append(("DRA_Accessions.tab", dra_path))

    return files


def get_required_nonempty_dirs() -> list[tuple[str, Path]]:
    """中身があるはずのディレクトリ。

    mount が外れていたり、別のホストパスを bind していたりすると、ディレクトリ自体は
    残ったまま空になる。存在確認だけでは気付けず、後段は「該当ファイル 0 件」として
    黙って進む (DRA の run から SRA ファイルのリンクが消える、など)。
    """
    return [
        ("DRA FASTQ/XML tree", DRA_BASE_PATH.joinpath("fastq")),
        ("DRA SRA file tree", DRA_BASE_PATH.joinpath("sra", "ByExp", "sra", "DRX")),
    ]


def find_missing(
    required_files: list[tuple[str, Path | None]],
    required_dirs: list[tuple[str, Path]],
) -> list[str]:
    missing: list[str] = []
    for name, path in required_files:
        if path is None:
            log_warn(f"missing: {name} (file not found)")
            missing.append(name)
            continue

        if path.exists():
            log_info(f"ok: {name}", file=str(path))
        else:
            log_warn(f"missing: {name}", file=str(path))
            missing.append(name)

    for name, path in required_dirs:
        if not path.is_dir():
            log_warn(f"missing: {name}", file=str(path))
            missing.append(name)
        elif next(path.iterdir(), None) is None:
            log_warn(f"empty: {name} (check the mount or the bind source)", file=str(path))
            missing.append(name)
        else:
            log_info(f"ok: {name}", file=str(path))

    return missing


def main() -> None:
    config = get_config()

    with run_logger(config=config):
        missing = find_missing(get_required_files(config.const_dir), get_required_nonempty_dirs())
        if missing:
            raise Exception(f"{len(missing)} required file(s) or director(ies) are missing or empty.")

        log_info("all required files exist")


if __name__ == "__main__":
    main()
