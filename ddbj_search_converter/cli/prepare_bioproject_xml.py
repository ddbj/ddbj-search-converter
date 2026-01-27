"""
BioProject XML の前処理 CLI。

BioProject XML を <Package> 単位で batch 分割する。
分割されたファイルは並列処理に使用される。

入力:
- bioproject.xml (NCBI/EBI BioProject)
- ddbj_bioproject.xml (DDBJ BioProject)

出力:
- {result_dir}/bioproject/tmp_xml/{YYYYMMDD}/ncbi_{n}.xml
- {result_dir}/bioproject/tmp_xml/{YYYYMMDD}/ddbj_{n}.xml
"""
from pathlib import Path
from typing import List

from ddbj_search_converter.config import (BIOPROJECT_WRAPPER_END,
                                          BIOPROJECT_WRAPPER_START,
                                          DDBJ_BIOPROJECT_XML,
                                          NCBI_BIOPROJECT_XML, get_config)
from ddbj_search_converter.logging.logger import log_info, run_logger
from ddbj_search_converter.xml_utils import get_tmp_xml_dir, split_xml

DEFAULT_BATCH_SIZE = 2000


def process_bioproject_xml(
    xml_path: Path,
    output_dir: Path,
    prefix: str,
    batch_size: int,
) -> List[Path]:
    """Process BioProject XML file.

    Raises:
        FileNotFoundError: If XML file is not found.
    """
    if not xml_path.exists():
        raise FileNotFoundError(f"file not found: {xml_path}")

    log_info(f"splitting {xml_path} with batch_size={batch_size}", file=str(xml_path))
    output_files = split_xml(
        xml_path,
        output_dir,
        batch_size,
        tag="Package",
        prefix=prefix,
        wrapper_start=BIOPROJECT_WRAPPER_START,
        wrapper_end=BIOPROJECT_WRAPPER_END,
    )

    return output_files


def main() -> None:
    config = get_config()

    with run_logger(config=config):
        output_dir = get_tmp_xml_dir(config, "bioproject")

        # Process NCBI BioProject XML
        ncbi_files = process_bioproject_xml(
            NCBI_BIOPROJECT_XML, output_dir, "ncbi", DEFAULT_BATCH_SIZE,
        )
        log_info(f"created {len(ncbi_files)} NCBI BioProject XML files")

        # Process DDBJ BioProject XML
        ddbj_files = process_bioproject_xml(
            DDBJ_BIOPROJECT_XML, output_dir, "ddbj", DEFAULT_BATCH_SIZE,
        )
        log_info(f"created {len(ddbj_files)} DDBJ BioProject XML files")

        total = len(ncbi_files) + len(ddbj_files)
        log_info(f"total {total} XML files created in {output_dir}")


if __name__ == "__main__":
    main()
