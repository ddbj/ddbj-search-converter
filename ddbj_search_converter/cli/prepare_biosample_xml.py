"""
BioSample XML の前処理 CLI。

gzip 圧縮された BioSample XML を展開し、batch 単位で分割する。
分割されたファイルは並列処理に使用される。

入力:
- biosample_set.xml.gz (NCBI/EBI BioSample マスター XML)
- ddbj_biosample_set.xml.gz (DDBJ BioSample XML)

出力:
- {result_dir}/biosample/tmp_xml/{YYYYMMDD}/ncbi_{n}.xml
- {result_dir}/biosample/tmp_xml/{YYYYMMDD}/ddbj_{n}.xml
"""
from pathlib import Path
from typing import List

from ddbj_search_converter.config import (DDBJ_BIOSAMPLE_XML,
                                          NCBI_BIOSAMPLE_XML, Config,
                                          get_config)
from ddbj_search_converter.logging.logger import log_info, log_warn, run_logger
from ddbj_search_converter.xml_utils import (extract_gzip, get_tmp_xml_dir,
                                             split_xml)

DEFAULT_BATCH_SIZE = 10000

# BioSample XML wrapper tags
BIOSAMPLE_XML_HEADER = b'<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>\n'
BIOSAMPLE_XML_FOOTER = b"</BioSampleSet>\n"


def process_biosample_xml(
    config: Config,
    gz_path: Path,
    prefix: str,
    batch_size: int,
) -> List[Path]:
    if not gz_path.exists():
        log_warn(f"file not found, skipping: {gz_path}", file=str(gz_path))
        return []

    tmp_dir = get_tmp_xml_dir(config, "biosample")

    # Extract gzip
    log_info(f"extracting {gz_path}", file=str(gz_path))
    xml_path = extract_gzip(gz_path, tmp_dir)

    # Split XML
    log_info(f"splitting {xml_path} with batch_size={batch_size}", file=str(xml_path))
    output_files = split_xml(
        xml_path,
        tmp_dir,
        batch_size,
        tag="BioSample",
        prefix=prefix,
        wrapper_start=BIOSAMPLE_XML_HEADER,
        wrapper_end=BIOSAMPLE_XML_FOOTER,
    )

    # Remove extracted XML (keep only split files)
    xml_path.unlink()
    log_info(f"removed temporary file: {xml_path}", file=str(xml_path))

    return output_files


def main() -> None:
    config = get_config()

    with run_logger(config=config):
        # Process NCBI BioSample XML
        ncbi_files = process_biosample_xml(
            config, NCBI_BIOSAMPLE_XML, "ncbi", DEFAULT_BATCH_SIZE,
        )
        log_info(f"created {len(ncbi_files)} NCBI BioSample XML files")

        # Process DDBJ BioSample XML
        ddbj_files = process_biosample_xml(
            config, DDBJ_BIOSAMPLE_XML, "ddbj", DEFAULT_BATCH_SIZE,
        )
        log_info(f"created {len(ddbj_files)} DDBJ BioSample XML files")

        total = len(ncbi_files) + len(ddbj_files)
        tmp_dir = get_tmp_xml_dir(config, "biosample")
        log_info(f"total {total} XML files created in {tmp_dir}")


if __name__ == "__main__":
    main()
