"""
BioProject/BioSample の大規模 XML を効率的に処理するための関数群。
"""
import gzip
import shutil
from pathlib import Path
from typing import Any, Dict, Generator, List, Literal

import xmltodict

from ddbj_search_converter.config import TODAY_STR, Config


def parse_xml(xml_bytes: bytes) -> Dict[str, Any]:
    """XML bytes を dict にパースする。標準パラメータを使用。"""
    return xmltodict.parse(
        xml_bytes, attr_prefix="", cdata_key="content", process_namespaces=False
    )


def get_tmp_xml_dir(config: Config, subdir: Literal["bioproject", "biosample"]) -> Path:
    tmp_dir = config.result_dir.joinpath(subdir, "tmp_xml", TODAY_STR)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    return tmp_dir


def iterate_xml_element(xml_file: Path, tag: str) -> Generator[bytes, None, None]:
    """行単位でタグを検出。属性付きタグ (<BioSample id="...") に対応するため startswith で判定。"""
    tag_start = f"<{tag}".encode()
    tag_end = f"</{tag}>".encode()

    inside_element = False
    buffer = bytearray()

    with xml_file.open(mode="rb") as f:
        for line in f:
            stripped = line.strip()

            if stripped.startswith(tag_start):
                inside_element = True
                buffer = bytearray(line)
            elif stripped.startswith(tag_end):
                inside_element = False
                buffer.extend(line)
                yield bytes(buffer)
                buffer.clear()
            elif inside_element:
                buffer.extend(line)


def split_xml(
    xml_file: Path,
    output_dir: Path,
    batch_size: int,
    tag: str,
    prefix: str,
    wrapper_start: bytes,
    wrapper_end: bytes,
) -> List[Path]:
    """並列処理用に XML を分割。出力: {prefix}_{n}.xml"""
    output_dir.mkdir(parents=True, exist_ok=True)

    output_files: List[Path] = []
    batch_buffer: List[bytes] = []
    file_count = 1

    for element in iterate_xml_element(xml_file, tag):
        batch_buffer.append(element)

        if len(batch_buffer) >= batch_size:
            output_file = output_dir.joinpath(f"{prefix}_{file_count}.xml")
            _write_split_file(output_file, batch_buffer, wrapper_start, wrapper_end)
            output_files.append(output_file)
            file_count += 1
            batch_buffer.clear()

    # 残りの要素を書き出し
    if batch_buffer:
        output_file = output_dir.joinpath(f"{prefix}_{file_count}.xml")
        _write_split_file(output_file, batch_buffer, wrapper_start, wrapper_end)
        output_files.append(output_file)
        batch_buffer.clear()

    return output_files


def _write_split_file(
    output_file: Path,
    elements: List[bytes],
    wrapper_start: bytes,
    wrapper_end: bytes,
) -> None:
    with output_file.open(mode="wb") as f:
        f.write(wrapper_start)
        f.write(b"\n")
        for element in elements:
            f.write(element)
        f.write(wrapper_end)


def extract_gzip(gz_file: Path, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir.joinpath(gz_file.stem)

    with gzip.open(gz_file, "rb") as f_in:
        with output_file.open("wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return output_file
