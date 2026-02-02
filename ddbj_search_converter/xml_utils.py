"""
BioProject/BioSample の大規模 XML を効率的に処理するための関数群。
"""
import gzip
import shutil
from pathlib import Path
from typing import Any, Dict, Generator, List, Literal, Optional, Union

from lxml import etree

from ddbj_search_converter.config import TODAY_STR, Config


def _element_to_dict(
    element: etree._Element,
    parent_nsmap: Optional[Dict[Optional[str], str]] = None,
) -> Union[Dict[str, Any], str, None]:
    """lxml Element を dict に変換する。xmltodict と同じ出力形式を維持。"""
    result: Dict[str, Any] = {}
    parent_nsmap = parent_nsmap or {}

    # 名前空間宣言を属性として追加（xmltodict の挙動に合わせる）
    for prefix, uri in element.nsmap.items():
        if parent_nsmap.get(prefix) != uri:
            if prefix is None:
                result["xmlns"] = uri
            else:
                result[f"xmlns:{prefix}"] = uri

    # 属性を追加（プレフィックスなし、attr_prefix="" に対応）
    for attr_key, attr_value in element.attrib.items():
        key: str = str(attr_key)
        if "}" in key:
            key = key.split("}")[1]
        result[key] = attr_value

    # テキストコンテンツを処理
    text = element.text
    if text is not None:
        text = text.strip()
        if text:
            if result:  # 属性または名前空間宣言がある場合
                result["content"] = text
            elif len(element) == 0:  # 子要素がない場合
                return text

    # 子要素を処理
    children: Dict[str, List[Any]] = {}
    for child in element:
        child_tag: str = str(child.tag)
        if "}" in child_tag:
            child_tag = child_tag.split("}")[1]

        child_value = _element_to_dict(child, element.nsmap)
        if child_tag in children:
            children[child_tag].append(child_value)
        else:
            children[child_tag] = [child_value]

    # 単一要素のリストを値に変換
    for child_key, child_list in children.items():
        if len(child_list) == 1:
            result[child_key] = child_list[0]
        else:
            result[child_key] = child_list

    if not result:
        return None

    return result


def parse_xml(xml_bytes: bytes) -> Dict[str, Any]:
    """XML bytes を dict にパースする。lxml ベースで高速化。"""
    root = etree.fromstring(xml_bytes)
    root_tag: str = str(root.tag)
    if "}" in root_tag:
        root_tag = root_tag.split("}")[1]
    return {root_tag: _element_to_dict(root)}


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
