import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import xmltodict
from lxml import etree
from pydantic import BaseModel

from ddbj_search_converter.config import (LOGGER, Config, default_config,
                                          get_config, set_logging_level)
from ddbj_search_converter.utils import bulk_insert_to_es

# 型定義

class Organism(BaseModel):
    identifier: str
    name: str


class Distribution(BaseModel):
    contentUrl: str
    EncodingWarning: str = "JSON"


class Organization(BaseModel):
    content: str


class Grant(BaseModel):
    abbr: str
    content: str


class ExternalLink(BaseModel):
    label: Optional[str]
    URL: Optional[str]


class CommonDocument(BaseModel):
    """\
    # ref.: https://github.com/ddbj/rdf/wiki/JSON
    """
    identifier: str
    distribution: Distribution
    isPartOf: str = "BioProject"
    type: str = "bioproject"
    name: Optional[str]
    url: Optional[str]
    organism: Optional[Organism]
    title: Optional[str]
    description: Optional[str]
    organization: List[Organization]
    publication: List[str]
    grant: List[Grant]
    externalLink: List[ExternalLink]
    download: Optional[str]
    status: str = "public"
    visibility: str = "unrestricted-access"
    datePublished: str
    dateCreated: str
    dateModified: str


AccessionsData = Dict[str, Tuple[str, str, str]]


# 処理

class Args(BaseModel):
    xml_file: Path
    output_file: Optional[Path]
    accessions_tab_file: Optional[Path]
    bulk_es: bool
    batch_size: int = 200


def parse_args(args: List[str]) -> Tuple[Config, Args]:
    """
    優先順位: コマンドライン引数 > 環境変数 > デフォルト値 (config.py)変数を取得する
    Args:
        args (List[str]): _description_
    Returns:
        Tuple[Config, Args]: xml_file, output_file, accession_tab_file,bulk_es, batch_size
    """
    pass


def main() -> None:
    config, args = parse_args(sys.argv[1:])
    # LOGGER ~
    




