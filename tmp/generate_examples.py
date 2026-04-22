"""tests/fixtures の IDF から GEA / MetaboBank の ES document example を 1〜2 件生成する。

dbxrefs は省略 (dblink の duckdb が無いと取得できないため)。
出力先: tmp/{accession}.json (pretty-print)。
"""

from pathlib import Path

from ddbj_search_converter.jsonl.gea import create_gea_entry
from ddbj_search_converter.jsonl.idf_common import parse_idf
from ddbj_search_converter.jsonl.metabobank import create_metabobank_entry

FIXTURES = Path("/app/tests/fixtures")
OUTPUT_DIR = Path("/app/tmp")

GEA_TARGETS = ["E-GEAD-1005", "E-GEAD-1060"]
METABOBANK_TARGETS = ["MTBKS264", "MTBKS70"]


def dump_gea(accession: str) -> Path:
    idf_path = FIXTURES / f"usr/local/resources/gea/experiment/E-GEAD-1000/{accession}/{accession}.idf.txt"
    idf = parse_idf(idf_path)
    entry = create_gea_entry(accession, idf)
    out_path = OUTPUT_DIR / f"{accession}.json"
    out_path.write_text(entry.model_dump_json(by_alias=True, indent=2) + "\n", encoding="utf-8")
    return out_path


def dump_metabobank(accession: str) -> Path:
    idf_path = FIXTURES / f"usr/local/shared_data/metabobank/study/{accession}/{accession}.idf.txt"
    idf = parse_idf(idf_path)
    entry = create_metabobank_entry(accession, idf)
    out_path = OUTPUT_DIR / f"{accession}.json"
    out_path.write_text(entry.model_dump_json(by_alias=True, indent=2) + "\n", encoding="utf-8")
    return out_path


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for acc in GEA_TARGETS:
        print(f"wrote {dump_gea(acc)}")
    for acc in METABOBANK_TARGETS:
        print(f"wrote {dump_metabobank(acc)}")


if __name__ == "__main__":
    main()
