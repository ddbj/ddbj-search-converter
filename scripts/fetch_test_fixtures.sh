#!/bin/bash
# テスト用 fixture データを本番サーバから取得するスクリプト
#
# 使い方:
#   本番サーバ (遺伝研スパコン内) で実行:
#   ./scripts/fetch_test_fixtures.sh
#
# 出力先: tests/fixtures/
#
# 本番環境の volume 構造を再現:
#   tests/fixtures/
#   ├── home/w3ddbjld/const/
#   ├── lustre9/open/database/ddbj-dbt/dra-private/
#   │   ├── mirror/SRA_Accessions/SRA_Accessions.tab
#   │   └── tracesys/batch/logs/livelist/ReleaseData/public/DRA_Accessions.tab
#   └── usr/local/
#       ├── shared_data/
#       │   ├── dblink/
#       │   ├── jga/metadata-history/metadata/
#       │   └── metabobank/study/
#       └── resources/
#           ├── bioproject/
#           ├── biosample/
#           ├── dra/
#           ├── trad/
#           └── gea/experiment/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURES_DIR="$PROJECT_ROOT/tests/fixtures"

# =============================================================================
# ヘルパー関数
# =============================================================================

# テキストファイルを head で取得
# Usage: fetch_text_file <src> <dst> <lines> <label>
fetch_text_file() {
    local src="$1"
    local dst="$2"
    local lines="$3"
    local label="$4"

    mkdir -p "$(dirname "$dst")"
    if [ -f "$src" ]; then
        head -n "$lines" "$src" > "$dst"
        echo "  $label: OK ($lines rows)"
    else
        echo "  $label: スキップ (ファイルなし)"
    fi
}

# XML を N entries 抽出 (非圧縮)
# Usage: extract_xml_entries <src> <dst> <start_tag> <end_tag> <wrapper_start> <wrapper_end> <count> <label>
extract_xml_entries() {
    local src="$1"
    local dst="$2"
    local start_tag="$3"
    local end_tag="$4"
    local wrapper_start="$5"
    local wrapper_end="$6"
    local count="$7"
    local label="$8"

    mkdir -p "$(dirname "$dst")"
    if [ -f "$src" ]; then
        awk -v start_tag="$start_tag" -v end_tag="$end_tag" \
            -v wrapper_start="$wrapper_start" -v wrapper_end="$wrapper_end" \
            -v max_count="$count" \
            'BEGIN { print wrapper_start }
             $0 ~ start_tag { start=1; buf="" }
             start { buf=buf $0 "\n" }
             $0 ~ end_tag { if (start && ++n <= max_count) printf "%s", buf; start=0; if (n >= max_count) exit }
             END { print wrapper_end }' \
            "$src" > "$dst"
        echo "  $label: OK ($count entries)"
    else
        echo "  $label: スキップ (ファイルなし)"
    fi
}

# XML を N entries 抽出 (gz 圧縮入力 → gz 圧縮出力)
# Usage: extract_xml_entries_gz <src.gz> <dst.gz> <start_tag> <end_tag> <wrapper_start> <wrapper_end> <count> <label>
extract_xml_entries_gz() {
    local src="$1"
    local dst="$2"
    local start_tag="$3"
    local end_tag="$4"
    local wrapper_start="$5"
    local wrapper_end="$6"
    local count="$7"
    local label="$8"

    mkdir -p "$(dirname "$dst")"
    if [ -f "$src" ]; then
        zcat "$src" 2>/dev/null | awk -v start_tag="$start_tag" -v end_tag="$end_tag" \
            -v wrapper_start="$wrapper_start" -v wrapper_end="$wrapper_end" \
            -v max_count="$count" \
            'BEGIN { print wrapper_start }
             $0 ~ start_tag { start=1; buf="" }
             start { buf=buf $0 "\n" }
             $0 ~ end_tag { if (start && ++n <= max_count) printf "%s", buf; start=0; if (n >= max_count) exit }
             END { print wrapper_end }' \
            | gzip > "$dst"
        echo "  $label: OK ($count entries, gzipped)"
    else
        echo "  $label: スキップ (ファイルなし)"
    fi
}

# =============================================================================
# メイン処理
# =============================================================================

echo "=== テスト用 fixture データの取得 ==="
echo "出力先: $FIXTURES_DIR"

# === ディレクトリ構造の作成 ===
echo ""
echo "--- ディレクトリ構造を作成中 ---"
mkdir -p "$FIXTURES_DIR/home/w3ddbjld/const/"{bp,bs,sra,dblink,metabobank}
mkdir -p "$FIXTURES_DIR/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions"
mkdir -p "$FIXTURES_DIR/lustre9/open/database/ddbj-dbt/dra-private/tracesys/batch/logs/livelist/ReleaseData/public"
mkdir -p "$FIXTURES_DIR/usr/local/shared_data/"{dblink,metabobank/study}
mkdir -p "$FIXTURES_DIR/usr/local/shared_data/jga/metadata-history/metadata"
mkdir -p "$FIXTURES_DIR/usr/local/resources/"{bioproject,biosample,dra,trad}
mkdir -p "$FIXTURES_DIR/usr/local/resources/gea/experiment"

# === const ===
# コードで使用されるファイル (config.py 参照):
#   - bp/blacklist.txt (BP_BLACKLIST_REL_PATH)
#   - bs/blacklist.txt (BS_BLACKLIST_REL_PATH)
#   - dblink/bp_bs_preserved.tsv (BP_BS_PRESERVED_REL_PATH)
#   - metabobank/mtb_id_bioproject_preserve.tsv (MTB_BP_PRESERVED_REL_PATH)
#   - metabobank/mtb_id_biosample_preserve.tsv (MTB_BS_PRESERVED_REL_PATH)
#   - sra/blacklist.txt (SRA_BLACKLIST_REL_PATH)
echo ""
echo "--- const ディレクトリ ---"
CONST_SRC="/home/w3ddbjld/const"
CONST_DST="$FIXTURES_DIR/home/w3ddbjld/const"

echo "blacklist ファイルを取得中..."
fetch_text_file "$CONST_SRC/bp/blacklist.txt" "$CONST_DST/bp/blacklist.txt" 10 "bp/blacklist.txt"
fetch_text_file "$CONST_SRC/bs/blacklist.txt" "$CONST_DST/bs/blacklist.txt" 10 "bs/blacklist.txt"
fetch_text_file "$CONST_SRC/sra/blacklist.txt" "$CONST_DST/sra/blacklist.txt" 10 "sra/blacklist.txt"

echo "preserved ファイルを取得中..."
fetch_text_file "$CONST_SRC/dblink/bp_bs_preserved.tsv" "$CONST_DST/dblink/bp_bs_preserved.tsv" 11 "dblink/bp_bs_preserved.tsv"
fetch_text_file "$CONST_SRC/metabobank/mtb_id_bioproject_preserve.tsv" "$CONST_DST/metabobank/mtb_id_bioproject_preserve.tsv" 11 "metabobank/mtb_id_bioproject_preserve.tsv"
fetch_text_file "$CONST_SRC/metabobank/mtb_id_biosample_preserve.tsv" "$CONST_DST/metabobank/mtb_id_biosample_preserve.tsv" 11 "metabobank/mtb_id_biosample_preserve.tsv"

# === BioProject ===
echo ""
echo "--- BioProject XML ---"
BP_SRC="/usr/local/resources/bioproject"
BP_DST="$FIXTURES_DIR/usr/local/resources/bioproject"
BP_WRAPPER_START='<?xml version="1.0" encoding="UTF-8"?>\n<PackageSet>'
BP_WRAPPER_END='</PackageSet>'

# NCBI (非圧縮)
extract_xml_entries "$BP_SRC/bioproject.xml" "$BP_DST/bioproject.xml" \
    "<Package>" "</Package>" "$BP_WRAPPER_START" "$BP_WRAPPER_END" 10 "NCBI BioProject"

# DDBJ (非圧縮)
extract_xml_entries "$BP_SRC/ddbj_core_bioproject.xml" "$BP_DST/ddbj_core_bioproject.xml" \
    "<Package>" "</Package>" "$BP_WRAPPER_START" "$BP_WRAPPER_END" 10 "DDBJ BioProject"

# === BioSample ===
echo ""
echo "--- BioSample XML ---"
BS_SRC="/usr/local/resources/biosample"
BS_DST="$FIXTURES_DIR/usr/local/resources/biosample"
BS_WRAPPER_START='<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>'
BS_WRAPPER_END='</BioSampleSet>'

# NCBI (gz)
extract_xml_entries_gz "$BS_SRC/biosample_set.xml.gz" "$BS_DST/biosample_set.xml.gz" \
    "<BioSample " "</BioSample>" "$BS_WRAPPER_START" "$BS_WRAPPER_END" 10 "NCBI BioSample"

# DDBJ (gz)
extract_xml_entries_gz "$BS_SRC/ddbj_biosample_set.xml.gz" "$BS_DST/ddbj_biosample_set.xml.gz" \
    "<BioSample " "</BioSample>" "$BS_WRAPPER_START" "$BS_WRAPPER_END" 10 "DDBJ BioSample"

# === SRA Accessions ===
# SRA: /mirror/SRA_Accessions/YYYY/MM/SRA_Accessions.tab.YYYYMMDD
# DRA: /ReleaseData/public/YYYYMMDD.DRA_Accessions.tab
echo ""
echo "--- SRA/DRA Accessions ---"

SRA_BASE="/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions"
DRA_BASE="/lustre9/open/database/ddbj-dbt/dra-private/tracesys/batch/logs/livelist/ReleaseData/public"

# 今日または前日のファイルを取得
fetch_dated_sra_accessions() {
    local base="$1"
    local dst_base="$2"
    local date_str=$(date +%Y%m%d)
    local year=$(date +%Y)
    local month=$(date +%m)
    local src="$base/$year/$month/SRA_Accessions.tab.$date_str"

    # 今日のファイルがなければ前日を試す
    if [ ! -f "$src" ]; then
        date_str=$(date -d "yesterday" +%Y%m%d)
        year=$(date -d "yesterday" +%Y)
        month=$(date -d "yesterday" +%m)
        src="$base/$year/$month/SRA_Accessions.tab.$date_str"
    fi

    local dst_dir="$dst_base/$year/$month"
    local dst="$dst_dir/SRA_Accessions.tab.$date_str"

    mkdir -p "$dst_dir"
    if [ -f "$src" ]; then
        head -n 101 "$src" > "$dst"
        echo "  SRA_Accessions.tab.$date_str: OK (101 rows)"
    else
        echo "  SRA_Accessions.tab: スキップ (ファイルなし)"
    fi
}

fetch_dated_dra_accessions() {
    local base="$1"
    local dst_base="$2"
    local date_str=$(date +%Y%m%d)
    local src="$base/$date_str.DRA_Accessions.tab"

    # 今日のファイルがなければ前日を試す
    if [ ! -f "$src" ]; then
        date_str=$(date -d "yesterday" +%Y%m%d)
        src="$base/$date_str.DRA_Accessions.tab"
    fi

    local dst="$dst_base/$date_str.DRA_Accessions.tab"

    mkdir -p "$dst_base"
    if [ -f "$src" ]; then
        head -n 101 "$src" > "$dst"
        echo "  $date_str.DRA_Accessions.tab: OK (101 rows)"
    else
        echo "  DRA_Accessions.tab: スキップ (ファイルなし)"
    fi
}

fetch_dated_sra_accessions "$SRA_BASE" "$FIXTURES_DIR$SRA_BASE"
fetch_dated_dra_accessions "$DRA_BASE" "$FIXTURES_DIR$DRA_BASE"

# === JGA ===
# 新実装で使用するファイル (config.py, dblink/jga.py 参照):
#   - jga-study.xml (JGA_STUDY_XML)
#   - *-relation.csv (JGA_*_CSV)
# 旧実装で使用するファイル (jga/jga_generate_jsonl.py 参照):
#   - jga-study.xml, jga-dataset.xml, jga-dac.xml, jga-policy.xml
#   - study.date.csv, dataset.date.csv, dac.date.csv, policy.date.csv
echo ""
echo "--- JGA ---"
JGA_SRC="/usr/local/shared_data/jga/metadata-history/metadata"
JGA_DST="$FIXTURES_DIR/usr/local/shared_data/jga/metadata-history/metadata"
mkdir -p "$JGA_DST"

# XML (10 entries 抽出)
echo "JGA XML を取得中..."
extract_xml_entries "$JGA_SRC/jga-study.xml" "$JGA_DST/jga-study.xml" \
    "<STUDY " "</STUDY>" '<?xml version="1.0" encoding="UTF-8"?>\n<STUDY_SET>' '</STUDY_SET>' \
    10 "jga-study.xml"
extract_xml_entries "$JGA_SRC/jga-dataset.xml" "$JGA_DST/jga-dataset.xml" \
    "<DATASET " "</DATASET>" '<?xml version="1.0" encoding="UTF-8"?>\n<DATASETS>' '</DATASETS>' \
    10 "jga-dataset.xml"
extract_xml_entries "$JGA_SRC/jga-dac.xml" "$JGA_DST/jga-dac.xml" \
    "<DAC " "</DAC>" '<?xml version="1.0" encoding="UTF-8"?>\n<DAC_SET>' '</DAC_SET>' \
    10 "jga-dac.xml"
extract_xml_entries "$JGA_SRC/jga-policy.xml" "$JGA_DST/jga-policy.xml" \
    "<POLICY " "</POLICY>" '<?xml version="1.0" encoding="UTF-8"?>\n<POLICY_SET>' '</POLICY_SET>' \
    10 "jga-policy.xml"

# Date CSV (ヘッダ + 10 行)
echo "JGA Date CSV を取得中..."
fetch_text_file "$JGA_SRC/study.date.csv" "$JGA_DST/study.date.csv" 11 "study.date.csv"
fetch_text_file "$JGA_SRC/dataset.date.csv" "$JGA_DST/dataset.date.csv" 11 "dataset.date.csv"
fetch_text_file "$JGA_SRC/dac.date.csv" "$JGA_DST/dac.date.csv" 11 "dac.date.csv"
fetch_text_file "$JGA_SRC/policy.date.csv" "$JGA_DST/policy.date.csv" 11 "policy.date.csv"

# Relation CSV (ヘッダ + 10 行)
echo "JGA Relation CSV を取得中..."
fetch_text_file "$JGA_SRC/dataset-analysis-relation.csv" "$JGA_DST/dataset-analysis-relation.csv" 11 "dataset-analysis-relation.csv"
fetch_text_file "$JGA_SRC/analysis-study-relation.csv" "$JGA_DST/analysis-study-relation.csv" 11 "analysis-study-relation.csv"
fetch_text_file "$JGA_SRC/dataset-data-relation.csv" "$JGA_DST/dataset-data-relation.csv" 11 "dataset-data-relation.csv"
fetch_text_file "$JGA_SRC/data-experiment-relation.csv" "$JGA_DST/data-experiment-relation.csv" 11 "data-experiment-relation.csv"
fetch_text_file "$JGA_SRC/experiment-study-relation.csv" "$JGA_DST/experiment-study-relation.csv" 11 "experiment-study-relation.csv"
fetch_text_file "$JGA_SRC/dataset-policy-relation.csv" "$JGA_DST/dataset-policy-relation.csv" 11 "dataset-policy-relation.csv"
fetch_text_file "$JGA_SRC/policy-dac-relation.csv" "$JGA_DST/policy-dac-relation.csv" 11 "policy-dac-relation.csv"

# === GEA ===
# ディレクトリ構造: /gea/experiment/E-GEAD-000/E-GEAD-XXXX/E-GEAD-XXXX.idf.txt
echo ""
echo "--- GEA IDF/SDRF ---"
GEA_SRC="/usr/local/resources/gea/experiment"
GEA_DST="$FIXTURES_DIR/usr/local/resources/gea/experiment"

if [ -d "$GEA_SRC" ]; then
    # IDF を 10 個コピー
    echo "GEA IDF を取得中..."
    find "$GEA_SRC" -type f -name "*.idf.txt" 2>/dev/null | head -10 | while read f; do
        entry_dir=$(dirname "$f")
        entry_name=$(basename "$entry_dir")
        parent_name=$(basename "$(dirname "$entry_dir")")
        dst_dir="$GEA_DST/$parent_name/$entry_name"
        mkdir -p "$dst_dir"
        cp "$f" "$dst_dir/"
        echo "  $parent_name/$entry_name/$(basename "$f"): OK"
    done

    # SDRF を 10 個コピー
    echo "GEA SDRF を取得中..."
    find "$GEA_SRC" -type f -name "*.sdrf.txt" 2>/dev/null | head -10 | while read f; do
        entry_dir=$(dirname "$f")
        entry_name=$(basename "$entry_dir")
        parent_name=$(basename "$(dirname "$entry_dir")")
        dst_dir="$GEA_DST/$parent_name/$entry_name"
        mkdir -p "$dst_dir"
        cp "$f" "$dst_dir/"
        echo "  $parent_name/$entry_name/$(basename "$f"): OK"
    done
else
    echo "  GEA: スキップ (ディレクトリなし)"
fi

# === MetaboBank ===
# ディレクトリ構造を確認して適宜調整
echo ""
echo "--- MetaboBank IDF/SDRF ---"
MB_SRC="/usr/local/shared_data/metabobank/study"
MB_DST="$FIXTURES_DIR/usr/local/shared_data/metabobank/study"

if [ -d "$MB_SRC" ]; then
    # IDF を 10 個コピー
    echo "MetaboBank IDF を取得中..."
    find "$MB_SRC" -type f -name "*.idf.txt" 2>/dev/null | head -10 | while read f; do
        # ディレクトリ構造を保持してコピー
        rel_path="${f#$MB_SRC/}"
        dst_file="$MB_DST/$rel_path"
        mkdir -p "$(dirname "$dst_file")"
        cp "$f" "$dst_file"
        echo "  $rel_path: OK"
    done

    # SDRF を 10 個コピー
    echo "MetaboBank SDRF を取得中..."
    find "$MB_SRC" -type f -name "*.sdrf.txt" 2>/dev/null | head -10 | while read f; do
        rel_path="${f#$MB_SRC/}"
        dst_file="$MB_DST/$rel_path"
        mkdir -p "$(dirname "$dst_file")"
        cp "$f" "$dst_file"
        echo "  $rel_path: OK"
    done
else
    echo "  MetaboBank: スキップ (ディレクトリなし)"
fi

# === DRA XML ===
# DRA_Accessions.tab に含まれる accession の XML をコピー
# ディレクトリ構造: /dra/fastq/DRA000/DRA000001/DRA000001.*.xml
echo ""
echo "--- DRA XML ---"
DRA_XML_SRC="/usr/local/resources/dra/fastq"
DRA_XML_DST="$FIXTURES_DIR/usr/local/resources/dra/fastq"

# 取得済みの DRA_Accessions.tab から accession を抽出してコピー
DRA_ACC_FILE="$FIXTURES_DIR$DRA_BASE"
DRA_ACC_TAB=$(find "$DRA_ACC_FILE" -name "*.DRA_Accessions.tab" 2>/dev/null | head -1)

if [ -n "$DRA_ACC_TAB" ] && [ -f "$DRA_ACC_TAB" ] && [ -d "$DRA_XML_SRC" ]; then
    echo "DRA XML を取得中..."
    # Accessions.tab から Submission (DRA で始まる) accession を抽出 (ヘッダスキップ、重複除去、10個)
    tail -n +2 "$DRA_ACC_TAB" | cut -f2 | grep "^DRA" | sort -u | head -10 | while read acc; do
        # DRA000001 -> DRA000/DRA000001
        prefix="${acc:0:6}"  # DRA000
        src_dir="$DRA_XML_SRC/$prefix/$acc"
        if [ -d "$src_dir" ]; then
            dst_dir="$DRA_XML_DST/$prefix/$acc"
            mkdir -p "$dst_dir"
            # XML ファイルをコピー
            for xml in "$src_dir"/*.xml; do
                [ -f "$xml" ] && cp "$xml" "$dst_dir/"
            done
            echo "  $prefix/$acc: OK"
        fi
    done
else
    echo "  DRA XML: スキップ (Accessions.tab またはソースディレクトリなし)"
fi

# === TRAD ===
# check_external_resources.py で使用するファイル:
#   - wgs/WGS_ORGANISM_LIST.txt
#   - tls/TLS_ORGANISM_LIST.txt
#   - tsa/TSA_ORGANISM_LIST.txt
#   - tpa/wgs/TPA_WGS_ORGANISM_LIST.txt
#   - tpa/tsa/TPA_TSA_ORGANISM_LIST.txt
#   - tpa/tls/TPA_TLS_ORGANISM_LIST.txt
echo ""
echo "--- TRAD ---"
TRAD_SRC="/usr/local/resources/trad"
TRAD_DST="$FIXTURES_DIR/usr/local/resources/trad"

echo "TRAD ORGANISM_LIST を取得中..."
fetch_text_file "$TRAD_SRC/wgs/WGS_ORGANISM_LIST.txt" "$TRAD_DST/wgs/WGS_ORGANISM_LIST.txt" 11 "wgs/WGS_ORGANISM_LIST.txt"
fetch_text_file "$TRAD_SRC/tls/TLS_ORGANISM_LIST.txt" "$TRAD_DST/tls/TLS_ORGANISM_LIST.txt" 11 "tls/TLS_ORGANISM_LIST.txt"
fetch_text_file "$TRAD_SRC/tsa/TSA_ORGANISM_LIST.txt" "$TRAD_DST/tsa/TSA_ORGANISM_LIST.txt" 11 "tsa/TSA_ORGANISM_LIST.txt"
fetch_text_file "$TRAD_SRC/tpa/wgs/TPA_WGS_ORGANISM_LIST.txt" "$TRAD_DST/tpa/wgs/TPA_WGS_ORGANISM_LIST.txt" 11 "tpa/wgs/TPA_WGS_ORGANISM_LIST.txt"
fetch_text_file "$TRAD_SRC/tpa/tsa/TPA_TSA_ORGANISM_LIST.txt" "$TRAD_DST/tpa/tsa/TPA_TSA_ORGANISM_LIST.txt" 11 "tpa/tsa/TPA_TSA_ORGANISM_LIST.txt"
fetch_text_file "$TRAD_SRC/tpa/tls/TPA_TLS_ORGANISM_LIST.txt" "$TRAD_DST/tpa/tls/TPA_TLS_ORGANISM_LIST.txt" 11 "tpa/tls/TPA_TLS_ORGANISM_LIST.txt"

# === 完了 ===
echo ""
echo "=== 完了 ==="
echo "取得したファイル:"
find "$FIXTURES_DIR" -type f | sort
echo ""
echo "ディレクトリ構造:"
find "$FIXTURES_DIR" -type d | sort
