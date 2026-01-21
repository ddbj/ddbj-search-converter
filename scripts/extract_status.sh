#!/bin/bash
# 書き捨てスクリプト: XML から Status/Access/Visibility を抽出して uniq -c | sort -rn
# 使い方: ./extract_status.sh [BASE_DIR] [SHARED_DATA_DIR]

BASE_DIR="${1:-/usr/local/resources}"
SHARED_DATA_DIR="${2:-/usr/local/shared_data}"

echo "=========================================="
echo "BioProject NCBI: <Access> 要素"
echo "=========================================="
grep -oP '(?<=<Access>)[^<]+' "${BASE_DIR}/bioproject/bioproject.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "BioProject NCBI: visibility"
echo "=========================================="
grep -oiP 'visibility[=">][^<"]*["<]?' "${BASE_DIR}/bioproject/bioproject.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "BioProject DDBJ: <Access> 要素"
echo "=========================================="
grep -oP '(?<=<Access>)[^<]+' "${BASE_DIR}/bioproject/ddbj_core_bioproject.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "BioProject DDBJ: visibility"
echo "=========================================="
grep -oiP 'visibility[=">][^<"]*["<]?' "${BASE_DIR}/bioproject/ddbj_core_bioproject.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "BioSample NCBI: access 属性"
echo "=========================================="
zcat "${BASE_DIR}/biosample/biosample_set.xml.gz" 2>/dev/null | grep -oP '(?<=<BioSample )[^>]+' | grep -oP 'access="[^"]+"' | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "BioSample NCBI: <Status status=...>"
echo "=========================================="
zcat "${BASE_DIR}/biosample/biosample_set.xml.gz" 2>/dev/null | grep -oP '<Status status="[^"]+"' | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "BioSample NCBI: visibility"
echo "=========================================="
zcat "${BASE_DIR}/biosample/biosample_set.xml.gz" 2>/dev/null | grep -oiP 'visibility[=">][^<"]*["<]?' | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "BioSample DDBJ: access 属性"
echo "=========================================="
zcat "${BASE_DIR}/biosample/ddbj_biosample_set.xml.gz" 2>/dev/null | grep -oP '(?<=<BioSample )[^>]+' | grep -oP 'access="[^"]+"' | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "BioSample DDBJ: <Status status=...>"
echo "=========================================="
zcat "${BASE_DIR}/biosample/ddbj_biosample_set.xml.gz" 2>/dev/null | grep -oP '<Status status="[^"]+"' | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "BioSample DDBJ: visibility"
echo "=========================================="
zcat "${BASE_DIR}/biosample/ddbj_biosample_set.xml.gz" 2>/dev/null | grep -oiP 'visibility[=">][^<"]*["<]?' | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "JGA Study: status"
echo "=========================================="
grep -oP 'status="[^"]+"' "${SHARED_DATA_DIR}/jga/metadata-history/metadata/jga-study.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "JGA Study: visibility"
echo "=========================================="
grep -oiP 'visibility[=">][^<"]*["<]?' "${SHARED_DATA_DIR}/jga/metadata-history/metadata/jga-study.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "JGA Dataset: status"
echo "=========================================="
grep -oP 'status="[^"]+"' "${SHARED_DATA_DIR}/jga/metadata-history/metadata/jga-dataset.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "JGA Dataset: visibility"
echo "=========================================="
grep -oiP 'visibility[=">][^<"]*["<]?' "${SHARED_DATA_DIR}/jga/metadata-history/metadata/jga-dataset.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "JGA DAC: status"
echo "=========================================="
grep -oP 'status="[^"]+"' "${SHARED_DATA_DIR}/jga/metadata-history/metadata/jga-dac.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "JGA DAC: visibility"
echo "=========================================="
grep -oiP 'visibility[=">][^<"]*["<]?' "${SHARED_DATA_DIR}/jga/metadata-history/metadata/jga-dac.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "JGA Policy: status"
echo "=========================================="
grep -oP 'status="[^"]+"' "${SHARED_DATA_DIR}/jga/metadata-history/metadata/jga-policy.xml" 2>/dev/null | sort | uniq -c | sort -rn

echo ""
echo "=========================================="
echo "JGA Policy: visibility"
echo "=========================================="
grep -oiP 'visibility[=">][^<"]*["<]?' "${SHARED_DATA_DIR}/jga/metadata-history/metadata/jga-policy.xml" 2>/dev/null | sort | uniq -c | sort -rn
