#!/bin/bash

# Check argument
if [ -z "$1" ]; then
    echo "Usage: $0 <dblink_basepath>" >&2
    exit 1
fi

BASEPATH="$1"

# DBLink output files (directory/filename)
FILES=(
    "assembly_genome-bp/assembly_genome2bp.tsv"
    "assembly_genome-bs/assembly_genome2bs.tsv"
    "assembly_genome-insdc/assembly_genome2insdc.tsv"
    "insdc_master-bioproject/insdc_master2bioproject.tsv"
    "insdc_master-biosample/insdc_master2biosample.tsv"
    "bioproject-biosample/bioproject2biosample.tsv"
    "biosample-bioproject/biosample2bioproject.tsv"
    "bioproject_umbrella-bioproject/bioproject_umbrella2bioproject.tsv"
    "gea-bioproject/gea2bioproject.tsv"
    "gea-biosample/gea2biosample.tsv"
    "mtb2bp/mtb_id_bioproject.tsv"
    "mtb2bs/mtb_id_biosample.tsv"
    "jga_study-humID/jga_study2humID.tsv"
    "jga_study-pubmed_id/jga_study2pubmed_id.tsv"
    "jga_study-jga_dataset/jga_study2jga_dataset.tsv"
)

# Build JSON output
echo "{"
first=true
for file in "${FILES[@]}"; do
    filepath="${BASEPATH}/${file}"

    if [ -f "$filepath" ]; then
        head_content=$(head -1 "$filepath" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g')
        line_count=$(wc -l < "$filepath")
    else
        head_content="FILE_NOT_FOUND"
        line_count=0
    fi

    if [ "$first" = true ]; then
        first=false
    else
        echo ","
    fi

    printf '  "%s": {"head-1": "%s", "wc-l": %d}' "$file" "$head_content" "$line_count"
done
echo ""
echo "}"
