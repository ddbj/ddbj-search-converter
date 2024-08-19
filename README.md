# ddbj-search-converter

- [DDBJ-Search](https://ddbj.nig.ac.jp) のデータ投入用 script 群。
`biosample_set.xml` や `bioproject.xml` といった XML file を JSON-Lines (ES bulk data file) に変換し、Elasticsearch に投入する。

## Installation and Start up

`ddbj-search-converter` は、Python 3.8 以上での使用を想定している。

pip を用いた install として:

```bash
python3 -m pip install .
```

これにより、以下の CLI が install される。

- `create_accession_db`: `SRA_accessions.tab` からそれぞれの accession の関係をまとめた、SQLite database を生成する
- `create_dblink_db`: `/lustre9/open/shared_data/dblink` 以下の各 TSV file から、SQLite database を生成する
- `bp_xml2jsonl`: BioProject XML を ES bulk insert 用の JSON-Lines に変換する
- `bp_split_jsonl`: 変換された JSON-Lines を bulk insert に向けて分割する
- `bp_bulk_insert`: ES に bulk insert する
- `bs_xml2jsonl`: BioSample XML を ES bulk insert 用の JSON-Lines に変換する
- `bs_split_jsonl`: 変換された JSON-Lines を bulk insert に向けて分割する
- `bs_bulk_insert`: ES に bulk insert する

例として、

```bash
$ bp_xml2jsonl --help
usage: bp_xml2jsonl [-h] [--accessions-tab-file [ACCESSIONS_TAB_FILE]]
                    [--bulk-es] [--es-base-url ES_BASE_URL]
                    [--batch-size BATCH_SIZE] [--debug]
                    xml_file [output_file]

Convert BioProject XML to JSON-Lines

...
```

### Using Docker

また、Docker を用いて環境を構築することも可能である。

```bash
$ docker network create ddbj-search-network
$ docker compose up -d
$ docker compose exec app bp_xml2jsonl --help
...
```

## Usage

まず、全手順を通して、必要な外部 resource として、

```bash
- /lustre9/open/shared_data/dblink/assembly_genome-bp/assembly_genome2bp.tsv
- /lustre9/open/shared_data/dblink/assembly_genome-bs/assembly_genome2bs.tsv
- /lustre9/open/shared_data/dblink/bioproject-biosample/bioproject2biosample.tsv
- /lustre9/open/shared_data/dblink/bioproject_umbrella-bioproject/bioproject_umbrella2bioproject.tsv
- /lustre9/open/shared_data/dblink/biosample-bioproject/biosample2bioproject.tsv
- /lustre9/open/shared_data/dblink/gea-bioproject/gea2bioproject.tsv
- /lustre9/open/shared_data/dblink/gea-biosample/gea2biosample.tsv
- /lustre9/open/shared_data/dblink/insdc-bioproject/insdc2bioproject.tsv
- /lustre9/open/shared_data/dblink/insdc-biosample/insdc2biosample.tsv
- /lustre9/open/shared_data/dblink/insdc_master-bioproject/insdc_master2bioproject.tsv
- /lustre9/open/shared_data/dblink/insdc_master-biosample/insdc_master2biosample.tsv
- /lustre9/open/shared_data/dblink/mtb2bp/mtb_id_bioproject.tsv
- /lustre9/open/shared_data/dblink/mtb2bs/mtb_id_biosample.tsv
- /lustre9/open/shared_data/dblink/ncbi_biosample_bioproject/ncbi_biosample_bioproject.tsv
- /lustre9/open/shared_data/dblink/taxonomy_biosample/trace_biosample_taxon2bs.tsv
```

また、手順実行後の directory 構成は以下の通りである。

```bash
# TODO update
```

### 0.1. Prepare External Resources

```bash
cd /home/w3ddbjld/tasks/sra/resources
wget ftp://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab

## biosample_set.xml.gz展開

gunzip -c /usr/local/resources/biosample/biosample_set.xml.gz > /home/w3ddbjld/tasks/biosample_jsonl/biosample_set.xml
gunzip -c /usr/local/resources/biosample/ddbj_biosample_set.xml.gz > /home/w3ddbjld/tasks/biosample_jsonl/ddbj/ddbj_biosample_set.xml
```

### 1.1. Create Accession Database (`create_accession_db`)

```bash
cd /home/w3ddbjld/tasks/ddbj-search-converter/src
source .venv/bin/activate
mkdir /home/w3ddbjld/tasks/sra/resources/$(date +%Y%m%d)
cd utils
time ./split_sra_accessions.pl /home/w3ddbjld/tasks/sra/resources/SRA_Accessions.tab /home/w3ddbjld/tasks/sra/resources/$(date +%Y%m%d)
cd ../dblink
time python create_accession_db_m.py /home/w3ddbjld/tasks/sra/resources/$(date +%Y%m%d)  /home/w3ddbjld/tasks/sra/resources/sra_accessions.sqlite
```

### 1.2. Create DBLink Database (`create_dblink_db`)

```bash
time python create_dblink_db.py
```

### 2.1. Convert BioProject XML to JSON-Lines (`bp_xml2jsonl`)

```bash
time python bp_xml2jsonl.py /usr/local/resources/bioproject/bioproject.xml /home/w3ddbjld/tasks/bioproject_jsonl/bioproject.jsonl
cp /usr/local/resources/bioproject/ddbj_core_bioproject.xml /home/w3ddbjld/tasks/bioproject_jsonl/ddbj/ddbj_core_bioproject.xml
time python bp_xml2jsonl.py /home/w3ddbjld/tasks/bioproject_jsonl/ddbj/
```

### 2.2. Split JSON-Lines for Bulk Insert (`bp_split_jsonl`)

```bash
time python split_jsonl.py /home/w3ddbjld/tasks/bioproject_jsonl/bioproject.jsonl  /home/w3ddbjld/tasks/bioproject_jsonl
```

### 2.3. Bulk Insert BioProject Data (`bp_bulk_insert`)

```bash
time python bp_bulk_insert.py /home/w3ddbjld/tasks/bioproject_jsonl/$(date -d yesterday +%Y%m%d)  /home/w3ddbjld/tasks/bioproject_jsonl$(date +%Y%m%d)
```

### 3.1. Convert BioSample XML to JSON-Lines (`bs_xml2jsonl`)

```bash
mkdir /home/w3ddbjld/tasks/biosample_jsonl/$(date +%Y%m%d)
time ./split_xml.pl /home/w3ddbjld/tasks/biosample_jsonl/biosample_set.xml /home/w3ddbjld/tasks/biosample_jsonl/$(date +%Y%m%d)
time python bs_xml2jsonl_mp.py /home/w3ddbjld/tasks/biosample_jsonl/$(date +%Y%m%d) /home/w3ddbjld/tasks/biosample_jsonl/$(date +%Y%m%d)
time python bs_xml2jsonl_mp.py /home/w3ddbjld/tasks/biosample_jsonl/ddbj   /home/w3ddbjld/tasks/biosample_jsonl/$(date +%Y%m%d)
mv /home/w3ddbjld/tasks/biosample_jsonl/ddbj/ddbj_biosample_set.jsonl  /home/w3ddbjld/tasks/biosample_jsonl/$(date +%Y%m%d)
```

### 3.2. Split JSON-Lines for Bulk Insert (`bs_split_jsonl`)

```bash

```

### 3.3. Bulk Insert BioSample Data (`bs_bulk_insert`)

```bash
time python bs_bulk_insert.py /home/w3ddbjld/tasks/biosample_jsonl/ $(date -d yesterday +%Y%m%d)  /home/w3ddbjld/tasks/biosample_jsonl/$(date +%Y%m%d)
```

## Development

開発用環境として、

```bash
$ docker network create ddbj-search-network-dev
$ docker compose -f docker-compose.dev.yml up -d
$ docker compose -f docker-compose.dev.yml exec app bash
# inside the container
$ bp_xml2jsonl --help
...
```

### Linting, Formatting and Testing

lint, format, test は以下のコマンドで実行できる。

```bash
# Lint and Format
$ pylint ./ddbj_search_converter
$ mypy ./ddbj_search_converter
$ isort ./ddbj_search_converter

# Test
$ pytest
```

また、CI / CD は GitHub Actions により実行される。See [`.github/workflows`](./.github/workflows) for more details.

## License

This project is licensed under the [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) license. See the [LICENSE](./LICENSE) file for details.
