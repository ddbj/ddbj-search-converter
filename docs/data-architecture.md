# データアーキテクチャ

DDBJ-Search Converter のデータフローと構造。

## データフロー

```
+-----------------------------------------------------------------------------+
| External Resources                                                          |
|   BioProject XML, BioSample XML, SRA/DRA Accessions.tab, SRA/DRA XML,       |
|   JGA XML/CSV, GEA IDF/SDRF, MetaboBank IDF/SDRF,                           |
|   NCBI Assembly summary, TRAD ORGANISM_LIST                                 |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 1: DBLink Build                                                       |
|                                                                             |
|   prepare_bioproject_xml     -- XML batch split --> {result}/tmp_xml/bp/    |
|   prepare_biosample_xml      -- XML batch split --> {result}/tmp_xml/bs/    |
|   build_sra_and_dra_accessions_db ------------> {const}/sra/*.duckdb        |
|                                                                             |
|   init_dblink_db                                                            |
|   create_dblink_bp_bs_relations      -- parse XML, preserved.tsv            |
|   create_dblink_bp_relations         -- umbrella, hum-id                    |
|   create_dblink_assembly_and_master  -- fetch assembly_summary, ORGANISM    |
|   create_dblink_gea_relations        -- parse IDF/SDRF                      |
|   create_dblink_metabobank_relations -- parse IDF/SDRF, preserved.tsv       |
|   create_dblink_jga_relations        -- parse XML/CSV                       |
|   create_dblink_sra_internal         -- from Accessions DB                  |
|   finalize_dblink_db -------------> {const}/dblink/dblink.duckdb            |
|   dump_dblink_files --------------> {DBLINK_PATH}/*.tsv (16 files)          |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 2: JSONL Generation                                                   |
|                                                                             |
|   build_bp_bs_date_cache -- PostgreSQL --> {const}/bp_bs_date.duckdb        |
|   sync_ncbi_tar          -- download/merge --> {const}/sra/NCBI_SRA.tar     |
|   sync_dra_tar           -- archive DRA XML -> {const}/sra/DRA.tar          |
|                                                                             |
|   generate_bp_jsonl  -- tmp_xml + dblink + date_cache + blacklist           |
|   generate_bs_jsonl  -- tmp_xml + dblink + date_cache + blacklist           |
|   generate_sra_jsonl -- tar + dblink + accessions_db + blacklist            |
|   generate_jga_jsonl -- XML/CSV + dblink                                    |
|                                                                             |
|   Output: {result}/jsonl/{YYYYMMDD}/*.jsonl (12 files)                      |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 3: Elasticsearch Ingestion                                            |
|                                                                             |
|   es_create_index -- bioproject, biosample, sra, jga                        |
|   es_bulk_insert  -- 12 indexes + 3 aliases (sra, jga, entries)             |
+-----------------------------------------------------------------------------+
```

## 外部リソース

### XML/CSV/TSV

| リソース | パス |
|---------|------|
| NCBI BioProject XML | `/usr/local/resources/bioproject/bioproject.xml` |
| DDBJ BioProject XML | `/usr/local/resources/bioproject/ddbj_core_bioproject.xml` |
| NCBI BioSample XML | `/usr/local/resources/biosample/biosample_set.xml.gz` |
| DDBJ BioSample XML | `/usr/local/resources/biosample/ddbj_biosample_set.xml.gz` |
| NCBI SRA Accessions | `/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions/{YYYY}/{MM}/SRA_Accessions.tab.{YYYYMMDD}` |
| DRA Accessions | `/lustre9/open/database/ddbj-dbt/dra-private/tracesys/batch/logs/livelist/ReleaseData/public/{YYYYMMDD}.DRA_Accessions.tab` |
| DRA Metadata XML | `/usr/local/resources/dra/fastq/{DRA000}/{DRA000XXX}/{DRA000XXX}.*.xml` |
| NCBI SRA Metadata | `/lustre9/open/database/ddbj-dbt/dra-private/mirror/Metadata/Metadata/` |
| JGA XML | `/usr/local/shared_data/jga/metadata-history/metadata/jga-{study,dataset,dac,policy}.xml` |
| JGA CSV | `/usr/local/shared_data/jga/metadata-history/metadata/*.csv` |
| NCBI Assembly | `https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt` |

### IDF/SDRF

| リソース | パス |
|---------|------|
| GEA | `/usr/local/resources/gea/experiment/E-GEAD-{N000}/E-GEAD-{NNNN}/E-GEAD-{NNNN}.{idf,sdrf}.txt` |
| MetaboBank | `/usr/local/shared_data/metabobank/study/MTBKS{N}/MTBKS{N}.{idf,sdrf}.txt` |

### TRAD ORGANISM_LIST

| リソース | パス |
|---------|------|
| WGS | `/usr/local/resources/trad/wgs/WGS_ORGANISM_LIST.txt` |
| TLS | `/usr/local/resources/trad/tls/TLS_ORGANISM_LIST.txt` |
| TSA | `/usr/local/resources/trad/tsa/TSA_ORGANISM_LIST.txt` |
| TPA WGS | `/usr/local/resources/trad/tpa/wgs/TPA_WGS_ORGANISM_LIST.txt` |
| TPA TSA | `/usr/local/resources/trad/tpa/tsa/TPA_TSA_ORGANISM_LIST.txt` |
| TPA TLS | `/usr/local/resources/trad/tpa/tls/TPA_TLS_ORGANISM_LIST.txt` |

## const ディレクトリ

`{const_dir}` 以下に配置するファイル。

### Blacklist

JSONL 生成時に除外する accession のリスト。

| ファイル | 用途 |
|---------|------|
| `bp/blacklist.txt` | BioProject |
| `bs/blacklist.txt` | BioSample |
| `sra/blacklist.txt` | SRA |

### Preserved

DBLink 構築時に追加する手動管理の関連。

| ファイル | 用途 |
|---------|------|
| `dblink/bp_bs_preserved.tsv` | BioProject - BioSample |
| `metabobank/mtb_id_bioproject_preserve.tsv` | MetaboBank - BioProject |
| `metabobank/mtb_id_biosample_preserve.tsv` | MetaboBank - BioSample |

## 中間データベース

### Accessions DB

SRA/DRA の Accessions.tab を DuckDB にロードしたもの。

| ファイル | 用途 |
|---------|------|
| `{const_dir}/sra/sra_accessions.duckdb` | NCBI SRA |
| `{const_dir}/sra/dra_accessions.duckdb` | DRA |

### Metadata tar

SRA/DRA の Metadata XML をまとめた tar ファイル。`generate_sra_jsonl` で使用。

| ファイル | 用途 |
|---------|------|
| `{const_dir}/sra/NCBI_SRA_Metadata.tar` | NCBI SRA (sync_ncbi_tar で作成) |
| `{const_dir}/sra/DRA_Metadata.tar` | DRA (sync_dra_tar で作成) |

### DBLink DB

accession 間の関連を格納する DuckDB。

- `{const_dir}/dblink/dblink.duckdb`

```sql
CREATE TABLE relation (
    src_type TEXT,       -- AccessionType
    src_accession TEXT,
    dst_type TEXT,       -- AccessionType
    dst_accession TEXT
)
```

無向グラフとして管理（`(A, B)` と `(B, A)` は正規化により同一）。

### Date Cache DB

PostgreSQL から取得した BioProject/BioSample の日付情報をキャッシュした DuckDB。

- `{const_dir}/bp_bs_date.duckdb`

JSONL 生成時に `dateCreated`, `dateModified`, `datePublished` を付与するために使用。

## AccessionType 一覧

DBLink では以下の 21 種類の accession タイプを管理する。

| AccessionType | 例 |
|--------------|-----|
| `bioproject` | PRJDB12345, PRJNA123456 |
| `umbrella-bioproject` | PRJDB99999 |
| `biosample` | SAMD00000001, SAMN12345678 |
| `sra-submission` | DRA000001, SRA000001 |
| `sra-study` | DRP000001, SRP000001 |
| `sra-experiment` | DRX000001, SRX000001 |
| `sra-run` | DRR000001, SRR000001 |
| `sra-sample` | DRS000001, SRS000001 |
| `sra-analysis` | DRZ000001, SRZ000001 |
| `jga-study` | JGAS000001 |
| `jga-dataset` | JGAD000001 |
| `jga-dac` | JGAC000001 |
| `jga-policy` | JGAP000001 |
| `gea` | E-GEAD-1 |
| `metabobank` | MTBKS1 |
| `insdc-assembly` | GCA_000000001.1 |
| `insdc-master` | ABCD00000000 |
| `hum-id` | hum0001 |
| `pubmed-id` | 12345678 |
| `geo` | GSE12345 |
| `taxonomy` | 9606 |

## DBLink TSV 出力（16 種類）

`{DBLINK_PATH}/` 以下に出力される。relation を表す 2 カラムの TSV。

| ファイル | 関連 |
|---------|------|
| `assembly_genome-bp/assembly_genome2bp.tsv` | insdc-assembly - bioproject |
| `assembly_genome-bs/assembly_genome2bs.tsv` | insdc-assembly - biosample |
| `assembly_genome-insdc/assembly_genome2insdc.tsv` | insdc-assembly - insdc-master |
| `insdc_master-bioproject/insdc_master2bioproject.tsv` | insdc-master - bioproject |
| `insdc_master-biosample/insdc_master2biosample.tsv` | insdc-master - biosample |
| `biosample-bioproject/biosample2bioproject.tsv` | biosample - bioproject |
| `bioproject-biosample/bioproject2biosample.tsv` | bioproject - biosample |
| `bioproject_umbrella-bioproject/bioproject_umbrella2bioproject.tsv` | bioproject - umbrella-bioproject |
| `bioproject-humID/bioproject2humID.tsv` | bioproject - hum-id |
| `gea-bioproject/gea2bioproject.tsv` | gea - bioproject |
| `gea-biosample/gea2biosample.tsv` | gea - biosample |
| `mtb2bp/mtb_id_bioproject.tsv` | metabobank - bioproject |
| `mtb2bs/mtb_id_biosample.tsv` | metabobank - biosample |
| `jga_study-humID/jga_study2humID.tsv` | jga-study - hum-id |
| `jga_study-pubmed_id/jga_study2pubmed_id.tsv` | jga-study - pubmed-id |
| `jga_study-jga_dataset/jga_study2jga_dataset.tsv` | jga-study - jga-dataset |

## JSONL 出力（12 種類）

`{result_dir}/jsonl/{YYYYMMDD}/` 以下に出力される。

| ファイル | ES Index |
|---------|----------|
| `bioproject.jsonl` | `bioproject` |
| `biosample.jsonl` | `biosample` |
| `submission.jsonl` | `sra-submission` |
| `study.jsonl` | `sra-study` |
| `experiment.jsonl` | `sra-experiment` |
| `run.jsonl` | `sra-run` |
| `sample.jsonl` | `sra-sample` |
| `analysis.jsonl` | `sra-analysis` |
| `jga-study.jsonl` | `jga-study` |
| `jga-dataset.jsonl` | `jga-dataset` |
| `jga-dac.jsonl` | `jga-dac` |
| `jga-policy.jsonl` | `jga-policy` |

## Elasticsearch Index Alias

| Alias | 対象 Index |
|-------|-----------|
| `sra` | `sra-submission`, `sra-study`, `sra-experiment`, `sra-run`, `sra-sample`, `sra-analysis` |
| `jga` | `jga-study`, `jga-dataset`, `jga-dac`, `jga-policy` |
| `entries` | すべてのインデックス |
