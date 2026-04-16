# データアーキテクチャ

DDBJ Search Converter のデータフローと構造。

## データフロー

```plain
+-----------------------------------------------------------------------------+
| External Resources                                                          |
|   BioProject XML, BioSample XML, SRA/DRA Accessions.tab, SRA/DRA XML,       |
|   JGA XML/CSV, GEA IDF/SDRF, MetaboBank IDF/SDRF,                           |
|   NCBI Assembly summary, TRAD ORGANISM_LIST, TRAD PostgreSQL (g/e/w-actual) |
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
|   create_dblink_bp_relations         -- umbrella (-> umbrella.duckdb), humandbs|
|   create_dblink_assembly_and_master  -- fetch assembly_summary, ORGANISM    |
|   create_dblink_gea_relations        -- parse IDF/SDRF                      |
|   create_dblink_metabobank_relations -- parse IDF/SDRF, preserved.tsv       |
|   create_dblink_jga_relations        -- parse XML/CSV, humandbs TSV         |
|   create_dblink_sra_internal         -- SRA internal + BP/BS <-> SRA        |
|   create_dblink_insdc_relations     -- preserved.tsv, TRAD PostgreSQL       |
|   finalize_dblink_db -----> {const}/dblink/dblink.duckdb, umbrella.duckdb   |
|   dump_dblink_files --------------> {DBLINK_PATH}/*.tsv (18 files)          |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 2: JSONL Generation                                                   |
|                                                                             |
|   build_bp_bs_date_cache   -- PostgreSQL --> {const}/bp_bs_date.duckdb      |
|   build_bp_bs_status_cache -- Livelist --> {result}/bp_bs_status.duckdb     |
|   sync_ncbi_tar            -- download/merge --> {result}/sra_tar/NCBI_SRA  |
|   sync_dra_tar             -- archive DRA XML -> {result}/sra_tar/DRA.tar   |
|                                                                             |
|   generate_bp_jsonl  -- tmp_xml + date_cache + blacklist [+ dblink]         |
|   generate_bs_jsonl  -- tmp_xml + date_cache + blacklist [+ dblink]         |
|   generate_sra_jsonl -- tar + accessions_db + blacklist [+ dblink]          |
|   generate_jga_jsonl -- XML/CSV + blacklist [+ dblink]                      |
|   ※ dblink (dbXrefs) はデフォルトで含めない。--include-dbxrefs で有効化     |
|                                                                             |
|   Output: {result}/jsonl/{YYYYMMDD}/*.jsonl (12 files)                      |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 3: Elasticsearch Ingestion                                            |
|                                                                             |
|   es_create_index      -- bioproject, biosample, sra, jga                   |
|   es_bulk_insert       -- 12 indexes + 3 aliases (sra, jga, entries)        |
|   es_delete_blacklist  -- blacklist に含まれる doc を ES から削除           |
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
| BP Livelist | `/lustre9/open/archive/tape/ddbj-dbt/bp-collab/bioproject/{YYYYMMDD}.bioproject.ddbj.{public,suppressed,withdrawn}.txt` |
| BS Livelist | `/lustre9/open/archive/tape/ddbj-dbt/bs-collab/biosample/{YYYYMMDD}.biosample.ddbj.{public,suppressed,withdrawn}.txt` |
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

### TRAD PostgreSQL

INSDC 配列 accession と BioProject/BioSample のマッピングを保持する PostgreSQL データベース。`DDBJ_SEARCH_CONVERTER_TRAD_POSTGRES_URL` で接続先を指定する。

| DB 名 | ポート | 内容 |
|--------|--------|------|
| `g-actual` | 54308 | Genome、Gene などの配列データ |
| `e-actual` | 54309 | EST、TSA、TLS などの配列データ |
| `w-actual` | 54310 | WGS 由来の配列データ |

## const ディレクトリ

`{const_dir}` 以下に配置するファイル。

### Blacklist

JSONL 生成時に除外する accession のリスト。

| ファイル | 用途 |
|---------|------|
| `bp/blacklist.txt` | BioProject |
| `bs/blacklist.txt` | BioSample |
| `sra/blacklist.txt` | SRA (Submission, Study, Experiment, Run, Sample, Analysis) |
| `jga/blacklist.txt` | JGA (Study, Dataset, DAC, Policy) |

### Preserved

DBLink 構築時に追加する手動管理の関連。

| ファイル | 用途 |
|---------|------|
| `dblink/bp_bs_preserved.tsv` | BioProject - BioSample |
| `dblink/insdc_bp_preserved.tsv` | INSDC - BioProject |
| `dblink/insdc_bs_preserved.tsv` | INSDC - BioSample |
| `metabobank/mtb_id_bioproject_preserve.tsv` | MetaboBank - BioProject |
| `metabobank/mtb_id_biosample_preserve.tsv` | MetaboBank - BioSample |

### Blacklist / Preserved ファイルの挙動と役割

#### Blacklist ファイル

Blacklist ファイルは、公開すべきでないデータを除外するための仕組み。

**ファイル形式:**

- 1 行 1 accession
- `#` で始まる行はコメントとして無視
- 空行は無視

**使用されるタイミング:**

| ファイル | DBLink 構築 | JSONL 生成 | ES 削除 |
|---------|-------------|-----------|---------|
| `bp/blacklist.txt` | ○ (関連を除外) | ○ (エントリを除外) | ○ |
| `bs/blacklist.txt` | ○ (関連を除外) | ○ (エントリを除外) | ○ |
| `sra/blacklist.txt` | ○ (関連を除外) | ○ (エントリを除外) | ○ |
| `jga/blacklist.txt` | ○ (関連を除外) | ○ (エントリを除外) | ○ |

**挙動の詳細:**

1. **DBLink 構築時**: blacklist に含まれる accession を含む関連ペアを除外。両側のいずれかが blacklist に含まれていれば除外される。
2. **JSONL 生成時**: blacklist に含まれる accession のエントリは JSONL に出力されない。
3. **ES 削除 (`es_delete_blacklist`)**: パイプライン実行後、blacklist に含まれる accession を Elasticsearch から削除。過去にインデックスされていたが後から blacklist に追加されたエントリを削除するために使用。

**運用フロー:**

```plain
1. 問題のある accession を特定
2. 該当する blacklist.txt に追記
3. パイプラインを実行 (JSONL に含まれなくなる)
4. es_delete_blacklist で ES から削除
```

#### Preserved ファイル

Preserved ファイルは、XML/CSV などの元データに記載されていない関連を手動で追加するための仕組み。

**ファイル形式:**

- TSV (タブ区切り)
- ヘッダ行: `from_id` と `to_id` (または同等のカラム名)
- `#` で始まる行はコメントとして無視

**使用されるタイミング:**

| ファイル | 使用コマンド | 挙動 |
|---------|-------------|------|
| `dblink/bp_bs_preserved.tsv` | `create_dblink_bp_bs_relations` | BioProject-BioSample 関連に追加 |
| `dblink/insdc_bp_preserved.tsv` | `create_dblink_insdc_relations` | INSDC-BioProject 関連に追加 |
| `dblink/insdc_bs_preserved.tsv` | `create_dblink_insdc_relations` | INSDC-BioSample 関連に追加 |
| `metabobank/mtb_id_bioproject_preserve.tsv` | `create_dblink_metabobank_relations` | MetaboBank-BioProject 関連に追加 |
| `metabobank/mtb_id_biosample_preserve.tsv` | `create_dblink_metabobank_relations` | MetaboBank-BioSample 関連に追加 |

**挙動の詳細:**

1. **DBLink 構築時**: preserved ファイルの関連ペアを DBLink DB に追加。元データから抽出した関連と合算される。
2. **JSONL 生成時**: DBLink DB から読み込まれた関連が `dbXrefs` フィールドに反映される。preserved で追加された関連も含まれる。

**ユースケース:**

- BioSample の XML に BioProject ID が記載されていないが、関連があることが分かっている場合
- MetaboBank のデータで、IDF/SDRF から抽出できない関連を補完する場合
- 過去のデータで関連情報が欠落している場合の補完

## 中間データベース

### ID 変換マッピング

NCBI XML から抽出した数字 ID -> accession のマッピング。SRA Accessions.tab や TRAD ORGANISM_LIST で数字 ID が使われている場合に変換するために使用。

| ファイル | 用途 |
|---------|------|
| `{result_dir}/bp_id_to_accession.tsv` | BioProject 数字 ID -> accession |
| `{result_dir}/bs_id_to_accession.tsv` | BioSample 数字 ID -> accession |

`create_dblink_bp_bs_relations` で生成され、`create_dblink_assembly_and_master_relations` や `create_dblink_sra_internal_relations` で使用される。

### Accessions DB

SRA/DRA の Accessions.tab を DuckDB にロードしたもの。

| ファイル | 用途 |
|---------|------|
| `{const_dir}/sra/sra_accessions.duckdb` | NCBI SRA |
| `{const_dir}/sra/dra_accessions.duckdb` | DRA |

### Metadata tar

SRA/DRA の Metadata XML をまとめた tar ファイル。`generate_sra_jsonl` で使用。
SSD 上の `{result_dir}/sra_tar/` に配置してランダムアクセスを高速化。

| ファイル | 用途 |
|---------|------|
| `{result_dir}/sra_tar/NCBI_SRA_Metadata.tar` | NCBI SRA (sync_ncbi_tar で作成) |
| `{result_dir}/sra_tar/DRA_Metadata.tar` | DRA (sync_dra_tar で作成) |
| `{result_dir}/sra_tar/*.tar.index.pkl` | tar インデックスキャッシュ (並列読み込み高速化) |

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

### Umbrella DB

BioProject の umbrella 階層構造（親子関連）を有向グラフとして管理する DuckDB。

- `{const_dir}/dblink/umbrella.duckdb`

```sql
CREATE TABLE umbrella_relation (
    parent_accession TEXT NOT NULL,  -- 親 BioProject (umbrella)
    child_accession TEXT NOT NULL    -- 子 BioProject
)
```

- 有向グラフ: `parent_accession` → `child_accession` の方向が明確
- 1 つの child が複数の parent を持つ DAG（有向非巡回グラフ）構造に対応
- `init_dblink_db` で初期化、`create_dblink_bp_relations` でデータ挿入、`finalize_dblink_db` で確定
- JSONL 生成時に `parentBioProjects` / `childBioProjects` フィールドを設定するために使用

**階層構造の特性:**

- 木構造ではなく DAG。1 つの child が複数の parent を持つケースが約 6,700 件ある
- 最大深度は 5 だが、99.6% は depth 1（umbrella → leaf のみ）
- XML に `ProjectTypeTopAdmin` がないが TopAdmin Link で子を持つ BioProject が 138 件存在する。これらは `objectType=BioProject` のまま `childBioProjects` にデータが入る

### Date Cache DB

PostgreSQL から取得した BioProject/BioSample の日付情報をキャッシュした DuckDB。

- `{const_dir}/bp_bs_date.duckdb`

JSONL 生成時に `dateCreated`, `dateModified`, `datePublished` を付与するために使用。

### Status Cache DB

Livelist ファイルから取得した BioProject/BioSample の status 情報をキャッシュした DuckDB。

- `{result_dir}/bp_bs_status.duckdb`

JSONL 生成時に `status` (public/suppressed/withdrawn) を付与するために使用。
キャッシュに accession が存在しない場合は、XML から取得した値 (デフォルト "public") をそのまま使用する。

### DRA ファイルインデックス DB

DRA の FASTQ ディレクトリと SRA ファイルの存在情報を格納した DuckDB。

- `{result_dir}/sra_tar/dra_file_index.duckdb`

```sql
CREATE TABLE dra_fastq_dir (submission TEXT NOT NULL, experiment TEXT NOT NULL);
CREATE TABLE dra_sra_file (run TEXT NOT NULL);
```

`sync_dra_tar` / `build_dra_tar` の末尾で構築される。JSONL 生成時に DRA エントリーの distribution（FASTQ / SRA ダウンロードリンク）の有無を判定するために使用。

## AccessionType 一覧

DBLink では以下の 21 種類の accession タイプを管理する。

| AccessionType | 例 |
|--------------|-----|
| `bioproject` | PRJDB12345, PRJNA123456 |
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
| `insdc` | AB000001, CP035466 |
| `insdc-assembly` | GCA_000000001.1 |
| `insdc-master` | ABCD00000000 |
| `humandbs` | hum0001 |
| `pubmed` | 12345678 |
| `geo` | GSE12345 |
| `taxonomy` | 9606 |

## DBLink TSV 出力（18 種類）

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
| `bioproject-humID/bioproject2humID.tsv` | bioproject - humandbs |
| `gea-bioproject/gea2bioproject.tsv` | gea - bioproject |
| `gea-biosample/gea2biosample.tsv` | gea - biosample |
| `mtb2bp/mtb_id_bioproject.tsv` | metabobank - bioproject |
| `mtb2bs/mtb_id_biosample.tsv` | metabobank - biosample |
| `jga_study-humID/jga_study2humID.tsv` | jga-study - humandbs |
| `jga_dataset-humID/jga_dataset2humID.tsv` | jga-dataset - humandbs |
| `jga_study-pubmed_id/jga_study2pubmed_id.tsv` | jga-study - pubmed |
| `jga_study-jga_dataset/jga_study2jga_dataset.tsv` | jga-study - jga-dataset |
| `insdc-bioproject/insdc2bioproject.tsv` | insdc - bioproject |
| `insdc-biosample/insdc2biosample.tsv` | insdc - biosample |

umbrella 関連は Umbrella DB (`umbrella.duckdb`) で管理し、TSV エクスポートは行わない。

## JSONL 出力

`{result_dir}/{type}/jsonl/{YYYYMMDD}/` 以下に出力される。

### BioProject / BioSample

| ファイルパターン | ES Index |
|-----------------|----------|
| `ddbj_*.jsonl`, `ncbi_*.jsonl` | `bioproject` |
| `ddbj_*.jsonl`, `ncbi_*.jsonl` | `biosample` |

XML ファイル単位で分割されたファイルが出力される。

BioProject エントリーは umbrella 階層構造に対応しており、`parentBioProjects` / `childBioProjects` フィールドで直接の親子関係（推移的閉包ではない）を保持する。これらのフィールドの値は Umbrella DB から取得される。Xref の type は `"bioproject"` で統一し、フィールド名自体が方向を示す。umbrella 関連は `dbXrefs` には含めない。

### SRA

| ファイルパターン | ES Index |
|-----------------|----------|
| `{dra,ncbi}_submission_{NNNN}.jsonl` | `sra-submission` |
| `{dra,ncbi}_study_{NNNN}.jsonl` | `sra-study` |
| `{dra,ncbi}_experiment_{NNNN}.jsonl` | `sra-experiment` |
| `{dra,ncbi}_run_{NNNN}.jsonl` | `sra-run` |
| `{dra,ncbi}_sample_{NNNN}.jsonl` | `sra-sample` |
| `{dra,ncbi}_analysis_{NNNN}.jsonl` | `sra-analysis` |

並列処理のためバッチ単位（5000 submissions/batch）で分割されたファイルが出力される。
`es_bulk_insert` では `--pattern` オプションで対象ファイルを絞り込む。

### JGA

| ファイルパターン | ES Index |
|-----------------|----------|
| `jga-study.jsonl` | `jga-study` |
| `jga-dataset.jsonl` | `jga-dataset` |
| `jga-dac.jsonl` | `jga-dac` |
| `jga-policy.jsonl` | `jga-policy` |

JGA エントリーが `sameAs`（SECONDARY_ID）を持つ場合、ES bulk insert 時に Secondary ID を `_id` とするエイリアスドキュメントを同一インデックスに追加投入する。エイリアスドキュメントの `_source` は Primary ドキュメントと同一（`identifier` は Primary ID のまま）。これにより Secondary ID でも API からエントリーを取得できる。ただしプレフィックス（英字部分）が Primary ID と異なる Secondary ID（例: `AGDD_000001`）は除外する。

### properties フィールド

各エントリーの `properties` フィールドには、元の XML を dict に変換した構造がそのまま格納される。基本は xmltodict の標準挙動（単一子要素は dict、複数子要素は list、テキストノード・XML の attribute 値・None はスカラー）。

**例外（Attribute 要素の正規化）**: XML 要素名に `Attribute`（例: `Attribute`, `STUDY_ATTRIBUTE`）を含むフィールドは、要素が 1 件でも必ず配列として格納する。これは、同一フィールドが 1 件のときに dict、複数件のときに list となる不整合を避けるための正規化。XML の属性（attribute）値とは別の概念なので注意。

対象 index とパス:

| Index | 配列化されるパス |
|---|---|
| `biosample` | `properties.BioSample.Attributes.Attribute` |
| `sra-study` | `properties.STUDY_SET.STUDY.STUDY_ATTRIBUTES.STUDY_ATTRIBUTE` |
| `sra-experiment` | `properties.EXPERIMENT_SET.EXPERIMENT.EXPERIMENT_ATTRIBUTES.EXPERIMENT_ATTRIBUTE` |
| `sra-run` | `properties.RUN_SET.RUN.RUN_ATTRIBUTES.RUN_ATTRIBUTE` |
| `sra-sample` | `properties.SAMPLE_SET.SAMPLE.SAMPLE_ATTRIBUTES.SAMPLE_ATTRIBUTE` |
| `sra-analysis` | `properties.ANALYSIS_SET.ANALYSIS.ANALYSIS_ATTRIBUTES.ANALYSIS_ATTRIBUTE` |
| `jga-study` | `properties.STUDY_ATTRIBUTES.STUDY_ATTRIBUTE` |

`bioproject` / `sra-submission` / `jga-dataset` / `jga-dac` / `jga-policy` は対象外（XML に Attribute 要素なし）。

例 1: JGA Study で `STUDY_ATTRIBUTE` が 1 件のみでも配列で格納される。

```json
{
  "properties": {
    "DESCRIPTOR": {
      "STUDY_TITLE": "Example study"
    },
    "STUDY_ATTRIBUTES": {
      "STUDY_ATTRIBUTE": [
        {"TAG": "NBDC Number", "VALUE": "hum0018"}
      ]
    }
  }
}
```

例 2: BioSample で `Attribute` が 1 件のみでも配列で格納される。親の `Attributes` は dict のまま。

```json
{
  "properties": {
    "BioSample": {
      "Description": {"Title": "Example sample"},
      "Attributes": {
        "Attribute": [
          {"attribute_name": "host", "content": "Homo sapiens"}
        ]
      }
    }
  }
}
```

### Distribution

各エントリーの `distribution` フィールドに格納するダウンロードリンク（Schema.org `DataDownload`）。

| エントリータイプ | JSON | JSON-LD | XML | FASTQ | SRA |
|-----------------|------|---------|-----|-------|-----|
| BioProject | o | o | - | - | - |
| BioSample | o | o | - | - | - |
| SRA (NCBI) | o | o | - | - | - |
| SRA (DRA) 全般 | o | o | o | - | - |
| SRA (DRA) sra-run | o | o | o | o* | o* |
| JGA | o | o | - | - | - |

`*` = DRA ファイルインデックス DB にファイルが存在する場合のみ

## Elasticsearch Index Alias

| Alias | 対象 Index |
|-------|-----------|
| `sra` | `sra-submission`, `sra-study`, `sra-experiment`, `sra-run`, `sra-sample`, `sra-analysis` |
| `jga` | `jga-study`, `jga-dataset`, `jga-dac`, `jga-policy` |
| `entries` | すべてのインデックス |
