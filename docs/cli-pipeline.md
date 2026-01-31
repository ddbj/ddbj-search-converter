# CLI パイプライン

DDBJ-Search Converter のパイプライン実行と差分更新。

## パイプライン概要

パイプラインは 3 フェーズで構成される。

```
Phase 1: 前処理 + DBLink 構築
    外部リソース -> 前処理コマンド -> DBLink DB -> TSV

Phase 2: JSONL 生成
    XML + DBLink DB -> JSONL

Phase 3: ES 投入
    JSONL -> Elasticsearch
```

## 初回実行（Full）

初回実行時は `--full` フラグを使用し、全データを処理する。

```bash
# === Phase 1: 前処理 + DBLink 構築 ===

# 1. 外部リソースの存在確認
check_external_resources

# 2. XML 準備 (batch 分割)
prepare_bioproject_xml
prepare_biosample_xml
# 出力: {result_dir}/tmp_xml/bp/, {result_dir}/tmp_xml/bs/

# 3. SRA/DRA Accessions DB 構築
build_sra_and_dra_accessions_db
# 出力: {const_dir}/sra/sra_accessions.duckdb, dra_accessions.duckdb

# 4. DBLink DB 作成
init_dblink_db
create_dblink_bp_bs_relations
create_dblink_bp_relations
create_dblink_assembly_and_master_relations
create_dblink_gea_relations
create_dblink_metabobank_relations
create_dblink_jga_relations
create_dblink_sra_internal_relations
finalize_dblink_db
# 出力: {const_dir}/dblink/dblink.duckdb

# 5. TSV 出力
dump_dblink_files
# 出力: {DBLINK_PATH}/*.tsv (16 files)

# === Phase 2: JSONL 生成 ===

# 6. 日付キャッシュ構築 (BP/BS の JSONL 生成前に必須)
build_bp_bs_date_cache
# 出力: {const_dir}/bp_bs_date.duckdb

# 7. SRA/DRA Metadata tar 構築 (SRA JSONL 生成前に必須)
sync_ncbi_tar
sync_dra_tar
# 出力: {const_dir}/sra/NCBI_SRA_Metadata.tar, DRA_Metadata.tar

# 8. JSONL 生成 (初回は --full)
generate_bp_jsonl --full
generate_bs_jsonl --full
generate_sra_jsonl --full
generate_jga_jsonl
# 出力: {result_dir}/jsonl/{YYYYMMDD}/*.jsonl (12 files)

# === Phase 3: ES 投入 ===

# 9. インデックス作成
es_create_index --index bioproject
es_create_index --index biosample
es_create_index --index sra
es_create_index --index jga

# 10. データ投入
es_bulk_insert --index bioproject
es_bulk_insert --index biosample
es_bulk_insert --index sra-submission
es_bulk_insert --index sra-study
es_bulk_insert --index sra-experiment
es_bulk_insert --index sra-run
es_bulk_insert --index sra-sample
es_bulk_insert --index sra-analysis
es_bulk_insert --index jga-study
es_bulk_insert --index jga-dataset
es_bulk_insert --index jga-dac
es_bulk_insert --index jga-policy
```

## 差分更新（Incremental）

初回実行後は、`--full` フラグなしで差分更新モードで実行する。

```bash
# JSONL 生成 (差分更新)
generate_bp_jsonl
generate_bs_jsonl
generate_sra_jsonl
generate_jga_jsonl
```

### last_run.json

差分更新の基準となるタイムスタンプを管理する。

```json
{
  "bioproject": "2026-01-19T00:00:00Z",
  "biosample": "2026-01-19T00:00:00Z",
  "sra": "2026-01-19T00:00:00Z",
  "jga": null
}
```

- 各 JSONL 生成コマンドは、完了時に `last_run.json` を更新する
- `null` の場合は全件処理（`--full` 相当）
- ファイルパス: `{result_dir}/last_run.json`

### margin_days

差分判定時に安全マージンを設ける。

- デフォルト: 30 日
- `last_run.json` のタイムスタンプから `margin_days` を引いた日時以降のデータを処理
- 例: `last_run = 2026-01-30`, `margin_days = 30` だと `2025-12-31` 以降を処理

### データタイプ別の差分判定

| データタイプ | 差分判定方法 |
|-------------|-------------|
| BioProject | XML の `date_modified` フィールド |
| BioSample | XML の `last_update` フィールド |
| SRA | Accessions.tab の `Updated` カラム |
| JGA | 常に全件処理（`null` 固定） |

## 一括実行スクリプト

`scripts/` 以下に、パイプラインを一括実行するスクリプトを用意している。依存関係を考慮し、可能な範囲で並列実行する。

### run_full_pipeline.sh（初回・フル実行）

ES インデックス作成から行う完全なパイプライン実行。

```bash
# 基本的な使い方
./scripts/run_full_pipeline.sh

# 日付を指定
./scripts/run_full_pipeline.sh --date 20260201

# dry-run で実行内容を確認
./scripts/run_full_pipeline.sh --dry-run

# 並列数を変更（デフォルト: 4）
./scripts/run_full_pipeline.sh --parallel 8

# DBLink 構築をスキップ（JSONL + ES のみ）
./scripts/run_full_pipeline.sh --skip-dblink

# ES 操作をスキップ（DBLink + JSONL のみ）
./scripts/run_full_pipeline.sh --skip-es
```

**オプション一覧:**

| オプション | 説明 |
|-----------|------|
| `--date YYYYMMDD` | 処理日付（デフォルト: 今日） |
| `--skip-dblink` | DBLink 構築をスキップ |
| `--skip-tar-sync` | tar ファイル同期をスキップ |
| `--skip-es` | Elasticsearch 操作をスキップ |
| `--dry-run` | 実行内容を表示のみ（実際には実行しない） |
| `--parallel N` | 最大並列数（デフォルト: 4） |

**実行フロー:**

```
PHASE 1: DBLink Construction
├── [並列] prepare_bioproject_xml
├── [並列] prepare_biosample_xml
└── [並列] build_sra_and_dra_accessions_db
    ↓
init_dblink_db
    ↓
├── [並列] create_dblink_* (7 コマンド)
    ↓
finalize_dblink_db → dump_dblink_files

PHASE 2: JSONL Generation
├── [並列] sync_ncbi_tar
├── [並列] sync_dra_tar
└── [並列] build_bp_bs_date_cache
    ↓
├── [並列] generate_bp_jsonl --full
├── [並列] generate_bs_jsonl --full
├── [並列] generate_sra_jsonl --full
└── [並列] generate_jga_jsonl

PHASE 3: Elasticsearch
es_create_index --index all
    ↓
[順次] es_bulk_insert (12 インデックス)
```

### run_incremental_pipeline.sh（日次バッチ・差分実行）

日次バッチ用の差分更新パイプライン。DBLink 再構築 + JSONL 差分生成 + 既存 ES インデックスへの投入。

```bash
# 基本的な使い方（日次バッチ）
./scripts/run_incremental_pipeline.sh

# 日付を指定
./scripts/run_incremental_pipeline.sh --date 20260201

# dry-run で実行内容を確認
./scripts/run_incremental_pipeline.sh --dry-run

# DBLink 構築をスキップ（JSONL のみ再生成したい場合）
./scripts/run_incremental_pipeline.sh --skip-dblink

# ES 投入をスキップ（JSONL 生成のみ）
./scripts/run_incremental_pipeline.sh --skip-es
```

**オプション一覧:**

| オプション | 説明 |
|-----------|------|
| `--date YYYYMMDD` | 処理日付（デフォルト: 今日） |
| `--skip-dblink` | DBLink 構築をスキップ（既存の DBLink を使用） |
| `--skip-xml-prep` | XML 準備をスキップ（既存の分割 XML を使用） |
| `--skip-tar-sync` | tar ファイル同期をスキップ |
| `--skip-es` | Elasticsearch 操作をスキップ |
| `--dry-run` | 実行内容を表示のみ（実際には実行しない） |
| `--parallel N` | 最大並列数（デフォルト: 4） |

**実行フロー:**

```
PHASE 1: DBLink Construction (デフォルトで実行)
├── [並列] prepare_bioproject_xml
├── [並列] prepare_biosample_xml
└── [並列] build_sra_and_dra_accessions_db
    ↓
init_dblink_db → [並列] create_dblink_* → finalize_dblink_db

PHASE 2: Preparation
├── [並列] sync_ncbi_tar
├── [並列] sync_dra_tar
└── [並列] build_bp_bs_date_cache

PHASE 3: JSONL Generation (差分モード)
├── [並列] generate_bp_jsonl
├── [並列] generate_bs_jsonl
├── [並列] generate_sra_jsonl
└── [並列] generate_jga_jsonl

PHASE 4: Elasticsearch Bulk Insert
[順次] es_bulk_insert (12 インデックス)
```

### 環境変数

両スクリプトは以下の環境変数を参照する:

| 環境変数 | 説明 |
|---------|------|
| `DDBJ_SEARCH_CONVERTER_RESULT_DIR` | 出力先ディレクトリ |
| `DDBJ_SEARCH_CONVERTER_CONST_DIR` | const ディレクトリ |
| `DDBJ_SEARCH_CONVERTER_ES_URL` | Elasticsearch URL |
| `DDBJ_SEARCH_CONVERTER_DATE` | 処理日付（`--date` オプションで上書き可） |

### cron 設定例

```bash
# 毎日 AM 3:00 に日次バッチを実行
0 3 * * * /path/to/scripts/run_incremental_pipeline.sh --date $(date +\%Y\%m\%d) >> /var/log/ddbj_search_converter.log 2>&1
```

## Hotfix: regenerate_jsonl

特定の accession の JSONL を再生成するための hotfix/debug 用コマンド。

### 基本的な使い方

```bash
# accession を直接指定
regenerate_jsonl --type bioproject --accessions PRJDB12345 PRJDB67890

# ファイルから指定 (1行1accession)
regenerate_jsonl --type biosample --accession-file /path/to/accessions.txt

# 両方を組み合わせ (union)
regenerate_jsonl --type sra --accessions DRR000001 --accession-file /path/to/more.txt
```

### 出力

- デフォルト出力先: `{result_dir}/regenerate/{date}/`
- `--output-dir` で変更可能

| type | 出力ファイル |
|------|-------------|
| `bioproject` | `bioproject.jsonl` |
| `biosample` | `biosample.jsonl` |
| `sra` | `submission.jsonl`, `study.jsonl`, `experiment.jsonl`, `run.jsonl`, `sample.jsonl`, `analysis.jsonl` |
| `jga` | `jga-study.jsonl`, `jga-dataset.jsonl`, `jga-dac.jsonl`, `jga-policy.jsonl` |

SRA / JGA は該当エントリがある type のファイルのみ生成される。

### ES への投入

```bash
# BioProject
es_bulk_insert --index bioproject \
  --file ddbj_search_converter_results/regenerate/20260128/bioproject.jsonl

# BioSample
es_bulk_insert --index biosample \
  --file ddbj_search_converter_results/regenerate/20260128/biosample.jsonl

# SRA (entity type ごとに index が異なる)
es_bulk_insert --index sra-run \
  --file ddbj_search_converter_results/regenerate/20260128/run.jsonl

# JGA (entity type ごとに index が異なる)
es_bulk_insert --index jga-study \
  --file ddbj_search_converter_results/regenerate/20260128/jga-study.jsonl
```

`_op_type: "index"` のため既存ドキュメントは上書き (upsert 相当) される。

### オプション一覧

| オプション | 必須 | 説明 |
|-----------|------|------|
| `--type` | Yes | `bioproject`, `biosample`, `sra`, `jga` |
| `--accessions` | No* | accession をスペース区切りで指定 |
| `--accession-file` | No* | 1行1accession のファイルパス |
| `--output-dir` | No | 出力先ディレクトリ |

(*) `--accessions` と `--accession-file` は少なくとも1つ必須。両方指定時は union。

**重要**: `regenerate_jsonl` は `last_run.json` を更新しない。

## CLI コマンドリファレンス

全コマンドは環境変数から設定を読み込む。CLI オプションは処理内容の制御のみ。

| 環境変数 | 説明 |
|---------|------|
| `DDBJ_SEARCH_CONVERTER_RESULT_DIR` | 出力先ディレクトリ |
| `DDBJ_SEARCH_CONVERTER_CONST_DIR` | const ディレクトリ（blacklist, DB 等） |
| `DDBJ_SEARCH_CONVERTER_DATE` | 処理日付 (YYYYMMDD) |
| `DDBJ_SEARCH_CONVERTER_ES_URL` | Elasticsearch URL |
| `DDBJ_SEARCH_CONVERTER_POSTGRES_URL` | PostgreSQL URL |

### 外部リソース確認・前処理

| コマンド | オプション | 説明 |
|---------|----------|------|
| `check_external_resources` | - | 必要な外部リソースの存在確認 |
| `prepare_bioproject_xml` | - | BioProject XML を batch 分割 |
| `prepare_biosample_xml` | - | BioSample XML を展開・batch 分割 |

### SRA/DRA

| コマンド | オプション | 説明 |
|---------|----------|------|
| `build_sra_and_dra_accessions_db` | - | SRA/DRA Accessions.tab を DuckDB にロード |
| `sync_ncbi_tar` | `--force-full` | NCBI SRA Metadata tar を同期 |
| `sync_dra_tar` | `--force-rebuild` | DRA Metadata tar を同期 |

### DBLink 構築

| コマンド | オプション | 説明 |
|---------|----------|------|
| `init_dblink_db` | - | DBLink DB を初期化 |
| `create_dblink_bp_bs_relations` | - | BioProject-BioSample 関連を抽出 |
| `create_dblink_bp_relations` | - | BioProject 内部関連 (umbrella, hum-id) を抽出 |
| `create_dblink_assembly_and_master_relations` | - | Assembly/INSDC Master 関連を抽出 |
| `create_dblink_gea_relations` | - | GEA 関連を抽出 |
| `create_dblink_metabobank_relations` | - | MetaboBank 関連を抽出 |
| `create_dblink_jga_relations` | - | JGA 関連を抽出 |
| `create_dblink_sra_internal_relations` | - | SRA 内部関連を抽出 |
| `finalize_dblink_db` | - | DBLink DB を確定 |
| `dump_dblink_files` | - | DBLink DB から TSV ファイルを出力 |

### JSONL 生成

| コマンド | オプション | 説明 |
|---------|----------|------|
| `build_bp_bs_date_cache` | - | BP/BS 日付情報を PostgreSQL から DuckDB キャッシュに構築 |
| `generate_bp_jsonl` | `--full`, `--parallel-num` | BioProject JSONL 生成 |
| `generate_bs_jsonl` | `--full`, `--parallel-num` | BioSample JSONL 生成 |
| `generate_sra_jsonl` | `--full`, `--parallel-num` | SRA JSONL 生成 |
| `generate_jga_jsonl` | - | JGA JSONL 生成（常に全件処理） |
| `regenerate_jsonl` | `--type`, `--accessions`, `--accession-file`, `--output-dir` | 特定 accession の JSONL 再生成 |

### Elasticsearch 操作

| コマンド | オプション | 説明 |
|---------|----------|------|
| `es_create_index` | `--index`, `--skip-existing` | Elasticsearch インデックス作成 |
| `es_delete_index` | `--index`, `--force`, `--skip-missing` | Elasticsearch インデックス削除 |
| `es_bulk_insert` | `--index`, `--dir`, `--file`, `--batch-size` | JSONL を Elasticsearch に一括挿入 |
| `es_list_indexes` | - | 登録済みインデックス一覧 |
| `es_health_check` | `-v` | クラスタヘルス確認 |
| `es_snapshot` | サブコマンド形式 | スナップショット管理 |

### ログ・デバッグ

| コマンド | オプション | 説明 |
|---------|----------|------|
| `show_log_summary` | `--date`, `--raw` | run_name ごとのサマリー |
| `show_log` | `--date`, `--run-name`, `--level`, `--latest` | ログ詳細表示 |
| `show_dblink_counts` | - | DBLink relation 件数を JSON 出力 |
