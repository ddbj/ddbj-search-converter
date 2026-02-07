# ddbj-search-converter

[DDBJ-Search](https://ddbj.nig.ac.jp) のデータ投入用パイプラインツール。

## 概要

DDBJ-Search Converter は、生命科学データベース間の関連情報（DBLink）と BioProject / BioSample / SRA / JGA データを構築し、Elasticsearch へのデータ投入を支援するツール。

**主な機能:**

- 各種 XML/CSV からの関連抽出と DBLink 構築
- JSONL ファイル生成（差分更新対応）
- Elasticsearch へのインデックス作成・bulk insert

**対象データソース:**

| データソース | 説明 |
|-------------|------|
| BioProject | NCBI/DDBJ BioProject |
| BioSample | NCBI/DDBJ BioSample |
| SRA/DRA | INSDC SRA/DRA |
| JGA | Japan Genotype-Phenotype Archive |
| GEA | Gene Expression Archive |
| MetaboBank | MetaboBank |
| INSDC Assembly | INSDC アセンブリ |
| INSDC Master | INSDC WGS/TLS/TSA マスター |

**出力:**

- TSV: 16 種類（DBLink 関連ファイル）
- JSONL: 12 種類（ES 投入用）
- ES Index: 12 種類

## クイックスタート

### 前提条件

- Podman（本番/ステージング）または Docker（開発）
- 遺伝研スパコン上のリソースへのアクセス

### 環境起動（Staging / Production）

```bash
# 1. 環境変数と override を設定
cp env.staging .env # または env.production
cp compose.override.podman.yml compose.override.yml

# 2. Podman network 作成（初回のみ、既に存在していてもエラーにならない）
podman network create ddbj-search-network-staging || true
# production の場合: podman network create ddbj-search-network-production || true

# 3. 起動
podman-compose up -d --build

# 4. コンテナに入る
podman-compose exec app bash
```

### 初回実行

```bash
# 外部リソースの存在確認
check_external_resources

# DBLink 構築
prepare_bioproject_xml && prepare_biosample_xml
build_sra_and_dra_accessions_db
init_dblink_db
create_dblink_bp_bs_relations
create_dblink_bp_relations
create_dblink_assembly_and_master_relations
create_dblink_gea_relations
create_dblink_metabobank_relations
create_dblink_jga_relations
create_dblink_sra_internal_relations
finalize_dblink_db
dump_dblink_files

# JSONL 生成
build_bp_bs_date_cache
sync_ncbi_tar
sync_dra_tar
generate_bp_jsonl --full
generate_bs_jsonl --full
generate_sra_jsonl --full
generate_jga_jsonl --full

# ES 投入
es_create_index --index bioproject
es_create_index --index biosample
es_create_index --index sra
es_create_index --index jga
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

詳細は [docs/cli-pipeline.md](docs/cli-pipeline.md) を参照。

## データアーキテクチャ

```plain
+-----------------------------------------------------------------------------+
| External Resources                                                          |
|   BioProject XML, BioSample XML, SRA/DRA Accessions.tab,                    |
|   JGA XML/CSV, GEA IDF/SDRF, MetaboBank IDF/SDRF,                           |
|   NCBI Assembly summary, TRAD ORGANISM_LIST                                 |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 1: DBLink Build                                                       |
|   prepare_*_xml -> create_dblink_* -> finalize_dblink_db -> dump_dblink_*   |
|                                                                             |
|   Output: DBLink DB (DuckDB) + TSV files (16 types)                         |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 2: JSONL Generation                                                   |
|   generate_bp_jsonl, generate_bs_jsonl, generate_sra_jsonl, generate_jga_*  |
|                                                                             |
|   Output: JSONL files (12 types)                                            |
|     bioproject, biosample, submission, study, experiment, run, sample,      |
|     analysis, jga-study, jga-dataset, jga-dac, jga-policy                   |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 3: Elasticsearch Ingestion                                            |
|   es_create_index -> es_bulk_insert                                         |
|                                                                             |
|   Output: ES Index (12 types) + Alias (sra, jga, entries)                   |
+-----------------------------------------------------------------------------+
```

詳細は [docs/data-architecture.md](docs/data-architecture.md) を参照。

## 環境構築

### 環境ファイル

| ファイル | 説明 |
|---------|------|
| `compose.yml` | 統合版 Docker Compose |
| `compose.override.podman.yml` | Podman 用の差分設定 |
| `env.dev` | 開発環境（fixtures 使用、出力ローカル、ES 512MB） |
| `env.staging` | ステージング環境（入力本番パス、出力ローカル、ES 64GB） |
| `env.production` | 本番環境（入出力とも本番パス、ES 64GB） |

### .env の主要設定

```bash
# === Environment ===
DDBJ_SEARCH_ENV=production     # dev, staging, production

# === Elasticsearch Settings ===
DDBJ_SEARCH_ES_MEM_LIMIT=128g              # コンテナメモリ上限
DDBJ_SEARCH_ES_JAVA_OPTS=-Xms64g -Xmx64g   # JVM ヒープサイズ

# === Volume Paths (for compose.yml) ===
DDBJ_SEARCH_CONVERTER_RESULT_PATH=./ddbj_search_converter_results   # 結果出力先
DDBJ_SEARCH_CONVERTER_CONST_PATH=/home/w3ddbjld/const               # blacklist, preserved 等
DBLINK_PATH=/usr/local/shared_data/dblink     # DBLink TSV 出力先
BIOPROJECT_PATH=/usr/local/resources/bioproject
BIOSAMPLE_PATH=/usr/local/resources/biosample
# ... 他のマウントパス
```

`DDBJ_SEARCH_ENV` により、コンテナ名（`ddbj-search-converter-{env}`, `ddbj-search-es-{env}`）と Docker network 名（`ddbj-search-network-{env}`）が自動決定される。

**DATE 固定**: 過去日のデータで再現・検証する場合は `DDBJ_SEARCH_CONVERTER_DATE=YYYYMMDD` を設定する。

## 開発

Development Container を前提とする。

### 環境起動（Dev）

```bash
# 1. 環境変数を設定
cp env.dev .env

# 2. Docker network 作成（初回のみ、既に存在していてもエラーにならない）
docker network create ddbj-search-network-dev || true

# 3. 起動
docker compose up -d --build

# 4. コンテナに入る
docker compose exec app bash
```

dev 環境では `tests/fixtures/` のテストデータを使用する。外部リソースへのアクセスは不要。

### テスト fixtures

テスト用の fixtures は `tests/fixtures/` に git 管理されている。
fixtures の更新が必要な場合は `scripts/fetch_test_fixtures.sh` を遺伝研スパコン上で実行する。

```bash
# 遺伝研スパコン上で実行
./scripts/fetch_test_fixtures.sh
```

### セットアップ

```bash
# uv がインストールされていない場合
curl -LsSf https://astral.sh/uv/install.sh | sh

# 依存パッケージのインストール
uv sync --extra tests
```

### パッケージ管理

```bash
# パッケージ追加
uv add <package>

# 開発用パッケージ追加
uv add --optional tests <package>

# パッケージ削除
uv remove <package>
```

`uv add` / `uv remove` で `pyproject.toml` と `uv.lock` が更新される。

### テスト・リント

```bash
uv run pytest -v
uv run pylint ./ddbj_search_converter
uv run mypy ./ddbj_search_converter
uv run isort ./ddbj_search_converter
```

詳細は [tests/README.md](tests/README.md) を参照。

## ドキュメント

- [docs/data-architecture.md](docs/data-architecture.md) - データフロー、AccessionType、出力ファイル
- [docs/cli-pipeline.md](docs/cli-pipeline.md) - パイプライン詳細、差分更新、コマンドリファレンス
- [docs/elasticsearch.md](docs/elasticsearch.md) - ES 操作、スナップショット管理
- [docs/logging.md](docs/logging.md) - ログとデバッグ

## License

This project is licensed under the [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) license. See the [LICENSE](./LICENSE) file for details.
