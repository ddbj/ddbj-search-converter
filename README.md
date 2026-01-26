# ddbj-search-converter

[DDBJ-Search](https://ddbj.nig.ac.jp) のデータ投入用パイプラインツール。BioProject、BioSample、SRA/DRA、JGA、GEA、MetaboBank などの生命科学データベース間の関連情報 (DBLink) を構築する。

## 環境構成

- `compose.yml`: 本番環境（遺伝研スパコン）
- `compose.dev.yml`: 開発環境（fixtures 使用）
- `compose.elasticsearch.yml`: Elasticsearch 単体起動用

## DBLink 作成

### 本番環境での実行

```bash
# Docker network 作成 & Elasticsearch 起動
docker network create ddbj-search-network
mkdir -p ./elasticsearch/{data,logs,backup}
chmod 777 ./elasticsearch/{data,logs,backup}
docker compose -f compose.elasticsearch.yml up -d

# Converter container 起動
docker compose up -d --build
docker compose exec app bash

# === ここから container 内 ===

# 外部リソースの存在確認
check_external_resources

# XML 準備 (batch 分割)
prepare_bioproject_xml
prepare_biosample_xml

# SRA/DRA Accessions DB 構築
build_sra_and_dra_accessions_db

# DBLink DB 作成
init_dblink_db
create_dblink_bp_bs_relations
create_dblink_bp_relations
create_dblink_assembly_and_master_relations
create_dblink_gea_relations
create_dblink_metabobank_relations
create_dblink_jga_relations
create_dblink_sra_internal_relations
finalize_dblink_db

# TSV 出力
dump_dblink_files
```

### 開発環境での実行

```bash
# 開発環境起動 (fixtures 使用)
docker compose -f compose.dev.yml up -d --build
docker compose -f compose.dev.yml exec app bash

# 以降のコマンドは本番環境と同じ
```

## CLI コマンド一覧

### DBLink 作成

| コマンド | 説明 |
|---------|------|
| `check_external_resources` | 必要な外部リソースの存在確認 |
| `prepare_bioproject_xml` | BioProject XML を batch 分割 |
| `prepare_biosample_xml` | BioSample XML を展開・batch 分割 |
| `build_sra_and_dra_accessions_db` | SRA/DRA Accessions.tab を DuckDB にロード |
| `init_dblink_db` | DBLink DB を初期化 |
| `create_dblink_bp_bs_relations` | BioProject-BioSample 関連を抽出 |
| `create_dblink_bp_relations` | BioProject 内部関連 (umbrella, hum-id) を抽出 |
| `create_dblink_assembly_and_master_relations` | Assembly/INSDC Master 関連を抽出 |
| `create_dblink_gea_relations` | GEA 関連を抽出 |
| `create_dblink_metabobank_relations` | MetaboBank 関連を抽出 |
| `create_dblink_jga_relations` | JGA 関連を抽出 |
| `create_dblink_sra_internal_relations` | SRA 内部関連 (Study-Experiment-Run-Sample) を抽出 |
| `finalize_dblink_db` | DBLink DB を確定 |
| `dump_dblink_files` | DBLink DB から TSV ファイルを出力 |
| `sync_ncbi_tar` | NCBI SRA Metadata tar を同期 |
| `sync_dra_tar` | DRA Metadata tar を同期 |

### JSONL 生成

| コマンド | 説明 |
|---------|------|
| `generate_bp_jsonl` | BioProject JSONL 生成 |
| `generate_bs_jsonl` | BioSample JSONL 生成 |
| `generate_sra_jsonl` | SRA JSONL 生成（submission/study/experiment/run/sample/analysis） |
| `generate_jga_jsonl` | JGA JSONL 生成 |

### Elasticsearch 操作

| コマンド | 説明 |
|---------|------|
| `es_create_index` | Elasticsearch インデックス作成 |
| `es_delete_index` | Elasticsearch インデックス削除 |
| `es_bulk_insert` | JSONL を Elasticsearch に一括挿入 |
| `es_list_indexes` | 登録済みインデックス一覧 |

### ログ管理

| コマンド | 説明 |
|---------|------|
| `log_summary` | CLI × debug_category の count 一覧を出力 |
| `log_show_debug` | CLI と category を指定して DEBUG ログの詳細を表示 |

DBLink 作成コマンドは引数を取らず、環境変数から設定を読み込む。
JSONL 生成・Elasticsearch コマンドは `--help` で引数を確認可能。

## 環境変数

| 環境変数 | 説明 | デフォルト |
|---------|------|-----------|
| `DDBJ_SEARCH_CONVERTER_RESULT_DIR` | 結果出力先ディレクトリ | `./ddbj_search_converter_results` |
| `DDBJ_SEARCH_CONVERTER_CONST_DIR` | 定数/共有リソースディレクトリ | `/home/w3ddbjld/const` |
| `DDBJ_SEARCH_CONVERTER_POSTGRES_URL` | PostgreSQL URL | `postgresql://const:const@at098:54301` |
| `DDBJ_SEARCH_CONVERTER_ES_URL` | Elasticsearch URL | `http://ddbj-search-elasticsearch:9200` |

## Logging

### 出力先

- **JSONL ファイル**: `{result_dir}/logs/{run_id}.log.jsonl` (全ログ)
- **DuckDB**: `{result_dir}/log.duckdb` (集計用、JSONL から自動挿入)
- **stderr**: INFO 以上のログのみ出力 (DEBUG は出力しない)

### Log Level

| Level | 用途 | stderr 出力 | 例 |
|-------|------|-------------|-----|
| `CRITICAL` | 処理が続行できない（例外で止まる） | ○ | リソース欠落、DB 接続不可 |
| `ERROR` | 失敗してスキップ | ○ | ファイル処理失敗 → スキップ |
| `WARNING` | 成功だが不完全（空/デフォルトで埋める） | ○ | パース失敗 → 空リストで続行 |
| `INFO` | 進捗、完了、統計 | ○ | `Processing batch 1/10` |
| `DEBUG` | 詳細情報（想定内のスキップなど） | × | ID パターン不一致 |

### ログ集計

```bash
# CLI × debug_category の count 一覧
log_summary --days 7

# 特定の DEBUG ログ詳細を表示
log_show_debug --run-name create_dblink_bp_bs_relations --category invalid_biosample_id --limit 100
```

## 開発

### テスト・リント

```bash
pytest
pylint ./ddbj_search_converter
mypy ./ddbj_search_converter
isort ./ddbj_search_converter
```

## License

This project is licensed under the [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) license. See the [LICENSE](./LICENSE) file for details.
