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

### ログ・デバッグ

| コマンド | 説明 |
|---------|------|
| `show_log_summary` | ログ集計サマリー（run status + debug category 別カウント + ログレベル別カウント） |
| `show_log` | 指定した run_name のログ詳細表示 (全レベル対応、--level でフィルタ可) |
| `show_dblink_counts` | dblink.tmp.duckdb の relation 件数を (src_type, dst_type) ペアごとに JSON 出力 |
| `dump_debug_report` | 上記を全部まとめて debug_log/ に出力 |

DBLink 作成コマンドは引数を取らず、環境変数から設定を読み込む。
JSONL 生成・Elasticsearch コマンドは `--help` で引数を確認可能。

## 環境変数

| 環境変数 | 説明 | デフォルト |
|---------|------|-----------|
| `DDBJ_SEARCH_CONVERTER_RESULT_DIR` | 結果出力先ディレクトリ | `./ddbj_search_converter_results` |
| `DDBJ_SEARCH_CONVERTER_CONST_DIR` | 定数/共有リソースディレクトリ | `/home/w3ddbjld/const` |
| `DDBJ_SEARCH_CONVERTER_POSTGRES_URL` | PostgreSQL URL | `postgresql://const:const@at098:54301` |
| `DDBJ_SEARCH_CONVERTER_ES_URL` | Elasticsearch URL | `http://ddbj-search-elasticsearch:9200` |
| `DDBJ_SEARCH_CONVERTER_DATE` | TODAY を固定（`YYYYMMDD` 形式） | 未設定（当日日付） |

## Logging

### 出力先

- **JSONL ファイル**: `{result_dir}/logs/{run_id}.log.jsonl` (全ログ)
- **DuckDB**: `{result_dir}/log.duckdb` (集計用、JSONL から自動挿入)
- **stderr**: INFO 以上のログのみ出力 (DEBUG は出力しない)

### メッセージ形式

- **先頭は小文字**: `"processing ..."`, NOT `"Processing ..."`
- 固有名詞 (BioProject, NCBI, SRA 等) は中置なら元の表記を維持
- 先頭に来る場合も小文字: `"bioproject blacklist not found"`
- 英語で記述し、動詞始まりを推奨: `"failed to parse ..."`, `"processing batch ..."`

### Log Level

| Level | 用途 | stderr 出力 | `error=e` 必須 | 例 |
|-------|------|-------------|---------------|-----|
| `CRITICAL` | 処理が続行できない（例外で止まる） | ○ | — (自動付与: `log_failed`) | リソース欠落、DB 接続不可 |
| `ERROR` | ファイル/ワーカー単位の処理失敗 → スキップ | ○ | ○ | `log_error("failed to process ...", error=e, file=...)` |
| `WARNING` | パース失敗 → 空/デフォルトで続行 (レコード単位) | ○ | × | パース失敗 → 空リストで続行 |
| `INFO` | 進捗、完了、統計、設定 | ○ | × | `"processing batch 1/10"` |
| `DEBUG` | 想定内のスキップ、normalize 失敗、unsupported 値 | × | × | ID パターン不一致、`debug_category` 必須 |

**WARNING vs ERROR の境界**: WARNING はレコード単位で空/デフォルトで続行する場合。ERROR はファイル/ワーカー単位で処理をスキップする場合。

### Extra Fields

| フィールド | 付与条件 | 例 |
|-----------|---------|-----|
| `file` | ファイル処理に関連するログ全般 | `file=str(xml_path)` |
| `accession` | 特定の accession に関連するエラー/警告/デバッグ | `accession="PRJDB12345"` |
| `source` | データソース区別可能な場合 | `source="ncbi"`, `"ddbj"`, `"sra"`, `"dra"`, `"preserved"` |
| `debug_category` | `DEBUG` レベルのログには **必ず** 付与 | `debug_category=DebugCategory.INVALID_BIOSAMPLE_ID` |
| `error` | `ERROR` レベルのログには **必ず** 付与 | `error=e` |

### DebugCategory 一覧

| Category | 用途 |
|----------|------|
| `INVALID_BIOSAMPLE_ID` | BioSample accession が無効 (SAM で始まらない) |
| `INVALID_BIOPROJECT_ID` | BioProject accession が無効 (PRJ で始まらない) |
| `PARSE_FALLBACK` | パース失敗でフォールバック |
| `NORMALIZE_BIOSAMPLE_SET_ID` | BioSampleSet ID の正規化失敗 |
| `NORMALIZE_LOCUS_TAG_PREFIX` | LocusTagPrefix の正規化失敗 |
| `NORMALIZE_LOCAL_ID` | LocalID の正規化失敗 |
| `NORMALIZE_ORGANIZATION_NAME` | Organization Name の正規化失敗 |
| `NORMALIZE_GRANT_AGENCY` | Grant Agency の正規化失敗 |
| `NORMALIZE_OWNER_NAME` | Owner Name の正規化失敗 |
| `NORMALIZE_MODEL` | Model の正規化失敗 |
| `FETCH_DATES_FAILED` | XML からの日付取得失敗 |
| `XML_ACCESSION_COLLECT_FAILED` | XML からの accession 収集失敗 |
| `UNSUPPORTED_EXTERNAL_LINK_DB` | 未対応の ExternalLink DB |

### ログ集計

```bash
# CLI × debug_category の count 一覧
show_log_summary --days 7

# 特定の DEBUG ログ詳細を表示
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG
```

## Debugging

### DATE 固定

環境変数 `DDBJ_SEARCH_CONVERTER_DATE` で `TODAY` / `TODAY_STR` を固定できる。過去日のデータで再現・検証する際に使用する。

```bash
DDBJ_SEARCH_CONVERTER_DATE=20260125 init_dblink_db
```

### Debug CLI

| コマンド | 説明 |
|---------|------|
| `show_log_summary` | ログ集計サマリー（run status + debug category 別カウント + ログレベル別カウント） |
| `show_log` | 指定した run_name のログ詳細表示 (全レベル対応、--level でフィルタ可) |
| `show_dblink_counts` | dblink.tmp.duckdb の relation 件数を (src_type, dst_type) ペアごとに JSON 出力 |
| `dump_debug_report` | 上記を全部まとめて debug_log/ に出力 |

### Debugging ワークフロー

dblink パイプラインの各ステップ実行後に確認する手順:

```bash
# 2. ログサマリー確認（各ステップの SUCCESS/FAILED を確認）
show_log_summary --days 1

# 3. relation 件数確認（期待する件数が入っているか）
show_dblink_counts

# 4. 特定のカテゴリの debug ログ詳細確認
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG

# 5. 全 debug 情報をまとめて出力
dump_debug_report
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
