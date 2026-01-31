# ddbj-search-converter

[DDBJ-Search](https://ddbj.nig.ac.jp) のデータ投入用パイプラインツール。BioProject、BioSample、SRA/DRA、JGA、GEA、MetaboBank などの生命科学データベース間の関連情報 (DBLink) を構築する。

## 環境構成

環境変数ファイルで dev / staging / production を切り替える:

| ファイル | 説明 |
|---------|------|
| `compose.yml` | 統合版 Docker Compose |
| `compose.override.podman.yml` | Podman 用の差分設定 |
| `compose.override.yml` | 実際に使う override（gitignore 済み） |
| `env.dev` | 開発環境（fixtures 使用、ES 1GB） |
| `env.staging` | ステージング環境（本番パス、ES 128GB） |
| `env.production` | 本番環境（本番パス、ES 128GB） |
| `.env` | 使用する環境変数（gitignore 済み） |

## DBLink 作成

### 環境起動

**開発環境 (Docker)**:

```bash
# 1. Docker network 作成（初回のみ）
docker network create ddbj-search-network

# 2. 環境変数を設定
cp env.dev .env

# 3. 起動
docker compose up -d --build

# 4. コンテナに入る
docker compose exec app bash
```

**ステージング・本番環境 (Podman)**:

```bash
# 1. Podman network 作成（初回のみ）
podman network create ddbj-search-network

# 2. 環境変数と override を設定
cp env.staging .env                            # または env.production
cp compose.override.podman.yml compose.override.yml

# 3. 起動
podman-compose up -d --build

# 4. コンテナに入る
podman-compose exec app bash
```

### パイプライン実行

```bash
# === container 内 ===

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

# === JSONL 生成 ===

# 日付キャッシュ構築 (BP/BS の JSONL 生成前に必須)
build_bp_bs_date_cache

# JSONL 生成
generate_bp_jsonl --full   # 初回は --full、以降は差分更新
generate_bs_jsonl --full
generate_sra_jsonl --full
generate_jga_jsonl --full
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
| `build_bp_bs_date_cache` | BP/BS 日付情報を PostgreSQL → DuckDB キャッシュに構築 |
| `generate_bp_jsonl` | BioProject JSONL 生成 |
| `generate_bs_jsonl` | BioSample JSONL 生成 |
| `generate_sra_jsonl` | SRA JSONL 生成（submission/study/experiment/run/sample/analysis） |
| `generate_jga_jsonl` | JGA JSONL 生成 |
| `regenerate_jsonl` | 特定 accession の JSONL 再生成（hotfix/debug 用） |

### Elasticsearch 操作

| コマンド | 説明 |
|---------|------|
| `es_create_index` | Elasticsearch インデックス作成 |
| `es_delete_index` | Elasticsearch インデックス削除 |
| `es_bulk_insert` | JSONL を Elasticsearch に一括挿入 |
| `es_list_indexes` | 登録済みインデックス一覧 |
| `es_health_check` | クラスタヘルス確認 (`-v` で詳細表示) |
| `es_snapshot` | スナップショット管理 (サブコマンド形式) |

#### スナップショット管理

```bash
# リポジトリ登録 (ES の path.repo と一致するパスを指定)
es_snapshot repo register --name backup --path /usr/share/elasticsearch/backup

# リポジトリ一覧
es_snapshot repo list

# スナップショット作成 (名前は自動生成)
es_snapshot create --repo backup

# スナップショット作成 (名前指定 + 特定インデックスのみ)
es_snapshot create --repo backup --snapshot my_snapshot --indexes bioproject,biosample

# スナップショット一覧
es_snapshot list --repo backup -v

# スナップショット復元
es_snapshot restore --repo backup --snapshot my_snapshot

# インデックス設定エクスポート (移行検証用)
es_snapshot export-settings --output settings.json
```

#### 定期バックアップ (cron)

`scripts/backup_es.sh` を cron で実行する:

```bash
# 毎日 2:00 AM にバックアップ、7日間保持
0 2 * * * /path/to/scripts/backup_es.sh --repo backup --retention 7 >> /var/log/es_backup.log 2>&1
```

### ログ・デバッグ

| コマンド | 説明 |
|---------|------|
| `show_log_summary` | run_name ごとのサマリー (status, duration, log level counts) |
| `show_log` | 指定した run_name のログ詳細表示 (全レベル対応、--level でフィルタ可) |
| `show_dblink_counts` | dblink.tmp.duckdb の relation 件数を (src_type, dst_type) ペアごとに JSON 出力 |

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
# run_name ごとのサマリー (status, duration, log level counts)
show_log_summary

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
| `show_log_summary` | run_name ごとのサマリー (status, duration, log level counts) |
| `show_log` | 指定した run_name のログ詳細表示 (全レベル対応、--level でフィルタ可) |
| `show_dblink_counts` | dblink.tmp.duckdb の relation 件数を (src_type, dst_type) ペアごとに JSON 出力 |

### 特定 accession の JSONL 再生成 + ES 投入

`regenerate_jsonl` は特定の accession ID を指定して JSONL を再生成する hotfix/debug 用コマンド。通常パイプラインとは独立し、`last_run.json` は更新しない。

```bash
# 基本的な使い方
regenerate_jsonl --type bioproject --accessions PRJDB12345 PRJDB67890
regenerate_jsonl --type biosample --accession-file /path/to/accessions.txt
regenerate_jsonl --type sra --accessions DRR000001 SRR000001
regenerate_jsonl --type jga --accessions JGAS000123 JGAD000456
```

**出力先**: `{result_dir}/regenerate/{date}/` (デフォルト)。`--output-dir` で変更可能。

| type | 出力ファイル |
|------|-------------|
| `bioproject` | `bioproject.jsonl` |
| `biosample` | `biosample.jsonl` |
| `sra` | `submission.jsonl`, `study.jsonl`, `experiment.jsonl`, `run.jsonl`, `sample.jsonl`, `analysis.jsonl` |
| `jga` | `jga-study.jsonl`, `jga-dataset.jsonl`, `jga-dac.jsonl`, `jga-policy.jsonl` |

SRA / JGA は該当エントリがある type のファイルのみ生成される。

**ES に投入** するには `es_bulk_insert` の `--file` を使う。`_op_type: "index"` のため既存ドキュメントは上書き (upsert 相当) される。

```bash
# BioProject
es_bulk_insert --index bioproject \
  --file ddbj_search_converter_results/regenerate/20260128/bioproject.jsonl

# SRA (entity type ごとに index が異なる)
es_bulk_insert --index sra-run \
  --file ddbj_search_converter_results/regenerate/20260128/run.jsonl
es_bulk_insert --index sra-study \
  --file ddbj_search_converter_results/regenerate/20260128/study.jsonl

# JGA
es_bulk_insert --index jga-study \
  --file ddbj_search_converter_results/regenerate/20260128/jga-study.jsonl
```

全オプション:

| オプション | 必須 | 説明 |
|-----------|------|------|
| `--type` | Yes | `bioproject`, `biosample`, `sra`, `jga` |
| `--accessions` | No* | accession をスペース区切りで指定 |
| `--accession-file` | No* | 1行1accession のファイルパス |
| `--output-dir` | No | 出力先ディレクトリ |
| `--result-dir` | No | result ベースディレクトリ |
| `--date` | No | BP/BS の tmp_xml 入力日付 + デフォルト出力ディレクトリ名 |
| `--jga-base-path` | No | JGA XML/CSV ファイルパス |
| `--debug` | No | debug mode |

(*) `--accessions` と `--accession-file` は少なくとも1つ必須。両方指定時は union。

### Debugging ワークフロー

dblink パイプラインの各ステップ実行後に確認する手順:

```bash
# 2. ログサマリー確認（各ステップの SUCCESS/FAILED を確認）
show_log_summary

# 3. relation 件数確認（期待する件数が入っているか）
show_dblink_counts

# 4. 特定のカテゴリの debug ログ詳細確認
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG
```

## 開発

### セットアップ

```bash
# uv がインストールされていない場合
curl -LsSf https://astral.sh/uv/install.sh | sh

# 依存パッケージのインストール (.venv が自動作成される)
uv sync --extra tests
```

### テスト・リント

```bash
uv run pytest -s
uv run pylint ./ddbj_search_converter
uv run mypy ./ddbj_search_converter
uv run isort ./ddbj_search_converter
```

### 依存パッケージの追加

```bash
uv add <package>
# → pyproject.toml と uv.lock が両方更新される
```

## License

This project is licensed under the [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) license. See the [LICENSE](./LICENSE) file for details.
