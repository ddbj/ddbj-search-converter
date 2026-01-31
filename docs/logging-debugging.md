# ログとデバッグ

DDBJ-Search Converter のログ出力とデバッグ方法。

## ログ出力

### 出力先

| 出力先 | 説明 |
|-------|------|
| **JSONL ファイル** | `{result_dir}/logs/{run_id}.log.jsonl` (全ログ) |
| **DuckDB** | `{result_dir}/log.duckdb` (集計用、JSONL から自動挿入) |
| **stderr** | INFO 以上のログのみ出力 (DEBUG は出力しない) |

### メッセージ形式

- **先頭は小文字**: `"processing ..."`, NOT `"Processing ..."`
- 固有名詞 (BioProject, NCBI, SRA 等) は中置なら元の表記を維持
- 先頭に来る場合も小文字: `"bioproject blacklist not found"`
- 英語で記述し、動詞始まりを推奨: `"failed to parse ..."`, `"processing batch ..."`

## Log Level

| Level | 用途 | stderr 出力 | `error=e` 必須 | 例 |
|-------|------|-------------|---------------|-----|
| `CRITICAL` | 処理が続行できない（例外で止まる） | ○ | — (自動付与: `log_failed`) | リソース欠落、DB 接続不可 |
| `ERROR` | ファイル/ワーカー単位の処理失敗 → スキップ | ○ | ○ | `log_error("failed to process ...", error=e, file=...)` |
| `WARNING` | パース失敗 → 空/デフォルトで続行 (レコード単位) | ○ | × | パース失敗 → 空リストで続行 |
| `INFO` | 進捗、完了、統計、設定 | ○ | × | `"processing batch 1/10"` |
| `DEBUG` | 想定内のスキップ、normalize 失敗、unsupported 値 | × | × | ID パターン不一致、`debug_category` 必須 |

**WARNING vs ERROR の境界**: WARNING はレコード単位で空/デフォルトで続行する場合。ERROR はファイル/ワーカー単位で処理をスキップする場合。

## Extra Fields

| フィールド | 付与条件 | 例 |
|-----------|---------|-----|
| `file` | ファイル処理に関連するログ全般 | `file=str(xml_path)` |
| `accession` | 特定の accession に関連するエラー/警告/デバッグ | `accession="PRJDB12345"` |
| `source` | データソース区別可能な場合 | `source="ncbi"`, `"ddbj"`, `"sra"`, `"dra"`, `"preserved"` |
| `debug_category` | `DEBUG` レベルのログには **必ず** 付与 | `debug_category=DebugCategory.INVALID_BIOSAMPLE_ID` |
| `error` | `ERROR` レベルのログには **必ず** 付与 | `error=e` |

## DebugCategory 一覧

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

## デバッグコマンド

### show_log_summary

run_name ごとのサマリー (status, duration, log level counts) を表示。

```bash
show_log_summary
```

### show_log

指定した run_name のログ詳細を表示。

```bash
# 最新の run のログを表示
show_log --run-name create_dblink_bp_bs_relations --latest

# レベルでフィルタ
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG
show_log --run-name create_dblink_bp_bs_relations --latest --level ERROR
```

### show_dblink_counts

dblink.tmp.duckdb の relation 件数を (src_type, dst_type) ペアごとに JSON 出力。

```bash
show_dblink_counts
```

## デバッグワークフロー

dblink パイプラインの各ステップ実行後に確認する手順。

```bash
# 1. コマンド実行
create_dblink_bp_bs_relations

# 2. ログサマリー確認（各ステップの SUCCESS/FAILED を確認）
show_log_summary

# 3. relation 件数確認（期待する件数が入っているか）
show_dblink_counts

# 4. 特定のカテゴリの debug ログ詳細確認
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG
```

## DATE 固定

環境変数 `DDBJ_SEARCH_CONVERTER_DATE` で `TODAY` / `TODAY_STR` を固定できる。過去日のデータで再現・検証する際に使用する。

```bash
# 2026年1月25日として実行
DDBJ_SEARCH_CONVERTER_DATE=20260125 init_dblink_db
DDBJ_SEARCH_CONVERTER_DATE=20260125 create_dblink_bp_bs_relations

# 全コマンドに適用
export DDBJ_SEARCH_CONVERTER_DATE=20260125
init_dblink_db
create_dblink_bp_bs_relations
# ...
```

フォーマット: `YYYYMMDD` (例: `20260125`)
