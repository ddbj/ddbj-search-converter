# ログとデバッグ

DDBJ Search Converter のログ確認とデバッグ方法。

## ログ出力先

| 出力先 | 説明 |
|-------|------|
| **JSONL ファイル** | `{result_dir}/logs/{run_id}.log.jsonl` (全ログ) |
| **DuckDB** | `{result_dir}/log.duckdb` (集計用、JSONL から自動挿入) |
| **stderr** | INFO 以上のログのみ出力 (DEBUG は出力しない) |

## DuckDB スキーマと run_id lifecycle

`log.duckdb` には 1 テーブル `log_records` がある。

```sql
CREATE TABLE log_records (
    run_id UUID NOT NULL,
    run_name TEXT NOT NULL,
    run_date DATE NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    log_level TEXT NOT NULL,         -- DEBUG / INFO / WARNING / ERROR / CRITICAL
    lifecycle TEXT,                   -- 'start' / 'end' / 'failed' (else NULL)
    message TEXT,
    extra JSON,                       -- accession, file, debug_category, error 等
);
-- UNIQUE (run_id, lifecycle) WHERE lifecycle IS NOT NULL
```

`lifecycle` フィールドは 1 つの run の境界マーカーで、通常 INFO/DEBUG/WARNING/ERROR ログでは NULL。`run_logger` context manager が以下のルールで自動付与する:

| lifecycle | 付与タイミング | log_level |
|---|---|---|
| `start` | run 開始時 (context 入口) | INFO |
| `end` | run が例外なく終了したとき | INFO |
| `failed` | run が例外で終了したとき | CRITICAL |

**UNIQUE (run_id, lifecycle) WHERE lifecycle IS NOT NULL** の partial unique 制約により、各 run_id について:

- `start` は最大 1 行
- (`end` または `failed`) は最大 1 行 (= 1 つの run は成功 or 失敗のどちらか 1 度だけ完了する)

通常の INFO/DEBUG ログ (lifecycle が NULL) は UNIQUE 制約の対象外で、同一 run_id について何度でも追加できる。

この制約は `get_last_successful_run_date` (`logging/db.py`) が「INFO + lifecycle='end'」フィルタで run の終了時刻を取り出す経路の前提条件 (同一 run_id について end が複数あれば最新を取らされ、意味の取れない値になるため)。

既存 DB に重複行がある場合は `python -m ddbj_search_converter.logging.migrate_unique_run_id --keep latest --db <path>` で migration できる (1 つの (run_id, lifecycle) について最新 (or 最古) を残し他を削除)。migration を流す前に UNIQUE インデックスは作成できない。

## デバッグコマンド

各コマンドの引数は `--help` を参照する。docs では使い分けと連携例だけを示す。

- **`show_log_summary`**: 対象日 (デフォルト今日) の各 run の SUCCESS / FAILED / IN_PROGRESS とログレベル別件数を出す。最初に流すコマンド
- **`show_log`**: 特定 run の生ログを JSONL で出す。`--latest` で最新 run_id を自動選択、`--level` でフィルタ。jq に流して集計するのが基本動線
- **`show_dblink_counts`**: dblink DB の無向 edge 数を type ペアごとに出す。半辺化スキーマで 1 edge が 2 行持つことを考慮し、`(LEAST(a,b), GREATEST(a,b))` で canonical 化した上で `COUNT / 2` を取るため、表示値はそのまま無向 edge 数と一致する

### jq との連携例

`show_log` は JSONL 出力なので、jq で集計や絞り込みを重ねるのが速い。

```bash
# レベル別カウント
show_log --run-name create_dblink_bp_bs_relations --latest | \
  jq -s 'group_by(.log_level) | map({level: .[0].log_level, count: length})'

# DEBUG をカテゴリ別に集計 (どの normalize 経路が落ちているかの俯瞰)
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG | \
  jq -s 'group_by(.debug_category) | map({category: .[0].debug_category, count: length})'

# 特定カテゴリだけ抽出
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG | \
  jq 'select(.debug_category == "invalid_biosample_id")'

# エラーが多い accession を特定
show_log --run-name create_dblink_bp_bs_relations --latest | \
  jq -r '.accession // empty' | sort | uniq -c | sort -rn | head -20
```

## 典型的なデバッグフロー

```bash
# 1. コマンド実行
create_dblink_bp_bs_relations

# 2. 実行結果を確認（SUCCESS/FAILED）
show_log_summary --raw

# 3. FAILED なら ERROR ログを確認
show_log --run-name create_dblink_bp_bs_relations --latest --level ERROR

# 4. 無向 edge 数が期待通りか確認
show_dblink_counts

# 5. 必要に応じて DEBUG ログを確認（想定内のスキップなど）
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG | \
  jq -s 'group_by(.debug_category) | map({category: .[0].debug_category, count: length})'
```

---

## リファレンス

### Log Level

| Level | 用途 | stderr | 例 |
|-------|------|--------|-----|
| `CRITICAL` | 処理続行不可（例外で停止） | ○ | リソース欠落、DB 接続不可 |
| `ERROR` | ファイル/ワーカー単位の失敗 → スキップ | ○ | `log_error("failed to ...", error=e)` |
| `WARNING` | レコード単位のパース失敗 → 空/デフォルトで続行 | ○ | パース失敗 → 空リストで続行 |
| `INFO` | 進捗、完了、統計、設定 | ○ | `"processing batch 1/10"` |
| `DEBUG` | 想定内のスキップ、正規化失敗 | × | ID パターン不一致 |

### Extra Fields

| フィールド | 付与条件 |
|-----------|---------|
| `file` | ファイル処理に関連するログ |
| `accession` | 特定の accession に関連 |
| `source` | データソース (`"ncbi"`, `"ddbj"`, `"sra"`, `"dra"`) |
| `debug_category` | DEBUG レベルのログには必須 |
| `error` | ERROR レベルのログには必須 |

### DebugCategory

| Category | 用途 |
|----------|------|
| `CONFIG` | 設定情報の出力 |
| `INVALID_ACCESSION_ID` | accession ID が無効 |
| `INVALID_GCF_FORMAT` | GCF フォーマットが無効 |
| `INVALID_WGS_RANGE` | WGS range が無効 |
| `FILE_NOT_FOUND` | ファイルが見つからない |
| `EMPTY_RESULT` | 結果が空 |
| `BLACKLIST_NO_MATCH` | blacklist にマッチなし |
| `PARSE_FALLBACK` | パース失敗でフォールバック |
| `NORMALIZE_BIOSAMPLE_SET_ID` | BioSample set ID の正規化失敗 |
| `NORMALIZE_LOCUS_TAG_PREFIX` | locus tag prefix の正規化失敗 |
| `NORMALIZE_LOCAL_ID` | local ID の正規化失敗 |
| `NORMALIZE_ORGANIZATION_NAME` | organization name の正規化失敗 |
| `NORMALIZE_OWNER_NAME` | owner name の正規化失敗 |
| `NORMALIZE_MODEL` | model の正規化失敗 |
| `FETCH_DATES_FAILED` | XML からの日付取得失敗 |
| `XML_ACCESSION_COLLECT_FAILED` | XML からの accession 収集失敗 |
| `UNSUPPORTED_EXTERNAL_LINK_DB` | 未対応の ExternalLink DB |
