# ログとデバッグ

DDBJ Search Converter のログ確認とデバッグ方法。

## ログ出力先

| 出力先 | 説明 |
|-------|------|
| **JSONL ファイル** | `{result_dir}/logs/{run_id}.log.jsonl` (全ログ) |
| **DuckDB** | `{result_dir}/log.duckdb` (集計用、JSONL から自動挿入) |
| **stderr** | INFO 以上のログのみ出力 (DEBUG は出力しない) |

## デバッグコマンド

### show_log_summary: 全体把握

コマンド実行状況 (SUCCESS/FAILED/IN_PROGRESS) と各レベルのログ件数を確認。

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `--date YYYYMMDD` | 対象日を指定 | 今日 |
| `--raw` | 人間向けテキスト出力 | - |
| `--json` | JSON 出力 | ○ |

```bash
# 今日の実行サマリー
show_log_summary

# 人間向けの見やすい出力
show_log_summary --raw

# 特定日のサマリー
show_log_summary --date 20260125
```

### show_log: ログ詳細

特定コマンドのログを JSONL で出力。jq と組み合わせて使う。

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `--date YYYYMMDD` | 対象日を指定 | 今日 |
| `--run-name NAME` | コマンド名を指定（省略時は対話選択） | - |
| `--latest` | 複数 run_id があるとき最新を自動選択 | - |
| `--level LEVEL` | ログレベルでフィルタ (DEBUG/INFO/WARNING/ERROR/CRITICAL) | 全レベル |
| `--limit N` | 出力件数を制限（0 = 無制限） | 0 |
| `--raw` | 人間向けテキスト出力 | - |
| `--jsonl` | JSONL 出力 | ○ |

```bash
# 最新 run のログを見る
show_log --run-name create_dblink_bp_bs_relations --latest

# ERROR のみ抽出
show_log --run-name create_dblink_bp_bs_relations --latest --level ERROR

# DEBUG ログを見る（想定内のスキップ、正規化失敗など）
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG

# 人間向けの見やすい出力
show_log --run-name create_dblink_bp_bs_relations --latest --raw

# 最新 100 件のみ
show_log --run-name create_dblink_bp_bs_relations --latest --limit 100
```

jq と組み合わせた例：

```bash
# レベル別のカウント
show_log --run-name create_dblink_bp_bs_relations --latest | \
  jq -s 'group_by(.log_level) | map({level: .[0].log_level, count: length})'

# DEBUG ログをカテゴリ別に集計
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG | \
  jq -s 'group_by(.debug_category) | map({category: .[0].debug_category, count: length})'

# 特定カテゴリのみ抽出
show_log --run-name create_dblink_bp_bs_relations --latest --level DEBUG | \
  jq 'select(.debug_category == "invalid_biosample_id")'

# エラーが多い accession を特定
show_log --run-name create_dblink_bp_bs_relations --latest | \
  jq -r '.accession // empty' | sort | uniq -c | sort -rn | head -20
```

### show_dblink_counts: relation 件数

dblink DB の relation 件数を (src_type, dst_type) ペアごとに確認。オプションなし。

```bash
show_dblink_counts
```

## 典型的なデバッグフロー

```bash
# 1. コマンド実行
create_dblink_bp_bs_relations

# 2. 実行結果を確認（SUCCESS/FAILED）
show_log_summary --raw

# 3. FAILED なら ERROR ログを確認
show_log --run-name create_dblink_bp_bs_relations --latest --level ERROR

# 4. relation 件数が期待通りか確認
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
| `NORMALIZE_GRANT_AGENCY` | grant agency の正規化失敗 |
| `NORMALIZE_OWNER_NAME` | owner name の正規化失敗 |
| `NORMALIZE_MODEL` | model の正規化失敗 |
| `FETCH_DATES_FAILED` | XML からの日付取得失敗 |
| `XML_ACCESSION_COLLECT_FAILED` | XML からの accession 収集失敗 |
| `UNSUPPORTED_EXTERNAL_LINK_DB` | 未対応の ExternalLink DB |
