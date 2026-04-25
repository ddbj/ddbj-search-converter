# Integration テストシナリオ

実 Elasticsearch / 実 PostgreSQL (TRAD・XSM) / 本番想定パスの fixture に対する E2E 検証シナリオの一覧。各シナリオは「これが落ちたらどんなバグが検出されたことになるか」に答えられる粒度で書く。

このファイルは **シナリオ列挙の SSOT**。具体的なコードは `tests/py_tests/integration/test_*.py` にあり、運用上の注意 (環境変数・fixture 戦略・件数 drift 対策) は [integration-note.md](integration-note.md) を参照する。

> **注**: 本ファイルは枠だけ用意した状態。各シナリオの本文は次の作業で埋める。

## ID 体系

`IT-{機能}-{連番 2 桁}` の形式で振る。例: `IT-DBLINK-01`, `IT-JSONL-03`, `IT-BLUEGREEN-12`。

- 機能ごとに連番をリセット
- 削除したシナリオの ID は **再利用しない** (履歴互換性)
- 機能名は固定リスト (下記カテゴリ): `RESOURCE`, `DBLINK`, `UMBRELLA`, `JSONL`, `DIFF`, `ES`, `BLUEGREEN`, `RDF`, `LOG`, `PIPELINE`

## シナリオテンプレート

各シナリオは以下 4 項目で記述する。件数の実測値は書かない (drift で壊れるため、構造的不変条件のみ書く)。

```markdown
### IT-XXX-NN: <短いタイトル>

**対象**: CLI コマンド / モジュール / パイプライン段階

**前提**: 必要な fixture / 環境変数 / 事前ステップ

**不変条件**:
- 構造的に守るべき条件 1
- 構造的に守るべき条件 2

**回帰元**: なぜこのテストが必要か (どのバグ・どの仕様章が背景)。コミット SHA または docs/ の節を引用

**関連 unit テスト**: SSOT としての unit ファイル + クラス名 (例: `tests/py_tests/dblink/test_db.py::TestFinalizeDBLinkDB`)
```

## カテゴリ別シナリオ

### IT-RESOURCE-*: 外部リソース・接続性

`check_external_resources` と外部 I/O の存在確認。

- 外部 fixture パスの存在確認 (BioProject XML、BioSample XML、SRA/DRA Accessions、Livelist、TRAD ORGANISM_LIST、IDF/SDRF)
- TRAD PostgreSQL (`g-actual` / `e-actual` / `w-actual`) への接続性
- XSM PostgreSQL への接続性 (BP/BS の日付取得用)
- NCBI Assembly summary の HTTP fetch (ネットワーク到達性)
- リソース欠落時のエラーログ出力

(本文は次の作業で埋める)

### IT-DBLINK-*: DBLink DB 構築

`init_dblink_db` から `finalize_dblink_db` までのフロー、半辺化スキーマの不変条件。

- `raw_edges` への canonical 形 insert (各 `create_dblink_*` コマンド)
- `finalize_dblink_db` の `build_dbxref_table` で `(A→B)` と `(B→A)` の両方が `dbxref` に存在
- UNIQUE 制約 (`idx_dbxref_unique`) と物理 ORDER BY の整合性
- `raw_edges` テーブルが finalize 後に DROP されている
- atomic replace (tmp DB → final DB) の整合性
- blacklist 適用 (両端のいずれかが blacklist にある edge は除外)
- preserved TSV の関連が DBLink に追加されている
- `show_dblink_counts` の COUNT/2 が無向 edge 数と一致

(本文は次の作業で埋める)

### IT-UMBRELLA-*: Umbrella DB

BioProject の親子 DAG。

- `umbrella_relation` への有向 edge insert (`create_dblink_bp_relations`)
- 1 child が複数 parent を持つ DAG の表現 (multi-parent)
- depth 1 (約 99.6%) の典型構造
- 最大深度 5 の境界
- `objectType=BioProject` のまま `childBioProjects` を持つ TopAdmin Link 138 件

(本文は次の作業で埋める)

### IT-JSONL-*: JSONL 生成

XML / IDF / SDRF → Pydantic モデル → JSONL の変換。

- `generate_bp_jsonl` の出力 schema (Pydantic で round-trip 可能)
- `generate_bs_jsonl` の `Attribute` 配列正規化 (1 件でも list)
- `generate_sra_jsonl` の type 分類 (submission / study / experiment / run / sample / analysis)
- `generate_jga_jsonl` の sameAs alias ドキュメント生成
- `generate_gea_jsonl` の IDF 全件走査
- `generate_metabobank_jsonl` の欠損 IDF 除外
- blacklist 適用でエントリーが JSONL に出力されない
- `dbXrefs` フィールドのデフォルトでの非含有 (`--include-dbxrefs` で有効化)
- date_cache / status_cache の値が JSONL に反映される
- `isPartOf` / `type` / `distribution` の値整合
- `parentBioProjects` / `childBioProjects` が Umbrella DB から展開される
- `regenerate_jsonl` で `last_run.json` が更新されない

(本文は次の作業で埋める)

### IT-DIFF-*: 差分更新

`last_run.json` ベースの差分処理。

- `last_run.json` が `null` のとき `--full` 相当の動作
- `margin_days` (デフォルト 30 日) を引いた日時以降のエントリーが処理対象
- 差分判定の基準 (BP: `date_modified`, BS: `last_update`, SRA: `Updated`)
- JGA / GEA / MetaboBank は常に全件処理 (`last_run.json` に含めない)
- JSONL 生成完了後に `last_run.json` が更新される
- `regenerate_jsonl` は `last_run.json` を更新しない

(本文は次の作業で埋める)

### IT-ES-*: Elasticsearch 操作

実 ES に対する index / bulk insert / alias / blacklist 削除。

- `es_create_index --index <group>` で alias 込みで作成
- `es_bulk_insert` の `_op_type: "index"` で既存 doc が上書き
- bulk insert 中の refresh interval 切替 (`-1` → `1s` 復元)
- `es_delete_blacklist` で 404 を `not_found` としてカウント
- alias 構成 (`sra` / `jga` / `entries` の対象 index)
- mapping エラー時の bulk insert ハンドリング
- `_id` 衝突時の挙動 (alias ドキュメントとの整合)
- `es_health_check` のクラスタ status 判定

(本文は次の作業で埋める)

### IT-BLUEGREEN-*: Blue-Green Alias Swap

ゼロダウンタイム更新フロー。

- `es_create_index --date-suffix YYYYMMDD` で alias なしの dated index を作成
- `es_bulk_insert --target-index NAME-YYYYMMDD` で旧 index を触らずに投入
- `es_swap_aliases` 実行中も検索断ゼロ (alias 経由で旧→新に切替)
- 14 個全 index が atomic に切り替わる (部分失敗なし)
- `es_delete_old_indexes` で旧 dated index が削除される
- `es_migrate_to_blue_green` の初回マイグレーション (固定名 → dated への `_clone`)
- ロールバック (旧 dated index が残っていれば swap で戻せる)

(本文は次の作業で埋める)

### IT-RDF-*: RDF パイプライン

`insdc-rdf` を経由した独立パイプライン。

- 4 source (bioproject / biosample / sra / sra-experiment) の並列変換
- 出力ディレクトリ (`{result_dir}/rdf/{source}/{ttl,jsonld,nt}/`) の上書き挙動
- `--from-step validate` での再開
- chunk size 指定 (`--chunk-size`)
- converter 側の `ontology/*.ttl` 語彙との整合 (rename 追随)

(本文は次の作業で埋める)

### IT-LOG-*: ログ・デバッグ

run_id ライフサイクル、JSONL ログ、DuckDB 集計。

- 各コマンドが run_id を生成し、JSONL ログを出力する
- run_id 完了時に SUCCESS / FAILED が記録される
- DuckDB (`log.duckdb`) への自動 insert
- `show_log_summary` の集計 (run_name × status)
- `show_log` の filter (`--level`, `--latest`, `--limit`)
- ERROR / CRITICAL レベルの stderr 出力
- DEBUG ログの `debug_category` 必須

(本文は次の作業で埋める)

### IT-PIPELINE-*: 全体パイプライン

`scripts/run_pipeline.sh` の通し実行。

- Phase 0 → 1 → 2 → 3 の順序
- `--dry-run` で実行内容のみ表示
- `--from-step <name>` で任意ステップから再開
- `--full` と `--blue-green` の組み合わせ
- `--clean-es` と `--blue-green` の排他チェック
- `--parallel N` で JSONL 生成の並列度
- 各 Phase の中間成果物が次 Phase の入力として保持される
- 失敗時の途中停止と、`--from-step` での再開可能性

(本文は次の作業で埋める)

## 移植トレーサビリティ

過去のレビューで検出した bug fix から導かれた検証ケースを `IT-XXX` に紐付ける作業表。次会話で本文を埋める際、各 IT に「**回帰元**: コミット SHA + 主旨」を残し、後から「なぜこのテストがあるか」を辿れるようにする。

(対応表は次の作業で埋める)
