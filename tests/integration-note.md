# Integration テスト運用ノート

シナリオ本文 ([integration-scenarios.md](integration-scenarios.md)) からは切り離し、ES / PostgreSQL / fixture の用意・件数 drift 対策・CI 戦略といった「どう運用するか」を集める。

## 接続切替の方針

Integration テストは接続先を環境変数で切り替える。`DDBJ_SEARCH_INTEGRATION_*_URL` 系の env vars に値が無いリソースを必要とするテストは、session 開始時の疎通確認 fixture で `pytest.skip(allow_module_level=True)` で session 全体を skip する。CI でも開発時でも、リソースが立っていなければ自動的に飛ばされる。

具体的な env vars 名は `tests/py_tests/integration/conftest.py` を SSOT とする。現状は ES / TRAD PostgreSQL / XSM PostgreSQL の 3 系統 + alias swap 用の destructive gate (`DDBJ_SEARCH_INTEGRATION_ALLOW_DESTRUCTIVE_ALIAS=1`、本番 alias を持たない dev ES でのみ立てる)。

## ES の用意

本リポジトリの compose で起動する ES (`ddbj-search-es-{env}`) を使うのが基本。staging の ES に向けたい場合は env var (`DDBJ_SEARCH_INTEGRATION_ES_URL`) で接続先を切り替える。

固定名 index (`bioproject` / `biosample` / ...) は staging の運用 alias が指している物理 index と衝突するため、テスト中は **ありえない日付 suffix `99991231`** を付けた dated 物理 index (`bioproject-99991231` 等) で隔離する。staging の Blue-Green 物理 index (実日付 suffix) と被らないので、staging に向けて実行しても運用を壊さない。

ローカル用の固定 fixture (専用 mini インデックス) は今は持たない。テストで投入する場合は session 開始時に `create_index_with_suffix` + `bulk_insert_jsonl(..., target_index=...)` でセットアップし、終了時に `delete_physical_indexes` で teardown する。**共有 ES (staging / production) を汚さないため、teardown は確実に行う**。

## PostgreSQL の用意

TRAD と XSM はいずれも読み取り専用 (`SELECT` のみ) で staging / production 環境のものに接続する。ローカルで PostgreSQL を立てる選択肢はあるが、本番と同じスキーマ・データを再現するコストが大きいため、当面は staging 接続を前提とする。Future work として、最小スキーマと minimal データを用意して compose で起動する案も検討する。

## fixture 戦略

特定の不変条件を assert するためにいくつか「代表 accession」が必要。例: status filter のテストには `public` / `private` / `suppressed` / `withdrawn` の 4 値 (`schema.py::Status` Literal、`8308148` で `live` / `unpublished` から rename) の代表 ID。3 案を比較する。

| 案 | 内容 | メリット | デメリット |
|----|------|---------|-----------|
| A (推奨) | `tests/fixtures/` の本番構造再現 fixture を使う。代表 accession は `tests/py_tests/integration/conftest.py` に定数登録 | コード追跡可能、レビュー時に値が見える、外部リソース不要 | 本番のデータ更新で accession が消えると手動更新が必要 |
| B | テスト内で動的に seed (例: TRAD PostgreSQL を `SELECT ... LIMIT 1` で実測) | 値の劣化に強い | テスト失敗時の再現性が悪い、PostgreSQL の遅延でテストが遅くなる |
| C | テスト用 doc を XML / DuckDB に投入 → setUp/tearDown | 完全に決定的 | staging / production を汚染するリスク、teardown 失敗で残留 |

**推奨は案 A**。本番想定の fixture を `scripts/fetch_test_fixtures.sh` で更新する手順に「代表 accession の更新」を組み込む。案 B は補助的に「条件を満たす ID を 1 件以上見つける」スモークでだけ使う。案 C は禁止 (共有 ES / 共有 PostgreSQL を汚さない)。

## 件数 drift 対策

ES / PostgreSQL のデータは converter の更新で件数が変わる。固定値 assert は壊れる前提で書かない。代わりに **構造的不変条件** で書く。

```python
# 件数 drift に弱い (NG)
assert dblink_count == 26537  # 来月にはずれる

# 件数 drift に強い (OK)
half_edges = count_dbxref_rows()
undirected_edges = count_undirected_edges()
assert half_edges == undirected_edges * 2  # 半辺化スキーマの不変条件
assert undirected_edges > 0                # 「何かある」の最小保証

# 半辺化スキーマで (A→B) と (B→A) が両方存在
ab = count_dbxref(src="bioproject", dst="biosample")
ba = count_dbxref(src="biosample", dst="bioproject")
assert ab == ba  # 対称性

# 相対比較で regression を検出
total_es = count_es_entries("bioproject")
total_jsonl = count_jsonl_entries("bioproject")
total_blacklist = count_blacklist_entries("bioproject")
assert total_es == total_jsonl - total_blacklist  # blacklist 適用整合
```

使ってよい assert パターン:

- 集合の対称性: `count(A→B) == count(B→A)` (半辺化スキーマ)
- 集合の包含関係: `set(es_ids) >= set(jsonl_ids - blacklist)`
- 相対比較: `total_filtered <= total_all`、`total_alias <= total_primary`
- 最小保証: `count > 0`、`len(items) >= 1`
- 文字列一致: `parsed_xml.identifier == jsonl_record.identifier`

## 大規模データ実行の注意

実 ES への bulk insert を伴うテストは時間がかかる (本番想定の bp / bs JSONL は数百万件)。CI / 開発ローカルでは sample データのみ使い、フルパイプラインの通しテスト (`IT-PIPELINE-*`) は staging で実行する想定。

monkeypatch で各モジュールの定数 (`date_cache.db.CHUNK_SIZE`、`sra_accessions_tab.QUERY_BATCH_SIZE`、`es.settings.BULK_INSERT_SETTINGS["batch_size"]` など) を小さい値に置き換えれば、データ量を絞った状態で挙動を検証できる。

## CI 戦略

unit テストは `pytest` のデフォルト実行で通る (`pyproject.toml` の `addopts = ["-n", "auto"]` で並列)。integration は固定的な dated index (`*-99991231`) を共有 ES に作るため worker 間で競合するので、`pytest tests/py_tests/integration -n 0` で **直列実行** する (`-n 0` は xdist を有効に保ったまま worker 数 0 にする指定。`-p no:xdist` だと addopts の `-n` が unknown arg になる)。

GitHub Actions で integration を回すなら以下が必要。Future work。

- ES service container を `services:` で起動
- TRAD / XSM PostgreSQL の最小スキーマ + minimal データの seed (現状は staging 接続前提)
- 本番想定 fixture (`tests/fixtures/`) の git LFS 化 / artifact 化

unit の coverage は `pyproject.toml` の addopts で常に計測される。Integration は coverage 計測対象外として扱う (実 ES / 実 PostgreSQL の挙動を見るのが目的のため)。
