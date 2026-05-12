# Integration テストシナリオ

ddbj-search-converter の本番 deploy 前 rehearsal として staging で実走させるシナリオ集。unit テストでは検出できない「実 ES / 実 PostgreSQL / 実 fixture と組み合わせた挙動」を verify することで、本番に流れる前に bug を見つける。

このファイルは「やりたいこと」の選択リスト。実装の SSOT は test code 自身 (`tests/py_tests/integration/test_*.py`)。運用 (環境変数・隔離方針・直列実行) は [integration-note.md](integration-note.md) を参照する。

## 設計の要点

- staging-isolated: ありえない日付 suffix `99991231` (および swap 用に `99991230`) で物理 index を隔離する。staging の運用 alias / dated index は絶対に汚さない
- 構造的不変条件で assert: 件数固定値 assert は drift で壊れる。対称性・包含関係・相対比較・最小保証 (`> 0`) で書く
- 未充足 env では skip: `DDBJ_SEARCH_INTEGRATION_*_URL` 不在なら session skip

## ID 体系

`IT-{機能}-NN[-data type]`。例: `IT-MAPPING-02-bp`, `IT-SWAP-01`。

- 削除した ID は再利用しない (履歴互換性)
- `IT-MAPPING-02-{bp,bs,sra,jga,gea,mtb}` は data type 別 rehearsal の suffix

## ES mapping rehearsal (`IT-MAPPING-*`)

mapping JSON syntax / field type / unknown setting エラーを実 ES に PUT して即時検知。schema (`schema.py`) と mapping (`es/mappings/*`) の drift を本番 deploy 前に検出。

| ID | 不変条件 |
|---|---|
| IT-MAPPING-01 | 全 14 index で `create_index_with_suffix(config, "all", "99991231")` が ES に accept される + `properties` が retrievable |
| IT-MAPPING-02-{bp,bs,sra,jga,gea,mtb} | `schema.{Type}` から作った doc が対応する mapping に bulk insert で受け入れられる (`error_count == 0`) |

## ES alias swap (`IT-SWAP-*`)

`swap_aliases` が atomic に旧 dated index → 新 dated index へ alias を移すことを verify。zero-downtime の構造的不変条件 (`entries` が常に何かに張られている) を保つかを確認。

これらのテストは `entries` / `sra` / `jga` / per-index 名 (本番運用 alias と同名) に `put_alias` するため、本番 alias を持つ ES (= staging / production) で実行すると検索を一瞬壊す。`DDBJ_SEARCH_INTEGRATION_ALLOW_DESTRUCTIVE_ALIAS=1` を明示的に立てた環境 (= 本番 alias を持たない dev / compose ES) でだけ実行される。

| ID | 不変条件 |
|---|---|
| IT-SWAP-01 | 旧 + 新 dated index で `swap_aliases` 実行後、per-index alias と group alias (`entries` / `sra` / `jga`) が新側に移る |
| IT-SWAP-02 | swap 操作前後で `entries` alias が常に **14 個 (= `len(ALL_INDEXES)`)** の物理 index に解決される (空にならない) |
| IT-SWAP-03 | swap 後に旧 dated index を削除しても alias は新側で解決可能 |
| IT-SWAP-04 | group 単位 swap (例: `sra` group のみ新側へ) 後、`entries` alias は SRA-new + 他 5 group-old の **計 14 個** に解決される (partial swap 中も検索断ゼロ) |
| IT-SWAP-05 | group 単位 swap 中の `sra` alias は SRA-new 6 個に、`jga` alias は JGA-old 4 個に解決される (group 間で独立) |

## ES snapshot (`IT-SNAPSHOT-*`)

`ddbj_search_converter/es/snapshot.py` の register / create / restore / list / delete を実 ES で確認。共有 ES のバックアップを壊さないよう、テストでは専用 repository 名 (`it-rehearsal-snapshot`) と `99991231` suffix の隔離 dated index に閉じる。teardown で `delete_snapshot` + `delete_repository` を確実に走らせる。

| ID | 不変条件 |
|---|---|
| IT-SNAPSHOT-01 | `register_repository` 後、`list_snapshots` が repository を返し、`delete_repository` で清掃できる |
| IT-SNAPSHOT-02 | `99991231` suffix の隔離 index に対し `create_snapshot` 完了後、`get_snapshot_status` が `SUCCESS` を返す |
| IT-SNAPSHOT-03 | snapshot を `delete_physical_indexes` 経由で消した index に `restore_snapshot` で復元、`count` で元の doc 数と一致 |
| IT-SNAPSHOT-04 | `export_index_settings` の戻り値が `index` 設定 + `mappings` を両方含み、`number_of_shards` / `number_of_replicas` の実値が `INDEX_SETTINGS` と一致する |

## ES monitoring (`IT-MONITORING-*`)

`ddbj_search_converter/es/monitoring.py` の health / stats が staging 接続で正しい構造の応答を返すことを確認。本番状態を読み取るだけで write はしないので destructive gate 不要。

| ID | 不変条件 |
|---|---|
| IT-MONITORING-01 | `get_cluster_health` が `status in {"green", "yellow", "red"}`、`number_of_nodes >= 1`、`active_primary_shards >= 14` (14 index 前提) |
| IT-MONITORING-02 | `get_index_stats(index="entries")` が各 logical alias の `docs.count` を `>= 0` で返す (entries が空でも response 構造が崩れない) |
| IT-MONITORING-03 | `check_health` の返却値が `is_healthy: bool` で、disk 残量 / cluster status / shard 不整合を理由つきで列挙する (`reasons` フィールド) |

## ES bulk delete (`IT-DELETE-*`)

bulk delete の partial failure 分類。`89c0499` の sanitize 経路を実 ES で確認。

| ID | 不変条件 |
|---|---|
| IT-DELETE-01 | 既存 doc は `success_count`、不在 doc は `not_found_count`、`error_count == 0` |
| IT-DELETE-02 | 別 index に対する delete 呼び出しが seed index の doc を巻き込まない |

## PostgreSQL 接続 smoke (`IT-PG-*`)

TRAD / XSM の接続性 + SQL の現スキーマ整合を staging で確認。

| ID | 不変条件 |
|---|---|
| IT-PG-01 | XSM PostgreSQL の `bioproject` / `biosample` dbname それぞれで `SELECT 1` が通る |
| IT-PG-02 | TRAD PostgreSQL の `(dbname, port)` 全 3 件 (`g/e/w-actual`) で `SELECT 1` が通る |
| IT-PG-03 | `bp_date.py` / `bs_date.py` の SQL が現スキーマで `EXPLAIN` 通る (実行は不要) |

## 件数 drift / 構造的不変条件 (`IT-INVARIANT-*`)

件数固定値ではなく構造的不変条件で regression を検出。実 seed データを持つ ES / DBLink を前提とし、データが空なら自動 skip。

| ID | 不変条件 |
|---|---|
| IT-INVARIANT-01 | dblink DuckDB で半辺化 `dbxref` の対称性 (`count(A→B) == count(B→A)`) と、`accession` / `linked_accession` / `accession_type` / `linked_type` に NULL なし |
| IT-INVARIANT-02 | ES の `entries` alias 経由 docs_count が **各 logical alias の docs_count の合計** と一致 + 各 logical は単一物理 index に解決 + `sra` alias は 6 物理、`jga` alias は 4 物理 |

### Representative accessions (`INTEGRATION_REPRESENTATIVE_ACCESSIONS`)

`conftest.py` の同名定数を介して、(data type, status) ごとに代表 ID を 1 つずつ
固定する。下流シナリオが特定 status の accession を必要とするときの SSOT。

選定基準:

- `tests/fixtures/lustre9/.../bp-collab/bioproject/` および `bs-collab/biosample/`
  の livelist (public / suppressed / withdrawn) の先頭エントリを採用する
- fixture は `scripts/fetch_test_fixtures.sh` (staging で実行) で更新される。
  fixture を更新したら conftest 側もリフレッシュする
- staging に問い合わせて確認できる ID であること

## 外部リソース疎通 (`IT-RESOURCE-*`)

NCBI FTP / Livelist など外部 I/O の存在確認。staging のホストから到達可能か。

| ID | 不変条件 |
|---|---|
| IT-RESOURCE-01 | NCBI FTP の `assembly_summary_genbank.txt` が DL 可能 |
| IT-RESOURCE-02 | BP / BS livelist が staging から read 可能 |
| IT-RESOURCE-03 | DRA Accessions tab が staging から read 可能 |

## ログ round-trip (`IT-LOG-*`)

`log.duckdb` に書かれた run lifecycle が構造的に整合していることを verify。`logging/db.py::get_last_successful_run_date` の経路 (INFO + `lifecycle='end'`) も含む。

| ID | 不変条件 |
|---|---|
| IT-LOG-01 | log_records に start lifecycle と (end か failed) lifecycle の record が両方ある |
| IT-LOG-02 | 各 run_id は start <= 1 個、(end + failed) <= 1 個 (lifecycle 重複なし) |
| IT-LOG-03 | `INFO` + `lifecycle='end'` で抽出した最新 run_date が NULL でない (`get_last_successful_run_date` の round-trip) |
