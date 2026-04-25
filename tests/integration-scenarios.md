# Integration テストシナリオ

ddbj-search-converter の **本番 deploy 前 rehearsal** として staging で実走させるシナリオ集。unit テストでは検出できない「実 ES / 実 PostgreSQL / 実 fixture と組み合わせた挙動」を verify することで、本番に流れる前に bug を見つける。

このファイルは「やりたいこと」の選択リスト。実装の SSOT は test code 自身 (`tests/py_tests/integration/test_*.py`)。運用 (環境変数・隔離方針・直列実行) は [integration-note.md](integration-note.md) を参照する。

## 設計の要点

- **目的は本番 deploy 前確認**。シナリオ網羅は目的ではない
- **staging-isolated**: ありえない日付 suffix `99991231` で物理 index を隔離する。staging の運用 alias / dated index は絶対に汚さない
- **構造的不変条件で assert**: 件数固定値 assert は drift で壊れる。対称性・包含関係・相対比較・最小保証 (`> 0`) で書く
- **未充足 env では skip**: `DDBJ_SEARCH_INTEGRATION_*_URL` 不在なら session skip

## 優先度

- **P1 (Must)**: 本番 deploy 前に必ず通すべき。staging 実走可能、teardown 確実
- **P2 (Nice)**: あると安心。実走可能だが依存または時間の制約がある
- **P3 (Future)**: 専用環境 / 大データ / 追加の仕組みが必要

## Phase 1: ES mapping rehearsal (P1)

mapping JSON syntax / field type / unknown setting エラーを実 ES に PUT して即時検知。schema (`schema.py`) と mapping (`es/mappings/*`) の drift を本番 deploy 前に検出。

| ID | シナリオ | 状態 |
|---|---|---|
| IT-MAPPING-01 | 全 14 index で `create_index_with_suffix(config, "all", "99991231")` が ES に accept される + `properties` が retrievable | ✅ 実装済 (`test_es_index_rehearsal.py`) |
| IT-MAPPING-02-bp | `schema.BioProject` から作った doc が bp mapping に bulk insert で受け入れられる (error_count == 0) | ✅ 実装済 (`test_es_bulk_insert_rehearsal.py`) |
| IT-MAPPING-02-bs | `schema.BioSample` ↔ bs mapping (組成が一番ハード、drift 検知力が高い) | TODO |
| IT-MAPPING-02-sra | `schema.SRA` ↔ sra-* mapping (6 logical index) | TODO |
| IT-MAPPING-02-jga | `schema.JGA` ↔ jga-* mapping (4 logical index) | TODO |
| IT-MAPPING-02-gea | `schema.GEA` ↔ gea mapping | TODO |
| IT-MAPPING-02-mtb | `schema.MetaboBank` ↔ metabobank mapping | TODO |

## Phase 2: ES alias swap (P1)

Blue-Green の zero-downtime 切り替えを rehearsal。`entries` alias の不変条件「常に何かに張られている」を verify。

| ID | シナリオ | 状態 |
|---|---|---|
| IT-SWAP-01 | 旧 `*-99991230` + 新 `*-99991231` を作って `swap_aliases("99991231")` 実行、entries / sra / jga alias が新側に切り替わる | TODO |
| IT-SWAP-02 | swap 操作前後の各時点で `entries` alias が常に何かしらの index に解決可能 (空にならない) | TODO |
| IT-SWAP-03 | `delete_physical_indexes` で旧 dated index を削除しても alias は新側で解決可能 | TODO |

## Phase 3: ES bulk delete (blacklist 適用) (P2)

bulk delete の partial failure 分類。`89c0499` の sanitize 経路を実 ES で確認。

| ID | シナリオ | 状態 |
|---|---|---|
| IT-DELETE-01 | 既存 doc は `success_count`、不在 doc は `not_found_count` で `error_count` に積まれない | TODO |
| IT-DELETE-02 | 別 index の doc を blacklist に積んでも対象 index の doc しか削除されない | TODO |

## Phase 4: PostgreSQL 接続 smoke (P1)

TRAD / XSM の接続性 + SQL の現スキーマ整合を staging で確認。

| ID | シナリオ | 状態 |
|---|---|---|
| IT-PG-01 | TRAD PostgreSQL に staging から接続できる (`SELECT 1`) | TODO |
| IT-PG-02 | XSM PostgreSQL に staging から接続できる | TODO |
| IT-PG-03 | `bp_date.py` / `bs_date.py` の SQL が現スキーマで `EXPLAIN` 通る (実行は不要) | TODO |

## Phase 5: 件数 drift / 構造的不変条件 (P2)

件数固定値ではなく構造的不変条件で regression を検出。

| ID | シナリオ | 状態 |
|---|---|---|
| IT-INVARIANT-01 | dblink DuckDB で半辺化 `dbxref` の対称性 (`count(A→B) == count(B→A)`) | TODO |
| IT-INVARIANT-02 | ES `entries` alias の docs_count が前回 deploy 比で大きく drift していない (相対 assert、閾値は別途合意) | TODO |
| IT-INVARIANT-03 | JSONL 行数 - blacklist 件数 == ES の docs_count (blacklist 適用整合) | TODO |

## Phase 6: 外部リソース疎通 (P2)

NCBI FTP / Livelist など外部 I/O の存在確認。staging のホストから到達可能か。

| ID | シナリオ | 状態 |
|---|---|---|
| IT-RESOURCE-01 | NCBI FTP の `assembly_summary_genbank.txt` が DL 可能 | TODO |
| IT-RESOURCE-02 | BP / BS livelist が staging から read 可能 | TODO |
| IT-RESOURCE-03 | DRA Accessions tab が staging から read 可能 | TODO |

## Phase 7: フルパイプライン smoke (P3)

sample fixture でフルパイプライン (XML → JSONL → DBLink → ES) を staging で 1 周させる。時間がかかる + 依存が多いので、CI ではなく本番 deploy 前の最終確認用。

| ID | シナリオ | 状態 |
|---|---|---|
| IT-PIPELINE-01 | bp / bs サブセット fixture でフルパイプラインが完走 + ES に投入される | TODO |
| IT-PIPELINE-02 | 途中ステップ失敗で path-based skip + `--from-step` 再開が機能 | TODO |

## Phase 8: ログ・デバッグ round-trip (P3)

run_id ライフサイクル + log.duckdb への記録 + `show_log_summary` の round-trip。unit でカバー済みの部分が大きいので、integration 側は staging での round-trip だけ。

| ID | シナリオ | 状態 |
|---|---|---|
| IT-LOG-01 | パイプライン 1 周で log.duckdb に SUCCESS / FAILED が記録される | TODO |
| IT-LOG-02 | `show_log_summary` で staging の最新 run の状態が表示される | TODO |

## Phase 9: RDF パイプライン smoke (P3)

`insdc-rdf` を使った RDF 変換 (オプション機能)。

| ID | シナリオ | 状態 |
|---|---|---|
| IT-RDF-01 | sample JSONL から RDF への変換が完走する | TODO |

## ID 体系

`IT-{Phase 名}-NN` で振る。例: `IT-MAPPING-02-bp`, `IT-SWAP-01`, `IT-INVARIANT-03`。

- 同 Phase 内で連番リセット
- 削除した ID は再利用しない (履歴互換性)
- データ種類のサブ ID (`-bp`, `-bs`, `-sra`, `-jga`, `-gea`, `-mtb`) は data type ごとに rehearsal を分離する Phase 1 / Phase 2 で使う
