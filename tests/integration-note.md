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

## staging 安全性: ありえない日付 suffix と destructive gate

すべての書き込み系テストは **ありえない日付 suffix `99991231`** (alias swap rehearsal は加えて `99991230`) を付けた dated 物理 index にだけ触る。staging の Blue-Green 物理 index (実日付 suffix) と被らないので staging に向けて実行できる。teardown は `delete_physical_indexes` で確実に消す。

ただし **alias swap rehearsal (`IT-SWAP-*`) は `entries` / `sra` / `jga` / per-index 名 (本番運用 alias と同じ名前) に `put_alias` するため、staging で実行すると本番 alias を一時的に剥がして検索を一瞬壊す**。これらは `DDBJ_SEARCH_INTEGRATION_ALLOW_DESTRUCTIVE_ALIAS=1` を立てた環境 (= 本番 alias を持たない compose の dev ES) でだけ実行する設計。env var なしだと session skip。

## 件数 drift 対策

ES / PostgreSQL のデータは converter の更新で件数が変わる。固定値 assert は壊れる前提で書かない。代わりに **構造的不変条件** で書く (具体実装は `tests/py_tests/integration/test_*.py`)。

使ってよい assert パターン:

- 対称性: `count(A→B) == count(B→A)` (半辺化スキーマ、`IT-INVARIANT-01`)
- 整合: `count(entries) == sum(各 logical alias の count)` (`IT-INVARIANT-02`)
- 最小保証: `count > 0`、`len(items) >= 1`
- 包含関係: `unique(jsonl_ids) <= count(es)` (sameAs alias / blacklist の調整に注意)

避けるパターン:

- **件数固定値**: `assert count == 26537` は来月の deploy で壊れる
- **JSONL 行数 vs ES count**: INSDC mirror で同 ID が dra と ncbi の両方に出力されるため `JSONL 行数 > ES count` が正常。比較するなら unique identifier ベースで (ただし sra-experiment 40M 行レベルだと毎回数 GB / 数十分の集計コスト)

## 大規模データ実行の注意

bulk insert を伴うテストは staging データだと時間がかかる。ローカル compose では `tests/fixtures/` の小規模 fixture だけ使う設計。test code 内で `bulk_insert_jsonl(target_index=...)` の引数を ありえない日付 suffix にしておけば staging 実走時もデータを壊さない。

unit 側で大量データの挙動を見たい場合は、対象モジュールの定数 (`date_cache.db.CHUNK_SIZE`、`sra_accessions_tab.QUERY_BATCH_SIZE`、`es.settings.BULK_INSERT_SETTINGS["batch_size"]` 等) を `monkeypatch.setattr` で小さい値に置き換える。

## CI 戦略

unit テストは `pytest` のデフォルト実行で通る (`pyproject.toml` の `addopts = ["-n", "auto"]` で並列)。integration は固定的な dated index (`*-99991231`) を共有 ES に作るため worker 間で競合するので、`pytest tests/py_tests/integration -n 0` で **直列実行** する (`-n 0` は xdist を有効に保ったまま worker 数 0 にする指定。`-p no:xdist` だと addopts の `-n` が unknown arg になる)。

GitHub Actions で integration を回すなら以下が必要。Future work。

- ES service container を `services:` で起動
- TRAD / XSM PostgreSQL の最小スキーマ + minimal データの seed (現状は staging 接続前提)
- 本番想定 fixture (`tests/fixtures/`) の git LFS 化 / artifact 化

unit の coverage は `pyproject.toml` の addopts で常に計測される。Integration は coverage 計測対象外として扱う (実 ES / 実 PostgreSQL の挙動を見るのが目的のため)。
