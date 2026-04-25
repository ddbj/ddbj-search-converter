# テスト

ddbj-search-converter のテスト方針と project 固有の注意点。

結合シナリオは [integration-scenarios.md](integration-scenarios.md)、結合運用ノートは [integration-note.md](integration-note.md)。

## 実行方法

すべてコンテナ内で実行する ([docs/development.md](../docs/development.md))。

### Unit (デフォルト)

```bash
docker compose exec app pytest
```

`pyproject.toml` の `addopts = ["-n", "auto"]` で並列実行。Coverage は常に計測される。

### Integration

実 ES / 実 PostgreSQL / mounted path への接続が前提。env vars 未設定のリソースを必要とする test は session skip。

| Env var | 用途 |
|---|---|
| `DDBJ_SEARCH_INTEGRATION_ES_URL` | ES URL (compose の `http://elasticsearch:9200` か staging) |
| `DDBJ_SEARCH_INTEGRATION_XSM_POSTGRES_URL` | XSM PostgreSQL 接続文字列 (`IT-PG-01` / `IT-PG-03` で使う) |
| `DDBJ_SEARCH_INTEGRATION_TRAD_POSTGRES_URL` | TRAD PostgreSQL 接続文字列 (`IT-PG-02` で使う) |
| `DDBJ_SEARCH_INTEGRATION_ALLOW_DESTRUCTIVE_ALIAS` | 本番 alias を一時剥がす `IT-SWAP-*` を許可 (`=1`、本番 alias を持たない dev ES でだけ立てる) |

```bash
docker compose exec -T \
  -e DDBJ_SEARCH_INTEGRATION_ES_URL=http://elasticsearch:9200 \
  app pytest tests/py_tests/integration -n 0
```

`-n 0` は固定 dated index への worker 競合を避ける直列実行指定 ([integration-note.md](integration-note.md) 参照)。

## 目的

テストは **バグを見つけ、防ぐため** に書く。すべてのテストは「これが落ちたらどんなバグが検出されたことになるか」に答えられなければならない。通すために書くテスト、Happy path だけのテスト、アサーションのない smoke テストは書かない。

## 原則

- **TDD**: 仕様 ([docs/](../docs/) 配下) からテストを導出し、実装の前に書く。Red → Green → Refactor
- **PBT (Property-Based Testing)**: [hypothesis](https://hypothesis.readthedocs.io/) で入力空間を広く探索する
- **境界値・エッジケース・異常系を必ず書く**: 正常系だけでは脆い
- **mock は外部境界だけ**: 実 ES / 実 PostgreSQL / HTTP fetch を境界として mock し、内部 (Pydantic、normalize、parser、tmp_path 隔離 DuckDB) は実物を通す
- **テスト間の独立性**: 状態を共有しない、実行順序に依存しない
- **既知バグの文書化**: 発見済みバグは `xfail` やコメントでテスト内に文書化する

## テスト分類

2 バケツに分ける。基準は「実 ES / 実 PostgreSQL / 本番想定パスのリソースに接続するか」。

- **Unit**: 実 ES / 実 PostgreSQL に接続しない。`tmp_path` で隔離した実 DuckDB は unit 扱い (DuckDB の振る舞いが SSOT のため、mock しない)。`pytest` のデフォルト実行対象
- **Integration**: 実 ES、実 PostgreSQL (TRAD / XSM)、本番想定パスの fixture / DuckDB / log を使う。`tests/py_tests/integration/` に配置、unit のデフォルト実行に乗らない。シナリオは [integration-scenarios.md](integration-scenarios.md)、運用は [integration-note.md](integration-note.md) を参照

## Mock 戦略

外部境界 (実 ES、実 PostgreSQL、HTTP fetch) のレスポンスを mock し、内部実装は実物を通す。

DuckDB のクエリ自体は mock しない。`finalize_dblink_db` の挙動 (UNION ALL で半辺化、DISTINCT、ORDER BY、index 構築) は DuckDB の振る舞いが SSOT なので、`tmp_path` に DB ファイルを作って実 SQL で検証する。

CLI レベルのテストは subprocess で entrypoint を起動するのではなく、各 `main()` 関数を直接呼び出して内部状態を assert する。

## レイヤー別観点

テストを書くときの「どこに重点を置くか」の方針。具体的な Property や境界値はテストコード自身が SSOT なので、ここには書かない。現状の整備状況も併記する (整備中レイヤーは次のタスクで埋める想定)。

- **id_patterns / xml_utils / schema**: PBT を最も活用する。正規表現の境界、Pydantic バリデーション、Enum 受入/拒否、デフォルト値 (整備済)
- **dblink/**: 半辺化スキーマの不変条件 (`(A→B)` と `(B→A)` の両方存在)、UNIQUE 制約、ORDER BY、`raw_edges` の DROP、atomic replace (整備済)
- **jsonl/**: XML / IDF / SDRF → Pydantic モデル → JSONL の round-trip。blacklist / preserved 適用、`Attribute` 配列正規化、sameAs alias、`isPartOf` / `type` 値、distribution 生成 (整備済)
- **es/**: index 作成・削除、bulk insert (mock ES)、alias 構成、Blue-Green の swap ロジック、blacklist 削除。**unit で `bulk_insert` / `bulk_delete` / `client` / `index` / `mappings` を整備済。実 ES での mapping accept / alias swap / bulk delete 分類は integration (`IT-MAPPING-*` / `IT-SWAP-*` / `IT-DELETE-*`) でカバー。`snapshot` / `monitoring` は unit / integration とも未整備。**
- **date_cache / status_cache**: 外部入力 (PostgreSQL / Livelist) を mock し、DuckDB への bulk insert を `tmp_path` で実検証。**status_cache / date_cache とも build/db 両方整備済。**
- **postgres/**: 接続・クエリ結果整形。unit では `psycopg` の戻り値を mock し、`utils` のみ整備。**`bp_date` / `bs_date` の SQL は staging スキーマでの `EXPLAIN` を integration (`IT-PG-03`) でカバー、接続性は `IT-PG-01` / `IT-PG-02`。**
- **logging/**: run_id ライフサイクル、JSONL ログ出力、DuckDB への自動 insert、SUCCESS / FAILED 判定 (整備済)。

## バグ回帰テスト

修正したバグは `TestBug<N><Description>` クラスで再発防止テストを書く。コミットや PR の URL を docstring に残し、なぜそのテストがあるかを後から辿れるようにする。

## project 固有の注意点

- `pyproject.toml` の `addopts` で `-n auto` (pytest-xdist) がデフォルト有効。並列実行時に worker 間で競合する state (共有ファイル・グローバル変数・外部リソース) を作らない。`tmp_path` や `monkeypatch` でテストを隔離する
- `pdb` や `print` デバッグで出力が混ざるときは `-n 0` で直列実行する
- hypothesis の deadline は `tests/py_tests/conftest.py` の `default` プロファイルで無効化済み (並列化時の負荷で `DeadlineExceeded` が出るのを避けるため)
- 境界値を大量データで検証したいときは、対象モジュールの定数 (例: `date_cache.db.CHUNK_SIZE`、`sra_accessions_tab.QUERY_BATCH_SIZE`) を `monkeypatch.setattr` で小さい値に置き換える。数万件を実 DuckDB に insert すると 1 テストで数十秒かかるので避ける

## fixture データ

`tests/fixtures/` は本番想定の volume 構造を再現した小規模データセット (git 管理済み)。本番のデータ構造が変わって新しいケースを再現できなくなったら、遺伝研スパコン上で `scripts/fetch_test_fixtures.sh` を実行して更新する。手元では取得できない。
