# Elasticsearch

DDBJ Search Converter の Elasticsearch 設定方針と運用 (snapshot / Blue-Green)。

データ構造 (14 indexes / alias / blacklist ファイル) は [data-architecture.md](data-architecture.md) を、Blue-Green と `--clean-es` の選択基準は [deployment.md](deployment.md) を参照する。

## 設定の置き場所

ES 関連の設定は 4 箇所に分散している。実値は各ファイルが SSOT で、docs には WHY だけ書く。

| ファイル | 役割 |
|---------|------|
| `env.{dev,staging,production}` | 環境差分 (メモリ上限、JVM heap、接続先 URL) |
| `compose.yml` | ES コンテナ定義 (env を参照、named volume `es-data-${DDBJ_SEARCH_ENV}` / `es-backup-${DDBJ_SEARCH_ENV}`) |
| `elasticsearch/config/elasticsearch.yml` | ES 本体の調整 (bulk insert 最適化のため `http.max_content_length` を ES 上限 (2GB-1byte) に拡張、`thread_pool.write.queue_size` を無制限、`indices.memory.index_buffer_size` を 30% に増やしている) |
| `ddbj_search_converter/es/settings.py` | converter 側の bulk insert / index 操作の定数 (batch size、retries、refresh interval 切替) |

### bulk insert 中の refresh 無効化

`BULK_INSERT_SETTINGS` で `refresh_interval` を `-1` に切り替え、bulk insert 完了後に `1s` に戻す。bulk insert 中の refresh は数百万件投入時に大きなオーバーヘッドになる (refresh 頻発でスループットが落ちる) ため。

### bulk insert / bulk delete のリトライ

`helpers.parallel_bulk` / `helpers.bulk` 自体は retry 引数を受け付けないため、HTTP transport 層で吸収する。`ddbj_search_converter/es/client.py` の `get_es_client()` で `Elasticsearch(...)` 生成時に retry 設定を渡し、bulk 内部の各 HTTP リクエストが 429 / 502 / 503 / 504 を受けた場合に自動でリトライされる。値は `ddbj_search_converter/es/settings.py` の `BULK_MAX_RETRIES` / `BULK_RETRY_ON_STATUS` を SSOT とする。

| 設定 | 値 | 理由 |
|---|---|---|
| `max_retries` | 3 | 一過性のエラー (cluster reroute、shard relocation) は数秒で復旧することが多い |
| `retry_on_status` | 429, 502, 503, 504 | 過負荷 (429) と GW/proxy 系の一過性失敗のみ。400 系はリトライしない |
| `retry_on_timeout` | True | request_timeout (120s) を超えた接続は idempotent と仮定して 1 回まで再試行 |

### `bootstrap.memory_lock`

ES のヒープを swap させないため、`compose.yml` で `bootstrap.memory_lock=true` と `ulimits.memlock=-1` をセットしている。コンテナ環境で memlock が効くようにするための組み合わせ。

## インデックス管理

`es_create_index --index <group>` で group alias 込みで一括作成する。`<group>` は `bioproject` / `biosample` / `sra` / `jga` / `gea` / `metabobank` / `all`。各 group が含む物理 index と alias 構成は [data-architecture.md § Elasticsearch インデックス構成](data-architecture.md) を参照。

- `--skip-existing` で既存 index をスキップ
- `--date-suffix YYYYMMDD` で Blue-Green 用の dated index を作成 (alias なし)

`es_delete_index --index <group>` で削除。`--skip-missing` で不在エラーを無視。

## bulk insert

`es_bulk_insert --index <name>` で `{result_dir}/{type}/jsonl/{YYYYMMDD}/*.jsonl` を投入する。`_op_type: "index"` で投入するため、既存 doc は上書き (upsert 相当) になる。

落とし穴:

- 通常は alias 経由で投入される。Blue-Green の途中で `--target-index NAME-YYYYMMDD` を指定すると alias を経由せず物理 index に直接投入する
- batch size のデフォルトは 5000 (`--batch-size` で調整可能)。メモリと速度のバランス
- SRA は entity 別 (`sra-run` / `sra-study` 等) に分けて投入する。`--pattern '*_run_*.jsonl'` で entity 別 jsonl を絞り込める

## bulk insert / bulk delete の結果モデル

`bulk_insert` / `bulk_delete` は Pydantic モデル `BulkInsertResult` / `BulkDeleteResult` で結果を返す。両モデルとも `@model_validator(mode="after")` で **`success_count + not_found_count + error_count == total_*`** を assert する (戻り値を受け取る側で個別に集計検証する必要はない)。

| モデル | total フィールド | success | not_found | error |
|---|---|---|---|---|
| `BulkInsertResult` | `total_docs` | bulk API 成功 (201/200) | **HTTP 409 (version conflict)** | HTTP 5xx / connection error / その他想定外 |
| `BulkDeleteResult` | `total_requested` | bulk API 成功 | HTTP 404 (削除対象不在) | HTTP 5xx / connection error / その他想定外 |

`_op_type: "index"` は upsert 動作のため、bulk insert で 404 はほぼ発生しない。一方 **409 (version conflict)** は同一 `_id` に並列 write が発生したときや、ES が cluster block 中の場合に起きうる「ドキュメント状態が ES 側と converter 側で不整合」のシグナルなので、`bulk_delete` の `not_found_count` (削除対象が存在しない) と同じ「想定はしているが今回は反映されていない」枠として分類する。

不変条件を model 内 validator で守ることで:

- 戻り値を受けた API / CLI 層では `errors` リスト (詳細は最大 100 件) と各カウンタを直接そのまま信用してよい
- partial failure 検知時の運用ロジック (rollback / 再投入) も `not_found` と `error` を別経路で扱える

## blacklist 削除

`es_delete_blacklist` は blacklist ファイル (詳細は [data-architecture.md § Blacklist](data-architecture.md)) に含まれる accession を ES から削除する。

落とし穴:

- 存在しないドキュメント (404) はエラーとせず `not_found_count` としてカウントする (過去にインデックスされたが現在は不在のものを許容)
- accession の ID パターンから対象インデックスを判定するので、誤った blacklist ファイル (例: `bp/blacklist.txt` に SRA ID) に書いても効かない

## ヘルスチェック

`es_health_check` (`-v` で詳細) でクラスタ状態 / シャード / ディスク使用率を確認する。Blue-Green Full 更新前のディスク残量チェックに使う (新旧 index 同居時に容量が 2 倍になるため)。

## スナップショット管理

`es_snapshot` でリポジトリ登録・作成・復元・削除を扱う。`compose.yml` の `path.repo` と `es_snapshot repo register --path` は一致させる必要がある (両者とも `/usr/share/elasticsearch/backup`)。

### 定期バックアップ

`scripts/backup_es.sh` を cron で回す。スクリプトはヘルス確認 → スナップショット作成 → retention 超過分の削除を順に実行する。

```bash
# 毎日 2:00 AM、7 日保持
0 2 * * * /path/to/scripts/backup_es.sh --repo backup --retention 7 >> /var/log/es_backup.log 2>&1
```

### 別ノード移行

スナップショット作成 → `es-backup-${DDBJ_SEARCH_ENV}` volume を rsync/scp で別ノードに転送 → 転送先で `es_snapshot repo register` + `es_snapshot restore`。`podman volume inspect es-backup-${DDBJ_SEARCH_ENV}` (dev は `docker volume inspect`) でホスト側の実体パスを確認する。

### 障害復旧

`es_snapshot list --repo backup -v` で利用可能なスナップショットを確認し、`es_snapshot restore --force` で復元する。

### restore 時の live index 上書きガード

`es_snapshot restore` は復元対象の index 名が現在 live alias の target になっている場合、`--force` 無しでは `RuntimeError` を raise して中断する。live index を意図せず復元で上書きするとダウンタイムが発生するため、明示的なオプトインを要求する仕様。意図的に上書きする (障害復旧で旧 index を強制的に置き換える) ときだけ `--force` を付ける。`--force` 指定時は warning ログを残してから restore に進む。

## Blue-Green Alias Swap (ゼロダウンタイム更新)

Full 更新では全 14 インデックスを再構築する。「インデックス削除 → 再作成 → bulk insert」の通常フローでは、bulk insert 完了までの数十分〜数時間ダウンタイムが発生する。

Blue-Green Alias Swap パターンでは、新インデックスを `{name}-{YYYYMMDD}` という別名で作成し、bulk insert 完了後に alias を atomic に切り替える。alias swap は `_aliases` API で 1 トランザクションで実行されるため、検索断はゼロ。トレードオフはディスク使用量で、Full 更新中は新旧が同居して一時的に約 2 倍になる。mapping が変わらない更新は `--clean-es` で十分で、ディスク使用量は増えないが bulk insert 完了までの間は検索が空になる。差分更新は alias 経由で既存 index に upsert するので Blue-Green の影響を受けない。

### dated index と alias の関係

Blue-Green 適用後、物理 index は `{name}-{YYYYMMDD}` という日付サフィックス付き、alias は `{name}` (それ自体) と group alias (`sra` / `jga` / `entries`) の 2 段構成。API は alias 経由でアクセスするので、alias swap が透過的に効く。

### Full 更新フロー

```bash
# 1. dated index を作成 (alias なし)
es_create_index --index all --date-suffix 20260413

# 2. 新インデックスにデータ投入 (旧 index が検索に使われ続ける)
es_bulk_insert --index bioproject --target-index bioproject-20260413 --dir ${bp_dir}
# ... 14 インデックス分

# 3. 新インデックスから blacklist を削除
es_delete_blacklist --target-suffix 20260413 --force

# 4. alias を atomic に切り替え (ダウンタイム 0、旧サフィックスが stdout)
OLD_SUFFIX=$(es_swap_aliases --date-suffix 20260413 --force)

# 5. 旧インデックスを削除 (ディスク解放)
es_delete_old_indexes --date-suffix ${OLD_SUFFIX} --force
```

`scripts/run_pipeline.sh --full --blue-green` で一括実行できる。

### Group 単位の Blue-Green (部分更新)

特定の group (`sra` / `jga` / `bioproject` / `biosample` / `gea` / `metabobank`) だけ mapping や生成ロジックが変わった場合、その group のみを Blue-Green で更新できる。`es_create_index` / `es_delete_blacklist` / `es_swap_aliases` / `es_delete_old_indexes` の全てが `--index <group>` を受ける (デフォルト `all`)。

```bash
# 例: SRA group だけ Blue-Green で更新
es_create_index --index sra --date-suffix 20260507

# SRA 6 entity に dated index で投入
sra_dir=/data1/ddbj-search/result/sra/jsonl/20260507
for t in submission study experiment run sample analysis; do
  es_bulk_insert --index sra-$t --target-index sra-$t-20260507 --dir $sra_dir --pattern "*_${t}_*.jsonl"
done

es_delete_blacklist --index sra --target-suffix 20260507 --force
OLD_SUFFIX=$(es_swap_aliases --index sra --date-suffix 20260507 --force)
es_delete_old_indexes --index sra --date-suffix ${OLD_SUFFIX} --force
```

部分 swap の間は `entries` group alias が SRA-new + 他 5 group の old を指す状態になるが、解決数は 14 (= ALL_INDEXES) に保たれるため検索断は発生しない。

### swap 後の verification

`es_swap_aliases` は `_aliases` API 呼び出しの直後に `indices.get_alias()` で alias の target を読み直し、期待する dated index 集合と一致するかを assert する。一致しない場合は `RuntimeError` を raise し、CLI は exit code 1 で終了する。alias swap は冪等なので、verification 失敗時は alias 状態を確認の上、同じ `--date-suffix` で再実行すれば回復する。

### 差分更新フロー (変更なし)

差分更新は alias 経由で既存インデックスに upsert する。ES が alias を透過的に解決するため、Blue-Green 導入前と全く同じ操作で動作する。

### 初回マイグレーション

固定名インデックスを Blue-Green 構成に移行する 1 回限りの操作。`es_migrate_to_blue_green --date-suffix $(date +%Y%m%d) --force` で実行する。

内部処理:

1. 固定名インデックスを read-only に設定
2. `_clone` API でハードリンクベースのコピー (10 億件でも数秒)
3. クローン先の write block を解除
4. 固定名インデックスを削除 (**数秒のダウンタイム**)
5. `_aliases` API で全 alias を一括作成

初回マイグレーション時のみ Step 4-5 間に数秒のダウンタイムが発生する。以降の Full 更新はゼロダウンタイム。

### ロールバック

- 旧インデックスが未削除の場合: `es_swap_aliases --date-suffix OLD_SUFFIX --force` で alias を戻し、`es_delete_old_indexes --date-suffix NEW_SUFFIX --force` で新 index を削除
- 旧インデックスが削除済みの場合: `es_snapshot restore` で復元してから alias を再設定

### 注意事項

Full 更新中は新旧 index が同時存在して一時的にディスク使用量が 2 倍になる。事前に `es_health_check -v` でディスク残量を確認すること。
