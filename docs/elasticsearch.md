# Elasticsearch

DDBJ Search Converter の Elasticsearch 設定方針と運用 (snapshot / Blue-Green)。

データ構造 (14 indexes / alias / blacklist ファイル) は [data-architecture.md](data-architecture.md) を、Blue-Green と `--clean-es` の選択基準は [deployment.md](deployment.md) を参照する。

## 設定の置き場所

ES 関連の設定は 4 箇所に分散している。実値は各ファイルが SSOT で、docs には WHY だけ書く。

| ファイル | 役割 |
|---------|------|
| `env.{dev,staging,production}` | 環境差分 (メモリ上限、JVM heap、接続先 URL) |
| `compose.yml` | ES コンテナ定義 (env を参照、named volume `es-data` / `es-backup`) |
| `elasticsearch/config/elasticsearch.yml` | ES 本体の調整 (bulk insert 最適化のため `http.max_content_length` を ES 上限 (2GB-1byte) に拡張、`thread_pool.write.queue_size` を無制限、`indices.memory.index_buffer_size` を 30% に増やしている) |
| `ddbj_search_converter/es/settings.py` | converter 側の bulk insert / index 操作の定数 (batch size、retries、refresh interval 切替) |

### bulk insert 中の refresh 無効化

`BULK_INSERT_SETTINGS` で `refresh_interval` を `-1` に切り替え、bulk insert 完了後に `1s` に戻す。bulk insert 中の refresh は数百万件投入時に大きなオーバーヘッドになる (refresh 頻発でスループットが落ちる) ため。

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

## blacklist 削除

`es_delete_blacklist` は blacklist ファイル (詳細は [data-architecture.md § Blacklist](data-architecture.md)) に含まれる accession を ES から削除する。

落とし穴:

- 存在しないドキュメント (404) はエラーとせず `not_found` としてカウントする (過去にインデックスされたが現在は不在のものを許容)
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

スナップショット作成 → `es-backup` volume を rsync/scp で別ノードに転送 → 転送先で `es_snapshot repo register` + `es_snapshot restore`。`docker volume inspect es-backup` でホスト側の実体パスを確認する。

### 障害復旧

`es_snapshot list --repo backup -v` で利用可能なスナップショットを確認し、`es_snapshot restore --force` で復元する。

## Blue-Green Alias Swap (ゼロダウンタイム更新)

Full 更新では全 14 インデックスを再構築する。「インデックス削除 → 再作成 → bulk insert」の通常フローでは、bulk insert 完了までの数十分〜数時間ダウンタイムが発生する。

Blue-Green Alias Swap パターンでは、新インデックスを `{name}-{YYYYMMDD}` という別名で作成し、bulk insert 完了後に alias を atomic に切り替える。alias swap は `_aliases` API で 1 トランザクションで実行されるため、検索断はゼロ。トレードオフはディスク使用量で、Full 更新中は新旧が同居して一時的に約 2 倍になる。mapping が変わらない更新は `--clean-es` で十分で、ディスク使用量は増えないが bulk insert 完了までの間は検索が空になる。差分更新は alias 経由で既存 index に upsert するので Blue-Green の影響を受けない。

> **NOTE**: `Publication.Reference` → `reference` / `Publication.DbType` → `dbType` の rename、`Publication.dbType` 値の小文字化 (`ePubmed` → `pubmed` 等) と `Publication.status` フィールド廃止 (2026-04-22)、GEA / MetaboBank の新 index (`gea` / `metabobank`) 追加により、既存 index との mapping 互換性が失われる。次回 deploy 時は必ず Blue-Green Alias Swap で新 index を別名で作成し、alias を一括 swap すること。既存の `--clean-es` フローは mapping が変わらない場合にのみ安全。

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
