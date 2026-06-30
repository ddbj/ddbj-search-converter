# デプロイガイド

staging / production deploy で踏みやすい落とし穴と project 固有の運用注意点。

deploy の基本コマンドは [README.md](../README.md)、Blue-Green Alias Swap の詳細は [elasticsearch.md](elasticsearch.md) を参照する。本ドキュメントは「コードや compose ファイルを読んでも分からない運用上の注意点」だけを集める。

## podman override が無いと bind mount が壊れる

`compose.override.podman.yml` を `compose.override.yml` にコピーする (README のクイックスタート参照) のは見た目以上に重要。override が無いまま起動すると `userns_mode` や bind mount の `:U` 指定が効かず、bind mount したホスト側ファイルへの書き込みが UID ずれで黙って失敗する (read は通るので気付きにくい)。staging / production deploy の度に必ず確認する。api リポジトリも同じ運用。

## production の自動運用 (Rundeck)

production は `scripts/rundeck-job.yaml` を Rundeck に登録して日次差分更新を流している。job の中身は (1) `app` コンテナの再生成 (JGA mount のリフレッシュ用)、(2) `run_pipeline.sh --parallel 16` 実行、(3) `cleanup_old_results --keep 3` の 3 ステップ。Rundeck UI で実行履歴・失敗通知を確認する想定で、cron に直接登録はしていない。staging は手動実行のみ。

## 4 リポジトリ構成と deploy 単位

DDBJ Search は converter / api / front / nginx の 4 リポジトリに分かれており、staging / production の同一ホスト上に並べて配置する。各 compose project は独立しているが、network (`ddbj-search-network-{env}`) は共通で `external: true` 参照する。

- Elasticsearch は本リポジトリの compose で起動する。api / front は network 経由で接続するだけで、ES 自体は持たない
- deploy 単位はリポジトリごとに独立しているので、front だけ更新したいときに converter / api を触る必要はない
- 例外: 本リポジトリの Pydantic モデル / ES mapping を変更する deploy では、api 側も同時に更新する必要がある (api は `git+...@main` で本リポジトリを依存している)

## 依存パッケージ更新時は image を rebuild する

venv は image layer 内 (`/opt/venv/`) に焼き込んでいる。`pyproject.toml` / `uv.lock` を更新したあとは `podman-compose up -d --build` で image を再構築すれば反映される。

bind mount (`.:/app:rw`) は Python ソース変更を即時反映するが、venv 自体は image の外には出していない。これは過去にホスト UID と named volume の組み合わせで venv が壊れたケースを構造的に避ける狙い (UID mismatch があっても venv が共有領域に出ないため壊れない)。

## named volume の rename

`compose.yml` の named volume 名を変えたあとに、ホスト上の既存 volume データを保持したい場合は `podman volume rename` を使う (podman 4.4+)。

例: `<project>_es-data` を `es-data-staging` に rename する:

```bash
podman-compose down                                              # writers を止める
podman volume rename ddbj-search-converter_es-data es-data-staging
podman volume rename ddbj-search-converter_es-backup es-backup-staging
podman-compose up -d                                             # 新しい名前で同じ実体を mount
```

rename を挟まずに新 compose で `up -d` すると、新名で空の volume が作られて旧データから切り離される。気付かずに pipeline を回すと ES を再構築する羽目になるので、`down → rename → up -d` の順序を必ず守る。

## `DDBJ_SEARCH_ENV` 切替時に旧コンテナが残る

`.env` の `DDBJ_SEARCH_ENV` を変更して `down && up` する場合、**旧 `container_name` のコンテナは自動削除されない**。podman-compose は現在の compose spec に一致するコンテナだけを down 対象にするためで、旧コンテナが孤立して残る (named volume は env suffix で別物になるので volume の取り合いは起きないが、孤立コンテナだけはそのまま残る)。

切替前に旧コンテナを `podman stop && podman rm` で手動掃除しておく。

## api コンテナが `dblink.duckdb` を read lock で握っている

api コンテナは起動中ずっと `dblink.duckdb` を read mode で握っている。converter 側で書き込みモード (`access_mode='read_write'`) で開く必要があるパッチや調査スクリプトを動かす場合は、先に api を `podman stop` で止める必要がある。

通常の `finalize_dblink_db` は新しい DuckDB ファイルを atomic replace する設計なので本問題は発生しない。手動で既存ファイルを書き換える操作だけが該当する。

## 本番 deploy 前の rehearsal

本番 deploy 前に staging で integration test (`tests/py_tests/integration/`) を回せる。実行方法と env vars は [tests/README.md](../tests/README.md) を参照する。`IT-MAPPING-*` (現 mapping が ES に accept されるか)、`IT-PG-*` (TRAD / XSM 接続性 + SQL 整合)、`IT-INVARIANT-*` (dblink 半辺化対称性 + ES alias 整合) あたりが本番 deploy 直前のサニティとして有効。

`IT-SWAP-*` (alias swap rehearsal) は本番運用 alias を一時剥がすので staging では実行しない設計 (`DDBJ_SEARCH_INTEGRATION_ALLOW_DESTRUCTIVE_ALIAS=1` を立てた dev ES だけで動く)。

## ロールバック

2 手順。基本は (A) git rebuild、緊急時は (B) image tag backup を使う。

### A. git rebuild (default)

```bash
cd /data1/ddbj-search/ddbj-search-converter/
git checkout <previous-commit>
podman-compose down
podman-compose up -d --build
```

所要時間は build 込みで 5〜10 分。Python ソースだけのロールバックで Dockerfile / `pyproject.toml` / `uv.lock` が変わっていない場合は、`--build` を省略すれば bind mount だけで戻せる (`git checkout <commit> && podman-compose restart app`)。依存パッケージが変わっているときは `--build` 必須。

### B. image tag backup (緊急 option)

build を待たずに戻したいときの 30〜60 秒 rollback。deploy **前** に prev image を別 tag で保存しておくことが前提条件。

```bash
# deploy 直前 (latest を prev として退避)
podman tag ddbj-search-converter-${DDBJ_SEARCH_ENV}:latest ddbj-search-converter-${DDBJ_SEARCH_ENV}:prev
```

rollback 時:

```bash
podman-compose down
podman tag ddbj-search-converter-${DDBJ_SEARCH_ENV}:prev ddbj-search-converter-${DDBJ_SEARCH_ENV}:latest
podman-compose up -d   # --build なし
```

ES image (`docker.elastic.co/elasticsearch/elasticsearch:8.17.1`) は registry pull なので tag backup の対象外。converter 本体だけが対象。

### スキーマ変更が絡む場合

converter の Pydantic モデル / ES mapping 変更が絡むロールバックは、依存している api 側もロールバックする必要がある (api は `git+...@main` を見ているため、本リポジトリの main を戻すだけでは api 側の lock が古いまま)。

## Blue-Green を選ぶか `--clean-es` を選ぶか

ES の mapping が変わる Full 更新では Blue-Green Alias Swap を使う。alias swap で検索断ゼロになる代わりに、新旧 index が一時的に同居してディスク使用量が約 2 倍になる。

mapping が変わらない更新は `--clean-es` で十分。bulk insert 完了までの間 (数十分〜数時間) は検索が空になるが、ディスク使用量は増えない。詳細フローは [elasticsearch.md § Blue-Green Alias Swap](elasticsearch.md) を参照。
