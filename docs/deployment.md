# デプロイガイド

staging / production deploy で踏みやすい落とし穴と project 固有の運用注意点。

deploy の基本コマンドは [README.md](../README.md)、Blue-Green Alias Swap の詳細は [elasticsearch.md](elasticsearch.md) を参照する。本ドキュメントは「コードや compose ファイルを読んでも分からない運用上の注意点」だけを集める。

## podman override が無いと bind mount が壊れる

`compose.override.podman.yml` を `compose.override.yml` にコピーする (README のクイックスタート参照) のは見た目以上に重要。override が無いまま起動すると `userns_mode` や bind mount の `:U` 指定が効かず、bind mount したホスト側ファイルへの書き込みが UID ずれで黙って失敗する (read は通るので気付きにくい)。staging / production deploy の度に必ず確認する。api リポジトリも同じ運用。

## production の自動運用 (Rundeck)

production は `scripts/rundeck-job.yaml` を Rundeck に登録して日次差分更新を流している。job の中身は (1) `app` コンテナの再生成 (JGA mount のリフレッシュ用)、(2) `run_pipeline.sh --parallel 16` 実行、(3) `cleanup_old_results --keep 3 --include-spill`、(4) `mitsume ping` で完了を記録 (後述の監視用) の 4 ステップ。cleanup は pipeline の完了後に走るので、spill ディレクトリも同時に消してよい。Rundeck UI で実行履歴・失敗通知を確認する想定で、cron に直接登録はしていない。staging は手動実行のみ。

## production の監視 (mitsume)

production は [mitsume](https://github.com/suecharo/mitsume) で監視し、異常を Slack に通知する。何を監視するかは `scripts/mitsume/mitsume.json` に書いてあるものがすべて。

- 公開 URL と、ホスト上の nginx の公開ポート (localhost) の両方に HTTP で問い合わせる。外からだけ失敗していれば gateway より外側、両方失敗していれば nginx より内側の問題と切り分けられる。api は `/service-info` の `elasticsearch` が `ok` かまで見るので、ES に届いているかもここで分かる
- 日次 pipeline の走り忘れと失敗は dead-man's switch で検知する。Rundeck job の最後のステップが `mitsume ping` を送り、30 時間 ping が無ければ通知する。pipeline やその前のステップが失敗すると最後のステップまで進まない (`keepgoing: false`) ので、失敗もここで検知できる
- ホスト自体の停止は対象外

評価は、運用ユーザーの crontab から毎時 `scripts/mitsume/mitsume.sh check` を呼んで行う。`mitsume.sh` は webhook の env file を読み込み、設定と heartbeat file の path を mitsume に渡す。

初回のセットアップ:

```bash
# binary
curl -fL https://github.com/suecharo/mitsume/releases/download/v1.2.1/mitsume_1.2.1_linux_amd64.tar.gz | tar -xz -C /tmp mitsume
install -D -m 0755 /tmp/mitsume ~/.local/bin/mitsume
# Slack Incoming Webhook (repo には置かない)
install -d -m 0700 ~/.config/mitsume ~/.local/state/mitsume
echo 'MITSUME_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...' > ~/.config/mitsume/webhook.env
chmod 600 ~/.config/mitsume/webhook.env
# 試運転 (Slack には送らず、通知の中身を stderr に出す)
/data1/ddbj-search/ddbj-search-converter/scripts/mitsume/mitsume.sh check --dry-run
# 一度も ping が無いと dead-man's switch が毎時失敗するので、cron 登録前に 1 回記録する
/data1/ddbj-search/ddbj-search-converter/scripts/mitsume/mitsume.sh ping ddbj-search-daily
# crontab
0 * * * * /data1/ddbj-search/ddbj-search-converter/scripts/mitsume/mitsume.sh check >> $HOME/.local/state/mitsume/check.log 2>&1
```

## 4 リポジトリ構成と deploy 単位

DDBJ Search は converter / api / front / nginx の 4 リポジトリに分かれており、staging / production の同一ホスト上に並べて配置する。各 compose project は独立しているが、network (`ddbj-search-network-{env}`) は共通で `external: true` 参照する。

- Elasticsearch は本リポジトリの compose で起動する。api / front は network 経由で接続するだけで、ES 自体は持たない
- deploy 単位はリポジトリごとに独立しているので、front だけ更新したいときに converter / api を触る必要はない
- 例外: 本リポジトリの Pydantic モデル / ES mapping を変更する deploy では、api 側も同時に更新する必要がある (api は `git+...@main` で本リポジトリを依存している)

## image の build

Dockerfile は builder / runtime / dev の 3 つの stage を持つ。compose.yml の `app` は、`.env` の `DDBJ_SEARCH_APP_BUILD_TARGET` で runtime か dev のどちらの image を build するかを選ぶ。

- runtime (production): venv には実行に要る依存 package だけを入れる。uv と、test 用の依存 package (`pyproject.toml` の `dev` group) は入れない
- dev (dev と staging): runtime に uv と test 用の依存 package を足す。staging の `app` コンテナで integration test を実行するので、staging も dev を使う

venv は image の中 (`/opt/venv/`) に作る。venv を named volume に置くと、ホストとコンテナで UID がずれたときに venv が壊れるため。project は editable で venv に入れるので、bind mount (`.:/app:rw`) したソースの変更は `podman-compose restart app` で反映される。

`pyproject.toml` / `uv.lock` / Dockerfile を変えたときと、base image の修正 (OS と Python) を取り込むときは、`--pull` を付けて build し直してからコンテナを作り直す。同名のコンテナが残っていると podman-compose はコンテナを作り直さず、古い image のコンテナを起動するので、`down` してから `up` する。

```bash
podman-compose build --pull app
podman-compose down
podman-compose up -d
```

## Elasticsearch の version を上げる

ES の image は `compose.yml` で version まで固定している。上げる前に、release notes の breaking changes と upgrade の条件 (どの version から直接上げられるか) を読む。ES は新しい version で一度起動したデータを古い version では開けないので、上げる前に snapshot を取る ([elasticsearch.md](elasticsearch.md) の「スナップショット管理」)。戻すときは、古い version の空の ES にこの snapshot を復元する。

`compose.yml` の tag を変えたあと、ES のコンテナを消してから起動し直す。named volume のデータは残る。ES を止めている間は api の検索が失敗するので、production では日次の Rundeck job と重ならない時間に行う。

```bash
podman-compose exec app es_snapshot create --repo <repo>
podman-compose stop elasticsearch
podman rm ddbj-search-es-${DDBJ_SEARCH_ENV}
podman-compose up -d elasticsearch
podman-compose exec app es_health_check
```

## named volume の rename

`compose.yml` の named volume 名を変えたあとに、ホスト上の既存 volume データを保持したい場合は、`down` で writer を止めてから旧 volume のデータを新しい名前の volume に移す。

podman 5.x 以降なら `podman volume rename` で付け替える。

```bash
podman-compose down                                              # writers を止める
podman volume rename ddbj-search-converter_es-data es-data-staging
podman volume rename ddbj-search-converter_es-backup es-backup-staging
podman-compose up -d                                             # 新しい名前で同じ実体を mount
```

podman 4.x には `volume rename` が無い (`unrecognized command` になる)。新しい名前の volume を作り、旧 volume の `_data` 配下の entry を全部 mv する。同じ filesystem 内の mv なのでデータ量によらずすぐ終わる。一部の entry だけを mv すると ES が起動時に不整合を起こすので、`_data` 直下を丸ごと移す。

```bash
podman-compose down
podman volume create es-data-staging
src=$(podman volume inspect ddbj-search-converter_es-data --format '{{.Mountpoint}}')
dst=$(podman volume inspect es-data-staging --format '{{.Mountpoint}}')
find "$src" -mindepth 1 -maxdepth 1 -exec mv -t "$dst" {} +
# es-backup も同様
podman-compose up -d
```

どちらの方法でも、付け替えを挟まずに新 compose で `up -d` すると、新名で空の volume が作られて旧データから切り離される。気付かずに pipeline を回すと ES を再構築する羽目になるので、`down → 付け替え → up -d` の順序を必ず守る。

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
podman-compose up -d --build --pull
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

ES の image は registry から pull するので、tag backup の対象外。converter 本体だけが対象。

### スキーマ変更が絡む場合

converter の Pydantic モデル / ES mapping 変更が絡むロールバックは、依存している api 側もロールバックする必要がある (api は `git+...@main` を見ているため、本リポジトリの main を戻すだけでは api 側の lock が古いまま)。

## Blue-Green を選ぶか `--clean-es` を選ぶか

ES の mapping が変わる Full 更新では Blue-Green Alias Swap を使う。alias swap で検索断ゼロになる代わりに、新旧 index が一時的に同居してディスク使用量が約 2 倍になる。

mapping が変わらない更新は `--clean-es` で十分。bulk insert 完了までの間 (数十分〜数時間) は検索が空になるが、ディスク使用量は増えない。詳細フローは [elasticsearch.md § Blue-Green Alias Swap](elasticsearch.md) を参照。
