# デプロイガイド

staging / production deploy で踏みやすい落とし穴と project 固有の運用注意点。

deploy の基本コマンドは [README.md](../README.md)、Blue-Green Alias Swap の詳細は [elasticsearch.md](elasticsearch.md) を参照する。本ドキュメントは「コードや compose ファイルを読んでも分からない運用上の注意点」だけを集める。

## 4 リポジトリ構成と deploy 単位

DDBJ Search は converter / api / front / nginx の 4 リポジトリに分かれており、staging / production の同一ホスト上に並べて配置する。各 compose project は独立しているが、network (`ddbj-search-network-{env}`) は共通で `external: true` 参照する。

- Elasticsearch は本リポジトリの compose で起動する。api / front は network 経由で接続するだけで、ES 自体は持たない
- deploy 単位はリポジトリごとに独立しているので、front だけ更新したいときに converter / api を触る必要はない
- 例外: 本リポジトリの Pydantic モデル / ES mapping を変更する deploy では、api 側も同時に更新する必要がある (api は `git+...@main` で本リポジトリを依存している)

## 依存パッケージ更新時は `app-venv` を rm する

`podman-compose down` は named volume を削除しない。`app-venv` volume に古い `.venv` が残るため、`podman-compose up -d --build` だけではパッケージの更新が反映されないケースがある。

依存更新時は `down` と `up --build` の間に `podman volume rm <project>_app-venv` を挟む。とくに本リポジトリのスキーマを更新したあとに api 側を rebuild する場合、api の `app-venv` に converter の旧 `.venv` が残っていると import エラーで起動失敗する。

## `DDBJ_SEARCH_ENV` 切替時に旧コンテナが残る

`.env` の `DDBJ_SEARCH_ENV` を変更して `down && up` する場合、**旧 `container_name` のコンテナは自動削除されない**。podman-compose は現在の compose spec に一致するコンテナだけを down 対象にするためで、旧コンテナが孤立して残る。

切替前に旧コンテナを `podman stop && podman rm` で手動掃除する必要がある。Elasticsearch のように named volume を握っているコンテナを残したまま新コンテナを起動すると、volume lock で起動失敗する。

## nginx resolver の gateway IP は環境ごと・再作成ごとに変わる

nginx の `DDBJ_SEARCH_RESOLVER` は **コンテナが所属するネットワークの gateway IP**。podman network を再作成すると IP が変わるため、deploy のたびに `podman network inspect <name> --format '{{range .Subnets}}{{.Gateway}}{{end}}'` で確認して `.env` に反映する。

## api コンテナが `dblink.duckdb` を read lock で握っている

api コンテナは起動中ずっと `dblink.duckdb` を read mode で握っている。converter 側で書き込みモード (`access_mode='read_write'`) で開く必要があるパッチや調査スクリプトを動かす場合は、先に api を `podman stop` で止める必要がある。

通常の `finalize_dblink_db` は新しい DuckDB ファイルを atomic replace する設計なので本問題は発生しない。手動で既存ファイルを書き換える操作だけが該当する。

## ロールバック

ソースコードは bind mount (`.:/app:rw`) のため、`git checkout <previous-commit>` + `podman-compose restart app` だけで前バージョンに戻る。Dockerfile を変えていない限り rebuild 不要。

converter のスキーマ変更が絡むロールバックは、依存している api 側もロールバックする必要がある (api は `git+...@main` を見ているため、本リポジトリの main を戻すだけでは api の lock が古いまま)。

## Blue-Green を選ぶか `--clean-es` を選ぶか

ES の mapping が変わる Full 更新では Blue-Green Alias Swap を使う。alias swap で検索断ゼロになる代わりに、新旧 index が一時的に同居してディスク使用量が約 2 倍になる。

mapping が変わらない更新は `--clean-es` で十分。bulk insert 完了までの間 (数十分〜数時間) は検索が空になるが、ディスク使用量は増えない。詳細フローは [elasticsearch.md § Blue-Green Alias Swap](elasticsearch.md) を参照。
