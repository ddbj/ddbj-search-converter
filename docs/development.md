# 開発ガイド

開発時に踏みやすい落とし穴と project 固有の注意点。

dev 環境の起動手順は [README.md](../README.md)、テスト方針は [tests/README.md](../tests/README.md) を参照する。

## コンテナ内実行を貫く

すべての開発コマンド (`uv sync`, `pytest`, `ruff`, `mypy`, CLI コマンド全般) は **コンテナ内で実行する**。ホスト側の Python / uv で動かさない。理由は (a) Python バージョンを `pyproject.toml` の `requires-python` と揃える、(b) Linux 専用の依存ライブラリ (DuckDB extension など) でホスト依存差分を出さない、(c) staging / production と同じ環境で検証する、の 3 点。

dev の `compose.yml` は `command: sleep infinity` でコンテナを起動する。CLI コマンドは都度 `docker compose exec app <command>` の形で実行する。

## 破壊的変更の影響範囲

api は本リポジトリの Pydantic モデル (`schema.py`) と ES mapping を `git+https://...@main` で直接 import する。スキーマの SSOT は本リポジトリ側にあり、api は受信側として追従する関係。**本リポジトリの破壊的変更は main にマージした時点から api 側に影響する**ので、レビュー時に影響範囲を明示しておくと事故を減らせる。

- 同 PR で api 側の対応 PR の存在を確認する。api 側のテストは converter の Pydantic モデルを `hypothesis.strategies.builds()` で生成しているので、モデル変更で大量に落ちる場合がある
- 大きな mapping 変更は ES 側の Blue-Green Alias Swap が必要になることがある ([elasticsearch.md](elasticsearch.md))
- DuckDB のスキーマ変更は api 側の read 連携にも影響する (api コンテナは `dblink.duckdb` を起動中ずっと read mode で握っている)
- 同 git 並びでチェックアウトしておくと両方向の影響確認が楽 (`~/git/github.com/ddbj/ddbj-search-{converter,api}`)

## test fixtures の更新タイミング

`tests/fixtures/` は本番想定のデータ構造を再現した小規模データセットで、git 管理されている。**初期セットアップでは何もしなくて良い**。

更新が必要になるのは:

- 本番のデータ構造が変わって、現 fixture では新しいケースを再現できないとき
- 新しいエッジケースに対応する accession を追加したいとき

これらは遺伝研スパコン上で `scripts/fetch_test_fixtures.sh` を実行する。手元では取得できないため、必要が生じたタイミングでスパコン作業を依頼する。
