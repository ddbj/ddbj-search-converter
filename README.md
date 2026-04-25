# ddbj-search-converter

[DDBJ Search](https://ddbj.nig.ac.jp/search) のデータ投入用パイプラインツール。BioProject / BioSample / SRA / JGA / GEA / MetaboBank の生メタデータと INSDC / DRA Accessions を取り込み、Elasticsearch に投入する JSONL とデータベース間関連を保持する DuckDB (DBLink) を構築する。

## 関連プロジェクト

- ddbj-search-converter (本リポジトリ): ES と DuckDB へのデータ投入パイプライン。Pydantic スキーマと ES mapping の SSOT
- [ddbj-search-api](https://github.com/ddbj/ddbj-search-api): ES と DuckDB を読む REST API サーバー。converter のスキーマを `git+...@main` で直接 import する
- [ddbj-search-front](https://github.com/ddbj/ddbj-search-front): フロントエンド
- [ddbj-search](https://github.com/ddbj/ddbj-search): nginx reverse proxy

converter と api を分けているのは、データ投入バッチと API サーバーのライフサイクル・依存関係が大きく違うため。converter は重い ETL 依存を抱えるバッチ、api は軽量な FastAPI で応答性を重視する。両者を 1 つに混ぜると依存関係と Docker イメージが膨らみ、deploy が API のパフォーマンスにも影響する。

## クイックスタート

dev 環境はローカルで `tests/fixtures/` の小規模データを使うので外部リソース不要。

```bash
cp env.dev .env
docker network create ddbj-search-network-dev || true
docker compose up -d --build
docker compose exec app bash
```

staging / production は podman-compose で動かす。`env.staging` または `env.production` をコピーした `.env` と、podman 用 override をコピーで効かせる:

```bash
cp env.staging .env
cp compose.override.podman.yml compose.override.yml  # podman-compose が自動で読み込む
podman network create ddbj-search-network-staging || true
podman-compose up -d --build
```

詳細・落とし穴と production の Rundeck job (`scripts/rundeck-job.yaml` で日次運用) は [docs/deployment.md](docs/deployment.md)。

パイプラインの実行 (`scripts/run_pipeline.sh --full --blue-green` 等) と CLI コマンドリファレンスは [docs/cli-pipeline.md](docs/cli-pipeline.md)。

## ドキュメント

- [docs/cli-pipeline.md](docs/cli-pipeline.md) - パイプライン詳細・CLI コマンドリファレンス・差分更新
- [docs/data-architecture.md](docs/data-architecture.md) - データフロー・AccessionType・外部リソース・出力ファイル・主要 DB の設計判断
- [docs/elasticsearch.md](docs/elasticsearch.md) - ES 操作・スナップショット・Blue-Green Alias Swap
- [docs/logging.md](docs/logging.md) - ログとデバッグ
- [docs/rdf-pipeline.md](docs/rdf-pipeline.md) - RDF 変換パイプライン
- [docs/development.md](docs/development.md) - 開発時の落とし穴
- [docs/deployment.md](docs/deployment.md) - staging / production の運用注意点
- [tests/README.md](tests/README.md) - テスト方針
- [tests/integration-scenarios.md](tests/integration-scenarios.md) - 結合シナリオ
- [tests/integration-note.md](tests/integration-note.md) - 結合運用ノート
- [ontology/README.md](ontology/README.md) - JSON-LD Context / Vocabulary 定義

## License

This project is licensed under the [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) license. See the [LICENSE](./LICENSE) file for details.
