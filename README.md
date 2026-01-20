# ddbj-search-converter

[DDBJ-Search](https://ddbj.nig.ac.jp) のデータ投入用パイプラインツール。BioProject、BioSample、SRA/DRA、JGA、GEA、MetaboBank などの生命科学データベース間の関連情報 (DBLink) を構築する。

## 環境構成

- `compose.yml`: 本番環境（遺伝研スパコン）
- `compose.dev.yml`: 開発環境（fixtures 使用）
- `compose.elasticsearch.yml`: Elasticsearch 単体起動用

## DBLink 作成

### 本番環境での実行

```bash
# Docker network 作成 & Elasticsearch 起動
docker network create ddbj-search-network
mkdir -p ./elasticsearch/{data,logs,backup}
chmod 777 ./elasticsearch/{data,logs,backup}
docker compose -f compose.elasticsearch.yml up -d

# Converter container 起動
docker compose up -d --build
docker compose exec app bash

# === ここから container 内 ===

# 外部リソースの存在確認
check_external_resources

# XML 準備 (batch 分割)
prepare_bioproject_xml
prepare_biosample_xml

# SRA/DRA Accessions DB 構築
build_sra_and_dra_accessions_db

# DBLink DB 作成
init_dblink_db
create_dblink_bp_bs_relations
create_dblink_bioproject_relations
create_dblink_assembly_and_master_relations
create_dblink_gea_relations
create_dblink_metabobank_relations
create_dblink_jga_relations
create_dblink_sra_internal_relations
finalize_dblink_db

# TSV 出力
dump_dblink_files
```

### 開発環境での実行

```bash
# 開発環境起動 (fixtures 使用)
docker compose -f compose.dev.yml up -d --build
docker compose -f compose.dev.yml exec app bash

# 以降のコマンドは本番環境と同じ
```

## CLI コマンド一覧

| コマンド | 説明 |
|---------|------|
| `check_external_resources` | 必要な外部リソースの存在確認 |
| `prepare_bioproject_xml` | BioProject XML を batch 分割 |
| `prepare_biosample_xml` | BioSample XML を展開・batch 分割 |
| `build_sra_and_dra_accessions_db` | SRA/DRA Accessions.tab を DuckDB にロード |
| `init_dblink_db` | DBLink DB を初期化 |
| `create_dblink_bp_bs_relations` | BioProject-BioSample 関連を抽出 |
| `create_dblink_bioproject_relations` | BioProject 内部関連 (umbrella, hum-id) を抽出 |
| `create_dblink_assembly_and_master_relations` | Assembly/INSDC Master 関連を抽出 |
| `create_dblink_gea_relations` | GEA 関連を抽出 |
| `create_dblink_metabobank_relations` | MetaboBank 関連を抽出 |
| `create_dblink_jga_relations` | JGA 関連を抽出 |
| `create_dblink_sra_internal_relations` | SRA 内部関連 (Study-Experiment-Run-Sample) を抽出 |
| `finalize_dblink_db` | DBLink DB を確定 |
| `dump_dblink_files` | DBLink DB から TSV ファイルを出力 |
| `sync_ncbi_tar` | NCBI SRA Metadata tar を同期 |
| `sync_dra_tar` | DRA Metadata tar を同期 |

全てのコマンドは引数を取らず、環境変数から設定を読み込む。

## 環境変数

| 環境変数 | 説明 | デフォルト |
|---------|------|-----------|
| `DDBJ_SEARCH_CONVERTER_RESULT_DIR` | 結果出力先ディレクトリ | `./ddbj_search_converter_results` |
| `DDBJ_SEARCH_CONVERTER_CONST_DIR` | 定数/共有リソースディレクトリ | `/home/w3ddbjld/const` |
| `DDBJ_SEARCH_CONVERTER_POSTGRES_URL` | PostgreSQL URL | `postgresql://const:const@at098:54301` |
| `DDBJ_SEARCH_CONVERTER_ES_URL` | Elasticsearch URL | `http://ddbj-search-elasticsearch:9200` |

## 開発

### テスト・リント

```bash
pytest
pylint ./ddbj_search_converter
mypy ./ddbj_search_converter
isort ./ddbj_search_converter
```

## License

This project is licensed under the [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) license. See the [LICENSE](./LICENSE) file for details.
