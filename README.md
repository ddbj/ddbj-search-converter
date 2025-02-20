# ddbj-search-converter

- [DDBJ-Search](https://ddbj.nig.ac.jp) のデータ投入用 script 群。
- `biosample_set.xml` や `bioproject.xml` といった XML file を JSON-Lines (ES bulk data file) に変換し、Elasticsearch に投入する。

## Usage

- まず、基本的に遺伝研スパコン上の resource と密結合した実装となっている
- かつ、docker (or podman) での実行を前提としている
- 依存している遺伝研スパコン上の resource としては、下記の通りである
  - [`./compose.yml`](./compose.yml) も参照してください

```yaml
- /usr/local/resources/bioproject
- /usr/local/resources/biosample
- /lustre9/open/shared_data/dblink
- /lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions
```

### 環境変数

- それぞれの処理は、cli script として実装されている
  - [`./pyproject.toml`](./pyproject.toml) も参照してください
- cli script 一覧としては、下記の通りである

```toml
[project.scripts]
create_es_index = "ddbj_search_converter.es_mappings.create_es_index:main"
create_bp_date_db = "ddbj_search_converter.cache_db.bp_date:main"
create_bs_date_db = "ddbj_search_converter.cache_db.bs_date:main"
bp_xml_to_jsonl = "ddbj_search_converter.bioproject.bp_xml_to_jsonl:main"
bp_bulk_insert = "ddbj_search_converter.bioproject.bp_bulk_insert:main"
bp_relation_ids_bulk_update = "ddbj_search_converter.bioproject.bp_relation_ids_bulk_update:main"
bs_xml_to_jsonl = "ddbj_search_converter.biosample.bs_xml_to_jsonl:main"
bs_bulk_insert = "ddbj_search_converter.biosample.bs_bulk_insert:main"
bs_relation_ids_bulk_update = "ddbj_search_converter.biosample.bs_relation_ids_bulk_update:main"
```

- それぞれの script は、`--help` で特有の argument が存在する
- しかし、同時に、全体的な挙動を制御するための環境変数が存在する
  - `DDBJ_SEARCH_CONVERTER_DEBUG`
    - `true` or `false`
    - 基本的に logger の出力
    - debug mode で実行しておく方が、インシデント対応が楽だと思われる
  - `DDBJ_SEARCH_CONVERTER_WORK_DIR`
    - 出力される諸々の file の base path
  - `DDBJ_SEARCH_CONVERTER_POSTGRES_URL`
    - date 情報を取得する元となる PostgreSQL DB の URL
    - `postgresql://{username}:{password}@{host}:{port}` のような形式
  - `DDBJ_SEARCH_CONVERTER_ES_URL`
    - data 格納先の Elasticsearch URL
  - `DDBJ_SEARCH_CONVERTER_SRA_ACCESSIONS_TAB_BASE_PATH`
    - 最新の `SRA_Accessions.tab` file を find する際の base path
  - `DDBJ_SEARCH_CONVERTER_SRA_ACCESSIONS_TAB_FILE_PATH`
    - 上の `BASE_PATH` と排他制御
    - この環境変数が設定されている場合、その file path を用いる
  - `DDBJ_SEARCH_CONVERTER_DBLINK_BASE_PATH`
    - `dblink` 情報を取得するもととなる file の base path
- これらの環境変数は、[`./compose.yml`](./compose.yml) などに設定する
- `コマンドライン引数 > 環境変数 > デフォルト値 (config.py)` の優先度で処理される

### ともかく投入する

```bash
# Create docker network
$ docker network create ddbj-search-network

# Up Elasticsearch Container
$ mkdir -p ./elasticsearch/data
$ mkdir -p ./elasticsearch/logs
$ chmod 777 ./elasticsearch/data
$ chmod 777 ./elasticsearch/logs
$ docker compose -f compose.elasticsearch.yml up -d

# ES 動作確認
$ curl localhost:19200/_cluster/health?pretty

# Up Converter container
$ docker compose -f compose.dev.yml up -d --build
$ docker compose -f compose.dev.yml exec app bash

# Inside container
# Elasticsearch の index を作成する
$ create_es_index --index bioproject
$ create_es_index --index biosample

# Cache 用 sqlite db を作成する
$ create_bp_date_db
$ create_bs_date_db

# XML to JSON-Lines
$ bp_xml_to_jsonl --xml-file /usr/local/resources/bioproject/bioproject.xml
$ bp_xml_to_jsonl --xml-file /usr/local/resources/bioproject/ddbj_core_bioproject.xml --is-ddbj

$ bs_xml_to_jsonl --xml-file /usr/local/resources/biosample/biosample_set.xml.gz
$ bs_xml_to_jsonl --xml-file /usr/local/resources/biosample/ddbj_biosample_set.xml.gz --is-ddbj --use-existing-tmp-dir

# Bulk insert
$ bp_bulk_insert
$ bs_bulk_insert
```

### 時間メモ

- `create_bp_date_db`: 一瞬
- `create_bs_date_db`: 14m
- `create_es_index --index bioproject`: 一瞬
- `create_es_index --index biosample`: 一瞬
- `bp_xml_to_jsonl`: 2m
- `bp_xml_to_jsonl --is-ddbj`: 1m
- `bs_xml_to_jsonl`: 40m
- `bs_xml_to_jsonl --is-ddbj`: 2m
- `bp_bulk_insert`: 30m
- `bs_bulk_insert`: 9h

## Development

開発用環境として、`./ddbj_search_converter` が mount され、pip の development mode で install されている環境が存在する。

```bash
$ docker network create ddbj-search-network-dev
$ docker compose -f docker-compose.dev.yml up -d
$ docker compose -f docker-compose.dev.yml exec app bash
# inside the container
$ create_bp_date_db --help
...
```

### Linting, Formatting and Testing

lint, format, test は以下のコマンドで実行できる。

```bash
# Lint and Format
$ pylint ./ddbj_search_converter
$ mypy ./ddbj_search_converter
$ isort ./ddbj_search_converter

# Test
$ pytest
```

また、CI / CD は GitHub Actions により実行される。See [`.github/workflows`](./.github/workflows) for more details.

## License

This project is licensed under the [Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0) license. See the [LICENSE](./LICENSE) file for details.
