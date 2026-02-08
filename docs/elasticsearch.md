# Elasticsearch

DDBJ Search Converter の Elasticsearch 操作。

## 設定ファイル構成

Elasticsearch の設定は **4 箇所** に分散している。

| ファイル | 役割 |
|---------|------|
| `env.{dev,staging,production}` | 環境変数の定義。ES メモリ、接続先 URL |
| `compose.yml` | Docker Compose 設定。環境変数を参照して ES コンテナを起動 |
| `elasticsearch/config/elasticsearch.yml` | ES 本体の詳細設定 (bulk insert 最適化など) |
| `ddbj_search_converter/es/settings.py` | Python 側の ES 操作設定 (batch size, timeout など) |

### 1. 環境ファイル (env.*)

環境ごとの設定値を定義する。`cp env.dev .env` などで使用する環境を選択。

```bash
# === Environment ===
DDBJ_SEARCH_ENV=dev    # dev, staging, production

# === Elasticsearch Settings ===
DDBJ_SEARCH_ES_MEM_LIMIT=1g           # コンテナ全体のメモリ制限
DDBJ_SEARCH_ES_JAVA_OPTS=-Xms512m -Xmx512m  # JVM ヒープ

# === Application Settings (config.py) ===
DDBJ_SEARCH_CONVERTER_ES_URL=http://elasticsearch:9200
```

`DDBJ_SEARCH_ENV` により、コンテナ名とネットワーク名が自動決定される:

| リソース | 命名規則 |
|---------|---------|
| app コンテナ | `ddbj-search-converter-{env}` |
| ES コンテナ | `ddbj-search-es-{env}` |
| Docker network | `ddbj-search-network-{env}` |

**環境別の設定値:**

| 環境 | DDBJ_SEARCH_ES_MEM_LIMIT | DDBJ_SEARCH_ES_JAVA_OPTS |
|------|--------------------------|--------------------------|
| dev | 1g | `-Xms512m -Xmx512m` |
| staging | 128g | `-Xms31g -Xmx31g` |
| production | 128g | `-Xms64g -Xmx64g` |

### 2. Docker Compose (compose.yml)

環境変数を参照して ES コンテナを構成する。

**前提**: Docker network を事前に作成しておく（`external: true` のため）。

```bash
# Docker の場合（初回のみ、既に存在していてもエラーにならない）
docker network create ddbj-search-network-dev || true

# Podman の場合
podman network create ddbj-search-network-staging || true
```

```yaml
elasticsearch:
  image: docker.elastic.co/elasticsearch/elasticsearch:8.17.1
  container_name: ddbj-search-es-${DDBJ_SEARCH_ENV}
  environment:
    TZ: ${TZ:-Asia/Tokyo}
    discovery.type: "single-node"
    xpack.security.enabled: "false"
    bootstrap.memory_lock: "true"
    ES_JAVA_OPTS: ${DDBJ_SEARCH_ES_JAVA_OPTS}
    path.repo: "/usr/share/elasticsearch/backup"
  volumes:
    - es-data:/usr/share/elasticsearch/data
    - es-backup:/usr/share/elasticsearch/backup
```

- **volumes**: Docker named volumes (`es-data`, `es-backup`) を使用。手動でディレクトリ作成は不要
- **path.repo**: スナップショット用バックアップパス

### 3. Elasticsearch 設定 (elasticsearch/config/elasticsearch.yml)

大規模データの bulk insert に最適化した設定。compose.yml でコンテナにマウントされる。

```yaml
network.host: 0.0.0.0
http.max_content_length: 2147483647b   # 最大値 (2GB-1byte, ES の上限)
thread_pool.write.queue_size: -1       # 書き込みキュー無制限
indices.memory.index_buffer_size: 30%  # インデックスバッファ増加
path.repo: ["/usr/share/elasticsearch/backup"]
```

### 4. Python 側の ES 設定 (es/settings.py)

bulk insert やインデックス作成時の設定値。

```python
# Bulk insert 設定
BULK_INSERT_SETTINGS = {
    "batch_size": 5000,                # 1回の bulk リクエストあたりのドキュメント数
    "max_retries": 3,                  # リトライ回数
    "request_timeout": 600,            # タイムアウト (秒)
    "bulk_refresh_interval": "-1",     # bulk insert 中のリフレッシュ間隔 (無効化)
    "normal_refresh_interval": "1s",   # bulk insert 後のリフレッシュ間隔 (復元)
}

# インデックス設定
INDEX_SETTINGS = {
    "refresh_interval": "1s",                  # リフレッシュ間隔
    "mapping.nested_objects.limit": 100000,    # nested オブジェクト上限
    "number_of_shards": 1,                     # シャード数 (single-node では 1)
    "number_of_replicas": 0,                   # レプリカ数 (single-node では 0)
}
```

ES 接続先 URL は `config.py` の `get_config().es_url` で取得 (環境変数 `DDBJ_SEARCH_CONVERTER_ES_URL` から読み込み)。

## インデックス管理

### インデックス作成

```bash
# グループ単位で作成 (alias が自動設定される)
es_create_index --index bioproject
es_create_index --index biosample
es_create_index --index sra    # sra-submission, sra-study, ... が一括作成
es_create_index --index jga    # jga-study, jga-dataset, ... が一括作成

# 全インデックス作成
es_create_index --index all

# 既存インデックスをスキップ
es_create_index --index all --skip-existing
```

### インデックス一覧

```bash
es_list_indexes
```

### インデックス削除

```bash
# 確認プロンプトあり
es_delete_index --index bioproject

# 強制削除 (確認なし)
es_delete_index --index sra --force

# 存在しなくてもエラーにしない
es_delete_index --index bioproject --skip-missing
```

### Alias 構成

| Alias | 対象 Index |
|-------|-----------|
| `sra` | `sra-submission`, `sra-study`, `sra-experiment`, `sra-run`, `sra-sample`, `sra-analysis` |
| `jga` | `jga-study`, `jga-dataset`, `jga-dac`, `jga-policy` |
| `entries` | 全インデックス |

## データ投入

### bulk insert

```bash
# 通常のパイプライン (result_dir から自動検索)
es_bulk_insert --index bioproject
es_bulk_insert --index biosample
es_bulk_insert --index sra-submission
es_bulk_insert --index sra-study
es_bulk_insert --index sra-experiment
es_bulk_insert --index sra-run
es_bulk_insert --index sra-sample
es_bulk_insert --index sra-analysis
es_bulk_insert --index jga-study
es_bulk_insert --index jga-dataset
es_bulk_insert --index jga-dac
es_bulk_insert --index jga-policy

# 特定ファイルを指定
es_bulk_insert --index bioproject \
  --file ddbj_search_converter_results/regenerate/20260128/bioproject.jsonl

# バッチサイズを調整
es_bulk_insert --index bioproject --batch-size 1000

# パターンを指定（SRA の分割ファイル対応）
es_bulk_insert --index sra-run --dir ${sra_dir} --pattern '*_run_*.jsonl'
```

- `_op_type: "index"` のため既存ドキュメントは上書き (upsert 相当)
- デフォルトのバッチサイズは 5000

### blacklist 削除

blacklist ファイルに含まれる accession を Elasticsearch から削除する。

```bash
# dry-run で削除対象を確認
es_delete_blacklist --dry-run

# 特定のインデックスグループのみ
es_delete_blacklist --index jga --dry-run

# 実行 (確認なし)
es_delete_blacklist --force

# バッチサイズを調整
es_delete_blacklist --force --batch-size 500
```

**オプション:**

| オプション | 説明 |
|-----------|------|
| `--index` | インデックスグループ (`bioproject`, `biosample`, `sra`, `jga`, `all`)。デフォルト: `all` |
| `--force` | 確認なしで削除 |
| `--dry-run` | 削除せず対象を表示のみ |
| `--batch-size` | bulk delete のバッチサイズ。デフォルト: 1000 |

**挙動:**

1. 全 blacklist ファイル (`bp/blacklist.txt`, `bs/blacklist.txt`, `sra/blacklist.txt`, `jga/blacklist.txt`) を読み込む
2. 各 accession の ID パターンから対象インデックスを判定
3. bulk delete API で一括削除
4. 存在しないドキュメント (404) はエラーとせず `not_found` としてカウント

**ユースケース:**

- パイプライン実行後に blacklist に追加されたエントリを削除
- 過去にインデックスされたが、現在は非公開にすべきデータの削除

## ヘルスチェック

```bash
# 基本 (クラスタステータスのみ)
es_health_check

# 詳細表示 (ノード・インデックス統計)
es_health_check -v
```

監視項目:

- クラスタステータス (green/yellow/red)
- ノード数・データノード数
- シャード数 (active/unassigned)
- ディスク使用率・ヒープ使用率 (-v オプション)

## スナップショット管理

### ユースケース 1: 定期バックアップの設定

本番環境で毎日バックアップを取る場合の手順。

**1. リポジトリを登録する**

```bash
# path は compose.yml の path.repo と一致させる
es_snapshot repo register --name backup --path /usr/share/elasticsearch/backup
```

**2. 手動でスナップショットを作成 (動作確認)**

```bash
es_snapshot create --repo backup
# -> ddbj_search_YYYYMMDD_HHMMSS という名前で作成される
```

**3. cron で定期実行を設定**

```bash
# 毎日 2:00 AM にバックアップ、7日間保持
0 2 * * * /path/to/scripts/backup_es.sh --repo backup --retention 7 >> /var/log/es_backup.log 2>&1
```

`scripts/backup_es.sh` は以下を自動実行:

- ES クラスタのヘルス確認
- スナップショット作成 (`bioproject,biosample,sra-*,jga-*` を対象)
- 古いスナップショットの削除 (retention 日数を超えたもの)

### ユースケース 2: 別ノードへのデータ移行

staging で構築したインデックスを production に移す手順。

**staging 側 (エクスポート):**

```bash
# 1. スナップショットを作成
es_snapshot create --repo backup --snapshot migration_20260201

# 2. 作成確認
es_snapshot list --repo backup -v

# 3. スナップショットファイルを production に転送
#    (es-backup volume の実体を rsync/scp)
docker volume inspect es-backup  # マウントポイント確認
rsync -av /var/lib/docker/volumes/..._es-backup/_data/ user@production:/path/to/backup/
```

**production 側 (インポート):**

```bash
# 1. リポジトリを登録 (転送先パスを指定)
es_snapshot repo register --name migration --path /usr/share/elasticsearch/backup

# 2. スナップショット一覧を確認
es_snapshot list --repo migration

# 3. 既存インデックスを削除 (必要な場合)
es_delete_index --index all --force

# 4. スナップショットを復元
es_snapshot restore --repo migration --snapshot migration_20260201

# 5. 復元確認
es_list_indexes
```

**特定インデックスのみ移行:**

```bash
# 復元時にインデックスを指定
es_snapshot restore --repo migration --snapshot migration_20260201 \
  --indexes bioproject,biosample
```

### ユースケース 3: 障害復旧

クラスタ障害後に最新バックアップから復旧する手順。

```bash
# 1. バックアップ一覧を確認
es_snapshot list --repo backup -v

# 2. 最新のスナップショットを復元
es_snapshot restore --repo backup --snapshot ddbj_search_20260131_020000 --force

# 3. ヘルスチェック
es_health_check -v
```

### スナップショットコマンド一覧

| コマンド | 説明 |
|---------|------|
| `es_snapshot repo register --name NAME --path PATH` | リポジトリ登録 |
| `es_snapshot repo list` | リポジトリ一覧 |
| `es_snapshot repo delete --name NAME` | リポジトリ削除 |
| `es_snapshot create --repo NAME` | スナップショット作成 |
| `es_snapshot list --repo NAME [-v]` | スナップショット一覧 |
| `es_snapshot restore --repo NAME --snapshot NAME` | 復元 |
| `es_snapshot delete --repo NAME --snapshot NAME` | 削除 |
| `es_snapshot export-settings [-o FILE]` | インデックス設定エクスポート |

### 注意事項

- **path.repo の一致**: compose.yml と `repo register` で指定するパスは一致させる
- **復元前の確認**: 既存インデックスがある場合、`--force` なしでは確認プロンプトが出る
- **クラスタ状態**: red 状態では作成・復元が不完全になる可能性がある
