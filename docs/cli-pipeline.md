# CLI パイプライン

DDBJ Search Converter のパイプライン実行と差分更新。データ構造の詳細は [data-architecture.md](data-architecture.md) を、ES 操作・Blue-Green は [elasticsearch.md](elasticsearch.md) を参照する。

## パイプライン概要

パイプラインは 3 フェーズで構成される。

```plain
Phase 1: 前処理 + DBLink 構築
    外部リソース -> 前処理コマンド -> DBLink DB -> TSV

Phase 2: JSONL 生成
    XML + DBLink DB -> JSONL

Phase 3: ES 投入
    JSONL -> Elasticsearch
```

各 phase は明確な境界を持ち、独立して再起動・再実行できる。Phase 3 の ES 投入で失敗しても Phase 1/2 の成果は保持されるので、再投入は ES 操作だけで完結する。`run_pipeline.sh --from-step` で任意ステップから再開できる仕組みも、phase 境界がきれいだから実現できる。依存関係は一方向 (Phase 2 は Phase 1 の DBLink DB を読み、Phase 3 は Phase 2 の JSONL を読む)。

XML を直接 ES に投入せず JSONL を中間に挟むのは、(a) 差分更新の単位として JSONL ファイルが「ある日付時点で変換済みのエントリー集合」を表現できる、(b) JSONL 生成時と `es_delete_blacklist` の 2 段で blacklist を効かせられる、(c) ES 投入後の復元手段になる、(d) 人間が読める形でデバッグできる、の 4 点による。

## 一括実行

`scripts/run_pipeline.sh` で全 phase をまとめて実行する。`--list-steps` でステップ一覧、`--from-step <name>` で再開、`--dry-run` で実行内容のみ確認できる。

### Phase 1 の DuckDB 順次制約

DBLink 構築の `create_dblink_*` コマンド群は **順次実行** する必要がある。DuckDB は single-writer 制約があり、複数プロセスが同時に書き込めないため。`run_pipeline.sh` はこれを順次実行に固定している。

XML preparation (`prepare_bioproject_xml` / `prepare_biosample_xml` / `build_sra_and_dra_accessions_db`) は独立しているので並列実行する。

### Phase 2 の並列度

JSONL 生成は `--parallel-num` で各コマンド内部の並列度を指定する (デフォルト 4)。XML/IDF を batch 単位で処理するため並列化できる。`scripts/run_pipeline.sh --parallel N` で外側の並列度を指定する。

### 主要なフラグ

- `--full`: 差分判定なしの全件再生成 (初回または mapping 変更時)
- `--blue-green`: ゼロダウンタイム更新 ([elasticsearch.md § Blue-Green Alias Swap](elasticsearch.md))。`--clean-es` と排他
- `--clean-es`: ES の全 index を削除してから投入 (mapping が変わらない更新向け、bulk insert 中はダウンタイムあり)

### cron 設定例

```bash
# 毎日 AM 3:00 に差分更新
0 3 * * * /path/to/scripts/run_pipeline.sh --date $(date +\%Y\%m\%d) >> /var/log/ddbj_search_converter.log 2>&1
```

## 差分更新

### last_run.json

各 JSONL 生成コマンドが完了時に `{result_dir}/last_run.json` を更新する。`null` の場合は全件処理 (`--full` 相当)。

```json
{
  "bioproject": "2026-01-19T00:00:00Z",
  "biosample": "2026-01-19T00:00:00Z",
  "sra": "2026-01-19T00:00:00Z",
  "jga": null
}
```

### margin_days

差分判定時に安全マージン (デフォルト 30 日) を設ける。`last_run.json` のタイムスタンプから `margin_days` を引いた日時以降のデータが処理対象になる。マージンを引いているのは、外部リソース側の更新が記録された時刻と converter が処理した時刻のずれを吸収するため。

例: `last_run = 2026-01-30`、`margin_days = 30` だと `2025-12-31` 以降を処理。

### データタイプ別の差分判定基準

| データタイプ | 差分判定方法 |
|-------------|-------------|
| BioProject | XML の `date_modified` フィールド |
| BioSample | XML の `last_update` フィールド |
| SRA | Accessions.tab の `Updated` カラム |
| JGA | 常に全件処理 (`null` 固定) |
| GEA | 常に全件処理 (IDF 全走査、`last_run.json` に含めない) |
| MetaboBank | 常に全件処理 (IDF 全走査、`last_run.json` に含めない) |

JGA / GEA / MetaboBank は更新時刻フィールドがないため差分判定できない。

## Hotfix: regenerate_jsonl

特定の accession の JSONL を再生成する。bulk insert 後の 1 件パッチ用。`--type` は `bioproject` / `biosample` / `sra` / `jga`、accession は `--accessions` または `--accession-file` で指定。

出力ファイル (デフォルト `{result_dir}/regenerate/{date}/`):

| type | 出力ファイル |
|------|-------------|
| `bioproject` | `bioproject.jsonl` |
| `biosample` | `biosample.jsonl` |
| `sra` | type 別 6 ファイル (該当ありのみ生成) |
| `jga` | type 別 4 ファイル (該当ありのみ生成) |

**重要**: `regenerate_jsonl` は `last_run.json` を更新しない。次回の差分更新で同じ accession が再度処理される可能性がある。

ES への投入は `es_bulk_insert --index <name> --file <path>` で行う。SRA / JGA は entity type ごとに index が分かれるので、`--index sra-run` のように entity 別に投入する。

## メンテナンス: 古い日付ディレクトリの削除

`cleanup_old_results` で古い日付ディレクトリを削除する (デフォルト最新 3 件保持、`--keep N` で変更)。対象の親ディレクトリは以下:

- `{result_dir}/logs/{YYYYMMDD}/`
- `{result_dir}/{bioproject,biosample}/tmp_xml/{YYYYMMDD}/`
- `{result_dir}/{bioproject,biosample,sra,jga}/jsonl/{YYYYMMDD}/`
- `{result_dir}/regenerate/{YYYYMMDD}/`
- `{result_dir}/dblink/tmp/{YYYYMMDD}/`

各親ディレクトリで独立して N 件保持される。
