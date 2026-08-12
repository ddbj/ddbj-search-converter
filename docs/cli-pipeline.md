# CLI パイプライン

DDBJ Search Converter のパイプライン実行と差分更新。データ構造の詳細は [data-architecture.md](data-architecture.md) を、ES 操作・Blue-Green は [elasticsearch.md](elasticsearch.md) を参照する。

## パイプライン概要

パイプラインは Phase 0 / Phase 1 / Phase 2 / Phase 3 の 4 段で構成される。Phase 0 は外部リソースの存在チェックだけの軽量段で、後続 phase の前提条件を満たしているかを最初に確認する。

```plain
Phase 0: Pre-check
    check_external_resources (外部マウントの存在確認、欠落なら以降を abort)

Phase 1: 前処理 + DBLink 構築
    外部リソース -> 前処理コマンド -> DBLink DB -> TSV

Phase 2: JSONL 生成
    XML + DBLink DB + Date/Status Cache -> JSONL

Phase 3: ES 投入
    JSONL -> Elasticsearch
```

各 phase は明確な境界を持ち、独立して再起動・再実行できる。Phase 0 は副作用がなく失敗時も状態を残さず、Phase 3 の ES 投入で失敗しても Phase 1/2 の成果は保持されるので、再投入は ES 操作だけで完結する。`run_pipeline.sh --from-step` で任意ステップから再開できる仕組みも、phase 境界がきれいだから実現できる。依存関係は一方向 (Phase 2 は Phase 1 の DBLink DB を読み、Phase 3 は Phase 2 の JSONL を読む)。

XML を直接 ES に投入せず JSONL を中間に挟むのは、(a) 差分更新の単位として JSONL ファイルが「ある日付時点で変換済みのエントリー集合」を表現できる、(b) JSONL 生成時と `es_delete_blacklist` の 2 段で blacklist を効かせられる、(c) ES 投入後の復元手段になる、(d) 人間が読める形でデバッグできる、の 4 点による。

## 一括実行

`scripts/run_pipeline.sh` で全 phase をまとめて実行する。`--list-steps` でステップ一覧、`--from-step <name>` で再開、`--dry-run` で実行内容のみ確認できる。

### Phase 1 の DuckDB 順次制約

DBLink 構築の `create_dblink_*` コマンド群は **順次実行** する必要がある。DuckDB は single-writer 制約があり、複数プロセスが同時に書き込めないため。`run_pipeline.sh` はこれを順次実行に固定している。

XML preparation (`prepare_bioproject_xml` / `prepare_biosample_xml` / `build_sra_and_dra_accessions_db`) は独立しているので並列実行する。

### Phase 2 の並列度

JSONL 生成は `--parallel-num` で **各コマンド内部の worker 数** を指定する (CLI 単体起動時のデフォルトは `generate_bp_jsonl` / `generate_bs_jsonl` が 64、`generate_sra_jsonl` が 8)。XML/IDF を batch 単位で処理するため並列化できる。`generate_jga_jsonl` / `generate_gea_jsonl` / `generate_metabobank_jsonl` は内部並列を持たず `--parallel-num` を受け付けない。

`scripts/run_pipeline.sh --parallel N` は内部で **bp/bs/sra の各 jsonl コマンドにのみ** `--parallel-num N` として伝播する (jga/gea/metabobank には引数を渡さない、jsonl コマンド自体は順次実行)。デフォルトは 16 で、production の Rundeck job (`scripts/rundeck-job.yaml`) もこの値で運用している。

`generate_bp_jsonl` / `generate_bs_jsonl` には `--resume` フラグがあり、出力先に同名 JSONL が既に存在するファイル (XML 単位) はスキップする。`run_pipeline.sh` は bp/bs にこのフラグを常に渡し、途中で失敗したときに再実行で続きから処理できるようにしている。`generate_sra_jsonl` / `generate_jga_jsonl` には `--resume` がなく、`generate_sra_jsonl` の途中再開は `--from-step jsonl_sra` 等で粗く戻すことになる。

### 主要なフラグ

- `--full`: 差分判定なしの全件再生成 (初回または mapping 変更時)。JSONL 生成に加えて Date Cache DB の全件再構築も行う
- `--blue-green`: ゼロダウンタイム更新 ([elasticsearch.md § Blue-Green Alias Swap](elasticsearch.md))。`--clean-es` と排他
- `--clean-es`: ES の全 index を削除してから投入 (mapping が変わらない更新向け、bulk insert 中はダウンタイムあり)

production の日次運用は Rundeck (`scripts/rundeck-job.yaml`) で `run_pipeline.sh --parallel 16` を実行する。詳細は [deployment.md](deployment.md)。

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

時間窓は「データが変わったときに更新日時も動く」ことを前提にする。この前提が成り立たない入力があり、そこでは窓を広げても取りこぼしは解消しない (下記「SRA の差分判定」)。

### データタイプ別の差分判定基準

| データタイプ | 差分判定方法 |
|-------------|-------------|
| BioProject / BioSample (DDBJ) | [Date Cache DB](data-architecture.md) の `date_modified` を範囲検索し、処理対象の accession 集合を得る |
| BioProject / BioSample (NCBI) | XML から取り出した更新日を worker 内で `since` と比較する |
| SRA / DRA | Accessions.tab の `Updated` と `Published` (下記「SRA の差分判定」) |
| JGA | 常に全件処理 (`null` 固定) |
| GEA | 常に全件処理 (IDF 全走査、`last_run.json` に含めない) |
| MetaboBank | 常に全件処理 (IDF 全走査、`last_run.json` に含めない) |

JGA / GEA / MetaboBank は更新時刻フィールドがないため差分判定できない。

DDBJ 分の差分判定は Date Cache DB に依存する。したがって Date Cache DB の `date_modified` が実際の更新日とずれていると、そのエントリーは差分更新から漏れて ES に反映されない。`build_bp_bs_date_cache` が `generate_bp_jsonl` / `generate_bs_jsonl` より前に完了している必要があり、Date Cache DB がない状態で JSONL 生成を実行するとエラーで停止する。

### SRA の差分判定

`sync_dra_tar` (DRA XML を tar に取り込む) と `generate_sra_jsonl` (JSONL を作る) は、Accessions.tab の行から対象 submission を同じ条件で選ぶ。tar に XML が無い submission は JSONL を作れないので、両者の条件がずれると tar 側の取りこぼしがそのまま ES の欠落になる。

対象は次を満たす行の `Submission` 列 (重複排除):

```plain
Updated >= cutoff OR (Published >= cutoff AND Published <= 実行時刻)
```

3 つの制約があり、いずれも入力データの性質から来る。外すと取りこぼす。

- **`Type='SUBMISSION'` の行だけを見ない**: submission 配下の run / experiment だけが更新され、SUBMISSION 行の日付が動かないケースがある
- **`Updated` だけを見ない**: DRA_Accessions.tab の `Updated` はメタデータの更新日時で、公開解除では動かない。公開遅延 (embargo) 明けのデータは `Published` だけが動くため、`Updated` だけでは窓をどれだけ広げても対象にならない
- **`Published` の未来日付を除外する**: NCBI SRA_Accessions.tab の `Published` には公開予定日が入る。除外しないと差分対象がおよそ倍に膨らむ

DRA_Accessions.tab は公開分のみのリストなので、embargo 中の submission はそもそも載らず、公開日に初めて現れる。新規に現れた submission は `Published` が実行時刻以前かつ窓の内側に必ず入るため、この条件で捕捉できる。

### Date Cache DB の更新範囲

Date Cache DB 自身も差分で構築されるが、その範囲は `last_run.json` ではなく DB 内の `cache_meta.watermark` で管理する。両者は意味が違う (前者は「JSONL をどこまで出力したか」、後者は「cache がどこまで取り込み済みか」) ので別々に持つ。watermark を DB 本体と同じファイルに置くことで、両者が食い違った状態になり得ないようにしている。詳細は [data-architecture.md](data-architecture.md) を参照。

## メンテナンス: DRA tar の取りこぼし回収

`sync_dra_tar --repair` で、DRA_Accessions.tab にあって `DRA_Metadata.tar` に無い submission の XML を追記する。日次パイプラインには含めない。

差分同期は Accessions.tab の日付列に依存するので、日付が動かないまま状態が変わった submission を取りこぼすことがある。tar は追記でしか育たないため、取りこぼしは以後の同期でも回収されず累積する。集合差分で埋めるのがこのコマンドの役割。

- `dra_last_updated.txt` は進めない。通常の差分同期の起点を動かすと、修復ついでに未処理の期間を飛ばしてしまう
- DRA ファイルインデックスは作り直さない。tar とは独立に全 submission を走査しているので、tar に入っていない submission の情報も既に持っている
- `--force-rebuild` とは排他。作り直しは全 submission の XML を lustre から再収集するので桁違いに重い

tar を直しただけでは ES に反映されない。続けて `regenerate_jsonl --type sra` (下記) で JSONL を作って投入する。

## Hotfix: regenerate_jsonl

特定の accession の JSONL を再生成する。bulk insert 後の 1 件パッチ用。`--type` は `bioproject` / `biosample` / `sra` / `jga`、accession は `--accessions` または `--accession-file` で指定。

出力ファイル (デフォルト `{result_dir}/regenerate/{date}/`):

| type | 出力ファイル |
|------|-------------|
| `bioproject` | `bioproject.jsonl` |
| `biosample` | `biosample.jsonl` |
| `sra` | `submission.jsonl` / `study.jsonl` / `experiment.jsonl` / `run.jsonl` / `sample.jsonl` / `analysis.jsonl` (該当ありのみ生成) |
| `jga` | `jga-study.jsonl` / `jga-dataset.jsonl` / `jga-dac.jsonl` / `jga-policy.jsonl` (該当ありのみ生成) |

SRA の通常パイプラインの命名 (`{dra,ncbi}_{type}_{NNNN}.jsonl`) とは異なる点に注意。`es_bulk_insert` で投入する際は entity ごとに `--index` と `--file` を明示する。

**重要**: `regenerate_jsonl` は `last_run.json` を更新しない。次回の差分更新で同じ accession が再度処理される可能性がある。

ES への投入は `es_bulk_insert --index <name> --file <path>` で行う。SRA / JGA は entity type ごとに index が分かれるので、`--index sra-run` のように entity 別に投入する。

## メンテナンス: 古い日付ディレクトリの削除

`cleanup_old_results` で古い日付ディレクトリを削除する (デフォルト最新 3 件保持、`--keep N` で変更)。対象の親ディレクトリは以下:

- `{result_dir}/logs/{YYYYMMDD}/`
- `{result_dir}/{bioproject,biosample}/tmp_xml/{YYYYMMDD}/`
- `{result_dir}/{bioproject,biosample,sra,jga,gea,metabobank}/jsonl/{YYYYMMDD}/`
- `{result_dir}/regenerate/{YYYYMMDD}/`
- `{result_dir}/dblink/tmp/{YYYYMMDD}/`

各親ディレクトリで独立して N 件保持される。

### DuckDB の spill ディレクトリ削除

`--include-spill` で `{result_dir}/dblink/duckdb_tmp/{YYYYMMDD}/` 配下を **`--keep` を無視して全件削除** する (DuckDB の一時的な spill 用ディレクトリ、保持しても意味がないため)。デフォルトでは触らない。pipeline 実行中は当日分の spill を作っているので、`run_pipeline.sh` の合間に呼ぶか、明らかにアイドル状態のときだけ実行する。
