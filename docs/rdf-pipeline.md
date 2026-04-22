# RDF パイプライン

[insdc-rdf](https://github.com/inutano/insdc-rdf) を使って INSDC メタデータを RDF (Turtle, JSON-LD, N-Triples) に変換する独立パイプライン。

## データフロー

```plain
External Resources (既存マウント)
  bioproject.xml, biosample_set.xml.gz, SRA_Accessions.tab,
  NCBI_SRA_Metadata.tar (sync_ncbi_tar が生成)
                        |
                        v
              insdc-rdf convert (4 source)
                        |
                        v
        {result_dir}/rdf/{source}/{ttl,jsonld,nt}/
```

## 入力ファイル

既存パイプラインと同一の外部リソースを使用する。追加のマウント設定は不要。

| ソース | 入力ファイル | コンテナ内パス |
|--------|-------------|---------------|
| BioProject | `bioproject.xml` | `/usr/local/resources/bioproject/bioproject.xml` |
| BioSample | `biosample_set.xml.gz` | `/usr/local/resources/biosample/biosample_set.xml.gz` |
| SRA | `SRA_Accessions.tab` | `/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions/{YYYY}/{MM}/SRA_Accessions.tab.{YYYYMMDD}` |
| SRA Experiment | `NCBI_SRA_Metadata.tar` | `{result_dir}/sra_tar/NCBI_SRA_Metadata.tar` |

- SRA は日付ベースのパス。処理日付から 180 日遡って最新ファイルを自動探索する（既存の `find_latest_sra_accessions_tab_file()` と同じロジック）。
- SRA Experiment は `sync_ncbi_tar` が生成する非圧縮 tar を使用する。事前に `sync_ncbi_tar` を実行済みであること。

## 出力

常に最新の 1 セットのみ保持する（日付ディレクトリによるバージョン管理は行わない）。実行のたびに上書きされる。

```plain
{result_dir}/rdf/
├── bioproject/
│   ├── ttl/
│   │   ├── chunk_0000.ttl
│   │   └── ...
│   ├── jsonld/
│   │   ├── chunk_0000.jsonld
│   │   └── ...
│   ├── nt/
│   │   ├── chunk_0000.nt
│   │   └── ...
│   ├── manifest.json
│   ├── progress.json
│   └── errors.log
├── biosample/
│   └── (同上)
├── sra/
│   └── (同上)
└── sra-experiment/
    └── (同上)
```

## パイプラインスクリプト

`scripts/run_rdf_pipeline.sh` で実行する。`run_dblink_pipeline.sh` と同じ構造。

### 基本的な使い方

```bash
# 全 source を変換
./scripts/run_rdf_pipeline.sh

# 日付を指定（SRA_Accessions.tab のパス解決に使用）
./scripts/run_rdf_pipeline.sh --date 20260413

# dry-run で実行内容を確認
./scripts/run_rdf_pipeline.sh --dry-run

# 途中のステップから再開
./scripts/run_rdf_pipeline.sh --from-step validate

# ステップ一覧を表示
./scripts/run_rdf_pipeline.sh --list-steps

# チャンクサイズを変更
./scripts/run_rdf_pipeline.sh --chunk-size 500000

# validate をスキップ
./scripts/run_rdf_pipeline.sh --skip-validate
```

### ステップ

| ステップ名 | 説明 | 実行コマンド |
|-----------|------|-------------|
| `convert` | 4 source を並列変換 | `insdc-rdf convert --source {source} ...` |
| `validate` | 出力 RDF の検証 | `insdc-rdf validate ...` (4 回) |

convert ステップでは 4 つの source (bioproject, biosample, sra, sra-experiment) を並列実行する。validate は全 convert 完了後に逐次実行する。

### オプション

| オプション | 説明 |
|-----------|------|
| `--date YYYYMMDD` | 処理日付（デフォルト: 今日）。SRA_Accessions.tab のパス解決に使用 |
| `--from-step STEP` | 指定ステップから再開 |
| `--list-steps` | ステップ一覧表示 |
| `--dry-run` | 実行内容表示のみ |
| `--chunk-size N` | insdc-rdf のチャンクサイズ（デフォルト: 100000） |
| `--skip-validate` | validate ステップをスキップ |

### 環境変数

既存の環境変数を使用する。RDF パイプライン固有の環境変数は追加しない。

| 環境変数 | 説明 |
|---------|------|
| `DDBJ_SEARCH_CONVERTER_RESULT_DIR` | 出力先ディレクトリ |
| `DDBJ_SEARCH_CONVERTER_DATE` | 処理日付（`--date` で上書き可） |

## コンテナ設定

Dockerfile で GHCR の `insdc-rdf` イメージからバイナリをコピーする。

```dockerfile
COPY --from=ghcr.io/inutano/insdc-rdf:0.3.0 /usr/local/bin/insdc-rdf /usr/local/bin/insdc-rdf
```

## converter 側との語彙同期

converter (`ontology/*.ttl`) と insdc-rdf は独立パイプラインで、同じ名前空間 URI を共有する。converter 側の語彙変更時は insdc-rdf 側にも追随が必要。

- **`bioproject:Reference` → `bioproject:reference` / `bioproject:DbType` → `bioproject:dbType` rename**: converter の `Publication` 共通型を camelCase に統一したため、`ontology/bioproject.ttl` / `sra.ttl` / `jga.ttl` の語彙を rename 済。insdc-rdf 側の triple 生成は [insdc-rdf リポジトリの別 issue](https://github.com/inutano/insdc-rdf) で追随する想定。`insdc-rdf` のバイナリバージョンを bump する際に整合性を要確認。
- **`Publication.dbType` 値の小文字化 (2026-04-22)**: NCBI BP XML の `ePubmed` / `eDOI` / `ePMC` / `eNotAvailable` を converter 側で `pubmed` / `doi` / `pmc` / `None` に正規化するよう変更 (NCBI DATATOOL 由来の `e` prefix 撤廃)。`Publication.status` フィールドは廃止。insdc-rdf 側が triple 生成時に BP XML の生値をそのまま使っている場合は、converter 出力と値が揃わないので追随が必要。
- **GEA / MetaboBank の語彙新設**: `ontology/gea.ttl` / `ontology/metabobank.ttl` が新設。insdc-rdf は現状 4 source (BioProject/BioSample/SRA/SRA Experiment) のみ対応のため、GEA / MetaboBank は converter JSON-LD (`ontology/*.jsonld`) でのみ RDF 公開される。
