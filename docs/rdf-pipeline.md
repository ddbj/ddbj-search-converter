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

`scripts/run_rdf_pipeline.sh` で実行する。`run_dblink_pipeline.sh` と同じ構造で、ステップは `convert` (4 source を並列変換) → `validate` (出力 RDF の逐次検証) の 2 段。オプション・環境変数の一覧はスクリプトの `--help` (もしくは `--list-steps`) を参照する。

実装上の前提:

- `convert` は 4 つの source (bioproject, biosample, sra, sra-experiment) を並列に走らせる。`validate` はすべての convert 完了後に逐次実行する
- SRA_Accessions.tab は `--date` から最大 180 日遡って最新ファイルを自動探索する (本体パイプラインの `find_latest_sra_accessions_tab_file()` と同じロジック)
- RDF パイプライン固有の環境変数は追加しない。本体パイプラインと同じ `DDBJ_SEARCH_CONVERTER_RESULT_DIR` / `DDBJ_SEARCH_CONVERTER_DATE` を読む

## コンテナ設定

Dockerfile で GHCR の `insdc-rdf` イメージからバイナリをコピーする。

```dockerfile
COPY --from=ghcr.io/inutano/insdc-rdf:0.3.0 /usr/local/bin/insdc-rdf /usr/local/bin/insdc-rdf
```

## converter 側との語彙同期

converter (`ontology/*.ttl`) と insdc-rdf は独立パイプラインだが、同じ名前空間 URI を共有する。converter 側の語彙 (プロパティ名、列挙値) を変更した場合、insdc-rdf 側の triple 生成と値が揃わなくなるため、`insdc-rdf` バイナリバージョンを bump する際に整合性を確認する。

insdc-rdf は BioProject / BioSample / SRA / SRA Experiment の 4 source のみ対応のため、GEA / MetaboBank については converter 側の JSON-LD (`ontology/*.jsonld`) のみが RDF 公開チャネルとなる。
