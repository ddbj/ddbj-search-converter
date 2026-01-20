# テスト

## 実行方法

```bash
# 依存インストール
python3 -m pip install -e ".[tests]"

# テスト実行
python3 -m pytest

# カバレッジ付き (デフォルト)
python3 -m pytest --cov=ddbj_search_converter --cov-report=html:tests/htmlcov
```

## 開発環境 (Docker)

本番と同じパス構造で CLI コマンドをテストできる開発環境。

### セットアップ

```bash
# 1. 本番サーバで fixture 取得 (遺伝研スパコン内で実行)
./scripts/fetch_test_fixtures.sh

# 2. dev 環境起動
docker compose -f compose.dev.yml up -d

# 3. CLI コマンド実行テスト
docker compose -f compose.dev.yml exec app check_external_resources

# 4. 終了
docker compose -f compose.dev.yml down
```

### ES へのアクセス

```bash
curl http://localhost:9200
```

## Fixture データ

テスト用の小規模データセット (`tests/fixtures/`)。本番環境の volume 構造を再現。

### 取得・更新方法

本番サーバ (遺伝研スパコン内) で以下を実行:

```bash
./scripts/fetch_test_fixtures.sh
```

### ディレクトリ構造

本番環境と同じパス構造を再現:

```
tests/fixtures/
├── home/w3ddbjld/const/
│   ├── bp/blacklist.txt          # BioProject blacklist
│   ├── bs/blacklist.txt          # BioSample blacklist
│   ├── sra/blacklist.txt         # SRA blacklist
│   ├── dblink/bp_bs_preserved.tsv    # BP-BS preserved 関連
│   └── metabobank/
│       ├── mtb_id_bioproject_preserve.tsv
│       └── mtb_id_biosample_preserve.tsv
├── lustre9/open/database/ddbj-dbt/dra-private/
│   ├── mirror/SRA_Accessions/YYYY/MM/
│   │   └── SRA_Accessions.tab.YYYYMMDD  # SRA Accessions (100 rows)
│   └── tracesys/batch/logs/livelist/ReleaseData/public/
│       └── YYYYMMDD.DRA_Accessions.tab  # DRA Accessions (100 rows)
└── usr/local/
    ├── shared_data/
    │   ├── dblink/               # DBLink 生成物 (空)
    │   ├── jga/metadata-history/metadata/
    │   │   ├── jga-study.xml                   # JGA XML (10 entries each)
    │   │   ├── jga-dataset.xml
    │   │   ├── jga-dac.xml
    │   │   ├── jga-policy.xml
    │   │   ├── study.date.csv                  # date CSV (10 rows each)
    │   │   ├── dataset.date.csv
    │   │   ├── dac.date.csv
    │   │   ├── policy.date.csv
    │   │   ├── dataset-analysis-relation.csv   # relation CSV (10 rows each)
    │   │   ├── analysis-study-relation.csv
    │   │   ├── dataset-data-relation.csv
    │   │   ├── data-experiment-relation.csv
    │   │   ├── experiment-study-relation.csv
    │   │   ├── dataset-policy-relation.csv
    │   │   └── policy-dac-relation.csv
    │   └── metabobank/study/
    │       └── ...               # MetaboBank IDF/SDRF (10 each)
    └── resources/
        ├── bioproject/
        │   ├── bioproject.xml            # NCBI BioProject (10 entries)
        │   └── ddbj_core_bioproject.xml  # DDBJ BioProject (10 entries)
        ├── biosample/
        │   ├── biosample_set.xml.gz      # NCBI BioSample (10 entries, gzipped)
        │   └── ddbj_biosample_set.xml.gz # DDBJ BioSample (10 entries, gzipped)
        ├── dra/fastq/DRA000/DRA000XXX/  # DRA XML (Accessions.tab に含まれる accession)
        │   ├── *.experiment.xml
        │   ├── *.run.xml
        │   ├── *.sample.xml
        │   ├── *.study.xml
        │   └── *.submission.xml
        ├── trad/
        │   ├── wgs/WGS_ORGANISM_LIST.txt
        │   ├── tls/TLS_ORGANISM_LIST.txt
        │   ├── tsa/TSA_ORGANISM_LIST.txt
        │   └── tpa/
        │       ├── wgs/TPA_WGS_ORGANISM_LIST.txt
        │       ├── tsa/TPA_TSA_ORGANISM_LIST.txt
        │       └── tls/TPA_TLS_ORGANISM_LIST.txt
        └── gea/experiment/
            └── E-GEAD-XXX/E-GEAD-XXXX/  # GEA IDF/SDRF (10 each)
                ├── *.idf.txt
                └── *.sdrf.txt
```

