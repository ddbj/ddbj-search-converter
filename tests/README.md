# テスト

## テスト方針

- **TDD/PBT**: テストはバグを探すために書く。通るだけの場当たり的なテストは書かない
- **Property-Based Testing**: [hypothesis](https://hypothesis.readthedocs.io/) を使い、ランダム入力で不変条件を検証する
- **エッジケースの網羅**: 境界値、None、空文字列、異常系を必ずテストする
- **既知バグの文書化**: 発見済みバグは `xfail` やコメントでテスト内に文書化する

## テストの種類と使い分け

| 種類 | 用途 | 例 |
|------|------|-----|
| `@pytest.mark.parametrize` | 具体的な入出力の検証 | valid/invalid なアクセッション ID |
| `@given(...)` (hypothesis) | 不変条件の検証 | `normalize_edge(a,b) == normalize_edge(b,a)` |
| 通常テスト | エッジケース、統合テスト | 空ファイル、None 入力 |

## 実行方法

```bash
# 依存インストール (uv)
uv sync --extra tests

# テスト実行
uv run pytest -v

# カバレッジ付き (デフォルト設定)
uv run pytest --cov-report=term-missing

# hypothesis 統計表示
uv run pytest --hypothesis-show-statistics

# リント
uv run pylint ./ddbj_search_converter
uv run mypy ./ddbj_search_converter
uv run isort ./ddbj_search_converter
```

## ディレクトリ構成

```
tests/
    README.md                   # 本ファイル
    fixtures/                   # テスト用小規模データセット
    py_tests/
        conftest.py             # 共有 fixture (test_config, clean_ctx)
        strategies.py           # hypothesis カスタム strategies
        test_id_patterns.py     # id_patterns モジュール
        test_xml_utils.py       # xml_utils モジュール
        dblink/
            test_utils.py       # dblink.utils モジュール
            test_db.py          # dblink.db モジュール
            test_assembly_and_master.py
            test_bp_bs.py
        jsonl/
            test_utils.py       # jsonl.utils モジュール
            test_bp.py          # jsonl.bp モジュール
            test_bs.py          # jsonl.bs モジュール
            test_jga.py         # jsonl.jga モジュール
            test_sra.py         # jsonl.sra モジュール
            test_regenerate.py  # jsonl.regenerate モジュール
        archive/                # 旧テスト (pyproject.toml で除外)
```

## archive ディレクトリ

`tests/py_tests/archive/` には旧テストを保存している。`pyproject.toml` の `addopts` で `--ignore=tests/py_tests/archive` を指定しており、`pytest` 実行時には自動的に除外される。

## 開発環境 (Docker)

本番と同じパス構造で CLI コマンドをテストできる開発環境。

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
│   ├── bp/blacklist.txt
│   ├── bs/blacklist.txt
│   ├── sra/blacklist.txt
│   ├── dblink/bp_bs_preserved.tsv
│   └── metabobank/
│       ├── mtb_id_bioproject_preserve.tsv
│       └── mtb_id_biosample_preserve.tsv
├── lustre9/open/database/ddbj-dbt/dra-private/
│   ├── mirror/SRA_Accessions/YYYY/MM/
│   │   └── SRA_Accessions.tab.YYYYMMDD
│   └── tracesys/batch/logs/livelist/ReleaseData/public/
│       └── YYYYMMDD.DRA_Accessions.tab
└── usr/local/
    ├── shared_data/
    │   ├── dblink/
    │   ├── jga/metadata-history/metadata/
    │   │   ├── jga-study.xml, jga-dataset.xml, jga-dac.xml, jga-policy.xml
    │   │   ├── study.date.csv, dataset.date.csv, dac.date.csv, policy.date.csv
    │   │   └── (relation CSV files)
    │   └── metabobank/study/
    └── resources/
        ├── bioproject/
        ├── biosample/
        ├── dra/fastq/
        ├── trad/
        └── gea/experiment/
```
