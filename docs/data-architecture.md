# データアーキテクチャ

DDBJ Search Converter のデータフローと構造。

## データフロー

```plain
+-----------------------------------------------------------------------------+
| External Resources                                                          |
|   BioProject XML, BioSample XML, SRA/DRA Accessions.tab, SRA/DRA XML,       |
|   JGA XML/CSV, GEA IDF/SDRF, MetaboBank IDF/SDRF,                           |
|   NCBI Assembly summary, TRAD ORGANISM_LIST, TRAD PostgreSQL (g/e/w-actual) |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 0: Pre-check                                                          |
|   check_external_resources  -- 外部マウントの存在確認、欠落で以降を abort   |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 1: DBLink Build                                                       |
|                                                                             |
|   prepare_bioproject_xml     -- XML batch split --> {result}/tmp_xml/bp/    |
|   prepare_biosample_xml      -- XML batch split --> {result}/tmp_xml/bs/    |
|   build_sra_and_dra_accessions_db ------------> {const}/sra/*.duckdb        |
|                                                                             |
|   init_dblink_db                                                            |
|   create_dblink_bp_bs_relations      -- parse XML, preserved.tsv            |
|   create_dblink_bp_relations         -- umbrella (-> umbrella.duckdb), humandbs|
|   create_dblink_assembly_and_master_relations -- fetch assembly_summary    |
|   create_dblink_gea_relations        -- parse IDF/SDRF                      |
|   create_dblink_metabobank_relations -- parse IDF/SDRF, preserved.tsv       |
|   create_dblink_jga_relations        -- parse XML/CSV, humandbs TSV         |
|   create_dblink_sra_internal_relations -- SRA internal + BP/BS <-> SRA      |
|   create_dblink_insdc_relations     -- preserved.tsv, TRAD PostgreSQL       |
|   finalize_dblink_db -----> {const}/dblink/dblink.duckdb, umbrella.duckdb   |
|   dump_dblink_files --------------> {DBLINK_PATH}/*.tsv (18 files)          |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 2: JSONL Generation                                                   |
|                                                                             |
|   build_bp_bs_date_cache   -- PostgreSQL --> {const}/bp_bs_date.duckdb      |
|   build_bp_bs_status_cache -- Livelist --> {result}/bp_bs_status.duckdb     |
|   sync_ncbi_tar            -- download/merge --> {result}/sra_tar/NCBI_SRA  |
|   sync_dra_tar             -- archive DRA XML -> {result}/sra_tar/DRA.tar   |
|                                                                             |
|   generate_bp_jsonl          -- tmp_xml + date_cache + blacklist [+ dblink] |
|   generate_bs_jsonl          -- tmp_xml + date_cache + blacklist [+ dblink] |
|   generate_sra_jsonl         -- tar + accessions_db + blacklist [+ dblink]  |
|   generate_jga_jsonl         -- XML/CSV + blacklist [+ dblink]              |
|   generate_gea_jsonl         -- IDF [+ dblink]                              |
|   generate_metabobank_jsonl  -- IDF [+ dblink]                              |
|   ※ dblink (dbXrefs) はデフォルトで含めない。--include-dbxrefs で有効化     |
|                                                                             |
|   Output: {result}/{type}/jsonl/{YYYYMMDD}/*.jsonl                          |
+-----------------------------------------------------------------------------+
                                      |
                                      v
+-----------------------------------------------------------------------------+
| Phase 3: Elasticsearch Ingestion                                            |
|                                                                             |
|   es_create_index      -- bioproject, biosample, sra, jga, gea, metabobank  |
|   es_bulk_insert       -- 14 indexes + 3 aliases (sra, jga, entries)        |
|   es_delete_blacklist  -- blacklist に含まれる doc を ES から削除           |
+-----------------------------------------------------------------------------+
```

## 外部リソース

### XML/CSV/TSV

| リソース | パス |
|---------|------|
| NCBI BioProject XML | `/usr/local/resources/bioproject/bioproject.xml` |
| DDBJ BioProject XML | `/usr/local/resources/bioproject/ddbj_core_bioproject.xml` |
| NCBI BioSample XML | `/usr/local/resources/biosample/biosample_set.xml.gz` |
| DDBJ BioSample XML | `/usr/local/resources/biosample/ddbj_biosample_set.xml.gz` |
| NCBI SRA Accessions | `/lustre9/open/database/ddbj-dbt/dra-private/mirror/SRA_Accessions/{YYYY}/{MM}/SRA_Accessions.tab.{YYYYMMDD}` |
| DRA Accessions | `/lustre9/open/database/ddbj-dbt/dra-private/tracesys/batch/logs/livelist/ReleaseData/public/{YYYYMMDD}.DRA_Accessions.tab` |
| BP Livelist | `/lustre9/open/archive/tape/ddbj-dbt/bp-collab/bioproject/{YYYYMMDD}.bioproject.ddbj.{public,suppressed,withdrawn}.txt` |
| BS Livelist | `/lustre9/open/archive/tape/ddbj-dbt/bs-collab/biosample/{YYYYMMDD}.biosample.ddbj.{public,suppressed,withdrawn}.txt` |
| DRA Metadata XML | `/usr/local/resources/dra/fastq/{DRA000}/{DRA000XXX}/{DRA000XXX}.*.xml` |
| NCBI SRA Metadata | `/lustre9/open/database/ddbj-dbt/dra-private/mirror/Metadata/Metadata/` |
| JGA XML | `/usr/local/shared_data/jga/metadata-history/metadata/jga-{study,dataset,dac,policy}.xml` |
| JGA CSV | `/usr/local/shared_data/jga/metadata-history/metadata/*.csv` |
| NCBI Assembly | `https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt` |

### IDF/SDRF

| リソース | パス |
|---------|------|
| GEA | `/usr/local/resources/gea/experiment/E-GEAD-{N000}/E-GEAD-{NNNN}/E-GEAD-{NNNN}.{idf,sdrf}.txt` |
| MetaboBank | `/usr/local/shared_data/metabobank/study/MTBKS{N}/MTBKS{N}.{idf,sdrf}.txt` |

### TRAD ORGANISM_LIST

| リソース | パス |
|---------|------|
| WGS | `/usr/local/resources/trad/wgs/WGS_ORGANISM_LIST.txt` |
| TLS | `/usr/local/resources/trad/tls/TLS_ORGANISM_LIST.txt` |
| TSA | `/usr/local/resources/trad/tsa/TSA_ORGANISM_LIST.txt` |
| TPA WGS | `/usr/local/resources/trad/tpa/wgs/TPA_WGS_ORGANISM_LIST.txt` |
| TPA TSA | `/usr/local/resources/trad/tpa/tsa/TPA_TSA_ORGANISM_LIST.txt` |
| TPA TLS | `/usr/local/resources/trad/tpa/tls/TPA_TLS_ORGANISM_LIST.txt` |

### TRAD PostgreSQL

INSDC 配列 accession と BioProject/BioSample のマッピングを保持する PostgreSQL データベース。`DDBJ_SEARCH_CONVERTER_TRAD_POSTGRES_URL` で接続先を指定する (URL に port を書いても下表の固定 port が優先される)。`create_dblink_insdc_relations` は環境変数が空のときは TRAD 抽出を skip し preserved TSV のみを使う (dev 環境向けの挙動)。

| DB 名 | ポート | 内容 |
|--------|--------|------|
| `g-actual` | 54308 | Genome、Gene などの配列データ |
| `e-actual` | 54309 | EST、TSA、TLS などの配列データ |
| `w-actual` | 54310 | WGS 由来の配列データ |

#### 接続文字列の許容 scheme

`DDBJ_SEARCH_CONVERTER_TRAD_POSTGRES_URL` / `DDBJ_SEARCH_CONVERTER_XSM_POSTGRES_URL` に渡せる scheme は以下の 3 種のみ。他は `parse_postgres_url` (`ddbj_search_converter/postgres/utils.py`) が `ValueError` を raise する。

| scheme | 用途 |
|---|---|
| `postgresql://` | 標準 (psycopg / asyncpg 共通) |
| `postgresql+psycopg://` | SQLAlchemy 風だが psycopg 直 import 経由でも accept する |
| `postgres://` | レガシー alias (libpq 互換) |

URL に scheme を書き忘れたり、`mysql://` のように他 DB の scheme を書くと early fail する設計。これは converter 起動時に「黙って localhost に fallback して接続不能を後段で出す」挙動を避けるため。

## const ディレクトリ

`{const_dir}` 以下に配置するファイル。

### Blacklist

JSONL 生成時に除外する accession のリスト。

| ファイル | 用途 |
|---------|------|
| `bp/blacklist.txt` | BioProject |
| `bs/blacklist.txt` | BioSample |
| `sra/blacklist.txt` | SRA (Submission, Study, Experiment, Run, Sample, Analysis) |
| `jga/blacklist.txt` | JGA (Study, Dataset, DAC, Policy) |

### Preserved

DBLink 構築時に追加する手動管理の関連。

| ファイル | 用途 |
|---------|------|
| `dblink/bp_bs_preserved.tsv` | BioProject - BioSample |
| `dblink/insdc_bp_preserved.tsv` | INSDC - BioProject |
| `dblink/insdc_bs_preserved.tsv` | INSDC - BioSample |
| `dblink/jga_study_hum_id.tsv` | JGA Study - humandbs |
| `dblink/jga_dataset_hum_id.tsv` | JGA Dataset - humandbs |
| `metabobank/mtb_id_bioproject_preserve.tsv` | MetaboBank - BioProject |
| `metabobank/mtb_id_biosample_preserve.tsv` | MetaboBank - BioSample |

### Blacklist と Preserved の precedence

blacklist は「公開すべきでない accession」、preserved は「元データに記載が無いが運用上必要なリンク」を表す。両方に同じ accession が含まれるとき、その accession を含む関連ペアは **blacklist を優先して除外する**。converter 内部の処理順は:

1. 元データ (XML / IDF / TSV) からペアを抽出
2. preserved ファイルのペアを **同じ集合に合流** (`bs_to_bp.update(preserved_pairs)`)
3. 合流済みの集合に対して blacklist で filter (片側でも blacklist にあれば除外)

つまり「preserved の追加」 < 「blacklist の除外」。blacklist は最終的な公開可否を絶対値で表現する設定であり、両者が衝突したときは「公開しない」側を勝たせる安全寄りの仕様。両者を排他的に使いたい (= blacklist にも preserved にも入れない) 運用にしたい場合は、設定ファイル側でレビューする。converter は重複を検出して warn することはしない。

blacklist は JSONL 生成時の除外だけでなく `es_delete_blacklist` でも参照され、過去にインデックスされていたが後から blacklist に追加された ES doc を削除する経路がある。

## 中間データベース

converter は途中段階の状態を複数の DuckDB に保持する。主要な DB の設計判断は以下の通り。

- **DBLink DB を ES と分ける**: 1 BioProject エントリーが数千〜数千万件の関連 ID を持つことがあるため、ES の `nested` フィールドで持たせるとインデックスサイズが膨大になり検索負荷も悪化する。逆引き (関連 ID → 元エントリー) も必要なので、ES とは独立した DuckDB に正規化して持たせる
- **Umbrella DB を DBLink DB と分ける**: umbrella の親子関係は方向のある関係なので、対称性を仮定する DBLink の半辺化スキーマと相性が悪い。有向 edge として独立 DB に持つことで、1 child が複数 parent を持つ DAG (約 6,700 件) を自然に表現でき、半辺化スキーマに方向情報を埋め込まずに済む
- **Date Cache / Status Cache を事前構築する**: 日付・status は外部 (XSM PostgreSQL、Livelist) から取得する。JSONL 生成時に毎回問い合わせると数百万エントリーで N+1 クエリ問題が発生し、外部リソースの負荷とネットワーク往復で生成時間が大きく伸びる。DuckDB に bulk fetch で集約し、JSONL 生成本体を外部リソースから切り離す

### ID 変換マッピング

NCBI XML から抽出した数字 ID -> accession のマッピング。SRA Accessions.tab や TRAD ORGANISM_LIST で数字 ID が使われている場合に変換するために使用。

| ファイル | 用途 |
|---------|------|
| `{result_dir}/bp_id_to_accession.tsv` | BioProject 数字 ID -> accession |
| `{result_dir}/bs_id_to_accession.tsv` | BioSample 数字 ID -> accession |

`create_dblink_bp_bs_relations` で生成され、`create_dblink_assembly_and_master_relations` や `create_dblink_sra_internal_relations` で使用される。

### Accessions DB

SRA/DRA の Accessions.tab を DuckDB にロードしたもの。

| ファイル | 用途 |
|---------|------|
| `{const_dir}/sra/sra_accessions.duckdb` | NCBI SRA |
| `{const_dir}/sra/dra_accessions.duckdb` | DRA |

### Metadata tar

SRA/DRA の Metadata XML をまとめた tar ファイル。`generate_sra_jsonl` で使用。
SSD 上の `{result_dir}/sra_tar/` に配置してランダムアクセスを高速化。

| ファイル | 用途 |
|---------|------|
| `{result_dir}/sra_tar/NCBI_SRA_Metadata.tar` | NCBI SRA (sync_ncbi_tar で作成) |
| `{result_dir}/sra_tar/DRA_Metadata.tar` | DRA (sync_dra_tar で作成) |
| `{result_dir}/sra_tar/*.tar.index.pkl` | tar インデックスキャッシュ (並列読み込み高速化) |

NCBI SRA Metadata には DDBJ origin の SRA accession (DRA / DRR / DRX / DRZ / DRS / DRP) も含まれている。これらは DRA バッチ (`source="dra"`) 側が DDBJ 公開ストレージへの XML / FASTQ / SRA distribution link を含む完全な doc を生成するため、NCBI バッチ (`source="sra"`) の JSONL では除外する (`ddbj_search_converter/jsonl/sra.py::process_submission_xml` で skip)。両バッチが同じ identifier を JSONL に出すと `es_bulk_insert` が `_id` で上書きするため (順序により後勝ち)、NCBI 版の不完全 doc が ES に残る原因になる。

NCBI バッチで処理される NCBI/EBI-origin entry には ddbj.nig.ac.jp ミラーへの XML / SRA distribution link を付与する (FASTQ は自極のみ)。テンプレート詳細は [§Distribution URL スキーム](#distribution-url-スキーム) を参照。

### DBLink DB

accession 間の関連を格納する DuckDB。

- `{const_dir}/dblink/dblink.duckdb`

```sql
CREATE TABLE dbxref (
    accession_type TEXT,     -- このエントリー側の AccessionType
    accession TEXT,
    linked_type TEXT,        -- 隣接する AccessionType
    linked_accession TEXT
);
-- 物理 sort: ORDER BY accession_type, accession, linked_type, linked_accession
-- index: idx_dbxref_accession (accession_type, accession)
--        idx_dbxref_unique    (accession_type, accession, linked_type, linked_accession) UNIQUE
```

**半辺化スキーマ (half-edge)**。無向 edge `{A, B}` は `dbxref` に 2 行として保存される (`A→B` と `B→A`)。これにより `WHERE accession_type=? AND accession=?` の単一 WHERE だけで両 endpoint の隣接を取得でき、DuckDB の zone map が常に効く (point lookup でも SEQ_SCAN にならない)。UNION ALL による逆方向検索が不要になる。

ストレージは canonical 形の約 2 倍になるが、`normalize_edge` によって TSV 段階では `(A, B)` 1 行で済む (A ≤ B 正規化)。DB 構築時に `build_dbxref_table` が `UNION ALL` で両方向を mirror する。

#### 中間 table: `raw_edges`

DBLink 構築中の一時テーブル。各 `create_dblink_*` コマンドが canonical edge をここに append し、`finalize_dblink_db` で `dbxref` に変換した後 `DROP TABLE raw_edges` される。

```sql
CREATE TABLE raw_edges (
    src_type TEXT,
    src_accession TEXT,
    dst_type TEXT,
    dst_accession TEXT
);
-- normalize_edge() により (src_type, src_accession) <= (dst_type, dst_accession) の canonical 形で挿入される
```

#### finalize_dblink_db の内部処理

`finalize_dblink_db` は以下を順に実行する:

1. `build_dbxref_table`: `raw_edges` を UNION ALL で両方向に mirror し、`SELECT DISTINCT ... ORDER BY accession_type, accession, linked_type, linked_accession` で `dbxref` を構築
2. `create_dbxref_indexes`: `idx_dbxref_accession` と `idx_dbxref_unique` を作成
3. `DROP TABLE raw_edges`
4. tmp DB から final DB へ atomic replace

#### 無向 edge 数の算出 (`show_dblink_counts` が内部で使う集計)

`dbxref` は 1 つの無向 edge を 2 行で持つため、単純な GROUP BY だと件数が 2 倍になる。無向 edge 数を出すときは canonical にまとめて COUNT/2 する:

```sql
SELECT
    LEAST(accession_type, linked_type) AS type_a,
    GREATEST(accession_type, linked_type) AS type_b,
    COUNT(*) / 2 AS edge_count
FROM dbxref
GROUP BY type_a, type_b
ORDER BY type_a, type_b;
```

self-pair (同一 type 同士の関連、例えば `(bioproject, bioproject)`) も、`LEAST`/`GREATEST` で同じキーに落ちて COUNT=2 → /2 で 1 edge となり整合する。

### Umbrella DB

BioProject の umbrella 階層構造（親子関連）を有向グラフとして管理する DuckDB。

- `{const_dir}/dblink/umbrella.duckdb`

```sql
CREATE TABLE umbrella_relation (
    parent_accession TEXT NOT NULL,  -- 親 BioProject (umbrella)
    child_accession TEXT NOT NULL    -- 子 BioProject
)
```

- 有向グラフ: `parent_accession` → `child_accession` の方向が明確
- 1 つの child が複数の parent を持つ DAG（有向非巡回グラフ）構造に対応
- `init_dblink_db` で初期化、`create_dblink_bp_relations` でデータ挿入、`finalize_dblink_db` で確定
- JSONL 生成時に `parentBioProjects` / `childBioProjects` フィールドを設定するために使用

**階層構造の特性:**

- 木構造ではなく DAG。1 つの child が複数の parent を持つケースが約 6,700 件ある
- 最大深度は 5 だが、99.6% は depth 1（umbrella → leaf のみ）
- XML に `ProjectTypeTopAdmin` がないが TopAdmin Link で子を持つ BioProject が 138 件存在する。これらは `objectType=BioProject` のまま `childBioProjects` にデータが入る

### Date Cache DB

PostgreSQL から取得した BioProject/BioSample の日付情報をキャッシュした DuckDB。

- `{result_dir}/bp_bs_date.duckdb`

JSONL 生成時に `dateCreated`, `dateModified`, `datePublished` を付与するために使用。

### Status Cache DB

Livelist ファイルから取得した BioProject/BioSample の status 情報をキャッシュした DuckDB。

- `{result_dir}/bp_bs_status.duckdb`

JSONL 生成時に `status` (`public` / `private` / `suppressed` / `withdrawn` の 4 値) を付与するために使用。
キャッシュに accession が存在しない場合は、XML から取得した値 (デフォルト "public") をそのまま使用する。BP/BS の Livelist 由来は実態として 3 値 (`public` / `suppressed` / `withdrawn`) のみ。SRA は `unpublished -> private` 等の正規化 (`ddbj_search_converter/jsonl/sra.py::_normalize_status`) で 4 値が出る。JGA / GEA / MetaboBank は `public` 固定 (`accessibility` で controlled-access / public-access を区別)。

#### SRA Accessions: 同 accession の status 重複時の優先順位

SRA Accessions.tab に **同一 accession の行が複数の status で出現する** ケースがある (mirror 同期遅延、submission 履歴の重複登録など)。`ddbj_search_converter/sra_accessions_tab.py::get_accession_info_bulk` は以下の status priority で 1 つに決める:

| priority | status | 意味 |
|---|---|---|
| 0 (最強) | `live` | 公開済 (Accessions.tab で `Status=live`) |
| 1 | `public` | 公開済 (一部 livelist では `public`) |
| 2 | `suppressed` | DDBJ/NCBI 側で表示停止 |
| 3 (最弱) | `withdrawn` | 取り下げ |

priority が小さい (= 強い) status が勝つ。tie のときは tab の登場順 (先勝ち) で確定する。

`live` > `public` の順位は、Accessions.tab が「live」を使い、Livelist の一部が「public」を使う表記揺れに対応するため (実態は同等の公開状態を指す)。`suppressed` / `withdrawn` を残しても運用上は問題ないが、検索 UX 上は `live` が見える方が望ましいので強くしている。

### DRA ファイルインデックス DB

DRA の FASTQ ディレクトリと SRA ファイルの存在情報を格納した DuckDB。

- `{result_dir}/sra_tar/dra_file_index.duckdb`

```sql
CREATE TABLE dra_fastq_dir (submission TEXT NOT NULL, experiment TEXT NOT NULL);
CREATE TABLE dra_sra_file (run TEXT NOT NULL);
```

`sync_dra_tar` / `build_dra_tar` の末尾で構築される。JSONL 生成時に DRA エントリーの distribution（FASTQ / SRA ダウンロードリンク）の有無を判定するために使用。

## AccessionType 一覧

DBLink では以下の 21 種類の accession タイプを管理する。

| AccessionType | 例 |
|--------------|-----|
| `bioproject` | PRJDB12345, PRJNA123456 |
| `biosample` | SAMD00000001, SAMN12345678 |
| `sra-submission` | DRA000001, SRA000001 |
| `sra-study` | DRP000001, SRP000001 |
| `sra-experiment` | DRX000001, SRX000001 |
| `sra-run` | DRR000001, SRR000001 |
| `sra-sample` | DRS000001, SRS000001 |
| `sra-analysis` | DRZ000001, SRZ000001 |
| `jga-study` | JGAS000001 |
| `jga-dataset` | JGAD000001 |
| `jga-dac` | JGAC000001 |
| `jga-policy` | JGAP000001 |
| `gea` | E-GEAD-1 |
| `metabobank` | MTBKS1 |
| `insdc` | AB000001, CP035466 |
| `insdc-assembly` | GCA_000000001.1 |
| `insdc-master` | ABCD00000000 |
| `humandbs` | hum0001 |
| `pubmed` | 12345678 |
| `geo` | GSE12345 |
| `taxonomy` | 9606 |

## Publication フィールド

各 entry の `publication[].dbType` は以下の 4 値および `None` のいずれかを取る。スキーマ定義は [`schema.py`](../ddbj_search_converter/schema.py) の `PublicationDbType`。

| dbType | URL テンプレート | 主な出現元 |
|---|---|---|
| `pubmed` | `https://pubmed.ncbi.nlm.nih.gov/{id}/` | BioProject XML, JGA XML, SRA, IDF (GEA / MetaboBank) |
| `doi` | `https://doi.org/{doi}` (id が `http(s)://...` で始まるときはそのまま) | BioProject XML, JGA XML, IDF |
| `pmc` | `https://www.ncbi.nlm.nih.gov/pmc/articles/{PMC...}/` (id が `PMC` 始まりのときのみ URL 生成) | BioProject XML |
| `other` | URL 生成なし | 上記いずれにも当てはまらないが id だけは保持するケース |
| `None` | URL 生成なし | dbType 不明 (`eNotAvailable` や未知文字列) |

上流 (BioProject XML / JGA XML / IDF) の `DbType` 表記揺れは [`jsonl/utils.py`](../ddbj_search_converter/jsonl/utils.py) の `normalize_publication_dbtype` で吸収する。`.strip().lower()` 後に lookup するため `pubmed` / `ePubmed` / `PUBMED` は同一視され、`eDOI` → `doi`, `ePMC` → `pmc`, `eNotAvailable` → `None` に正規化される。URL 生成は `build_pubmed_url` / `build_doi_url` を共通 helper として使う。

BioProject XML 固有の挙動として以下 2 種類の fallback がある:

- `DbType` フィールドに数字 PMID が直書きされるケース (例: `DbType="12345678"`) は `pubmed` にフォールバックし URL を構築する
- `DbType="ePMC"` だが id が `10.xxx/...` の DOI 形式のときは `dbType` を `doi` に倒して URL も DOI 側で生成する (id/dbType/url の整合維持)

## DBLink TSV 出力（18 種類）

`{DBLINK_PATH}/` 以下に出力される。relation を表す 2 カラムの TSV。

| ファイル | 関連 |
|---------|------|
| `assembly_genome-bp/assembly_genome2bp.tsv` | insdc-assembly - bioproject |
| `assembly_genome-bs/assembly_genome2bs.tsv` | insdc-assembly - biosample |
| `assembly_genome-insdc/assembly_genome2insdc.tsv` | insdc-assembly - insdc-master |
| `insdc_master-bioproject/insdc_master2bioproject.tsv` | insdc-master - bioproject |
| `insdc_master-biosample/insdc_master2biosample.tsv` | insdc-master - biosample |
| `biosample-bioproject/biosample2bioproject.tsv` | biosample - bioproject |
| `bioproject-biosample/bioproject2biosample.tsv` | bioproject - biosample |
| `bioproject-humID/bioproject2humID.tsv` | bioproject - humandbs |
| `gea-bioproject/gea2bioproject.tsv` | gea - bioproject |
| `gea-biosample/gea2biosample.tsv` | gea - biosample |
| `mtb2bp/mtb_id_bioproject.tsv` | metabobank - bioproject |
| `mtb2bs/mtb_id_biosample.tsv` | metabobank - biosample |
| `jga_study-humID/jga_study2humID.tsv` | jga-study - humandbs |
| `jga_dataset-humID/jga_dataset2humID.tsv` | jga-dataset - humandbs |
| `jga_study-pubmed_id/jga_study2pubmed_id.tsv` | jga-study - pubmed |
| `jga_study-jga_dataset/jga_study2jga_dataset.tsv` | jga-study - jga-dataset |
| `insdc-bioproject/insdc2bioproject.tsv` | insdc - bioproject |
| `insdc-biosample/insdc2biosample.tsv` | insdc - biosample |

umbrella 関連は Umbrella DB (`umbrella.duckdb`) で管理し、TSV エクスポートは行わない。

## JSONL 出力

`{result_dir}/{type}/jsonl/{YYYYMMDD}/` 以下に出力される。

### 配列フィールドの契約

各 entry の以下のリスト系フィールドは **空でも JSON key として必ず出力される**。これは ddbj-search-api 側の OpenAPI で `*DetailResponse` の配列フィールドを required として扱うための SSOT 側の契約で、[schema.py](../ddbj_search_converter/schema.py) で型レベル必須化されており、JSONL 書き出し (`ddbj_search_converter/jsonl/utils.py` の `write_jsonl`) も `model_dump_json(by_alias=True)` のみで `exclude_none` / `exclude_unset` / `exclude_defaults` を一切使わない。

| Entry | 必須リストフィールド |
|---|---|
| BioProject | distribution, projectType, relevance, organization, publication, grant, externalLink, dbXrefs, parentBioProjects, childBioProjects, sameAs |
| BioSample | distribution, derivedFrom, organization, model, dbXrefs, sameAs |
| SRA | distribution, organization, publication, libraryStrategy, librarySource, librarySelection, instrumentModel, derivedFrom, dbXrefs, sameAs |
| JGA | distribution, organization, publication, grant, externalLink, studyType, datasetType, vendor, dbXrefs, sameAs |
| GEA | distribution, organization, publication, experimentType, dbXrefs, sameAs |
| MetaboBank | distribution, organization, publication, studyType, experimentType, submissionType, dbXrefs, sameAs |

### scalar フィールドの契約

各 entry の以下 scalar 系フィールドは入力データ (XML / IDF / TSV) のタグ不在や cache miss などで欠落しうるため、[schema.py](../ddbj_search_converter/schema.py) で型レベル `field: T | None = None` (optional) として宣言する。JSONL 書き出しは `model_dump_json(by_alias=True)` のみで `exclude_none` / `exclude_unset` / `exclude_defaults` を使わないため、値が None でも JSON key は `null` として保持される (ES bulk insert 後の `_source` も同様)。配列フィールドの「空配列でも key を出す」契約と対称な「null 値でも key を出す」契約。

| Entry | optional scalar フィールド |
|---|---|
| BioProject | name, organism, title, description, dateCreated, dateModified, datePublished |
| BioSample | name, organism, title, description, geoLocName, collectionDate, host, strain, isolate, dateCreated, dateModified, datePublished |
| SRA | name, organism, title, description, libraryName, libraryConstructionProtocol, collectionDate, geoLocName, dateCreated, dateModified, datePublished |
| JGA | name, organism, title, description, dateCreated, dateModified, datePublished |
| GEA | name, organism, title, description, dateCreated, dateModified, datePublished |
| MetaboBank | name, organism, title, description, dateCreated, dateModified, datePublished |

ES mapping (`ddbj_search_converter/es/mappings/`) は scalar に `null_value` を設定していないため、null 値はインデックス対象外 (検索ヒットしないだけで mapping は変更不要)。ddbj-search-api が継承する OpenAPI スキーマ上は対象 scalar が `nullable: true` の non-required として表現される。

### BioProject / BioSample

| ファイルパターン | ES Index |
|-----------------|----------|
| `ddbj_*.jsonl`, `ncbi_*.jsonl` | `bioproject` |
| `ddbj_*.jsonl`, `ncbi_*.jsonl` | `biosample` |

XML ファイル単位で分割されたファイルが出力される。

BioProject エントリーは umbrella 階層構造に対応しており、`parentBioProjects` / `childBioProjects` フィールドで直接の親子関係（推移的閉包ではない）を保持する。これらのフィールドの値は Umbrella DB から取得される。Xref の type は `"bioproject"` で統一し、フィールド名自体が方向を示す。umbrella 関連は `dbXrefs` には含めない。

BioProject の `relevance` は元 XML が 7 子要素 (Agricultural / Medical / Industrial / Environmental / Evolution / ModelOrganism / Other) で構成され、各タグの `"yes"` / `"no"` 値ではなく **`"yes"` だったタグ名の配列** として格納する。フロント側でファセット値として使いやすくするためのスキーマ整形で、生 XML の構造とは異なる。

BioSample の `derivedFrom` は NCBI の自由文埋め込みと DDBJ のカンマ区切りの両表記から BioSample ID を統一抽出する。一方 `isolate` は `strain` と意味的に区別される個別分離株識別子で、両者を別フィールドとして並べて持つ。詳細は [schema.py](../ddbj_search_converter/schema.py) を参照。

抽出に使う regex は [`id_patterns.py`](../ddbj_search_converter/id_patterns.py) で用途別に 2 種類を分けている。`ID_PATTERN_MAP["biosample"]` (`^SAM[NED](\w)?\d+\Z`) は文字列全体の validation 用で NCBI BioSample の拡張 char (`SAMD0...` 等の 1 文字 prefix) を `\w?` で許容する。`BIOSAMPLE_ID_FINDALL_RE` (`SAM[NDE]\d+`) は anchorless で、`derivedFrom` の自由文/カンマ区切りから ID 部分のみを `findall` で抽出する。後者は拡張 char を意図的に含めないことで、NCBI 由来の自由文中に紛れる類似文字列を誤抽出しない設計。

### SRA

| ファイルパターン | ES Index |
|-----------------|----------|
| `{dra,ncbi}_submission_{NNNN}.jsonl` | `sra-submission` |
| `{dra,ncbi}_study_{NNNN}.jsonl` | `sra-study` |
| `{dra,ncbi}_experiment_{NNNN}.jsonl` | `sra-experiment` |
| `{dra,ncbi}_run_{NNNN}.jsonl` | `sra-run` |
| `{dra,ncbi}_sample_{NNNN}.jsonl` | `sra-sample` |
| `{dra,ncbi}_analysis_{NNNN}.jsonl` | `sra-analysis` |

並列処理のためバッチ単位（5000 submissions/batch）で分割されたファイルが出力される。
`es_bulk_insert` では `--pattern` オプションで対象ファイルを絞り込む。

SRA は entity 別 (submission / sample / experiment) で独自フィールドの追加先が異なる。sra-submission の `Organization.department` には `lab_name` が center Organization にのみ付与され、broker には付けない (broker は提出経路の中継役で実際の研究室ではないため)。sra-sample の `derivedFrom` は BioSample と同じ ID 抽出ロジックを通すが nested Xref 構造で持つ。具体的な抽出元タグは [schema.py](../ddbj_search_converter/schema.py) と `ddbj_search_converter/jsonl/sra.py` を参照。

### JGA

| ファイルパターン | ES Index |
|-----------------|----------|
| `jga-study.jsonl` | `jga-study` |
| `jga-dataset.jsonl` | `jga-dataset` |
| `jga-dac.jsonl` | `jga-dac` |
| `jga-policy.jsonl` | `jga-policy` |

JGA エントリーが `sameAs`（SECONDARY_ID）を持つ場合、ES bulk insert 時に Secondary ID を `_id` とするエイリアスドキュメントを同一インデックスに追加投入する。エイリアスドキュメントの `_source` は Primary ドキュメントと同一（`identifier` は Primary ID のまま）。これにより Secondary ID でも API からエントリーを取得できる。ただしプレフィックス（英字部分）が Primary ID と異なる Secondary ID（例: `AGDD_000001`）は除外する。

alias ドキュメントを投入する理由は、API 側で Secondary ID を直打ちされても `_id` lookup で取得できるようにするため。本来は ES の nested query (`sameAs.identifier == ?`) でフォールバックできるが、nested query は expensive で、マッピング不在のインデックスへのクエリで 500 が返るリスクもある。alias ドキュメントを Primary と同じインデックスに投入することで、API は受信時に `_source.identifier` をリクエスト ID と照合して Primary ID を検出する形になる。

### GEA

| ファイルパターン | ES Index |
|-----------------|----------|
| `gea.jsonl` | `gea` |

全 IDF ファイルを 1 本の JSONL に集約して出力する。単一 index 構成（Microarray / Sequencing の区別は `experimentType` 値で表現）。

### MetaboBank

| ファイルパターン | ES Index |
|-----------------|----------|
| `metabobank.jsonl` | `metabobank` |

全 IDF ファイルを 1 本の JSONL に集約して出力する。`{accession}.idf.txt` が欠損しているディレクトリはログ出力のうえ除外する。

### properties フィールド

各エントリーの `properties` フィールドには、元の XML を dict に変換した構造がそのまま格納される。基本は xmltodict の標準挙動（単一子要素は dict、複数子要素は list、テキストノード・XML の attribute 値・None はスカラー）。

**例外（Attribute 要素の正規化）**: XML 要素名に `Attribute`（例: `Attribute`, `STUDY_ATTRIBUTE`）を含むフィールドは、要素が 1 件でも必ず配列として格納する。これは、同一フィールドが 1 件のときに dict、複数件のときに list となる不整合を避けるための正規化。XML の属性（attribute）値とは別の概念なので注意。

対象 index とパス:

| Index | 配列化されるパス |
|---|---|
| `biosample` | `properties.BioSample.Attributes.Attribute` |
| `sra-study` | `properties.STUDY_SET.STUDY.STUDY_ATTRIBUTES.STUDY_ATTRIBUTE` |
| `sra-experiment` | `properties.EXPERIMENT_SET.EXPERIMENT.EXPERIMENT_ATTRIBUTES.EXPERIMENT_ATTRIBUTE` |
| `sra-run` | `properties.RUN_SET.RUN.RUN_ATTRIBUTES.RUN_ATTRIBUTE` |
| `sra-sample` | `properties.SAMPLE_SET.SAMPLE.SAMPLE_ATTRIBUTES.SAMPLE_ATTRIBUTE` |
| `sra-analysis` | `properties.ANALYSIS_SET.ANALYSIS.ANALYSIS_ATTRIBUTES.ANALYSIS_ATTRIBUTE` |
| `jga-study` | `properties.STUDY_ATTRIBUTES.STUDY_ATTRIBUTE` |

`bioproject` / `sra-submission` / `jga-dataset` / `jga-dac` / `jga-policy` は対象外（XML に Attribute 要素なし）。

例 1: JGA Study で `STUDY_ATTRIBUTE` が 1 件のみでも配列で格納される。

```json
{
  "properties": {
    "DESCRIPTOR": {
      "STUDY_TITLE": "Example study"
    },
    "STUDY_ATTRIBUTES": {
      "STUDY_ATTRIBUTE": [
        {"TAG": "NBDC Number", "VALUE": "hum0018"}
      ]
    }
  }
}
```

例 2: BioSample で `Attribute` が 1 件のみでも配列で格納される。親の `Attributes` は dict のまま。

```json
{
  "properties": {
    "BioSample": {
      "Description": {"Title": "Example sample"},
      "Attributes": {
        "Attribute": [
          {"attribute_name": "host", "content": "Homo sapiens"}
        ]
      }
    }
  }
}
```

### Distribution

各エントリーの `distribution` フィールドに格納するダウンロードリンク（Schema.org `DataDownload`）。

| エントリータイプ | JSON | JSON-LD | XML | FASTQ | SRA |
|-----------------|------|---------|-----|-------|-----|
| BioProject | o | o | - | - | - |
| BioSample | o | o | - | - | - |
| SRA (DRA-origin) 全般 | o | o | o | - | - |
| SRA (DRA-origin) sra-run | o | o | o | o\* | o\* |
| SRA (NCBI/EBI-origin) 全般 | o | o | o | - | - |
| SRA (NCBI/EBI-origin) sra-run | o | o | o | - | o\*\* |
| JGA | o | o | - | - | - |
| GEA | o | o | - | - | - |
| MetaboBank | o | o | - | - | - |

`*` = DRA ファイルインデックス DB にファイルが存在する場合のみ
`**` = ddbj.nig.ac.jp ミラーへの URL を機械生成 (実在チェックなし)。ミラー欠落により 404 になりうる

### Distribution URL スキーム

SRA distribution の `contentUrl` は accession の origin と sra_type に応じて以下のテンプレートで生成される。ベース URL は `DRA_PUBLIC_BASE_URL = "https://ddbj.nig.ac.jp/public/ddbj_database/dra"` (`ddbj_search_converter/config.py`)。

| 種類 | テンプレート | 適用範囲 | 実在チェック |
|---|---|---|---|
| XML | `{base}/fastq/{submission[:6]}/{submission}/{submission}.{sra_type}.xml` | 全 origin | なし |
| FASTQ (run) | `{base}/fastq/{submission[:6]}/{submission}/{experiment}/` | DRA-origin のみ | `dra_fastq_dir` |
| SRA (DRA-origin run) | `{base}/sra/ByExp/sra/DRX/{experiment[:6]}/{experiment}/{run}/{run}.sra` | DRA-origin のみ | `dra_sra_file` |
| SRA (他極 run) | `{base}/sralite/ByExp/litesra/{experiment[:3]}/{experiment[:6]}/{experiment}/{run}/{run}.sra` | NCBI/EBI-origin | なし |

XML はミラー側に tar と同期して配置されるため、entry が作られる sra_type なら概ね 200 OK で取得できる (tar 同期間隔のずれ分は 404 になりうる)。

NCBI/EBI-origin の SRA path には `sralite/litesra` の文字列が入るが、配信されるファイルは通常の `.sra` バイナリ (SRA-Lite ではない)。ミラー欠落分は実在チェックを行わず URL を機械生成し、ダウンロード時の 404 は容認する。`experiment[:3]` は `SRX` (NCBI) または `ERX` (EBI)。

FASTQ は他極ミラーが存在しないため、自極 (DRA-origin) でのみ生成する。

## isPartOf / type フィールド

各エントリーには「どの DB に属するか」を示す `isPartOf` と、エントリータイプを示す `type` の 2 つのカテゴリ系フィールドがある。役割が異なるので使い分ける:

- `isPartOf`: index 粒度の粗カテゴリ。DB 切替ファセット (front の Database ToggleButton) で利用する
- `type`: 細分カテゴリ。SRA / JGA では submission / study / experiment などのエントリータイプを区別する

| Index | `isPartOf` | `type` |
|---|---|---|
| BioProject | `"bioproject"` | `"bioproject"` (加えて `objectType` で `UmbrellaBioProject` / `BioProject` を区別) |
| BioSample | `"biosample"` | `"biosample"` |
| SRA | `"sra"` | `"sra-submission"` / `"sra-study"` / `"sra-experiment"` / `"sra-run"` / `"sra-sample"` / `"sra-analysis"` |
| JGA | `"jga"` | `"jga-study"` / `"jga-dataset"` / `"jga-dac"` / `"jga-policy"` |
| GEA | `"gea"` | `"gea"` |
| MetaboBank | `"metabobank"` | `"metabobank"` |

値は全 index で snake_case に統一している。ES の mapping は `isPartOf` を `keyword` 型として定義する (`ddbj_search_converter/es/mappings/common.py`)。

## Elasticsearch インデックス構成

converter は 14 個の物理インデックスと 3 段の alias を管理する。

### 14 物理インデックス

| 物理 Index | グループ |
|---|---|
| `bioproject` | (singleton) |
| `biosample` | (singleton) |
| `sra-submission` / `sra-study` / `sra-experiment` / `sra-run` / `sra-sample` / `sra-analysis` | `sra` |
| `jga-study` / `jga-dataset` / `jga-dac` / `jga-policy` | `jga` |
| `gea` | (singleton) |
| `metabobank` | (singleton) |

### Alias 構成

| Alias | 対象 |
|---|---|
| `sra` | sra-* 6 indexes |
| `jga` | jga-* 4 indexes |
| `entries` | 全 14 indexes |

API 側はこれらの alias を経由してアクセスする。Blue-Green 適用後は `{name}` (例: `bioproject`) も alias になり、物理 index は `{name}-{YYYYMMDD}` の dated 形式になる (詳細は [elasticsearch.md § Blue-Green Alias Swap](elasticsearch.md))。
