# Integration テストシナリオ

実 Elasticsearch / 実 PostgreSQL (TRAD・XSM) / 本番想定パスの fixture に対する E2E 検証シナリオの一覧。各シナリオは「これが落ちたらどんなバグが検出されたことになるか」に答えられる粒度で書く。

このファイルは **シナリオ列挙の SSOT**。具体的なコードは `tests/py_tests/integration/test_*.py` にあり、運用上の注意 (環境変数・fixture 戦略・件数 drift 対策) は [integration-note.md](integration-note.md) を参照する。

## ID 体系

`IT-{機能}-{連番 2 桁}` の形式で振る。例: `IT-DBLINK-01`, `IT-JSONL-03`, `IT-BLUEGREEN-12`。

- 機能ごとに連番をリセット
- 削除したシナリオの ID は **再利用しない** (履歴互換性)
- 機能名は固定リスト (下記カテゴリ): `RESOURCE`, `DBLINK`, `UMBRELLA`, `JSONL`, `DIFF`, `ES`, `BLUEGREEN`, `RDF`, `LOG`, `PIPELINE`

## シナリオテンプレート

各カテゴリ見出しは `### IT-{CATEGORY}-*:` (h3)、各シナリオは `#### IT-{CATEGORY}-NN:` (h4) で書く。件数の実測値は書かない (drift で壊れるため、構造的不変条件で書く)。`docs/data-architecture.md` 等の固定値が必要なときは「現状 docs に記載」と明示する。

```markdown
#### IT-XXX-NN: <短いタイトル>

**対象**: CLI コマンド / モジュール / パイプライン段階

**前提**: 必要な fixture / 環境変数 / 事前ステップ

**不変条件**:
- 構造的に守るべき条件 1
- 構造的に守るべき条件 2

**回帰元**: なぜこのテストが必要か (どのバグ・どの仕様章が背景)。コミット SHA または docs/ の節を引用

**関連 unit テスト**: SSOT としての unit ファイル + クラス名 (例: `tests/py_tests/dblink/test_db.py::TestFinalizeDblinkDb`)。unit が SSOT としてカバーする範囲を明示し、integration では unit で見られない部分 (実 ES / 実 PostgreSQL / 実 fixture round-trip) に絞る。
```

## カテゴリ別シナリオ

### IT-RESOURCE-*: 外部リソース・接続性

`check_external_resources` と外部 I/O の存在確認。

#### IT-RESOURCE-01: 本番想定パスの fixture が揃っている

**対象**: `cli/check_external_resources.py::main` + `tests/fixtures/` 配下の本番想定 volume 構造

**前提**:
- `tests/fixtures/` を `const_dir` / `result_dir` として `Config` を組み立てる
- `SRA_Accessions.tab` / `DRA_Accessions.tab` は日付サブディレクトリに配置し、`find_latest_*_accessions_tab_file()` が解決できる構造

**不変条件**:
- `get_required_files(const_dir)` の戻り値で `path is not None and path.exists()` が全件 True
- 上記が成り立つとき `main()` が `Exception` を raise しない
- Livelist (`BP_LIVELIST_BASE_PATH` / `BS_LIVELIST_BASE_PATH`) 配下に最新の `find_latest_livelist_date()` が日付を返せる構造で配置されている (status_cache の前提)
- IDF / SDRF (`GEA_BASE_PATH` / `METABOBANK_BASE_PATH` 配下) も jsonl / dblink から参照可能なパスで存在

**回帰元**: `b562f01` (deployment / development / integration ガイド追加) で「pipeline 開始前の前提条件確認」を SSOT 化。`b7566b2` (18 dblink TSV) で必要 fixture が増えるたびに整合が崩れた経験。

**関連 unit テスト**: なし (本番想定の fixture 構造そのものを検証するため unit には載らない)

#### IT-RESOURCE-02: TRAD PostgreSQL (g-actual / e-actual / w-actual) 接続性

**対象**: `dblink/insdc.py::_fetch_from_db` (`TRAD_DBS = [("g-actual", 54308), ("e-actual", 54309), ("w-actual", 54310)]`)

**前提**:
- `DDBJ_SEARCH_CONVERTER_TRAD_POSTGRES_URL` が staging 以上の TRAD ホストに到達
- TCP keepalive (`keepalives=1, idle=30, interval=10, count=5`)

**不変条件**:
- 3 DB すべてに `psycopg2.connect()` が成功する
- 各 DB で `accession`, `link_pr_ac`, `project` テーブルが存在し、`INSDC_TO_BP_QUERY` / `INSDC_TO_BS_QUERY` を `LIMIT 1` で SELECT 可能
- 接続失敗時は `MAX_RETRIES=3` 回までリトライ、最終失敗で raise

**回帰元**: `35ce294` (INSDC sequence accession from TRAD)、`83208a8` (retry logic)、`8a048db` (TCP keepalive)、`c80527a` (TRAD host a011 → a012)

**関連 unit テスト**: `tests/py_tests/dblink/test_insdc.py::TestWriteInsdcRelations` (psycopg2 を mock)

#### IT-RESOURCE-03: XSM PostgreSQL (bioproject / biosample) 接続性

**対象**: `postgres/bp_date.py`、`postgres/bs_date.py`、`postgres/utils.py::postgres_connection`

**前提**:
- `DDBJ_SEARCH_CONVERTER_XSM_POSTGRES_URL` が staging 以上の XSM ホストに到達
- bioproject / biosample DB が両方存在

**不変条件**:
- `mass.bioproject_summary`, `mass.project`, `mass.biosample_summary`, `mass.sample` テーブルが存在
- `fetch_bp_dates_bulk(config, [<実在 accession>])` が ISO 8601 文字列を含むタプルを返す
- `fetch_bs_dates_bulk` も同様
- `fetch_bp_accessions_modified_since(config, <staging 内 timestamp>)` が `set` を返す (件数 0 でも OK)

**回帰元**: `8a048db` (TCP keepalive + materialize fetch results)、`6bf545b` (TIMESTAMPTZ 化で XSM 側のタイムゾーン扱いが変化)

**関連 unit テスト**: `tests/py_tests/postgres/test_utils.py::*` (`parse_postgres_url` / `format_date`)。`bp_date` / `bs_date` の SQL 整形は unit 未整備のため本シナリオでカバー。

#### IT-RESOURCE-04: NCBI Assembly summary の HTTP ストリーミング

**対象**: `dblink/assembly_and_master.py::_stream_assembly_summary` (`ASSEMBLY_SUMMARY_URL = "https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt"`)

**前提**:
- 外向き HTTPS 到達性 (NCBI FTP)

**不変条件**:
- `process_assembly_summary_file(...)` が例外を raise せずに完了する
- 一時的な切断 (`urllib`/`httpx` 系 transport error) を simulate しても、関数内固定の `max_retries = 3` (`assembly_and_master.py:100`) のリトライで成功する
- 1 件以上の assembly accession が `raw_edges` に積まれる (`count > 0`)
- リトライ間隔のバックオフ (`8f635c9` で導入) が指数的に延びる

**回帰元**: `8f635c9` (Add retry with backoff for NCBI FTP streaming)

**関連 unit テスト**: なし (実 HTTP に依存)

#### IT-RESOURCE-05: リソース欠落時の警告と例外

**対象**: `cli/check_external_resources.py::main`

**前提**:
- `tests/fixtures/` の一部のファイルを意図的に削除した状態の `const_dir`

**不変条件**:
- 欠落ファイル名と path が `log_warn` で JSONL ログに記録される
- 1 件以上の missing がある場合、最後に `Exception(f"{n} required file(s) are missing.")` が raise される
- raise の結果 `run_logger` が `lifecycle="failed"` の CRITICAL record を残し、`show_log_summary` が `FAILED` と判定する (`show_log_summary.py:95-99`、IT-LOG-04 と整合)

**回帰元**: 直接の bug fix はないが、CLAUDE.md / development.md で「pipeline 開始前の前提確認」を必須化したため、欠落検出の動作保証が必要。

**関連 unit テスト**: なし

### IT-DBLINK-*: DBLink DB 構築

`init_dblink_db` から `finalize_dblink_db` までのフロー、半辺化スキーマの不変条件。

#### IT-DBLINK-01: 各 create_dblink_* CLI が canonical 形で raw_edges に insert

**対象**: `dblink/{assembly_and_master, bioproject, bp_bs, gea, idf_sdrf, insdc, jga, metabobank, sra_internal}.py::main`、`dblink/db.py::normalize_edge`

**前提**:
- `init_dblink_db` 実行直後の空 `dblink.tmp.duckdb` (`raw_edges` テーブルのみ)
- 本番想定 fixture (XML / TSV / IDF / SDRF / preserved / blacklist)

**不変条件**:
- 各 CLI 完了後、`raw_edges` の任意の行 `(src_type, src_acc, dst_type, dst_acc)` が `(src_type, src_acc) <= (dst_type, dst_acc)` の canonical 形を満たす
- `raw_edges` の distinct な canonical `(src_type, dst_type)` ペアが `EXPORT_RELATIONS` 全 18 種をカバーする (umbrella 関係は raw_edges に乗らず `umbrella_relation` テーブル側、IT-UMBRELLA-01 と整合)
- 同一 edge を異なる向きで複数 source から登録しても、最終 `dbxref` で重複なし (`build_dbxref_table` の DISTINCT が効く)

**回帰元**: `f279f2f` (半辺化 refactor で canonical 化)、`4bac02d` (ORDER BY for deterministic output)、`d350082` (chunk size monkeypatch で速度化)

**関連 unit テスト**: `tests/py_tests/dblink/test_db.py::TestNormalizeEdge`, `TestNormalizeEdgePBT`, `TestWriteEdgesToTsv`, `TestLoadEdgesFromTsv`

#### IT-DBLINK-02: build_dbxref_table で半辺化と対称性が成立

**対象**: `dblink/db.py::build_dbxref_table`、`finalize_dblink_db`

**前提**: IT-DBLINK-01 の完了状態 (raw_edges に edges あり)

**不変条件**:
- 任意の `(type_a, type_b)` (異なる type) について、`SELECT count(*) FROM dbxref WHERE accession_type=type_a AND linked_type=type_b` と `SELECT count(*) FROM dbxref WHERE accession_type=type_b AND linked_type=type_a` が一致
- `(type_a, type_b)` の `(accession, linked_accession)` 集合と、`(type_b, type_a)` を `(linked_accession, accession)` に swap した集合が一致
- 任意の row について、(linked_type, linked_accession, accession_type, accession) を swap した行も `dbxref` に存在

**回帰元**: `f279f2f` (半辺化 refactor の中核)

**関連 unit テスト**: `tests/py_tests/dblink/test_db.py::TestBuildDbxrefTable`, `TestFinalizeDblinkDb`

#### IT-DBLINK-03: UNIQUE 制約 + ORDER BY で物理 sort 済み

**対象**: `dblink/db.py::create_dbxref_indexes`

**前提**: IT-DBLINK-02 の完了状態 (dbxref 構築済み)

**不変条件**:
- `CREATE UNIQUE INDEX idx_dbxref_unique ON dbxref (accession_type, accession, linked_type, linked_accession)` が成功 (重複なし)
- `dbxref` テーブルが `CREATE TABLE AS ... ORDER BY accession_type, accession, linked_type, linked_accession` で構築されているため、後段の `SELECT * FROM dbxref` (ORDER BY なし) も実装上 sort 順で返る (DuckDB の仕様保証ではないため、本シナリオは「ORDER BY 句で明示 sort した結果と insert 時の sort key が一致する」を assert)
- `idx_dbxref_accession` の prefix lookup (`WHERE accession_type=? AND accession=?`) が両端点の隣接を返す (半辺化により両方向の隣接が同じ prefix で取得可能)

**回帰元**: `4bac02d` (ORDER BY for deterministic output)、`f279f2f` (UNIQUE INDEX 設計)

**関連 unit テスト**: `tests/py_tests/dblink/test_db.py::TestBuildDbxrefTable`, `TestGetLinkedEntities`, `TestGetLinkedEntitiesBulk`

#### IT-DBLINK-04: raw_edges が finalize 後に DROP されている

**対象**: `dblink/db.py::build_dbxref_table` (末尾の `conn.execute("DROP TABLE raw_edges")`)

**前提**: `finalize_dblink_db` 実行直後の `dblink.duckdb`

**不変条件**:
- `dblink.duckdb` 内に `raw_edges` テーブルが存在しない (`SHOW TABLES` に出ない)
- `dbxref` テーブルのみ存在
- `idx_dbxref_unique`, `idx_dbxref_accession` の 2 index が存在

**回帰元**: `f279f2f` (raw_edges 中間テーブル化)

**関連 unit テスト**: `tests/py_tests/dblink/test_db.py::TestFinalizeDblinkDb`

#### IT-DBLINK-05: tmp → final atomic replace

**対象**: `dblink/db.py::finalize_dblink_db` (`tmp_path.replace(final_path)`)

**前提**:
- 既存の `dblink.duckdb` (旧版) が存在する状態
- 新規 `dblink.tmp.duckdb` を構築

**不変条件**:
- `finalize_dblink_db` 実行中、`dblink.duckdb` が中途半端な状態に置き換わらない (`Path.replace` は同一 FS 内で atomic rename)
- 完了後、`dblink.duckdb` の inode が新規 (旧版の inode と異なる)
- 旧 `dblink.duckdb` を read open しているプロセスは旧 inode を保持し続ける (replace 中も検索断ゼロ)

**回帰元**: `3d0d1be` (Replace shutil.move with os.replace for atomic file swap)、`6fd5cc2` (Path.replace 化)

**関連 unit テスト**: `tests/py_tests/dblink/test_db.py::TestFinalizeDblinkDb`

#### IT-DBLINK-06: blacklist 適用で両端のいずれかが blacklist の edge は除外

**対象**: `dblink/utils.py::filter_pairs_by_blacklist`、各 `dblink/*.py::main`

**前提**:
- `bp/blacklist.txt` / `bs/blacklist.txt` / `sra/blacklist.txt` / `jga/blacklist.txt` に検証用 accession を追加した fixture
- 該当 accession を含む edge が raw_edges に積まれた状態

**不変条件**:
- 完了後の `dbxref` に blacklist 該当 accession を含む行が存在しない (`accession_type=? AND accession=?` でも `linked_type=? AND linked_accession=?` でも 0 件)
- blacklist 該当しない edge は影響を受けない
- blacklist の trailing whitespace やコメント行 (`#` 始まり) は無視される

**回帰元**: `2154204` (preserved TSV 経由でも blacklist 適用される)、`2bf235a` (Org dedup) で blacklist と Organization の整合確認

**関連 unit テスト**: `tests/py_tests/dblink/test_utils.py::TestFilterByBlacklist`, `TestFilterPairsByBlacklist`, `TestFilterSraPairsByBlacklist`, `TestLoadBlacklist`, `TestLoadSraBlacklist`, `TestBug4TrailingWhitespace`, `TestBug5CommentLines`

#### IT-DBLINK-07: preserved TSV の関連が DBLink に追加される

**対象**: `dblink/insdc.py::_load_insdc_preserved_file`、`dblink/metabobank.py::main` (`MTB_BP_PRESERVED_REL_PATH` / `MTB_BS_PRESERVED_REL_PATH` 読み出し)、`dblink/bp_bs.py` (BP-BS preserved)、`config.py::INSDC_BP_PRESERVED_REL_PATH`, `INSDC_BS_PRESERVED_REL_PATH`, `MTB_BP_PRESERVED_REL_PATH`, `MTB_BS_PRESERVED_REL_PATH`, `BP_BS_PRESERVED_REL_PATH`

**前提**:
- 各 preserved TSV に検証用 (insdc_acc, target_acc) ペアを追加した fixture
- `trad_postgres_url` 未設定でも preserved 部分のみ動作する

**不変条件**:
- preserved TSV の各 (insdc_acc, target_acc) が `dbxref` に半辺化形で 2 行 (`accession_type=insdc, accession=insdc_acc, linked_type=bioproject` と逆向き) として存在
- target_acc がフォーマット不正 (`is_valid_accession(target_acc, dst_type) is False`) の場合は読み込まれない (DEBUG ログに `INVALID_ACCESSION_ID`)
- TSV 行が 2 列未満 (`len(parts) < 2`) や空行はスキップされる

**回帰元**: `2154204` (Add preserved TSV support for INSDC-BioProject and INSDC-BioSample)、`2ff0474` (input validation hardening for preserved TSVs)

**関連 unit テスト**: `tests/py_tests/dblink/test_insdc.py::TestLoadInsdcPreservedFile`

#### IT-DBLINK-08: show_dblink_counts の COUNT/2 が無向 edge 数と一致

**対象**: `cli/debug/show_dblink_counts.py::get_edge_counts` (LEAST/GREATEST + COUNT/2 集計)

**前提**: `finalize_dblink_db` 完了後の `dblink.duckdb`

**不変条件**:
- `get_edge_counts` の SQL は `LEAST(accession_type, linked_type) AS type_a` / `GREATEST(...) AS type_b` で canonical 化したうえで `COUNT(*) / 2` を取る。各行の `count` が、`dbxref` から直接 `SELECT count(*) WHERE accession_type=type_a AND linked_type=type_b` で取得した値と一致 (半辺化のため整数で割り切れる)
- 同一 type ペア (`type_a=type_b`) のケースでは `LEAST=GREATEST` で 1 行に集約され、自己ループの 1 edge が 1 行で表現される (現状そのケースは存在しないが将来 dblink に同 type 関係が追加された場合の挙動)
- 出力 JSON が `[{"type_a", "type_b", "count"}, ...]` 形式で count 降順

**回帰元**: `f279f2f` (show_dblink_counts を半辺化前提に書き換え)、`b7566b2` (18 dblink TSV)

**関連 unit テスト**: なし (実 DB 依存のため unit 未整備)

### IT-UMBRELLA-*: Umbrella DB

BioProject の親子 DAG。

#### IT-UMBRELLA-01: TopAdmin Link を持つ BioProject から umbrella_relation に有向 edge insert

**対象**: `dblink/bioproject.py::process_bioproject_xml_file` (`current_link.get("type") == "TopAdmin"` 判定)、`dblink/db.py::save_umbrella_relations`, `init_umbrella_db`, `finalize_umbrella_db`

**前提**:
- BioProject XML fixture に `<Link><Hierarchical type="TopAdmin">` を持つエントリーが存在
- `init_umbrella_db` 実行直後の空 `umbrella.tmp.duckdb`

**不変条件**:
- `umbrella_relation` テーブルに `(parent_accession, child_accession)` が登録される (NOT NULL 制約クリア)
- TopSingle (`<Hierarchical type="TopSingle">`) は登録されない (同一プロジェクトの別 ID なので親子ではない)
- `finalize_umbrella_db` 後に重複が排除され (`umbrella_relation_dedup` rename)、`idx_umbrella_parent`、`idx_umbrella_child` の 2 index が張られる

**回帰元**: `6dda94c` (Separate umbrella BioProject into dedicated DB)、`deb4166` (parent / child BioProject relationship properties to ontology)

**関連 unit テスト**: `tests/py_tests/dblink/test_db.py::TestUmbrellaDb`, `tests/py_tests/dblink/test_bioproject.py::TestProcessBioprojectXmlFile`

#### IT-UMBRELLA-02: 1 child が複数 parent を持つ DAG (multi-parent)

**対象**: `dblink/db.py::umbrella_relation` テーブル設計 (UNIQUE 制約なし、(parent, child) ペアで dedup のみ)

**前提**: 同じ child を持つ 2 つ以上の parent を含む BioProject XML fixture

**不変条件**:
- 同じ `child_accession` に対して複数の `parent_accession` 行が存在する
- `get_umbrella_parent_child_maps([child_accession])` の `parent_map[child_accession]` の長さ ≥ 2
- 各 parent も umbrella DB の lookup で逆方向取得できる (`child_map[parent_accession]` に child を含む)

**回帰元**: `6dda94c` (umbrella DB 設計、DAG 表現)、`docs/data-architecture.md` `## Umbrella DB` 節 (約 6,700 件の親子関連で multi-parent 構造を含む DAG を表現)

**関連 unit テスト**: `tests/py_tests/dblink/test_db.py::TestUmbrellaDb`

#### IT-UMBRELLA-03: depth 1 の典型構造で親子マップが正しく返る

**対象**: `dblink/db.py::get_umbrella_parent_child_maps`

**前提**: depth 1 (umbrella → leaf 1 階層) の BioProject 階層 fixture

**不変条件**:
- `parent_map[leaf_accession] == [umbrella_accession]`
- `child_map[umbrella_accession]` が leaf_accession を含む
- 推移的閉包は計算されない (depth 1 のままの直接親子のみ)
- `docs/data-architecture.md` の「99.6% は depth 1」の典型ケースを満たす

**回帰元**: `docs/data-architecture.md` `## Umbrella DB` 節の不変条件、`6dda94c`

**関連 unit テスト**: `tests/py_tests/dblink/test_db.py::TestUmbrellaDb`

#### IT-UMBRELLA-04: 最大深度 5 の境界

**対象**: `dblink/db.py::get_umbrella_parent_child_maps` の「直接の親子のみ返す」性質

**前提**: depth 5 の BioProject 階層 fixture (最大深度の境界)

**不変条件**:
- 最深 leaf に対して `parent_map[leaf]` が直近の親 1 件のみ (推移的閉包になっていない)
- 最上位 umbrella に対して `child_map[umbrella]` が直近の子のみ (再帰的に子孫を含まない)
- depth 6 以上の階層が staging データに発生していない (将来 6 段に増えたら本シナリオを再評価)

**回帰元**: `docs/data-architecture.md` `## Umbrella DB` 節の「最大深度は 5」の不変条件

**関連 unit テスト**: なし (実階層構造を要するため unit 未整備)

#### IT-UMBRELLA-05: objectType=BioProject のまま childBioProjects を持つ TopAdmin Link

**対象**: `jsonl/bp.py` の BioProject JSONL 生成、`get_umbrella_parent_child_maps` を介した `childBioProjects` フィールド

**前提**:
- `umbrella.duckdb` finalize 済み
- BioProject XML fixture に `<ProjectTypeTopAdmin>` がない `objectType=BioProject` だが TopAdmin Link で子を持つエントリー (件数 drift に弱いので「1 件以上」で assert、`docs/data-architecture.md` では 138 件と記載)

**不変条件**:
- 該当 BP の JSONL の `objectType` が `"BioProject"` のまま (`"BioProject Umbrella"` に変わらない)
- 該当 BP の `childBioProjects` が空でない (umbrella DB から展開される)
- `parentBioProjects` が空でない子 BP も同様に存在する

**回帰元**: `docs/data-architecture.md` `## Umbrella DB` 節の「`objectType=BioProject` のまま `childBioProjects` にデータが入る」不変条件、`6dda94c`、`deb4166`

**関連 unit テスト**: `tests/py_tests/jsonl/test_bp.py` (実 umbrella DB を要するため unit 単独では再現不可)

### IT-JSONL-*: JSONL 生成

XML / IDF / SDRF → Pydantic モデル → JSONL の変換。unit が個別 parser を網羅しているため、integration では「実 PostgreSQL/実 cache DB/実 fixture を要する E2E round-trip」と「list/dict shape のフルセット契約」に絞る。

#### IT-JSONL-01: generate_bp_jsonl の出力 schema が Pydantic で round-trip 可能

**対象**: `jsonl/bp.py::generate_bp_jsonl`、`jsonl/bp.py::xml_entry_to_bp_instance`、`schema.py::BioProject`

**前提**:
- 本番想定の NCBI/DDBJ BioProject XML fixture (`bioproject_set.xml.gz` / `ddbj_core_bioproject.xml`)
- date_cache, status_cache, umbrella DB が finalize 済み

**不変条件**:
- 全出力 JSONL の各行を `BioProject.model_validate_json(line)` で再構築でき、`model_dump(mode="json")` で再 dump した結果が JSON 等価 (round-trip 可能)
- list 型フィールド (`organization`, `publication`, `grant`, `externalLink`, `sameAs`, `dbXrefs`, `parentBioProjects`, `childBioProjects` 等) が 0 件でも空 list (`[]`) として出力される (commit `6c264b8` の API OpenAPI contract)
- 全件で `objectType` が `Literal["BioProject", "UmbrellaBioProject"]` のいずれか

**回帰元**: `6c264b8` (list fields required to preserve empty array keys)、`73264c6` (default schema list fields to empty)、`b0051e1` (isolate attribute)、`221f8c3` (facet/search fields)、`fe8af0e` (categorical mappings → text+keyword)

**関連 unit テスト**: `tests/py_tests/jsonl/test_bp.py::TestXmlEntryToBpInstanceProperties` (PBT)、`tests/py_tests/test_schema.py`

#### IT-JSONL-02: generate_bs_jsonl の Attribute 配列が常に list 化される

**対象**: `jsonl/bs.py::generate_bs_jsonl`、`jsonl/utils.py::ensure_attribute_list`、`_apply_attribute_list`

**前提**:
- 1 個しか `<Attribute>` を持たない BioSample が含まれる本番想定 fixture
- 0 件 / 1 件 / N 件の混在ケース

**不変条件**:
- JSONL 出力の任意の BioSample で `properties.BioSample.Attributes.Attribute` が常に list (1 件のときも list、0 件のときは省略 or 空 list)
- `Owner.Name`、`Models.Model` も同様にスカラー単独でも list 化されている (path-targeted normalization)
- ensure_attribute_list が他の path (Description, Comment 等) を巻き込んで list 化していない (`5abde23` で path-targeted に絞られた)

**回帰元**: `5abde23` (Replace ensure_list_children with path-targeted ensure_attribute_list)、`dd3895e` (normalize properties dict children to always use list wrappers)

**関連 unit テスト**: `tests/py_tests/jsonl/test_utils.py::TestEnsureAttributeList`, `TestEnsureAttributeListPBT`

#### IT-JSONL-03: generate_sra_jsonl の type 分類と submission 単位の整合

**対象**: `jsonl/sra.py::process_submission_xml`、`process_source`、`generate_sra_jsonl`

**前提**:
- DRA / NCBI の SRA tar fixture (submission/study/experiment/run/sample/analysis を含む)
- `sra_accessions.duckdb`, `dra_accessions.duckdb` が build 済み

**不変条件**:
- 6 種 (`submission` / `study` / `experiment` / `run` / `sample` / `analysis`) の JSONL がそれぞれ生成される
- 各 type の accession は対応する `id_patterns.py` の `ID_PATTERN_MAP` にマッチ (`SRA0` / `SRP` / `SRX` / `SRR` / `SRS` / `SRZ` 等)
- 同一 `submission_id` 配下の study/experiment/run/sample/analysis の cross-reference が JSONL の `isPartOf` / `dbXrefs` に整合
- `_get_text` が dict / list 値を含む場合でも `str()` で潰さず安全に coerce する (`82acd0d`)

**回帰元**: `82acd0d` (Harden SRA `_get_text`)、`44d35f3` (SRA organization/publication/library/platform fields)、`bbc3691` (skip lxml Comment/ProcessingInstruction)、`16e0b30` (SRA controlled vocab Literal → free str)

**関連 unit テスト**: `tests/py_tests/jsonl/test_sra.py::TestProcessSubmissionXml`, `TestBatchDedup`, `TestCreateSraEntryProperties`, `TestLxmlCommentInExperiment`, `TestGetText`

#### IT-JSONL-04: generate_jga_jsonl の sameAs alias ドキュメント

**対象**: `jsonl/jga.py::parse_same_as`、`generate_jga_jsonl`、`schema.py::JGA`

**前提**:
- JGA Study XML fixture に `<SECONDARY_ID>` を持つエントリーが含まれる
- 各 entity (study / dataset / dac / policy) で alias が複数あるケース

**不変条件**:
- 主 accession が SECONDARY_ID にエイリアスを持つ場合、JSONL の `sameAs` フィールドに alias xref が含まれる (alias 側の独立 doc は `es/bulk_insert.py::generate_bulk_actions` が `_op_type=index` の追加 action を yield して生成、IT-ES-07 でカバー)
- `sameAs` が空でも空 list として出力 (commit `6c264b8`)
- alias ID が JGA accession の format に一致しない場合は除外 (DEBUG ログ)

**回帰元**: `0d954bd` (sameAs alias documents in ES bulk insert)、`9fe9eb8` (sameAs を nested type に)、`a67f1cc` (JGA org / publication / grant / externalLink fields)

**関連 unit テスト**: `tests/py_tests/jsonl/test_jga.py::TestParseSameAs`, `TestJgaEntryToJgaInstanceProperties`

#### IT-JSONL-05: generate_gea_jsonl の IDF 全件走査

**対象**: `jsonl/gea.py::iterate_gea_idf_files`、`generate_gea_jsonl`、`dblink/idf_sdrf.py`

**前提**:
- `GEA_BASE_PATH` 配下の本番想定 IDF/SDRF fixture (E-GEAD-* ディレクトリ)
- IDF が存在しないディレクトリ、SDRF だけのディレクトリも fixture に含む

**不変条件**:
- IDF が存在する全 accession が JSONL に出力される (常に全件処理、`last_run.json` に含めない)
- `Comment[ArrayExpressAccession]` から取った accession が JSONL の `identifier` と一致
- IDF が tab / newline を含む quoted value を持つ行 (`Investigation Title\t"value with\ttab"`) でも csv.reader で正しく parse される (`cfd700f`)

**回帰元**: `cfd700f` (Parse IDF/SDRF files with csv.reader)、`966419b` (MAGE-TAB quoted values)、`f1823a1` (Expand GEA IDF/SDRF parser)、`e7dc145` (GEA / MetaboBank accession types)

**関連 unit テスト**: `tests/py_tests/jsonl/test_gea.py::TestGenerateGeaJsonlE2E`, `TestExtractTitle`, `TestExtractDates`, `tests/py_tests/jsonl/test_idf_common.py::TestParseIdf`

#### IT-JSONL-06: generate_metabobank_jsonl の欠損 IDF 除外

**対象**: `jsonl/metabobank.py::iterate_metabobank_idf_files`、`generate_metabobank_jsonl`

**前提**: `METABOBANK_BASE_PATH` 配下に「ディレクトリ名はあるが IDF ファイルがない」MetaboBank study が混在する fixture

**不変条件**:
- IDF が存在しないディレクトリは JSONL に含まれない (warn ログのみ)
- IDF があるが必須フィールド欠損のエントリーは JSONL に出力される (Pydantic 側の Optional フィールドはそのまま)
- MetaboBank `studyType` / `experimentType` / `submissionType` が controlled vocab Literal の値のみ受理 (`d2fdb0e`)

**回帰元**: `d2fdb0e` (MetaboBank Literal restriction)、`16e0b30` (controlled vocab relaxation)、`e7dc145`

**関連 unit テスト**: `tests/py_tests/jsonl/test_metabobank.py::TestGenerateMetabobankJsonlE2E`, `TestIterateMetabobankIdfFiles`

#### IT-JSONL-07: blacklist 該当 accession が JSONL に出力されない

**対象**: 各 `jsonl/{bp,bs,sra,jga}.py::generate_*_jsonl` での blacklist 適用

**前提**:
- 各 blacklist (`bp/blacklist.txt` / `bs/blacklist.txt` / `sra/blacklist.txt` / `jga/blacklist.txt`) に検証用 accession を追加
- 該当 accession が XML fixture に存在する状態

**不変条件**:
- 完了後の JSONL に blacklist 該当 accession が含まれない (`identifier` での 0 件確認)
- `es_delete_blacklist` で IT-ES 側でも除外されることを併せて確認 (IT-ES-04 と整合)
- blacklist 該当しない他 accession は出力に影響しない

**回帰元**: `2154204` (preserved + blacklist 整合)、`2bf235a` (Org dedup blacklist 文脈)

**関連 unit テスト**: `tests/py_tests/dblink/test_utils.py::TestLoadBlacklist`, `TestLoadSraBlacklist` (jsonl 側に直接の unit はない)

#### IT-JSONL-08: dbXrefs フィールドはデフォルトでの非含有 / --include-dbxrefs で有効化

**対象**: `jsonl/{bp,bs,sra,jga}.py::main` の `--include-dbxrefs` フラグ、`jsonl/utils.py::get_dbxref_map`

**前提**: `dblink.duckdb` finalize 済み、当該 accession に対応する dbxref edge が存在

**不変条件**:
- `--include-dbxrefs` なしで生成された JSONL には `dbXrefs` フィールドが空 list (`6c264b8` で list は必須化されたが値は空)
- `--include-dbxrefs` ありで生成された JSONL には dbxref が反映され、半辺化 dbxref から `(accession_type, accession)` lookup で得られる `(linked_type, linked_accession)` 集合が `dbXrefs` の Xref と一致
- `parentBioProjects` / `childBioProjects` は umbrella DB 経由で常に展開され、`--include-dbxrefs` フラグの影響を受けない

**回帰元**: `17a80e0` (Make dbXrefs opt-in via --include-dbxrefs flag)、`f279f2f` (半辺化 dbxref の lookup 経路)

**関連 unit テスト**: `tests/py_tests/jsonl/test_utils.py::TestGetDbxrefMap`

#### IT-JSONL-09: date_cache / status_cache の値が JSONL に反映される

**対象**: `jsonl/bp.py::_fetch_dates_ddbj` / `_fetch_dates_ncbi` / `_fetch_statuses`、`jsonl/bs.py` 同等関数、`date_cache/db.py`、`status_cache/db.py`

**前提**:
- `bp_bs_date.duckdb` / `bp_bs_status.duckdb` が build 済み (XSM PostgreSQL + Livelist 経由、IT-RESOURCE-03 / IT-PIPELINE 前提)
- DDBJ accession (PRJDB / SAMD) と NCBI accession (PRJEB / PRJNA / SAMN) の両方を含む XML fixture

**不変条件**:
- DDBJ accession の `dateCreated` / `dateModified` / `datePublished` が `bp_bs_date.duckdb` の値と一致 (XML から取得しない)
- NCBI accession の date は XML の値そのまま (PostgreSQL を見ない)
- `status` が `schema.py::Status = Literal["public", "private", "suppressed", "withdrawn"]` の 4 値のいずれか (livelist 内部表現の `live` / `unpublished` 等は `8308148` で rename 済、生値は出力されない)
- `accessibility` が `schema.py::Accessibility = Literal["public-access", "controlled-access"]` の 2 値のいずれか (NCBI の `public` を `public-access` に、DDBJ は常に `public-access`、`bp.py::parse_accessibility` / `bs.py::parse_accessibility` が正規化)

**回帰元**: `e358859` (Add BP/BS status cache from livelist)、`8308148` (Rename status values: live→public、status のみ。accessibility は別経路で常時 normalize)、`8a048db` (TCP keepalive + materialize fetch results)、`551cf0d` (livelist test fixtures)

**関連 unit テスト**: `tests/py_tests/jsonl/test_bp.py::TestFetchStatuses`, `tests/py_tests/jsonl/test_bs.py::TestFetchStatuses`, `tests/py_tests/status_cache/*`

#### IT-JSONL-10: isPartOf / type / distribution の値整合

**対象**: `jsonl/distribution.py::make_*_distribution`、各 `jsonl/{bp,bs,sra,jga,gea,metabobank}.py` の `isPartOf` / `type` 設定

**前提**: 本番想定 fixture で全 6 ソースの JSONL を生成

**不変条件**:
- `isPartOf` 値が `docs/data-architecture.md` `## isPartOf / type フィールド` 節の許容セットに含まれる (snake_case 統一、`4961167`)
- `type` が schema.py の `Literal` enum に一致
- `distribution` の URL が `docs/data-architecture.md` `### Distribution` の URL template と一致 (json / jsonld の 2 種、`d9336e3`)
- BP の URL は DDBJ getentry endpoint (`12bbf40` で NCBI nuccore から変更)
- GEA の URL は `SEARCH_BASE_URL` ではなくハードコード base URL (`2566668`)

**回帰元**: `4961167` (snake_case isPartOf)、`d9336e3` (distribution URLs)、`12bbf40` (BP URL → DDBJ getentry)、`2566668` (GEA URL fix)

**関連 unit テスト**: `tests/py_tests/jsonl/test_distribution.py::*`

#### IT-JSONL-11: parentBioProjects / childBioProjects が Umbrella DB から展開される

**対象**: `jsonl/utils.py::enrich_umbrella_relations`、`jsonl/bp.py` の umbrella enrichment フェーズ

**前提**:
- IT-UMBRELLA-01..05 と同じ前提 (`umbrella.duckdb` finalize 済み)
- multi-parent / depth>=2 のケースを含む BP fixture

**不変条件**:
- `child_accession` を持つ BP の JSONL の `parentBioProjects` に親リストが入る
- `parent_accession` を持つ BP の JSONL の `childBioProjects` に子リストが入る
- 両 list は `Xref` 形式で `type="bioproject"`
- multi-parent ケースで `parentBioProjects` が複数要素 (`6dda94c` の DAG 表現)
- 推移的閉包は計算されない (直近の親子のみ、IT-UMBRELLA-04 と整合)

**回帰元**: `6dda94c` (umbrella separate)、`deb4166` (parent/child BioProject ontology)

**関連 unit テスト**: `tests/py_tests/jsonl/test_bp.py::TestEnrichUmbrellaRelations`

#### IT-JSONL-12: regenerate_jsonl が last_run.json を更新しない

**対象**: `jsonl/regenerate.py::main`

**前提**:
- 既存の `last_run.json` (例: `bioproject: "2026-04-01T00:00:00Z"`)
- 任意の accession (例: `PRJDB12345`) を `--accessions` で指定して `regenerate_jsonl --type bioproject --accessions PRJDB12345` を実行

**不変条件**:
- 実行後の `last_run.json` の `bioproject` フィールドが実行前と同一 (mtime / 内容ともに変化なし)
- 出力先は `{result_dir}/regenerate/{date}/` に隔離され、通常パイプラインの `{result_dir}/{type}/jsonl/{date}/` を上書きしない
- `--type` に対して `accession` の format が合わない場合は warn ログのみで処理続行 (`validate_accessions`)

**回帰元**: `docs/cli-pipeline.md` `## Hotfix: regenerate_jsonl` 節の不変条件、`6dda94c` (regenerate.py の Umbrella DB 対応も含む大規模 refactor)

**関連 unit テスト**: `tests/py_tests/jsonl/test_regenerate.py::TestLoadAccessionsFromFile`, `TestValidateAccessions`, `TestValidateAccessionsPBT`

### IT-DIFF-*: 差分更新

`last_run.json` ベースの差分処理。`config.py::DEFAULT_MARGIN_DAYS = 30`、`apply_margin` / `read_last_run` / `write_last_run` を SSOT とする。

#### IT-DIFF-01: last_run.json が null のとき --full 相当の動作

**対象**: `jsonl/{bp,bs,sra}.py::main`、`config.py::read_last_run`

**前提**: `last_run.json` の対象 type 値が `null` (例: `{"bioproject": null}`)、または `last_run.json` 自体が存在しない

**不変条件**:
- `since` が未取得 (`None`) の場合、当該 type は全件処理モード (`--full` と等価)
- 全件処理ログ (`log_info("full update mode: ...")`) が記録される
- 完了後の JSONL 件数が、`--full` 明示で実行した場合と一致

**回帰元**: `docs/cli-pipeline.md` `### last_run.json` 節、`config.py::read_last_run` (`null` を許容)

**関連 unit テスト**: なし (実 `last_run.json` の I/O を要するため unit 未整備)

#### IT-DIFF-02: margin_days を引いた日時以降のエントリーが処理対象

**対象**: `config.py::apply_margin` (`DEFAULT_MARGIN_DAYS = 30`)、`jsonl/{bp,bs,sra}.py::main`

**前提**:
- `last_run.json` に既知の `since` (例: `2026-01-30T00:00:00Z`)
- date_modified が `since - 30 day = 2025-12-31` 以降のエントリーが fixture に存在

**不変条件**:
- `apply_margin(since, 30)` が `(since - 30 day)` の ISO8601 文字列を返す
- 差分処理の対象に `2025-12-31 <= date_modified` のエントリーが含まれる
- `2025-12-30` 以前のエントリーは処理されない (`fetch_*_accessions_modified_since` の境界)
- `--margin-days N` で挙動を上書きできる (CLI フラグが SSOT を override する)

**回帰元**: `docs/cli-pipeline.md` `### margin_days` 節、`apply_margin` 実装

**関連 unit テスト**: なし (config.py::apply_margin の純粋関数テストはあるべきだが unit 未整備、本シナリオで E2E 確認)

#### IT-DIFF-03: データタイプ別の差分判定基準

**対象**:
- BP: XML `date_modified` を `xsm_postgres_url` 経由で `fetch_bp_accessions_modified_since`
- BS: XML `last_update` 同 `fetch_bs_accessions_modified_since`
- SRA: `SRA_Accessions.tab` の `Updated` カラム (`sra_accessions_tab.py`)

**前提**:
- BP / BS / SRA 各 fixture の更新日時が境界値 (`since`、`since-1`、`since+1`) を含むよう設計
- 該当 PostgreSQL / SRA_Accessions が IT-RESOURCE-02/03 と整合

**不変条件**:
- BP の差分処理対象に `mass.project.modified_date >= since - margin` の accession が含まれる
- BS の差分処理対象に `mass.sample.modified_date >= since - margin` の accession が含まれる
- SRA の差分処理対象に `Accessions.tab.Updated >= since - margin` の行が含まれる
- 各 type で `since - margin` 未満のエントリーが処理対象に入らない

**回帰元**: `docs/cli-pipeline.md` `### データタイプ別の差分判定基準` 節、`6bf545b` (TIMESTAMPTZ 化で SRA Accessions の date 扱い変更)

**関連 unit テスト**: `tests/py_tests/test_sra_accessions_tab.py` (Accessions.tab の Updated カラム解析)

#### IT-DIFF-04: JGA / GEA / MetaboBank は常に全件処理

**対象**: `jsonl/{jga,gea,metabobank}.py::main`、`config.py::DataType` Literal

**前提**: 既存の `last_run.json`

**不変条件**:
- JGA / GEA / MetaboBank の処理は `last_run` の値を読まず常に全件処理
- 完了後の `last_run.json` に `gea` / `metabobank` キーが追加されない (含まれていればテスト失敗)
- JGA は schema 上 `last_run.json` に含まれるが値は常に `null` で更新される (cli-pipeline.md の表)
- 各 type の出力 JSONL の accession 数が、入力 fixture の対応 source 全件と一致

**回帰元**: `docs/cli-pipeline.md` `### データタイプ別の差分判定基準` 節 (「JGA / GEA / MetaboBank は更新時刻フィールドがないため差分判定できない」)、`e7dc145` (GEA / MetaboBank 追加時から全件処理)

**関連 unit テスト**: なし

#### IT-DIFF-05: JSONL 生成完了後に last_run.json が更新される

**対象**: `jsonl/{bp,bs,sra}.py::main` の `write_last_run(config, ...)` 呼び出し、`config.py::write_last_run`

**前提**: 既存の `last_run.json` (`bioproject: "2026-01-30T00:00:00Z"`)

**不変条件**:
- 実行成功 (run_logger status=SUCCESS) 時に対応 type が現在時刻に更新される
- 失敗 (例外で abort) 時は `last_run.json` が更新されない (`write_last_run` は処理完了後にのみ呼ばれる)
- JSON シリアライズ形式が `dict[DataType, str | None]` を維持 (key 順序は `read_last_run` で保証)

**回帰元**: `docs/cli-pipeline.md` `### last_run.json` 節、`config.py::write_last_run` の atomic write 設計

**関連 unit テスト**: なし

#### IT-DIFF-06: regenerate_jsonl 後に通常 incremental 実行が再処理する

**対象**: `jsonl/regenerate.py::main` と `jsonl/{bp,bs,sra}.py::main` の incremental 経路の連動

**前提**:
- 既存の `last_run.json` (`bioproject: "2026-01-30T00:00:00Z"` 等)
- regenerate を BP / BS / SRA 各 type で連続実行 (例: BP → SRA → BP)
- その後で通常 incremental (`generate_bp_jsonl` を `--full` なしで実行)

**不変条件**:
- IT-JSONL-12 の不変条件 (`last_run.json` の値・mtime 不変) が複数 type 連続でも保たれる
- regenerate で出力した accession (例: `PRJDB12345`) が `since` 以後の更新時刻なら、次の通常 incremental で再度処理対象に入る (regenerate が `last_run.json` を更新しないことの実害確認)
- 通常 incremental 実行後に `last_run.json` が更新される (IT-DIFF-05 と整合、regenerate は「`last_run.json` を進めない hotfix」、incremental は「進める」を独立に保つ)

**回帰元**: `docs/cli-pipeline.md` `## Hotfix: regenerate_jsonl` 節 (「次回の差分更新で同じ accession が再度処理される可能性がある」)

**関連 unit テスト**: なし (regenerate と通常 incremental を続けて回す flow は integration の独自検証範囲)

### IT-ES-*: Elasticsearch 操作

実 ES に対する index / bulk insert / alias / blacklist 削除。`ALL_INDEXES` の 14 物理 index、3 エイリアス (`sra` / `jga` / `entries`) を SSOT とする。

#### IT-ES-01: es_create_index --index <group> で alias 込みで作成

**対象**: `cli/es.py::main_create_index`、`es/index.py::create_index`、`get_indexes_for_group`、`ALIASES`

**前提**:
- 空の ES クラスタ (対象 index と alias が未作成)
- `--index all` / `--index sra` / `--index bioproject` のそれぞれを実行

**不変条件**:
- `--index all` 完了後、14 物理 index (`ALL_INDEXES`) が存在
- `sra` alias が `SRA_INDEXES` の 6 index、`jga` alias が `JGA_INDEXES` の 4 index、`entries` alias が 14 index 全部に張られる
- 個別 index alias (`bioproject`, `biosample`, `gea`, `metabobank`, ...) も同名 alias として張られる
- `--index sra` のみで 6 SRA index + sra/entries エイリアスが張られる (他は触られない)
- 既に存在する場合 `--skip-existing` なしで Exception (`d350082` 系の冪等性検証)

**回帰元**: `b5a8d6c` (Blue-Green alias swap 導入時に固定名 + alias 構造に正式化)

**関連 unit テスト**: `tests/py_tests/es/test_index.py` (mock ES)

#### IT-ES-02: es_bulk_insert の _op_type=index で既存 doc が上書き

**対象**: `es/bulk_insert.py::generate_bulk_actions` (`_op_type: "index"`)、`bulk_insert_jsonl`

**前提**:
- 既存 index に同一 `_id` (= `identifier`) のドキュメントが存在
- 別バージョンの JSONL を bulk insert

**不変条件**:
- 同一 `_id` の doc が新版で完全に上書きされる (`_source` の任意フィールドが新値)
- `BulkInsertResult.success_count` が JSONL 行数 + sameAs alias 件数と一致
- `_op_type: "create"` ではないため Conflict エラーは発生しない
- `error_count == 0`

**回帰元**: `0d954bd` (sameAs alias doc が success_count に含まれることの設計、`_op_type=index` 採用)、`generate_bulk_actions` が `_op_type: "index"` を選択している実装。bulk error 系の partial failure 扱いは IT-ES-06 でカバー

**関連 unit テスト**: `tests/py_tests/es/test_bulk_insert.py` (mock ES)

#### IT-ES-03: bulk insert 中の refresh interval 切替 (-1 → 1s 復元)

**対象**: `es/bulk_insert.py::bulk_insert_jsonl` の `set_refresh_interval` 呼び出し、`es/settings.py::BULK_INSERT_SETTINGS`

**前提**: 通常状態の index (`refresh_interval = "1s"`)

**不変条件**:
- bulk insert 開始時に index settings の `refresh_interval` が `"-1"` に変更される (`es/client.py::set_refresh_interval`)
- bulk insert 完了時 (例外 path 含む `try/finally`) に `"1s"` に復元される
- 完了時に `refresh_index` で manual refresh が呼ばれ、insert された doc が即座に検索可能

**回帰元**: `docs/elasticsearch.md` `### bulk insert 中の refresh 無効化` 節、`b5a8d6c` (Blue-Green でも同じロジック適用)

**関連 unit テスト**: `tests/py_tests/es/test_bulk_insert.py` (mock ES で settings 呼び出しを assert)

#### IT-ES-04: es_delete_blacklist で 404 を not_found としてカウント

**対象**: `cli/es.py::main_delete_blacklist`、`es/bulk_delete.py::bulk_delete_by_ids`

**前提**:
- blacklist に 3 種類の accession (① 既存 doc、② 存在しない doc、③ 別 index の doc)
- ES に既存 doc のみ投入済み

**不変条件**:
- ① は `success_count` に反映
- ② は 404 として `not_found_count` に反映 (errors に含まれない)
- ③ は対象 index 外なので削除されず、別 index 側で同様にカウント
- `helpers.bulk(..., raise_on_error=False)` のため process は abort しない
- ES が ApiError 等の非 dict object を返してきても `sanitize_error_info` で Pydantic シリアライズ可能な dict に正規化される (`89c0499`)

**回帰元**: `89c0499` (Share bulk error sanitizer)、`9d3959a`/`acd1a53` (Recursively sanitize)、`bde668b` (downgrade fatal → warning)

**関連 unit テスト**: `tests/py_tests/es/test_bulk_delete.py` (mock ES)

#### IT-ES-05: alias 構成 (sra / jga / entries) と対象 index の整合

**対象**: `es/index.py::ALIASES`、`SRA_INDEXES`、`JGA_INDEXES`、`ALL_INDEXES`

**前提**: `--index all` で全 index 作成済み

**不変条件**:
- ES の `_alias` API で `sra` alias を解決すると 6 SRA 物理 index と一致
- `jga` alias は 4 JGA 物理 index と一致
- `entries` alias は 14 物理 index 全部と一致
- 各個別 index 名 (例: `bioproject`) も alias として 1 物理 index に解決される (Blue-Green に備えて alias 化)
- `bioproject` alias 経由で search したときに `bioproject-YYYYMMDD` 物理 index が見える (Blue-Green 後)

**回帰元**: `b5a8d6c` (Blue-Green alias 構造)、`docs/elasticsearch.md` `### dated index と alias の関係` 節

**関連 unit テスト**: `tests/py_tests/es/test_index.py`

#### IT-ES-06: mapping エラー時の bulk insert ハンドリング

**対象**: `es/bulk_insert.py::bulk_insert_jsonl` の `parallel_bulk(raise_on_error=False, raise_on_exception=False)` 経路

**前提**:
- 意図的に schema 違反のドキュメント (例: `dateModified: "not a date"` で `date` mapping にマッチしない) を含む JSONL fixture
- 正常 doc も混在

**不変条件**:
- mapping エラー doc は `error_count` に積まれ、`errors` list に `max_errors` (関数引数、デフォルト 100) 件まで記録
- 正常 doc は `success_count` に積まれて投入される
- process は abort せず最後まで bulk が走る
- error info が Pydantic で再シリアライズ可能な dict (`_sanitize_error_info`)
- 現状 `bulk_insert_jsonl(max_errors=...)` は CLI から制御不能 (将来 CLI フラグ化する場合は本シナリオを更新)

**回帰元**: `bde668b` (downgrade fatal → warning)、`9d3959a`/`acd1a53`/`89c0499` (sanitize error info)、`82acd0d` (mapping エラーで dict 値が text に流れていた問題)

**関連 unit テスト**: `tests/py_tests/es/test_bulk_insert.py` (mock ES に error response を返させる)

#### IT-ES-07: _id 衝突時の挙動 (sameAs alias ドキュメントとの整合)

**対象**: `es/bulk_insert.py::generate_bulk_actions` の sameAs alias 生成 (`type_match_name` + prefix 一致条件)

**前提**:
- JGA 系で primary identifier `JGAS000001` と SECONDARY_ID `JGAS000099` を持つ doc
- 別 doc で identifier=`JGAS000099` (衝突) も存在

**不変条件**:
- alias doc は primary doc と同じ `_source` を持ち、`_id` だけ secondary id
- 衝突したとき、JSONL 投入順で後勝ち (`_op_type: "index"`)
- prefix 一致しない (`AGDD_000001`) の secondary id は alias doc を生成しない (`_extract_prefix` の正規表現)
- `type_match_name` が一致しない sameAs (`type=biosample` だが index=jga-study) は無視

**回帰元**: `0d954bd` (Add sameAs alias documents in ES bulk insert)、`9fe9eb8` (sameAs を nested searchable type に)

**関連 unit テスト**: `tests/py_tests/es/test_bulk_insert.py::test_extract_prefix` 系

#### IT-ES-08: es_health_check のクラスタ status 判定

**対象**: `cli/es.py::main_health_check`、`es/monitoring.py::get_cluster_health`、`get_node_stats`、`get_index_stats`、`check_health`、`HEALTH_CHECK_THRESHOLDS`

**前提**: staging クラスタ (1 ノード、shard replicas=0)

**不変条件**:
- `cluster.status` が `"green"` または `"yellow"` (single node では `replicas=0` で green)
- disk 使用率が `disk_warning_percent=80%` 未満 / `disk_critical_percent=90%` 未満で WARNING / CRITICAL のメッセージなし
- heap 使用率も同様 (`heap_warning_percent=75%` / `heap_critical_percent=90%`)
- index stats で `entries` alias の docs_count が `>= 0`
- `--verbose` で node / index 詳細が表示される

**回帰元**: `b562f01` (deployment guide で health check 操作を SSOT 化)、`e50213e` (ES JVM heap 31g) で heap 余裕を確保

**関連 unit テスト**: なし (実 ES 依存のため unit 未整備)

### IT-BLUEGREEN-*: Blue-Green Alias Swap

ゼロダウンタイム更新フロー。`docs/elasticsearch.md` `## Blue-Green Alias Swap` 節を SSOT とする。

#### IT-BLUEGREEN-01: es_create_index --date-suffix YYYYMMDD で alias なしの dated index を作成

**対象**: `es/index.py::create_index_with_suffix`、`make_physical_index_name`

**前提**:
- 通常運用中 (`bioproject` alias が `bioproject-20260401` に張られている等の状態)
- 新規 dated suffix `20260425` で `--date-suffix 20260425 --index all` を実行

**不変条件**:
- 14 個の dated 物理 index (`bioproject-20260425` 等) が新規作成される
- これらの新 index には alias が一切張られていない (search 経路に出ない)
- 旧 index `bioproject-20260401` と既存 alias は影響を受けない (検索断ゼロ)
- 同じ suffix で再実行すると `--skip-existing` なしで Exception

**回帰元**: `b5a8d6c` (Add Blue-Green alias swap)、`docs/elasticsearch.md` `### Full 更新フロー` 節 step 1

**関連 unit テスト**: `tests/py_tests/es/test_index.py` (mock ES)

#### IT-BLUEGREEN-02: es_bulk_insert --target-index で旧 index を触らずに投入

**対象**: `cli/es.py::main_bulk_insert` の `--target-index`、`es/bulk_insert.py::bulk_insert_jsonl` の `target_index` 引数、`generate_bulk_actions` の `logical_index` 渡し

**前提**: IT-BLUEGREEN-01 完了後、`bioproject-20260425` が空、`bioproject` alias は `bioproject-20260401` を指している

**不変条件**:
- `--index bioproject --target-index bioproject-20260425 --dir <jsonl>/` で投入したとき、`bioproject-20260425` のみに doc が積まれる
- 旧 `bioproject-20260401` の doc count が変化しない
- `bioproject` alias 経由で検索すると依然旧 index の結果のみ
- sameAs alias 生成も `logical_index="bioproject"` 経由で正しく型一致 (dated index 名で type 比較しない)
- refresh interval の切替が `bioproject-20260425` 側にだけ適用される

**回帰元**: `b5a8d6c` (Blue-Green の `--target-index` 経路)、`docs/elasticsearch.md` `### Full 更新フロー` 節 step 2

**関連 unit テスト**: `tests/py_tests/es/test_bulk_insert.py` (target_index 引数の振り分けを mock で確認)

#### IT-BLUEGREEN-03: es_swap_aliases 実行中も検索断ゼロ

**対象**: `cli/es.py::main_swap_aliases`、`es/index.py::swap_aliases` (`update_aliases` 単一 API call で remove/add を atomic 実行)

**前提**:
- 旧 `bioproject-20260401` に alias `bioproject` / `entries` が張られた状態
- 新 `bioproject-20260425` が空 alias で作成済 (IT-BLUEGREEN-01 完了)

**不変条件**:
- `swap_aliases(config, "20260425")` 完了後、`bioproject` alias が `bioproject-20260425` のみを指す
- `update_aliases` body の actions は単一 API call で送信 (remove + add が atomic、ES 側で部分適用なし)
- swap 中の任意のタイミングで `bioproject` alias を解決した結果が必ず 1 物理 index に解決される (中間状態で 0 物理 / 2 物理にならない)
- 戻り値の `old_indexes` に旧物理名 `bioproject-20260401` が記録される

**回帰元**: `b5a8d6c` (atomic swap)、`docs/elasticsearch.md` `### Full 更新フロー` 節 step 4

**関連 unit テスト**: `tests/py_tests/es/test_index.py` (mock の `update_aliases` 呼び出しが 1 回であることを assert)

#### IT-BLUEGREEN-04: 14 物理 index 全部が atomic に切り替わる (部分失敗なし)

**対象**: `es/index.py::swap_aliases` の単一 update_aliases body

**前提**:
- 14 dated index 全部 (`bioproject-20260425` ... `metabobank-20260425`) が事前作成済
- 旧 14 物理 index が alias で参照されている

**不変条件**:
- swap 完了後、14 個全 alias (個別 + group の `sra`/`jga`/`entries`) が新 dated index を指す
- 1 物理 index でも作成失敗していると `swap_aliases` 内で `Exception("New index '...' does not exist. Create it first.")` が raise され、ES への API call なし
- 結果 ES 側に部分適用が起きない (検索断ゼロ保証)
- `old_indexes` は dict で 14 entry (1 entry = 1 logical index → old physical name)

**回帰元**: `b5a8d6c`、`docs/elasticsearch.md` `### Full 更新フロー` 節「14 インデックス分」「atomic に切り替わる」

**関連 unit テスト**: `tests/py_tests/es/test_index.py`

#### IT-BLUEGREEN-05: es_delete_old_indexes で旧 dated index が削除される

**対象**: `cli/es.py::main_delete_old_indexes`、`es/index.py::delete_physical_indexes`

**前提**: IT-BLUEGREEN-03 完了後、旧 `bioproject-20260401` 等が alias なしで残っている状態

**不変条件**:
- `--date-suffix 20260401 --force` で 14 個の旧 dated index がすべて削除される
- 削除前に各 index の残存 alias が `delete_alias` で剥がされる (defensive)
- 新 `bioproject-20260425` 等は影響を受けない
- 存在しない index は skip (削除リストに入らない)
- ディスクが解放される (IT-ES-08 の disk usage と整合)

**回帰元**: `b5a8d6c`、`docs/elasticsearch.md` `### Full 更新フロー` 節 step 5

**関連 unit テスト**: `tests/py_tests/es/test_index.py`

#### IT-BLUEGREEN-06: es_migrate_to_blue_green の初回マイグレーション (固定名 → dated への _clone)

**対象**: `es/index.py::migrate_to_blue_green` (`_clone` API)

**前提**:
- 旧構造の固定名 index (`bioproject` 等) に doc 入りで存在
- alias は固定名 index 自身に張られている (旧運用)

**不変条件**:
- step 1: `index.blocks.write=True` が固定名 index に設定される (`_clone` の前提条件)
- step 2: 各 index が `_clone` API で `bioproject-20260425` 等の dated 名にコピーされる (hard-link、near-instant)
- step 3: dated index の write block が外される
- step 4: 固定名 index が削除される (この時点で短時間のダウンタイム発生、`log_warn` で事前通知)
- step 5: 新 alias (個別 + group) が dated index に張られる
- 既存 dated index がある場合は事前に Exception (覆い被せ防止)
- `_reindex` を使わない (`5cb4eff`)

**回帰元**: `b5a8d6c` (初版は `_reindex`)、`5cb4eff` (Replace reindex with clone API for near-instant)、`docs/elasticsearch.md` `### 初回マイグレーション` 節

**関連 unit テスト**: `tests/py_tests/es/test_index.py`

#### IT-BLUEGREEN-07: ロールバック (旧 dated index が残っていれば swap で戻せる)

**対象**: `swap_aliases` を「現在の dated → 旧 dated」方向に再実行

**前提**:
- 現在 alias が `bioproject-20260425` を指す
- 旧 `bioproject-20260401` がまだ削除されていない (IT-BLUEGREEN-05 を実行する前)

**不変条件**:
- `swap_aliases(config, "20260401")` を呼ぶと alias が `bioproject-20260401` に戻る
- 戻し中も検索断ゼロ (atomic swap)
- 旧 index の doc がすべて読める (delete されていない前提)
- 戻し後は `bioproject-20260425` が alias なしで残るので、再度 `delete_old_indexes --date-suffix 20260425` で除去

**回帰元**: `docs/elasticsearch.md` `### ロールバック` 節 (「旧 dated index が残っていれば swap で戻せる」)

**関連 unit テスト**: なし (運用シナリオのため unit 未整備)

### IT-RDF-*: RDF パイプライン

`insdc-rdf` を経由した独立パイプライン。`scripts/run_rdf_pipeline.sh` と `docs/rdf-pipeline.md` を SSOT とする。

#### IT-RDF-01: 4 source (bioproject / biosample / sra / sra-experiment) の並列変換

**対象**: `scripts/run_rdf_pipeline.sh` の `STEP_NAMES`、`run_parallel`

**前提**:
- `BIOPROJECT_XML` / `BIOSAMPLE_XML` / `SRA_TAR` / `SRA_Accessions.tab` 入力 fixture
- `insdc-rdf` コンテナ (Dockerfile / compose で定義) が利用可能

**不変条件**:
- 4 source の `convert` ステップが `run_parallel` で同時起動される (Bash の `wait` で全完了を待機)
- 各 source の出力ディレクトリ `{result_dir}/rdf/{source}/{ttl,jsonld,nt}/` が独立して書かれる
- 1 source の失敗が他 source の処理を中断しない (各 source の exit code を個別に集約)
- 全 source 完了後の summary log に成功 / 失敗の内訳が記録される

**回帰元**: `95561cf` (Add RDF conversion pipeline using insdc-rdf)、`docs/rdf-pipeline.md` `## データフロー` 節

**関連 unit テスト**: なし (insdc-rdf コンテナ依存のため unit 未整備)

#### IT-RDF-02: 出力ディレクトリの上書き挙動

**対象**: `scripts/run_rdf_pipeline.sh` の `RDF_OUTPUT_DIR` (`{result_dir}/rdf/{source}/{format}/`)

**前提**: 既存の `rdf/{source}/{ttl,jsonld,nt}/` 配下に古い出力ファイルが存在

**不変条件**:
- 同じ source / format のディレクトリは新ファイルで上書きされる (chunk 単位、`{prefix}_NNNN.{ext}`)
- `{format}` は `ttl` / `jsonld` / `nt` の 3 種すべて生成される
- 古い chunk 番号 (`*_9999.ttl`) が残っていても削除されない (cleanup は別コマンド)
- atomic write (途中 SIGTERM で部分書き込みファイルが残らない) は保証されない (insdc-rdf 側の挙動に委ねる、本シナリオでは「出力済みファイルが空でない」のみ確認)

**回帰元**: `95561cf` (RDF パイプライン初版)、`docs/rdf-pipeline.md` `## 出力` 節

**関連 unit テスト**: なし

#### IT-RDF-03: --from-step validate での再開

**対象**: `scripts/run_rdf_pipeline.sh` の `should_skip_step`、`STEP_NAMES`、`FROM_STEP_ORDER`

**前提**: 前回の実行で `convert` まで完了 (出力 ttl/jsonld/nt 揃っている)、`validate` の途中で中断

**不変条件**:
- `--from-step validate` で再実行すると `convert` 系ステップが skip される (log に `[SKIP] ... (--from-step)`)
- `validate` 以降のステップが実行される
- skip された出力ファイルは触られない (mtime が変わらない)
- `--from-step` に未知のステップ名を渡すと exit 1 + エラーメッセージ

**回帰元**: `95561cf`、`docs/rdf-pipeline.md` `### ステップ` 節

**関連 unit テスト**: なし (Bash スクリプトの挙動なので unit 未整備、shellcheck + smoke は別途)

#### IT-RDF-04: chunk size 指定 (--chunk-size)

**対象**: `scripts/run_rdf_pipeline.sh` の `CHUNK_SIZE` (デフォルト 100000)

**前提**: 100,001 件以上のレコードを含む source

**不変条件**:
- `--chunk-size 100000` (デフォルト) で 1 chunk あたり 100,000 件
- `--chunk-size 50000` で chunk 数が 2 倍になる
- 各 chunk ファイル名が `{prefix}_{NNNN}.{ext}` 形式で連番
- 1 chunk の中身は ≤ chunk_size 件 (最後の chunk は端数)

**回帰元**: `95561cf`、`docs/rdf-pipeline.md` `## パイプラインスクリプト` 節

**関連 unit テスト**: なし

#### IT-RDF-05: converter 側の ontology/*.ttl 語彙との整合 (rename 追随)

**対象**: `insdc-rdf` のスキーマ + converter `ddbj_search_converter/es/mappings/`、ontology TTL ファイル

**前提**:
- converter 側でフィールド rename (例: `dbType` → `dbtype` など) が発生したコミット
- RDF 出力に同フィールドが含まれる

**不変条件**:
- converter で使われる ontology 用語と RDF 出力の `rdfs:Class` / `rdf:Property` が同じ (rename 後に converter の `e.g. snake_case` が反映される)
- ontology TTL の `rdfs:label` が converter side の docstring と整合
- `c294cfa` (Drop e-prefix from Publication.dbType)、`74a410a` (Refine BioProject ontology comments) のような rename を起源として、本シナリオで両側のリグレッションを検出する

**回帰元**: `c294cfa`, `74a410a`, `15029e4` (Unify Grant.agency under shared Organization), `9529711` (Narrow Organization/Publication types), `docs/rdf-pipeline.md` `## converter 側との語彙同期` 節

**関連 unit テスト**: なし (insdc-rdf 側で完結するため converter 側の unit 未整備)

### IT-LOG-*: ログ・デバッグ

run_id ライフサイクル、JSONL ログ、DuckDB 集計。`docs/logging.md` を SSOT とする。

#### IT-LOG-01: 各コマンドが run_id を生成し、JSONL ログを出力する

**対象**: `logging/logger.py::init_logger`、`run_logger`、`_append_jsonl`

**前提**: 任意の CLI コマンドを `run_logger` 文脈で実行 (例: `init_dblink_db`)

**不変条件**:
- `run_id` が `{TODAY_STR}_{run_name}_{hex4}` 形式 (例: `20260425_init_dblink_db_a1b2`)
- `{result_dir}/logs/{TODAY_STR}/{run_name}_{hex4}.log.jsonl` ファイルが作成される
- 各 JSONL 行が `LogRecord.model_dump_json(exclude_none=True)` で valid な JSON
- `timestamp` が `Asia/Tokyo` の ISO8601 (`+09:00` suffix)
- `source` が呼び出し元 module path (`ddbj_search_converter.dblink.assembly_and_master` 等)

**回帰元**: `b562f01` (logging guide で SSOT 化)、`b7566b2` (NORMALIZE_GRANT_AGENCY tag 削除)、`logging/schema.py` の `LogRecord` 定義

**関連 unit テスト**: `tests/py_tests/logging/test_schema.py`, `tests/py_tests/logging/test_logger.py`

#### IT-LOG-02: run_id 完了時に SUCCESS / FAILED が記録される

**対象**: `logging/logger.py::run_logger` (`log_start` / `log_end` / `log_failed` / `finalize_logger`)

**前提**:
- 正常終了パターン: try ブロックで raise なし
- 異常終了パターン: try ブロックで例外 raise

**不変条件**:
- 正常終了時: 最初の record が `lifecycle="start"`、最後の record が `lifecycle="end"`、両方 INFO レベル
- 異常終了時: 最後の record が `lifecycle="failed"`、CRITICAL レベル、`error.type` / `error.message` / `error.traceback` がセット
- どちらでも finalize_logger が呼ばれて DuckDB insert が走る (`try/finally`)
- 例外は finalize 後に上に伝播する (process exit 1)

**回帰元**: `b562f01`、`logging/logger.py::run_logger` の `try/except/finally` 構造

**関連 unit テスト**: `tests/py_tests/logging/test_logger.py`

#### IT-LOG-03: DuckDB (log.duckdb) への自動 insert

**対象**: `logging/db.py::insert_log_records`、`logging/logger.py::finalize_logger`

**前提**: `run_logger` 文脈で複数 record を log_info / log_warn / log_error した後

**不変条件**:
- `{result_dir}/log.duckdb` が finalize 後に作成される
- `log_records` テーブルに JSONL 全 record が insert される (件数一致)
- `idx_run_name`, `idx_run_date`, `idx_log_level` の 3 index が張られる
- `error` / `extra` カラムが JSON 文字列として保存され、後で `json_extract_string` で読み出せる
- `get_last_successful_run_date(run_name)` が直近の SUCCESS run の date を返す (`lifecycle="end"` で判定)

**回帰元**: `b562f01`、`logging/db.py` 設計

**関連 unit テスト**: `tests/py_tests/logging/test_db.py`

#### IT-LOG-04: show_log_summary の集計 (run_name × status)

**対象**: `cli/debug/show_log_summary.py::main`

**前提**: 同一日に複数の run (例: `init_dblink_db` 成功 + `create_dblink_bp_relations` 失敗) が `log.duckdb` に記録された状態

**不変条件**:
- `show_log_summary` (default JSON 出力) で run_name 別の SUCCESS / FAILED / IN_PROGRESS のカウントが返る
- IN_PROGRESS は `lifecycle="start"` だけあって `lifecycle="end"` も `failed` もない run
- `--date YYYYMMDD` で対象日を指定できる
- `--raw` で人間可読の表形式

**回帰元**: `docs/logging.md` `### show_log_summary` 節、`show_log_summary.py:95-99` の `has_failed → FAILED` / `has_end → SUCCESS` / `else → IN_PROGRESS` 判定ロジック

**関連 unit テスト**: `tests/py_tests/cli/debug/test_run_order.py::TestPipelineOrder`, `TestSortRunNames`, `TestRunNameSortKey` (run_name の sort 順序)

#### IT-LOG-05: show_log の filter (--level / --latest / --limit)

**対象**: `cli/debug/show_log.py::main`

**前提**: 1 つの run_name に複数の hex_token がある状態 (再実行で複数 run_id)

**不変条件**:
- `--latest` で最新の run_id (timestamp 降順 1 件) のログのみ出力
- `--level ERROR` で `log_level=ERROR` のみ抽出
- `--level DEBUG` で DEBUG が見える (default では filter なし)
- `--limit 100` で先頭 100 件のみ
- `--limit 0` で全件 (デフォルト)
- `--raw` で人間可読、`--jsonl` (default) で 1 行 1 record

**回帰元**: `docs/logging.md` `### show_log` 節

**関連 unit テスト**: `tests/py_tests/cli/debug/test_show_log.py::TestRowToDict`

#### IT-LOG-06: ERROR / CRITICAL レベルの stderr 出力

**対象**: `logging/logger.py::_emit_stderr`

**前提**: 任意の `run_logger` 内で `log_info` / `log_warn` / `log_error` / `log_failed` / `log_debug` を実行

**不変条件**:
- DEBUG は stderr に出ない (`if record.log_level == "DEBUG": return`)
- INFO / WARNING / ERROR / CRITICAL は stderr に 1 行 1 record で出る
- 形式: `{timestamp(seconds)} - {run_name} - {LEVEL} - {message} [file=..., accession=...]`
- `extra.file`, `extra.accession` があれば `[...]` に追記

**回帰元**: `docs/logging.md` `### Log Level` 節 (stderr ○/×)

**関連 unit テスト**: `tests/py_tests/logging/test_logger.py`

#### IT-LOG-07: DEBUG ログの debug_category 必須

**対象**: `logging/schema.py::Extra::debug_category`、`DebugCategory` enum、各モジュールの `log_debug` 呼び出し規約

**前提**: 各 module で `log_debug` を呼ぶときに `debug_category` を渡している (規約)

**不変条件**:
- 全 DEBUG record が `extra.debug_category` 値を持つ (`docs/logging.md` `### Extra Fields` 表「DEBUG レベルのログには必須」)
- `debug_category` の値が `DebugCategory` enum に存在 (Pydantic バリデーション)
- 集計クエリ (`json_extract_string(extra, '$.debug_category')` で GROUP BY) で全 DEBUG record がカテゴリ付きで集計される
- 規約違反 (`debug_category` なしの DEBUG record) があれば本シナリオで 1 件以上検出される

**回帰元**: `docs/logging.md` `### DebugCategory` 節 (「DEBUG レベルのログには必須」)、`logging/schema.py::DebugCategory` enum 定義

**関連 unit テスト**: `tests/py_tests/logging/test_schema.py`

### IT-PIPELINE-*: 全体パイプライン

`scripts/run_pipeline.sh` の通し実行。`docs/cli-pipeline.md` `## パイプライン概要` 節を SSOT とする。

#### IT-PIPELINE-01: Phase 0 → 1 → 2 → 3 の順序

**対象**: `scripts/run_pipeline.sh` の `STEP_NAMES` 配列、各 Phase の `# PHASE N:` セクション

**前提**: clean な環境 (前回成果物なし) で `./scripts/run_pipeline.sh --date 20260425` を実行

**不変条件**:
- Phase 0 の `check_resources` (= `check_external_resources`) が最初に実行される
- Phase 1 (前処理 + DBLink): `prepare_*` (並列) → `init_dblink` → `dblink_*` (順次、DuckDB single-writer 制約) → `finalize_dblink` → `dump_dblink`
- Phase 2 (JSONL): `sync_tar` → `jsonl_bp` / `jsonl_bs` / `jsonl_sra` / `jsonl_jga` / `jsonl_gea` / `jsonl_metabobank` (順次、`--parallel-num N` で各コマンド内部並列)
- Phase 3 (ES): `es_create` → `es_bulk` → `es_delete_blacklist`、または `--blue-green` 時は `es_create_bg` → `es_bulk_bg` → `es_blacklist_bg` → `es_swap` → `es_cleanup_old`
- 各 Phase の境界で `log_section "Phase N: ..."` が出力され、log.duckdb で順序確認可能

**回帰元**: `docs/cli-pipeline.md` `## パイプライン概要` 節、`b7dad59` (parallel job default 4→16)、`docs/cli-pipeline.md` `### Phase 1 の DuckDB 順次制約` 節

**関連 unit テスト**: `tests/py_tests/cli/debug/test_run_order.py::TestPipelineOrder`

#### IT-PIPELINE-02: --dry-run で実行内容のみ表示

**対象**: `scripts/run_pipeline.sh` の `DRY_RUN` フラグ、`run_cmd` の dry-run 分岐

**前提**: 任意の状態で `./scripts/run_pipeline.sh --dry-run` を実行

**不変条件**:
- 各ステップの実行コマンドが log に表示されるが、実際のコマンドは起動しない (`run_cmd` が早期 return)
- 副作用がない: `result_dir` / `const_dir` 配下のファイルが新規作成・変更されない
- exit code 0
- log.duckdb にも record が追加されない (run_logger ベースのコマンドが起動しないため)

**回帰元**: `docs/cli-pipeline.md` `## 一括実行` 節 (「`--dry-run` で実行内容のみ確認」)

**関連 unit テスト**: なし (Bash スクリプトの挙動)

#### IT-PIPELINE-03: --from-step <name> で任意ステップから再開

**対象**: `scripts/run_pipeline.sh` の `FROM_STEP_ORDER` / `should_skip_step`

**前提**: 前回実行で `dblink_bp_bs` まで完了して中断、`init_dblink_db` の実行成果物 (`raw_edges` 入りの `dblink.tmp.duckdb`) が残っている状態

**不変条件**:
- `--from-step dblink_bp` で実行すると、`check_resources` / `prepare` / `init_dblink` / `dblink_bp_bs` が `[SKIP] ... (--from-step)` と記録され実行されない
- `dblink_bp` 以降のステップが実行され、`raw_edges` への新規 insert が累積される
- 不正なステップ名を渡すと exit 1 + エラー
- skip された step の出力ファイルは触られない

**回帰元**: `docs/cli-pipeline.md` `## 一括実行` 節、`scripts/run_pipeline.sh` の `--from-step` 引数

**関連 unit テスト**: なし

#### IT-PIPELINE-04: --full と --blue-green の組み合わせ

**対象**: `scripts/run_pipeline.sh` の `FULL_MODE` / `BLUE_GREEN` フラグ、Phase 2 の `--full` 伝播、Phase 3 の `es_*_bg` 経路

**前提**: 既存の dated index (`bioproject-20260424` 等) に alias が張られた状態で `--full --blue-green --date 20260425` を実行

**不変条件**:
- Phase 2 で `generate_*_jsonl` に `--full` が渡され、差分判定なしで全件生成
- Phase 3 で `es_create_bg` (新 dated index 作成) → `es_bulk_bg` (新 index に投入) → `es_blacklist_bg` (新 index 側で blacklist 削除) → `es_swap` (alias swap) → `es_cleanup_old` (旧 dated index 削除) の順
- swap 中も検索断ゼロ (IT-BLUEGREEN-03 と整合)
- 完了後 `bioproject-20260425` が alias `bioproject` を指す

**回帰元**: `b5a8d6c`、`docs/elasticsearch.md` `### Full 更新フロー` 節、`docs/cli-pipeline.md` `### 主要なフラグ` 節

**関連 unit テスト**: なし

#### IT-PIPELINE-05: --clean-es と --blue-green の排他チェック

**対象**: `scripts/run_pipeline.sh` の `# Validate --clean-es and --blue-green are mutually exclusive` ブロック

**前提**: `./scripts/run_pipeline.sh --clean-es --blue-green` で起動

**不変条件**:
- 起動直後にエラーメッセージ `"Error: --clean-es and --blue-green are mutually exclusive"` を出力
- exit code 非ゼロ (1)
- 何も実行されない (副作用ゼロ)
- `--clean-es` だけ、`--blue-green` だけならそれぞれ正常に Phase 3 を実行する

**回帰元**: `docs/cli-pipeline.md` `### 主要なフラグ` 節 (「`--clean-es` と排他」)

**関連 unit テスト**: なし

#### IT-PIPELINE-06: --parallel N で JSONL 生成の並列度

**対象**: `scripts/run_pipeline.sh` の `MAX_PARALLEL` (default 16)、Phase 2 の `--parallel-num ${MAX_PARALLEL}`

**前提**: `--parallel 4` を指定して実行

**不変条件**:
- 各 jsonl コマンド (`generate_bp_jsonl` 等) に `--parallel-num 4` が伝播
- BP / BS の XML batch 並列処理が `jsonl/bp.py::process_xml_file` (内部で `ProcessPoolExecutor(max_workers=parallel_num)` を起動、`bp.py:892`) で動き、worker entry は `_process_xml_file_worker`
- jsonl コマンド自体は順次実行 (Phase 2 内部の sequential 制約)
- production の Rundeck job の default 16 と整合 (`scripts/rundeck-job.yaml`)

**回帰元**: `b7dad59` (default 4 → 16)、`docs/cli-pipeline.md` `### Phase 2 の並列度` 節

**関連 unit テスト**: なし

#### IT-PIPELINE-07: 各 Phase の中間成果物が次 Phase の入力として保持される

**対象**: Phase 1 → Phase 2 → Phase 3 の依存関係

**前提**: clean run で全 Phase 通し実行

**不変条件**:
- Phase 1 完了時点で `{const_dir}/dblink/dblink.duckdb`, `umbrella.duckdb` が存在 (Phase 2 BP の `enrich_umbrella_relations` が読む)
- Phase 1 完了時点で `{const_dir}/dblink/dblink.duckdb` の `dbxref` が `--include-dbxrefs` で参照可能 (`get_dbxref_map`)
- Phase 2 完了時点で `{result_dir}/{type}/jsonl/{date}/*.jsonl` が存在 (Phase 3 `es_bulk_insert` の入力)
- Phase 3 失敗時に Phase 1/2 の成果物が削除されない (再実行時に Phase 3 のみ走らせ可能)

**回帰元**: `docs/cli-pipeline.md` `## パイプライン概要` 節 (「Phase 3 の ES 投入で失敗しても Phase 1/2 の成果は保持される」)

**関連 unit テスト**: なし

#### IT-PIPELINE-08: 失敗時の途中停止と --from-step での再開可能性

**対象**: `run_cmd` の exit code 伝播、`set -e` の挙動

**前提**: Phase 2 の `jsonl_sra` で意図的に失敗するケース (例: SRA tar 不在)

**不変条件**:
- `jsonl_sra` 失敗時にパイプラインが exit (Phase 3 に進まない)
- log.duckdb に `jsonl_sra` の `lifecycle="failed"` が記録される (IT-LOG-02 と整合)
- `show_log_summary` で FAILED と表示される
- 修正後 `--from-step jsonl_sra` で再開すると、`jsonl_sra` 以降が実行される
- `jsonl_bp` / `jsonl_bs` 等先行ステップの出力 JSONL が削除されない (path-based skip で `--resume` が使える)

**回帰元**: `docs/cli-pipeline.md` `## パイプライン概要` 節、`docs/cli-pipeline.md` `### Phase 2 の並列度` 節 (`--resume` フラグ)

**関連 unit テスト**: なし

## 移植トレーサビリティ

過去のレビューで検出した bug fix と仕様変更から導かれた検証ケースを `IT-XXX-NN` に紐付ける対応表。各 IT の「**回帰元**」項目と相互参照する。新規 commit で IT が増えたら本表にも追記する。

時系列降順 (新しい順)、commit SHA は短縮 7 文字。

| Commit | 主旨 | 関連 IT |
|---|---|---|
| `6c264b8` | entry schema list fields を required 化して空配列キーを保持 (OpenAPI contract) | IT-JSONL-01 |
| `89c0499` | bulk error sanitizer 共有化、bulk_delete の JSON シリアライズ修正 | IT-ES-02, IT-ES-04, IT-ES-06 |
| `b562f01` | docs を deployment / development / integration ガイドに再構成 | IT-RESOURCE-01, IT-RESOURCE-05, IT-LOG-01, IT-LOG-02, IT-LOG-03, IT-LOG-04, IT-ES-08 |
| `b0051e1` | BioSample schema / parser / ES mapping に `isolate` 属性追加 | IT-JSONL-01 |
| `221f8c3` | BP / BS / SRA mapping に facet / search フィールド追加 | IT-JSONL-01 |
| `fe8af0e` | categorical keyword を text+keyword subfield に変換 | IT-JSONL-01 |
| `2ff0474` | ExternalLink URL / preserved TSV / JGA publication DB type の入力検証強化 | IT-DBLINK-07 |
| `f279f2f` | dblink を半辺化 `dbxref` table に refactor (単一インデックス WHERE) | IT-DBLINK-01, IT-DBLINK-02, IT-DBLINK-03, IT-DBLINK-04, IT-DBLINK-08, IT-JSONL-08 |
| `cfd700f` | IDF / SDRF を csv.reader で parse して quoted tab/newline を保持 | IT-JSONL-05 |
| `2bf235a` | Organization dedup を role / organizationType でスコープ化、非 DOI URL を `other` に | IT-DBLINK-06, IT-JSONL-07 |
| `cdf5a7b` | Publication URL を id 欠損時に生成しないようガード | (parser 堅牢化、現状の IT で直接カバーされず。round-trip で副次的に検出可能 - IT-JSONL-01) |
| `82acd0d` | SRA `_get_text` で list / dict 値を安全に coerce | IT-JSONL-03, IT-ES-06 |
| `73264c6` | BP / BS parser で organization 重複排除、schema list fields のデフォルトを空に | IT-JSONL-01 |
| `b7566b2` | docs を 18 dblink TSV に追従、stale `NORMALIZE_GRANT_AGENCY` tag 除去 | IT-RESOURCE-01, IT-DBLINK-08, IT-LOG-01, IT-LOG-07 |
| `4961167` | BP / BS の `isPartOf` 値を snake_case に統一 | IT-JSONL-10 |
| `c294cfa` | Publication.dbType の `e-` prefix 削除、未使用 status 削除 | IT-RDF-05 |
| `16e0b30` | SRA / MetaboBank の controlled vocab を Literal から free str に緩和 | IT-JSONL-03, IT-JSONL-06 |
| `d350082` | pytest-xdist + chunk size monkeypatch でテスト高速化 | IT-DBLINK-01 |
| `f1823a1` | GEA IDF / SDRF parser を SRA run / experiment / JGA / humandbs まで拡張 | IT-JSONL-05 |
| `966419b` | IDF parser の MAGE-TAB quoted values 対応、Publication DOI prefix 正規化 | IT-JSONL-05 |
| `15029e4` | Grant.agency を共有 Organization 型に統一 | IT-RDF-05 |
| `d2fdb0e` | MetaboBank の study / experiment / submission type を Literal に制限、broker_name を Organization に | IT-JSONL-06 |
| `e7dc145` | GEA / MetaboBank accession 型 + JSONL / mapping / ontology 追加 | IT-JSONL-05, IT-JSONL-06, IT-DIFF-04 |
| `a67f1cc` | JGA に organization / publication / grant / externalLink / studyType / datasetType を追加 | IT-JSONL-04 |
| `44d35f3` | SRA に organization / publication / library / platform / analysisType を追加 | IT-JSONL-03 |
| `74a410a` | BioProject ontology コメントに enum 値を追記、reference / dbType を camelCase に | IT-RDF-05 |
| `bbc3691` | lxml Comment / ProcessingInstruction を `_element_to_dict` で skip | IT-JSONL-03 |
| `9529711` | Organization / Publication 型を Literal で narrow、フィールド名を camelCase に | IT-RDF-05 |
| `5abde23` | `ensure_list_children` を path-targeted `ensure_attribute_list` に置換 | IT-JSONL-02 |
| `83208a8` | INSDC PostgreSQL 接続にリトライ + keepalive 設定追加 | IT-RESOURCE-02 |
| `5cb4eff` | Blue-Green migration の reindex を `_clone` API に置換 (near-instant) | IT-BLUEGREEN-06 |
| `b5a8d6c` | Blue-Green alias swap によるゼロダウンタイム ES 更新 | IT-ES-01, IT-ES-03, IT-ES-05, IT-BLUEGREEN-01, IT-BLUEGREEN-02, IT-BLUEGREEN-03, IT-BLUEGREEN-04, IT-BLUEGREEN-05, IT-BLUEGREEN-06, IT-PIPELINE-04 |
| `95561cf` | insdc-rdf を使った RDF 変換パイプライン追加 | IT-RDF-01, IT-RDF-02, IT-RDF-03, IT-RDF-04 |
| `b8b91d9` | JGA hum-id 抽出を XML から TSV に切替、jga-dataset の hum-id 関連追加 | IT-DBLINK-01 (`create_dblink_jga_relations` 経由)、IT-DBLINK-07 (preserved TSV 系の入力ソース) |
| `b7dad59` | run_pipeline.sh の default max parallel を 4 → 16 | IT-PIPELINE-01, IT-PIPELINE-06 |
| `2154204` | INSDC-BP / INSDC-BS の preserved TSV を dblink パイプラインに追加 | IT-DBLINK-06, IT-DBLINK-07, IT-JSONL-07 |
| `dd3895e` | properties 配下の dict children を常に list wrapper に正規化 | IT-JSONL-02 |
| `0d954bd` | ES bulk insert で JGA SECONDARY_ID の sameAs alias doc を生成 | IT-ES-07, IT-JSONL-04 |
| `8308148` | status 値を INSDC 内部表現からユーザー向けラベルに改名 (live→public 等) | IT-JSONL-09 |
| `8a048db` | PostgreSQL 接続に TCP keepalive、fetch 結果の materialize、date_cache DB テスト追加 | IT-RESOURCE-02, IT-RESOURCE-03, IT-JSONL-09 |
| `12bbf40` | INSDC URL を NCBI nuccore から DDBJ getentry endpoint に変更 | IT-JSONL-10 |
| `2566668` | GEA URL を `SEARCH_BASE_URL` ではなくハードコード base URL に修正 | IT-JSONL-10 |
| `6fd5cc2` | ファイル移動を `os.replace` から `Path.replace` に | IT-DBLINK-05 |
| `3d0d1be` | DuckDB 読み取り専用 open + atomic file swap (`os.replace`) | IT-DBLINK-05 |
| `9fe9eb8` | sameAs mapping を disabled object から searchable nested type に | IT-JSONL-04, IT-ES-07 |
| `8f635c9` | NCBI FTP streaming にバックオフ付きリトライ追加 | IT-RESOURCE-04 |
| `e50213e` | ES JVM heap を 64g から 31g に削減 (compressed oops 閾値内) | IT-ES-08 |
| `4bac02d` | `deduplicate_relations` クエリに ORDER BY 追加 (deterministic 出力) | IT-DBLINK-01, IT-DBLINK-03 |
| `3cae572` | ES 接続 timeout のバックオフ付きリトライ追加、default request timeout を増加 | IT-ES-02 (bulk insert retry) |
| `17a80e0` | `dbXrefs` を `--include-dbxrefs` で opt-in 化 | IT-JSONL-08 |
| `bde668b` | bulk insert の partial failure を fatal error から warning に格下げ | IT-ES-02, IT-ES-04, IT-ES-06 |
| `acd1a53` | bulk error info の non-serializable 値を再帰的に sanitize | IT-ES-04, IT-ES-06 |
| `9d3959a` | bulk error info の非 dict 値を sanitize するヘルパ追加 | IT-ES-04, IT-ES-06 |
| `deb4166` | parent / child BioProject relationship を ontology に追加 | IT-UMBRELLA-01, IT-UMBRELLA-05, IT-JSONL-11 |
| `6dda94c` | Umbrella BioProject を専用 DB に分離 (parent-child relations) | IT-UMBRELLA-01, IT-UMBRELLA-02, IT-UMBRELLA-03, IT-UMBRELLA-04, IT-UMBRELLA-05, IT-JSONL-11, IT-JSONL-12 |
| `c80527a` | production / staging の TRAD PostgreSQL ホストを a011 → a012 に変更 | IT-RESOURCE-02 |
| `35ce294` | TRAD PostgreSQL から INSDC sequence accession 関連を取得、postgres_url を xsm/trad に分割 | IT-RESOURCE-02 |
| `d9336e3` | JSONL に distribution URL と DRA file index 追加 | IT-JSONL-10 |
| `551cf0d` | BioProject / BioSample livelist の test fixture 追加 (status cache) | IT-JSONL-09 |
| `e358859` | livelist ファイルから BP / BS status cache を構築 | IT-JSONL-09 |
| `6bf545b` | SRA accession date カラムに TIMESTAMPTZ を採用 | IT-RESOURCE-03, IT-DIFF-03 |
