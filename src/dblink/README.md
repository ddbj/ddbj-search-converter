# 関係情報の取得と書き出しについて

## jga関係情報

jgaのjga-dac, jga-dataset, jga-policy, jga-studyの各オブジェクト間の関係情報は
src/dblink/jga_converter/jga_xml2jsonl.pyを実行する際に生成されsqliteに保存されます。

jga_xml2jsonl.pyは以下のモジュールをjsonl変換の前に関係情報生成のために実行します。

- dbj-search-converter/src/dblink/create_jga_relation_db
- dbj-search-converter/src/dblink/create_jga_relation_table


### jga関係情報の生成に必要なファイル

jgaの関係情報の生成は次のファイルを利用します。

- analysis-study-relation.csv
- dataset-analysis-relation.csv
- dataset-policy-relation.csv
- policy-dac-relation.csv
- experiment-study-relation.csv
- data-experiment-relation.csv
- dataset-data-relation.csv

### jga関係情報の保存

関係情報は"RELATION_DB_PATH"で指定したsqliteの次のテーブルに保存されます。

- dataset_policy_relation.csv
- dataset_dac_relation.csv
- dataset_study_relation.csv
- study_dac_relation.csv
- study_policy_relation.csv
- policy_dac_relation.csv

### dataset-studyの関係について

dataset-studyは以下の二種類の関係情報をマージして生成しています。
- dataset-analysis-study
- dataset-data-experiment-study

## bioproject_umbrella2bioprojectの関係情報

BioProjectのumbrellaの親子関係についてはbioproject.xmlのProjectLinks.Linkから取得しています。

ProjectLinksには複数のLinkが含まれますが、bioproject-umbrellaの関係情報とし取得しているのは、
```Hierarchical[@type==TopAdmin]```のLinkの
ProjectIDRefとMemberIDのaccessionです。

```
<ProjectLinks>
<Link>
    <ProjectIDRef archive="NCBI" id="9616" accession="PRJNA9616"/>
    <Hierarchical type="TopSingle">
    <MemberID archive="NCBI" id="10735" accession="PRJNA10735"/>
    </Hierarchical>
</Link>
<Link>
    <ProjectIDRef archive="NCBI" id="9616" accession="PRJNA9616"/> # child
    <Hierarchical type="TopAdmin">
    <MemberID archive="NCBI" id="46297" accession="PRJNA46297"/> # parent
    </Hierarchical>
</Link>
<Link>
    <ProjectIDRef archive="NCBI" id="51583" accession="PRJNA51583"/>
    <PeerProject>
    <CommonInputData>eRefseqGenbank</CommonInputData>
    <MemberID archive="NCBI" id="9616" accession="PRJNA9616"/>
    </PeerProject>
</Link>
</ProjectLinks>
```

### 処理の実行

src/dblink/create_bioproject_relation.pyを実行すると、
sqliteにユニークなbioproject-umbrellaのペアがchild-parentカラムに保存され、
全てのペアが保存された後にcsvを書き出します。

### privateなBioProjectの除外

bioproject-umbrellaにはbioproject.xmlに含まれないaccessionが含まれます。
primaryがbioproject.xmlに含まれない場合はprivateなaccessionであると判断し、
保存する情報から除きます。