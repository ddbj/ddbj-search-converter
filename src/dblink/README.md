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

### privateなBioProjectの除外

bioproject-umbrellaにはbioproject.xmlに含まれないaccessionが含まれます。
primaryがbioproject.xmlに含まれない場合はprivateなaccessionであると判断し、
保存する情報から除外します。


### bioproject_umbrella2bioproject.csv書き出し処理の実行

実行方法
```
cd /home/w3ddbjld/tasks/ddbj-search-converter/src
source .venv/bin/activate
python dblink/create_bioproject_relation.py
```

取得方法
bioproject.xmlのProjectLinks.Link.Hierarchical@type == "TopSingle" である場合
```
member_id =  ProjectLinks.Link.Hierarchical.MemberID@accession -> member_id
project_id =  ProjectLinks.Link@accession -> project_id -> project_id
```
とする

書き出し先
- sqlite: /home/w3ddbjld/tasks/relations/bioproject_relation.sqlite
    - table:  "bioproject_umbrella2bioproject"
    - INSERT INTO bioproject_umbrella2bioproject (child, parent) VALUES (project_id, mamber_id);
- file: /home/w3ddbjld/tasks/relations/bioproject_umbrella2bioproject.csv
```
child, parent
project_id, member_id
...
```


