# ddbj-search-converter


## 開発環境の構築

```
cd ddbj-search-converter
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## xmltojsonlコンパーターの使い方

### BioProject

dbXrefsを付加するためSRA_Accessions.tabをsqliteに保存する必要がある.sqliteは空で良い

```
python import_sra_accessions.py <SRA_Accessions.tab> <sqlite_db_name>
```


bioproject.xmlからarchive="DDBJ"のエントリのみをjsonlに変換する場合（センター名はオプション）

```
source .venv/bin/activate
python bp_xml2json.py <bioproject_xml_path> <accessions_db_path> ddbj
```


## 確認事項
 1. dbXrefのulrがlocaolhost:8080/resource/biosample/SAME*等でありこのリンクは有効なのだがどの段階でxmlのサブセットを生成しているのか・必要なのか
 2. SRA_AccessionsのStatus, VisibilityとESのstatus, visibilityの値の記法が異なるがどのソースのどの項目を参照しているのか
