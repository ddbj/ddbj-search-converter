# ddbj-search-converter


## 環境の構築

```
cd ddbj-search-converter
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 利用方法

### bioproject.jsonlの生成とElasticSearchへのインポート

1. bioproject.xml, SRA_Accessions.tabをダウンロードする
__それぞれのファイルの準備方法については検討__

2. SRA_Accessions.tabをsqliteのtableに変換（60min程度の処理時間）
sliteはdb, table共にあらかじめ用意しておく必要は無い

```
cd ./src
python import_sra_accessions.py <SRA_Accessions file paht> <sqlite_db_path>
```

3. bioproject.xmlをdictに変換すると同時にsra_accessionsより関係データを生成しjsonlに書き出す

```
python bp_xml2json.py <bioproject_xml_path> <accessions_db_path> 
```

bioproject.xmlからarchive="DDBJ"のエントリのみをjsonlに変換する場合はセンター名をオプションとして追記する
```
python bp_xml2json.py <bioproject_xml_path> <accessions_db_path> ddbj
```

4. Elasticsearchへインポート

DDBJに限定したのBioProjectの場合は生成したbioproject.jsonlのファイルサイズがbulk APIの制限を超えないため、ファイルをそのままimportする（index名はjsonlのヘッダ行に含まれるので指定しない）。

```
curl -H "Content-Type: application/json" -X POST http://localhost:9200/_bulk?pretty --data-binary @bioproject.jsonl
```

センターを指定しない場合bioproject.jsonlが100Mを超えるため適度なサイズに分割してからbulk importする


## 要確認
- dbXrefのulrがlocaolhost:8080/resource/biosample/SAME*等でありこのリンクは有効なのだがどの段階でxmlのサブセットを生成しているのか・必要なのか
- DDBJ ES共通項目のVisibilityについて（何を参照しているか）
- ES共通項目のurl, downloadurl, distributionについて（localhostの参照は意味があるのか、メンテナンスされているか）
