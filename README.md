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

1. bioproject.xml, SRA_Accessions.tabをダウンロードする。
__それぞれのファイルの準備方法については検討__

2. SRA_Accessions.tabをsqliteのtableに変換（60min程度の処理時間）
sliteはdb, table共にあらかじめ用意しておく必要は無い

```
cd ./src
python import_sra_accessions.py <SRA_Accessions file paht> <accessions_db_path>
```

3. bioproject.xmlをdictに変換すると同時にsra_accessionsより関係データを生成しElasticsearchにバルクインサートする（またはjsonlに書き出す）

```
python bp_xml2es.py <bioproject_xml_path> <accessions_db_path> 
```

インポートするxmlがbioproject.xmlでもddbj_core_bioproject.xmlでも自動的にファイルのタイプに合わせた処理が行われます。


4.1 Elasticsearchへjsonlからbulkインサートを行う場合

bp_xml2esはpythonから直接データをElasticsearchにバルクインサートするため、bulk apiを叩く必要はないが、
個別にjsonlを挿入したい場合下記のスクリプトを使う

```
curl -H "Content-Type: application/json" -X POST http://localhost:9200/_bulk?pretty --data-binary @bioproject.jsonl
```

ddbj_core_bioproject.xmlから生成したbioproject.jsonlのファイルサイズはbulk APIの制限を超えないため、ファイルをそのままimportする（index名はjsonlのヘッダ行に含まれるので指定しない）。

bp_xml2es.pyからjsonlをファイルとして書き出すには以下の２つの方法をとることができる。

a. xml2jsonl関数で処理の最後にdict2jsonl()を追加する。この関数はElasticsearchにbulkインサートするオブジェクトと同様のオブジェクトをファイルとして書き出す
b. もしくはprint文でbulkインサートするオブジェクトを出力し、xml2es.pyの出力をファイルにリダイレクトすることでjsonlをファイルとして残すことができる


4.2 通常bioproject.jsonlのファイルサイズは100Mを超えるため、シェルスクリプトで適度なサイズに分割してからし分割したファイルごとにbulk importする

```
cd src/batch
# 分割
sh split.sh
# bulk import
sh bulk_import.sh
```


## 要確認
- dbXrefのulrがlocaolhost:8080/resource/biosample/SAME*等でありこのリンクは有効なのだがどの段階でxmlのサブセットを生成しているのか・必要なのか
- DDBJ ES共通項目のVisibilityについて（何を参照しているか）
- ES共通項目のurl, downloadurl, distributionについて（localhostの参照は意味があるのか、メンテナンスされているか）
