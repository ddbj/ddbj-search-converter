# ddbj-search-converter


## 環境の構築

```
cd ddbj-search-converter
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 利用方法

### bioproject.jsonlの生成

```
cd ./tasks/ddbj-search-converter/src
source .venv/bin/activate
python bp_xml2jsonl.py  /usr/local/resources/bioproject/bioproject.xml bioproject.jsonl
```

### JSONLを分割

```
python split_jsonl.py bioproject.jsonl ../../bioproject_jsonl
```
bioproject_json/に作業日付いたディレクトリが作られ分割されたファイルが書き出される

### ファイルの差分リストを生成しElasticsearchにbulk insertする

```
python bulk_insert_renewals.py {日付1} {日付2}
```
作業日と前回の作業日を引数に与えるとその日付のディレクトリから差分ファイルを抽出してElasticsearchにbulk insertする


