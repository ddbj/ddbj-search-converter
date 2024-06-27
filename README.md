# ddbj-search-converter

- biosample_set.xmlをjsonlに変換しElasticsearchに投入してデータベースを更新する
- bioproject.xmlをjsonl変換しElasticsearchに投入してデータベースを更新する

## 環境の構築

```
cd ddbj-search-converter/src
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## biosample_set.xmlのjsonl変換とElasticsearch更新手順

### 1. 環境の準備

```
cd ./ddbj-search-converter/src
source .venv/bin/activate
cd biosample_converter
```

### 2. xmlを分割する

```
 ./split_xml.pl <biosapmle_set.xmlのパス> <分割したファイルを保存するディレクトリのパス>
```

- perlでxmlを3万件のレコード毎分割しファイルに保存します。

### 3. xmlをjsonlに変換する

```
python bs_xml2jsonl_mp.py <分割したxmlが保存されたディレクトリのパス>  <変換したjsonlを書き出すディレクトリのパス>
```
- xmlをjsonl変換する処理をPythonのマルチプロセスで実行します
- 現在並列数は32に設定されていますがこの値は環境に合わせて置き換えてください


### 4. jsonlをElasticsearchにbulk insertする

```
python bs_bulk_insert.py <xmlが分割保存された一つ前の操作のディレクトリのパス> <xmlが分割保存された最新のディレクトリのパス>
```
- jsonlの保存されたディレクトリを指定しbulk apiを利用してElasticsearchにデータを保存します
- Elasitcsearchのbulk APIへのリクエストはPythonのマルチプロセスで実行します
- 現在並列数は32に設定されていますがこの値は環境に合わせて置き換えてください
- Elasticsearchにドキュメントが含まれない状態からの最初の登録の場合最新のディレクトリと一つ前のディレクトリに同じディレクトリを指定し "-f"をつけて実行してください（この仕様は変更する可能性があります）



## bioproject.xmlのjsonl変換とElasticsearchの更新手順

### 1. 環境の準備

```
cd ./ddbj-search-converter/src
source .venv/bin/activate
cd biosample_converter
```

### 2. bioproject.jsonlの生成

```
python bp_xml2jsonl.py  /usr/local/resources/bioproject/bioproject.xml bioproject.jsonl
```

### 3. JSONLを分割

```
python split_jsonl.py bioproject.jsonl <分割したファイルを書き出すディレクトリ>
```
- <分割したファイルを書き出すディレクトリ>に作業日をディレクトリ名としたディレクトリが作られ分割されたファイルが書き出される

### 4. ファイルの差分リストを生成しElasticsearchにbulk insertする

```
python bulk_insert_renewals.py {日付1} {日付2}
```
- 作業日と前回の作業日を引数に与えるとその日付のディレクトリから差分ファイルを抽出してElasticsearchにbulk insertする
- 最新のディレクトリとその直前の作業日のディレクトリの指定は、引数で渡さなくても自動的に取得されるよう改修予定です


