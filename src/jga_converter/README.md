# jga_converterの利用方法

##　jga converterの起動 

```
cd ~/tasks/ddbj-search-converter/src

# venvを仮想環境として利用する場合
cd jga_converter
source .venv/bin/activate
cd ..

python3 jga_converter/jga_xml2jsonl.py
```

## jga関連の関係データの更新
- dbj-search-converter/src/dblink/create_jga_relation_db
- dbj-search-converter/src/dblink/create_jga_relation_table
以上のモジュールによってjgaの関係データをsqliteに保存します。

上記はjga_xml2jsonl.pyによるjagデータ更新時に

```
create_jga_relation_db.create_jga_relation(LOCAL_FILE_PATH, RELATION_DB_PATH)
```
のようにモジュールとして呼び出されjga-datasetとjga-studyのrelation情報をcsvファイルから生成しsqliteに保存します。

