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

```
python bp_xml2json.py bioprojxt_xml_path
```