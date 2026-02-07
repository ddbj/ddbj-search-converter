# JSON-LD Context / Vocabulary 定義

Elasticsearch に投入する各ドキュメント型 (`ddbj_search_converter/schema.py`) に対して、JSON-LD の `@context` ファイルと RDF vocabulary 定義ファイルを提供する。

## ファイル構成

各データベースに対して 2 種類のファイルがある。

- **`.jsonld` (Context)**: JSON のキー名を URI にマッピングする辞書。詳細は各ファイルを参照。
- **`.ttl` (Vocabulary)**: 独自プロパティの意味定義 (ラベル、説明、プロパティの型)。詳細は各ファイルを参照。

## 名前空間

| データベース | 名前空間 URI | プレフィックス |
|-------------|-------------|--------------|
| BioProject | `http://ddbj.nig.ac.jp/ontologies/bioproject/` | `bioproject:` |
| BioSample | `http://ddbj.nig.ac.jp/ontologies/biosample/` | `biosample:` |
| SRA | `http://ddbj.nig.ac.jp/ontologies/sra/` | `sra:` |
| JGA | `http://ddbj.nig.ac.jp/ontologies/jga/` | `jga:` |

各 `.jsonld` で `@vocab` としてデフォルト名前空間を設定しているため、独自プロパティは context 内で明示しなくても自動的に各名前空間の URI に展開される。

## 使用している外部語彙

| 語彙 | プレフィックス | URI |
|------|--------------|-----|
| Schema.org | `schema:` | `https://schema.org/` |
| Dublin Core Terms | `dcterms:` | `http://purl.org/dc/terms/` |
| RDFS | `rdfs:` | `http://www.w3.org/2000/01/rdf-schema#` |

## デプロイに関する注意

名前空間 URI (例: `http://ddbj.nig.ac.jp/ontologies/bioproject/`) はオントロジー識別子として使用しているが、現時点でこの URL に vocabulary ファイルは公開されていない。

- **vocabulary のデプロイ**: `.ttl` ファイルを `http://ddbj.nig.ac.jp/ontologies/` 以下に配置し、名前空間 URI を解決可能 (dereferenceable) にする想定。DDBJ の既存オントロジー (`nucleotide.ttl`, `taxonomy.ttl`) と同じ運用。
- **リポジトリの移動**: 本ディレクトリのファイルは将来 [ddbj/rdf](https://github.com/ddbj/rdf) リポジトリに移管する可能性がある。ddbj/rdf には旧実装の BioProject / BioSample context が存在しており、本ファイル群はそれらの後継にあたる。
