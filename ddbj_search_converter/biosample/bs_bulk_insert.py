"""\
- bp_bulk_insert の実装をそのまま流用する。
- es の index 情報に関しては、jsonl の中に含まれるため、bioproject と biosample で共通の処理を行うことができる。
"""
from ddbj_search_converter.bioproject.bp_bulk_insert import \
    main as bp_bulk_insert_main


def main() -> None:
    bp_bulk_insert_main()


if __name__ == "__main__":
    main()
