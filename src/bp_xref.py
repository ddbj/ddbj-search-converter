import csv
import sys
import itertools
from collections import defaultdict
from typing import NewType, List

FilePath = NewType('FilePath', str)
sra_accessions_path = "./sample/SRA_Accessions_1000.tab"
batch_size = 50
bp_column = 18

relative_column = [
    {"type": "submission", "col": 1},
    {"type": "experiment", "col": 10},
    {"type": "sample", "col": 11},
    {"type": "study", "col": 12},
    {"type": "biosample", "col": 17}
]



xref_dct = defaultdict(set)

def get_xref(accessions: FilePath, bioproject: str) -> dict:
    """_summary_
    SRA_Accessionsより指定するBioProjectの関連するSRAを取得する
    Args:
        accessions (FilePath): _description_
    """
    with open(accessions, "r") as f:
        reader = csv.reader(f, delimiter="\t")
        # readerをbatch_size行ごと処理する
        for row in itertools.islice(reader, None):
            if row[bp_column] == bioproject:
                for col in relative_column:
                    acc = row[col["col"]]
                    if acc != "-":
                        xref_dct[col["type"]].add(acc)
    return dict(xref_dct)
                    

if __name__ == "__main__":
    accession = sys.argv[1]
    xref = get_xref(sra_accessions_path, accession)
    print(xref)