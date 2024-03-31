import csv
import sys
import itertools
import sqlite3
from collections import defaultdict
from typing import NewType, List

FilePath = NewType('FilePath', str)
xref_max_size = 25
# Todo: 取得したexperimentでexperiment->runの関係を追加するか検討

def get_relation(accessions_db: FilePath, bioproject: str) -> dict:
    """_summary_
    静的ファイルからの関係情報の取得が遅すぎるためsra_accessionsをsqliteから読み込む
    Args:
        sra_accessions (FilePath): _description_
        bioproject (str): _description_

    Returns:
        dict: _description_
    """
    xref_dct = defaultdict(set)
    table = 'sra_accessions'
    con = sqlite3.connect(accessions_db)
    cur = con.cursor()
    ## 巨大なxrefsが生成されるケースがあるため、取得サイズの上限をmax_sizeで設定する
    #q = f'select Submission,Experiment,Sample,Study,BioSample from {table} WHERE BioProject="{bioproject}" limit {xref_max_size}'
    q = f'select Accession,Type,BioSample from {table} WHERE BioProject="{bioproject}" LIMIT {xref_max_size}'
    cur.execute(q)
    xrefs = cur.fetchall()
    # 以下内包表記をオブジェクトの長さ分実行する方法で記述できる。速度は要検討。
    # Todo: 検討：biosample以外はaccessionから取得した方が良いかもしれない
    for r in xrefs:
        xref_dct[r[1].lower()].add(r[0])
        if r[2] !="-":
            xref_dct["biosample"].add(r[2])

    '''
    for r in xrefs:
        if r[0] != "-":
            xref_dct["submission"].add(r[0])
        if r[1] != "-":
            xref_dct["experiment"].add(r[1])
        if r[2] != "-":
            xref_dct["sample"].add(r[2])
        if r[3] != "-":
            xref_dct["study"].add(r[3])
        if r[4] != "-":
            xref_dct["biosample"].add(r[4])
    '''
    return dict(xref_dct)


if __name__ == "__main__":
    db = '/mnt/dra/sra_accessions_3.db'
    accession = sys.argv[1]
    print(get_relation(db, accession))
    #xref = get_xref(sra_accessions_path, accession)
