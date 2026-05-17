"""distribution 生成ヘルパーモジュール。

各エントリータイプの distribution リスト生成ロジックを 1 箇所に集約する。
"""

from ddbj_search_converter.config import DRA_PUBLIC_BASE_URL
from ddbj_search_converter.jsonl.utils import build_search_entry_url
from ddbj_search_converter.schema import Distribution


def _json_dist(entry_type: str, accession: str) -> Distribution:

    return Distribution(
        type="DataDownload",
        encodingFormat="JSON",
        contentUrl=build_search_entry_url(entry_type, accession, "json"),
    )


def _jsonld_dist(entry_type: str, accession: str) -> Distribution:

    return Distribution(
        type="DataDownload",
        encodingFormat="JSON-LD",
        contentUrl=build_search_entry_url(entry_type, accession, "jsonld"),
    )


def make_bp_distribution(accession: str) -> list[Distribution]:
    """BioProject の distribution を生成する。"""

    return [
        _json_dist("bioproject", accession),
        _jsonld_dist("bioproject", accession),
    ]


def make_bs_distribution(accession: str) -> list[Distribution]:
    """BioSample の distribution を生成する。"""

    return [
        _json_dist("biosample", accession),
        _jsonld_dist("biosample", accession),
    ]


def make_jga_distribution(index_name: str, accession: str) -> list[Distribution]:
    """JGA の distribution を生成する。"""

    return [
        _json_dist(index_name, accession),
        _jsonld_dist(index_name, accession),
    ]


def make_gea_distribution(accession: str) -> list[Distribution]:
    """GEA の distribution を生成する。"""

    return [
        _json_dist("gea", accession),
        _jsonld_dist("gea", accession),
    ]


def make_metabobank_distribution(accession: str) -> list[Distribution]:
    """MetaboBank の distribution を生成する。"""

    return [
        _json_dist("metabobank", accession),
        _jsonld_dist("metabobank", accession),
    ]


def _mirrored_sra_url(experiment: str, run: str) -> str:
    """他極 (NCBI/EBI) origin の run accession に対する DDBJ ミラー上の .sra URL を組み立てる。

    パス内に `sralite/litesra` が入るが実体は `.sra` ファイル。NCBI (`SRX*`) と EBI
    (`ERX*`) で path 構造は同一で、experiment の最初 3 文字を第 3 階層に使う。
    """
    return f"{DRA_PUBLIC_BASE_URL}/sralite/ByExp/litesra/{experiment[:3]}/{experiment[:6]}/{experiment}/{run}/{run}.sra"


def make_sra_distribution(
    entry_type: str,
    identifier: str,
    *,
    is_ddbj_origin: bool,
    sra_type: str,
    submission: str,
    experiment: str | None = None,
    fastq_dirs: set[str] | None = None,
    sra_file_runs: set[str] | None = None,
    analysis_dirs: set[str] | None = None,
) -> list[Distribution]:
    """SRA の distribution を生成する。

    Args:
        entry_type: SRA entry type (e.g. "sra-run")
        identifier: accession
        is_ddbj_origin: DDBJ-origin (DRA/DRR/DRX/DRZ/DRS/DRP) かどうか
        sra_type: XML type (e.g. "run")
        submission: submission accession
        experiment: experiment accession (run の場合のみ)
        fastq_dirs: FASTQ ディレクトリが存在する experiment の集合 (DRA-origin only)
        sra_file_runs: .sra ファイルが存在する run の集合 (DRA-origin only)
        analysis_dirs: analysis ディレクトリが存在する DRZ accession の集合 (DRA-origin only)
    """
    dists: list[Distribution] = [
        _json_dist(entry_type, identifier),
        _jsonld_dist(entry_type, identifier),
    ]

    # XML は全 origin で共通: tar 由来の entry なら NIG ミラーにも同じ submission 配下の XML がある
    sub_prefix = submission[:6]
    dists.append(
        Distribution(
            type="DataDownload",
            encodingFormat="XML",
            contentUrl=f"{DRA_PUBLIC_BASE_URL}/fastq/{sub_prefix}/{submission}/{submission}.{sra_type}.xml",
        )
    )

    if sra_type == "analysis":
        # DRA-origin の analysis ディレクトリ landing page (実在チェック済のみ)
        if is_ddbj_origin and analysis_dirs is not None and identifier in analysis_dirs:
            dists.append(
                Distribution(
                    type="DataDownload",
                    encodingFormat="DATA",
                    contentUrl=f"{DRA_PUBLIC_BASE_URL}/fastq/{sub_prefix}/{submission}/{identifier}/",
                )
            )
        return dists

    # FASTQ / SRA は run のみ
    if sra_type != "run" or experiment is None:
        return dists

    if is_ddbj_origin:
        # DRA: file index で実在チェックしたものだけリンク化
        if fastq_dirs is not None and experiment in fastq_dirs:
            dists.append(
                Distribution(
                    type="DataDownload",
                    encodingFormat="FASTQ",
                    contentUrl=f"{DRA_PUBLIC_BASE_URL}/fastq/{sub_prefix}/{submission}/{experiment}/",
                )
            )

        if sra_file_runs is not None and identifier in sra_file_runs:
            exp_prefix = experiment[:6]
            dists.append(
                Distribution(
                    type="DataDownload",
                    encodingFormat="SRA",
                    contentUrl=(
                        f"{DRA_PUBLIC_BASE_URL}/sra/ByExp/sra/DRX/"
                        f"{exp_prefix}/{experiment}/{identifier}/{identifier}.sra"
                    ),
                )
            )
    else:
        # NCBI/EBI: ミラー側にあるか不明なため URL を機械生成 (404 容認)
        dists.append(
            Distribution(
                type="DataDownload",
                encodingFormat="SRA",
                contentUrl=_mirrored_sra_url(experiment, identifier),
            )
        )

    return dists
