"""distribution 生成ヘルパーモジュール。

各エントリータイプの distribution リスト生成ロジックを 1 箇所に集約する。
"""

from ddbj_search_converter.config import DRA_PUBLIC_BASE_URL, SEARCH_BASE_URL
from ddbj_search_converter.schema import Distribution


def _json_dist(entry_type: str, accession: str) -> Distribution:

    return Distribution(
        type="DataDownload",
        encodingFormat="JSON",
        contentUrl=f"{SEARCH_BASE_URL}/search/entry/{entry_type}/{accession}.json",
    )


def _jsonld_dist(entry_type: str, accession: str) -> Distribution:

    return Distribution(
        type="DataDownload",
        encodingFormat="JSON-LD",
        contentUrl=f"{SEARCH_BASE_URL}/search/entry/{entry_type}/{accession}.jsonld",
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


def make_sra_distribution(
    entry_type: str,
    identifier: str,
    *,
    is_dra: bool,
    sra_type: str,
    submission: str,
    experiment: str | None = None,
    fastq_dirs: set[str] | None = None,
    sra_file_runs: set[str] | None = None,
) -> list[Distribution]:
    """SRA の distribution を生成する。

    Args:
        entry_type: SRA entry type (e.g. "sra-run")
        identifier: accession
        is_dra: DRA かどうか
        sra_type: XML type (e.g. "run")
        submission: submission accession
        experiment: experiment accession (run の場合のみ)
        fastq_dirs: FASTQ ディレクトリが存在する experiment の集合
        sra_file_runs: .sra ファイルが存在する run の集合
    """
    dists: list[Distribution] = [
        _json_dist(entry_type, identifier),
        _jsonld_dist(entry_type, identifier),
    ]

    if not is_dra:
        return dists

    # DRA: XML を追加
    sub_prefix = submission[:6]
    dists.append(
        Distribution(
            type="DataDownload",
            encodingFormat="XML",
            contentUrl=f"{DRA_PUBLIC_BASE_URL}/fastq/{sub_prefix}/{submission}/{submission}.{sra_type}.xml",
        )
    )

    # DRA run: FASTQ と SRA を追加
    if sra_type == "run" and experiment is not None:
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

    return dists
