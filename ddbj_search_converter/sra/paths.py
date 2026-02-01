"""SRA tar ファイル関連のパス構築ユーティリティ。"""
from pathlib import Path

from ddbj_search_converter.config import SRA_TAR_DIR_NAME, Config


def get_sra_tar_dir(config: Config) -> Path:
    """SRA tar ファイルを格納するディレクトリを取得する。"""
    return config.const_dir.joinpath(SRA_TAR_DIR_NAME)
