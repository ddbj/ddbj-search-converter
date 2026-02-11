"""tar インデックスのファイルキャッシュ。

並列 tar 読み込みで、各 worker がインデックスを再構築するのを避けるため、
インデックスを pickle でキャッシュする。
"""

import pickle
import tarfile
from pathlib import Path
from typing import Any, cast

from ddbj_search_converter.logging.logger import log_info


def get_index_cache_path(tar_path: Path) -> Path:
    """インデックスキャッシュファイルのパスを返す。"""
    return tar_path.with_suffix(".tar.index.pkl")


def save_index_cache(tar_path: Path, members: dict[str, tarfile.TarInfo]) -> None:
    """インデックスをキャッシュファイルに保存する。"""
    cache_path = get_index_cache_path(tar_path)
    log_info(f"saving tar index cache: {cache_path}")

    # TarInfo から必要な属性のみ抽出（軽量化）
    index_data = {
        name: {
            "name": info.name,
            "offset": info.offset,
            "offset_data": info.offset_data,
            "size": info.size,
        }
        for name, info in members.items()
    }

    with cache_path.open("wb") as f:
        pickle.dump(index_data, f, protocol=pickle.HIGHEST_PROTOCOL)

    log_info(f"tar index cache saved: {len(members)} entries")


def load_index_cache(tar_path: Path) -> dict[str, dict[str, Any]] | None:
    """キャッシュからインデックスを読み込む。

    キャッシュが存在しない、または tar より古い場合は None を返す。
    """
    cache_path = get_index_cache_path(tar_path)

    if not cache_path.exists():
        return None

    # キャッシュが tar より古い場合は無効
    if cache_path.stat().st_mtime < tar_path.stat().st_mtime:
        log_info("tar index cache is stale, will rebuild")
        return None

    log_info(f"loading tar index cache: {cache_path}")
    with cache_path.open("rb") as f:
        index_data = cast("dict[str, dict[str, Any]]", pickle.load(f))
    log_info(f"tar index cache loaded: {len(index_data)} entries")

    return index_data


def is_cache_valid(tar_path: Path) -> bool:
    """キャッシュが有効かどうかを判定する。"""
    cache_path = get_index_cache_path(tar_path)
    if not cache_path.exists():
        return False
    return cache_path.stat().st_mtime >= tar_path.stat().st_mtime
