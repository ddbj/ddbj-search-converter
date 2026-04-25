"""ES bulk 操作で共有する error info サニタイズ。

bulk_insert / bulk_delete はどちらも `helpers.bulk` / `helpers.parallel_bulk` を
`raise_on_exception=False` で動かしており、`failed` の戻り値に `ApiError` 等の
non-JSON-serializable オブジェクトが混じる場合がある。`BulkInsertResult` /
`BulkDeleteResult` を `model_dump_json()` するときに死なないよう、
JSON-safe な dict に再帰的に変換する。
"""

from typing import Any


def sanitize_value(value: Any) -> Any:
    """Recursively convert non-serializable values to JSON-safe types."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {k: sanitize_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_value(item) for item in value]
    return str(value)


def sanitize_error_info(info: Any) -> dict[str, Any]:
    """Convert bulk error info to a JSON-serializable dict.

    `info` が dict なら値を再帰的に sanitize、それ以外 (Exception など) は
    `{"error_type": ..., "error_message": ...}` 形式に正規化する。
    """
    if isinstance(info, dict):
        return {k: sanitize_value(v) for k, v in info.items()}
    return {"error_type": type(info).__name__, "error_message": str(info)}
