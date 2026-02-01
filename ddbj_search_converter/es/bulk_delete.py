"""Elasticsearch からドキュメントを一括削除するモジュール。"""
from typing import Any, Dict, Iterator, List, Set

from pydantic import BaseModel

from elasticsearch import helpers

from ddbj_search_converter.config import Config
from ddbj_search_converter.es.client import get_es_client
from ddbj_search_converter.logging.logger import log_info


class BulkDeleteResult(BaseModel):
    """削除操作の結果。"""

    index: str
    total_requested: int
    success_count: int
    not_found_count: int
    error_count: int
    errors: List[Dict[str, Any]]


def generate_delete_actions(
    accessions: Set[str],
    index: str,
) -> Iterator[Dict[str, Any]]:
    """削除アクションを生成する。"""
    for accession in accessions:
        yield {
            "_op_type": "delete",
            "_index": index,
            "_id": accession,
        }


def bulk_delete_by_ids(
    config: Config,
    index: str,
    accessions: Set[str],
    batch_size: int = 1000,
) -> BulkDeleteResult:
    """指定された accession を ES から一括削除。

    - 404 Not Found は not_found としてカウント（エラーにしない）
    - インデックスが存在しない場合もエラーにしない
    """
    if not accessions:
        return BulkDeleteResult(
            index=index,
            total_requested=0,
            success_count=0,
            not_found_count=0,
            error_count=0,
            errors=[],
        )

    es_client = get_es_client(config)

    # インデックスの存在確認
    if not es_client.indices.exists(index=index):
        log_info(f"index {index} does not exist, skipping")
        return BulkDeleteResult(
            index=index,
            total_requested=len(accessions),
            success_count=0,
            not_found_count=len(accessions),
            error_count=0,
            errors=[],
        )

    actions = generate_delete_actions(accessions, index)

    errors: List[Dict[str, Any]] = []
    not_found_count = 0

    # helpers.bulk で一括削除
    # raise_on_error=False で 404 を許容
    success, failed = helpers.bulk(
        es_client,
        actions,
        chunk_size=batch_size,
        stats_only=False,
        raise_on_error=False,
        max_retries=3,
        request_timeout=300,
    )

    if isinstance(failed, list):
        for err in failed:
            delete_info = err.get("delete", {})
            if delete_info.get("status") == 404:
                not_found_count += 1
            else:
                errors.append(err)

    return BulkDeleteResult(
        index=index,
        total_requested=len(accessions),
        success_count=success,
        not_found_count=not_found_count,
        error_count=len(errors),
        errors=errors[:100],  # 最大 100 件までエラーを保持
    )
