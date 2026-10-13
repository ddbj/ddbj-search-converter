"""humandbs の API から NDJSON を取得する。"""

import time

import httpx

from ddbj_search_converter.logging.logger import log_warn

HUMANDBS_FETCH_MAX_ATTEMPTS = 3
HUMANDBS_FETCH_RETRY_WAIT_SECONDS = 10
HUMANDBS_FETCH_TIMEOUT_SECONDS = 60.0


class HumandbsResponseError(Exception):
    """humandbs の応答が期待する NDJSON として読めない。"""


def get_humandbs_text(url: str) -> str:
    """humandbs の API を GET して本文を返す。

    接続エラー・timeout・5xx は retry する。それ以外の 2xx でない応答はすぐに raise する。

    Raises:
        httpx.HTTPError: retry しても接続できない、または 2xx 以外の応答の場合。
    """
    for attempt in range(1, HUMANDBS_FETCH_MAX_ATTEMPTS + 1):
        try:
            with httpx.Client(follow_redirects=True, timeout=HUMANDBS_FETCH_TIMEOUT_SECONDS) as client:
                response = client.get(url)
                response.raise_for_status()
                return response.text
        except httpx.HTTPError as e:
            retryable = isinstance(e, httpx.TransportError) or (
                isinstance(e, httpx.HTTPStatusError) and e.response.is_server_error
            )
            if not retryable or attempt == HUMANDBS_FETCH_MAX_ATTEMPTS:
                raise
            wait = HUMANDBS_FETCH_RETRY_WAIT_SECONDS * attempt
            log_warn(
                f"humandbs request failed ({e}), retrying in {wait}s (attempt {attempt}/{HUMANDBS_FETCH_MAX_ATTEMPTS})",
                url=url,
            )
            time.sleep(wait)
    raise AssertionError("unreachable")
