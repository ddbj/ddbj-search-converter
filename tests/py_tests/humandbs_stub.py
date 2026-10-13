"""humandbs の HTTP 応答を差し替える (外部 HTTP 境界)。"""

from collections.abc import Callable

import httpx


class HumandbsStub:
    """``httpx.Client`` を MockTransport に差し替えたときの応答と、受けた request・retry の sleep を記録する。"""

    def __init__(self) -> None:
        self.handler: Callable[[httpx.Request], httpx.Response] = lambda _: httpx.Response(404)
        self.requests: list[str] = []
        self.sleeps: list[float] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        return self.handler(request)

    def serve(self, *responses: httpx.Response | Exception) -> None:
        """呼ばれた順に responses を返す。最後の 1 つはそれ以降も返し続ける。"""
        queue = list(responses)

        def handler(request: httpx.Request) -> httpx.Response:
            item = queue.pop(0) if len(queue) > 1 else queue[0]
            if isinstance(item, Exception):
                raise item
            return item

        self.handler = handler


def ok_ndjson(text: str) -> httpx.Response:
    return httpx.Response(200, text=text, headers={"Content-Type": "application/x-ndjson"})
