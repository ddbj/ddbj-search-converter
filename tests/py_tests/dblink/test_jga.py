"""Tests for ddbj_search_converter.dblink.jga module."""

import json
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ddbj_search_converter.config import (
    JGA_DATASET_HUM_ID_REL_PATH,
    JGA_STUDY_HUM_ID_REL_PATH,
    Config,
)
from ddbj_search_converter.dblink.jga import (
    HUMANDBS_FETCH_MAX_ATTEMPTS,
    HumandbsResponseError,
    _load_jga_humandbs_file,
    extract_pubmed_ids,
    fetch_humandbs_pairs,
    join_relations,
    parse_humandbs_dblink_ndjson,
    read_relation_csv,
    reverse_relation,
    update_jga_humandbs_file,
)
from ddbj_search_converter.logging.logger import _ctx, init_logger
from tests.py_tests.strategies import st_humandbs_id, st_jga_dataset, st_jga_study


class TestReadRelationCsv:
    """Tests for read_relation_csv function."""

    def test_reads_valid_csv(self, tmp_path: Path) -> None:
        """正常な CSV を読み込む。"""
        csv_content = """id,from_id,to_id
1,JGAD001,JGAP001
2,JGAD002,JGAP002
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content, encoding="utf-8")

        result = read_relation_csv(csv_path)

        assert result == {("JGAD001", "JGAP001"), ("JGAD002", "JGAP002")}

    def test_skips_invalid_rows(self, tmp_path: Path) -> None:
        """不正な行をスキップする。"""
        csv_content = """id,from_id,to_id
1,JGAD001,JGAP001
2,JGAD002
3,JGAD003,JGAP003
"""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text(csv_content, encoding="utf-8")

        result = read_relation_csv(csv_path)

        assert result == {("JGAD001", "JGAP001"), ("JGAD003", "JGAP003")}

    def test_raises_when_file_not_exists(self, test_config: pytest.fixture) -> None:  # type: ignore[valid-type]
        """ファイルが存在しない場合は FileNotFoundError を発生する。"""
        from ddbj_search_converter.logging.logger import run_logger

        with run_logger(config=test_config), pytest.raises(FileNotFoundError):
            read_relation_csv(Path("/nonexistent/path/test.csv"))


class TestJoinRelations:
    """Tests for join_relations function."""

    def test_simple_join(self) -> None:
        """シンプルな join。"""
        ab: set[tuple[str, str]] = {("A1", "B1"), ("A2", "B2")}
        bc: set[tuple[str, str]] = {("B1", "C1"), ("B2", "C2")}

        result = join_relations(ab, bc)

        assert result == {("A1", "C1"), ("A2", "C2")}

    def test_one_to_many(self) -> None:
        """1 対多の join。"""
        ab: set[tuple[str, str]] = {("A1", "B1")}
        bc: set[tuple[str, str]] = {("B1", "C1"), ("B1", "C2")}

        result = join_relations(ab, bc)

        assert result == {("A1", "C1"), ("A1", "C2")}

    def test_no_match(self) -> None:
        """マッチがない場合は空。"""
        ab: set[tuple[str, str]] = {("A1", "B1")}
        bc: set[tuple[str, str]] = {("B2", "C1")}

        result = join_relations(ab, bc)

        assert result == set()

    def test_empty_input(self) -> None:
        """空の入力。"""
        ab: set[tuple[str, str]] = set()
        bc: set[tuple[str, str]] = {("B1", "C1")}

        result = join_relations(ab, bc)

        assert result == set()


class TestReverseRelation:
    """Tests for reverse_relation function."""

    def test_reverse(self) -> None:
        """関連を逆転する。"""
        relation: set[tuple[str, str]] = {("A1", "B1"), ("A2", "B2")}

        result = reverse_relation(relation)

        assert result == {("B1", "A1"), ("B2", "A2")}

    def test_empty(self) -> None:
        """空の入力。"""
        relation: set[tuple[str, str]] = set()

        result = reverse_relation(relation)

        assert result == set()


@pytest.fixture
def jga_config(tmp_path: Path) -> Config:
    config = Config(
        result_dir=tmp_path,
        const_dir=tmp_path.joinpath("const"),
    )
    config.const_dir.joinpath("dblink").mkdir(parents=True, exist_ok=True)

    return config


@pytest.fixture
def _setup_logger(jga_config: Config) -> Iterator[None]:
    init_logger(run_name="test_jga", config=jga_config)
    yield
    _ctx.set(None)


@pytest.mark.usefixtures("_setup_logger")
class TestLoadJgaHumandbsFile:
    """Tests for _load_jga_humandbs_file function."""

    def test_loads_study_humandbs(self, jga_config: Config) -> None:
        """jga-study -> humandbs を読み込む。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\nJGAS000002\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert result == {("JGAS000001", "hum0004"), ("JGAS000002", "hum0001")}

    def test_loads_dataset_humandbs(self, jga_config: Config) -> None:
        """jga-dataset -> humandbs を読み込む。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_DATASET_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAD000001\thum0004\nJGAD000002\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_DATASET_HUM_ID_REL_PATH, "jga-dataset")

        assert result == {("JGAD000001", "hum0004"), ("JGAD000002", "hum0001")}

    def test_skips_empty_lines(self, jga_config: Config) -> None:
        """空行をスキップする。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\n\nJGAS000002\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert len(result) == 2

    def test_skips_malformed_lines(self, jga_config: Config) -> None:
        """カラム不足の行をスキップする。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\nBADLINE\nJGAS000002\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert result == {("JGAS000001", "hum0004"), ("JGAS000002", "hum0001")}

    def test_skips_invalid_src_accession(self, jga_config: Config) -> None:
        """無効な src accession をスキップする。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\nINVALID\thum0001\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert result == {("JGAS000001", "hum0004")}

    def test_skips_invalid_humandbs(self, jga_config: Config) -> None:
        """無効な humandbs をスキップする。"""
        tsv_path = jga_config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)
        tsv_path.write_text("JGAS000001\thum0004\nJGAS000002\tINVALID_HUM\n")

        result = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert result == {("JGAS000001", "hum0004")}

    def test_raises_when_file_missing(self, jga_config: Config) -> None:
        """ファイルが存在しない場合は FileNotFoundError。"""
        with pytest.raises(FileNotFoundError):
            _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")


class TestExtractPubmedIds:
    """Tests for extract_pubmed_ids function."""

    def test_extracts_pubmed_ids(self) -> None:
        """PUBMED ID を抽出する。"""
        study_entry = {
            "accession": "JGAS000001",
            "PUBLICATIONS": {
                "PUBLICATION": [
                    {"id": "12345678", "DB_TYPE": "PUBMED"},
                    {"id": "87654321", "DB_TYPE": "PUBMED"},
                ]
            },
        }

        result = extract_pubmed_ids(study_entry)

        assert result == {"12345678", "87654321"}

    def test_single_publication(self) -> None:
        """単一の PUBLICATION の場合 (dict)。"""
        study_entry = {
            "accession": "JGAS000001",
            "PUBLICATIONS": {"PUBLICATION": {"id": "12345678", "DB_TYPE": "PUBMED"}},
        }

        result = extract_pubmed_ids(study_entry)

        assert result == {"12345678"}

    def test_filters_non_pubmed(self) -> None:
        """PUBMED 以外は除外。"""
        study_entry = {
            "accession": "JGAS000001",
            "PUBLICATIONS": {
                "PUBLICATION": [
                    {"id": "12345678", "DB_TYPE": "PUBMED"},
                    {"id": "DOI12345", "DB_TYPE": "DOI"},
                ]
            },
        }

        result = extract_pubmed_ids(study_entry)

        assert result == {"12345678"}

    def test_no_publications(self) -> None:
        """PUBLICATIONS がない場合は空。"""
        study_entry = {"accession": "JGAS000001"}

        result = extract_pubmed_ids(study_entry)

        assert result == set()

    def test_integer_id(self) -> None:
        """ID が整数の場合も文字列として返す。"""
        study_entry = {
            "accession": "JGAS000001",
            "PUBLICATIONS": {"PUBLICATION": {"id": 12345678, "DB_TYPE": "PUBMED"}},
        }

        result = extract_pubmed_ids(study_entry)

        assert result == {"12345678"}


# === humandbs API ===

HUMANDBS_URL = "https://humandbs.example"
STUDY_URL = f"{HUMANDBS_URL}/api/dblink/jga-study"


def _dblinks_line(identifier: str, xrefs: list[tuple[str, str]], src_type: str = "jga-study") -> str:
    return json.dumps(
        {
            "identifier": identifier,
            "type": src_type,
            "dbXrefs": [
                {"identifier": xref_id, "type": xref_type, "url": f"{HUMANDBS_URL}/{xref_id}"}
                for xref_type, xref_id in xrefs
            ],
        }
    )


def _ndjson(pairs: set[tuple[str, str]], src_type: str = "jga-study") -> str:
    by_acc: dict[str, list[tuple[str, str]]] = {}
    for acc, hum in sorted(pairs):
        by_acc.setdefault(acc, []).append(("humandbs", hum))
    return "".join(_dblinks_line(acc, xrefs, src_type) + "\n" for acc, xrefs in by_acc.items())


def _read_log_records() -> list[dict[str, Any]]:
    ctx = _ctx.get()
    assert ctx is not None
    if not ctx.log_file.exists():
        return []
    return [json.loads(line) for line in ctx.log_file.read_text(encoding="utf-8").splitlines() if line.strip()]


class _HumandbsStub:
    """humandbs の HTTP 応答を差し替える (外部 HTTP 境界)。"""

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


@pytest.fixture
def humandbs(monkeypatch: pytest.MonkeyPatch) -> _HumandbsStub:
    stub = _HumandbsStub()
    real_client = httpx.Client
    transport = httpx.MockTransport(stub.handle)
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: real_client(transport=transport, **kwargs))
    monkeypatch.setattr(time, "sleep", stub.sleeps.append)
    return stub


def _ok(text: str) -> httpx.Response:
    return httpx.Response(200, text=text, headers={"Content-Type": "application/x-ndjson"})


@pytest.mark.usefixtures("_setup_logger")
class TestParseHumandbsDblinkNdjson:
    """Tests for parse_humandbs_dblink_ndjson function."""

    def test_multiple_entries_and_xrefs_are_all_returned(self) -> None:
        text = (
            _dblinks_line("JGAS000001", [("humandbs", "hum0004")])
            + "\n"
            + _dblinks_line("JGAS000002", [("humandbs", "hum0001"), ("humandbs", "hum0216")])
            + "\n"
        )

        result = parse_humandbs_dblink_ndjson(text, "jga-study", STUDY_URL)

        assert result == {("JGAS000001", "hum0004"), ("JGAS000002", "hum0001"), ("JGAS000002", "hum0216")}

    def test_empty_dbxrefs_yields_no_pair(self) -> None:
        text = _dblinks_line("JGAS000001", []) + "\n"

        assert parse_humandbs_dblink_ndjson(text, "jga-study", STUDY_URL) == set()

    def test_empty_body_yields_no_pair(self) -> None:
        assert parse_humandbs_dblink_ndjson("", "jga-study", STUDY_URL) == set()

    def test_blank_lines_and_missing_final_newline_are_accepted(self) -> None:
        text = (
            "\n"
            + _dblinks_line("JGAS000001", [("humandbs", "hum0004")])
            + "\n\n  \n"
            + (_dblinks_line("JGAS000002", [("humandbs", "hum0001")]))
        )

        result = parse_humandbs_dblink_ndjson(text, "jga-study", STUDY_URL)

        assert result == {("JGAS000001", "hum0004"), ("JGAS000002", "hum0001")}

    def test_crlf_line_endings_are_accepted(self) -> None:
        text = _dblinks_line("JGAS000001", [("humandbs", "hum0004")]) + "\r\n"

        assert parse_humandbs_dblink_ndjson(text, "jga-study", STUDY_URL) == {("JGAS000001", "hum0004")}

    def test_xref_of_other_type_is_ignored(self) -> None:
        xrefs = [("jga-dataset", "JGAD000001"), ("jga-study", "hum0005"), ("humandbs", "hum0004")]
        text = _dblinks_line("JGAS000001", xrefs) + "\n"

        assert parse_humandbs_dblink_ndjson(text, "jga-study", STUDY_URL) == {("JGAS000001", "hum0004")}

    @pytest.mark.parametrize(
        ("identifier", "humandbs_id"),
        [
            ("JGAD000001", "hum0004"),  # 違う種類の JGA accession
            ("jgas000001", "hum0004"),
            ("JGAS", "hum0004"),
            ("JGAS000001", "HUM0004"),
            ("JGAS000001", "hum0004.v2"),
            ("JGAS000001", "hum"),
            ("JGAS000001\thum0009", "hum0004"),
            ("JGAS000001", "hum0004\nJGAS000002\thum0001"),
            ("JGAS000001\n", "hum0004"),
        ],
    )
    def test_invalid_accession_is_skipped(self, identifier: str, humandbs_id: str) -> None:
        text = (
            _dblinks_line(identifier, [("humandbs", humandbs_id)])
            + "\n"
            + _dblinks_line("JGAS000002", [("humandbs", "hum0001")])
            + "\n"
        )

        result = parse_humandbs_dblink_ndjson(text, "jga-study", STUDY_URL)

        assert result == {("JGAS000002", "hum0001")}

    @pytest.mark.parametrize(
        "line",
        [
            "{not json",
            "<html><body>302 Found</body></html>",
            "[]",
            '"JGAS000001"',
            "1",
            "null",
            '{"dbXrefs": []}',
            '{"identifier": "JGAS000001"}',
            '{"identifier": "JGAS000001", "dbXrefs": null}',
            '{"identifier": "JGAS000001", "dbXrefs": "hum0004"}',
            '{"identifier": "JGAS000001", "dbXrefs": [{"identifier": "hum0004"}]}',
            '{"identifier": "JGAS000001", "dbXrefs": [{"type": "humandbs"}]}',
            '{"identifier": "JGAS000001", "dbXrefs": ["hum0004"]}',
            '{"identifier": 1, "dbXrefs": []}',
            '{"identifier": "JGAS000001", "dbXrefs": [{"identifier": 4, "type": "humandbs"}]}',
            '{"identifier": "JGAS000001", "dbXrefs": [{"identifier": "hum0004", "type": null}]}',
        ],
    )
    def test_line_not_in_dblinks_shape_raises(self, line: str) -> None:
        text = _dblinks_line("JGAS000002", [("humandbs", "hum0001")]) + "\n" + line + "\n"

        with pytest.raises(HumandbsResponseError):
            parse_humandbs_dblink_ndjson(text, "jga-study", STUDY_URL)

    @settings(max_examples=100, deadline=None)
    @given(
        pairs=st.sets(st.tuples(st_jga_dataset(), st_humandbs_id()), max_size=30),
    )
    def test_ndjson_built_from_pairs_parses_back_to_same_pairs(self, pairs: set[tuple[str, str]]) -> None:
        text = _ndjson(pairs, "jga-dataset")

        assert parse_humandbs_dblink_ndjson(text, "jga-dataset", STUDY_URL) == pairs


@pytest.mark.usefixtures("_setup_logger")
class TestFetchHumandbsPairs:
    """Tests for fetch_humandbs_pairs function."""

    @pytest.mark.parametrize("base_url", [HUMANDBS_URL, f"{HUMANDBS_URL}/"])
    def test_requests_dblink_listing_of_src_type(self, humandbs: _HumandbsStub, base_url: str) -> None:
        humandbs.serve(_ok(_ndjson({("JGAD000001", "hum0004")}, "jga-dataset")))

        result = fetch_humandbs_pairs(base_url, "jga-dataset")

        assert result == {("JGAD000001", "hum0004")}
        assert humandbs.requests == [f"{HUMANDBS_URL}/api/dblink/jga-dataset"]

    @pytest.mark.parametrize(
        "first",
        [
            httpx.Response(502),
            httpx.Response(503),
            httpx.ConnectError("refused"),
            httpx.ReadTimeout("timed out"),
        ],
    )
    def test_transient_failure_is_retried(self, humandbs: _HumandbsStub, first: httpx.Response | Exception) -> None:
        humandbs.serve(first, _ok(_ndjson({("JGAS000001", "hum0004")})))

        result = fetch_humandbs_pairs(HUMANDBS_URL, "jga-study")

        assert result == {("JGAS000001", "hum0004")}
        assert len(humandbs.requests) == 2
        assert len(humandbs.sleeps) == 1

    def test_server_error_on_every_attempt_raises_after_max_attempts(self, humandbs: _HumandbsStub) -> None:
        humandbs.serve(httpx.Response(500))

        with pytest.raises(httpx.HTTPStatusError):
            fetch_humandbs_pairs(HUMANDBS_URL, "jga-study")

        assert len(humandbs.requests) == HUMANDBS_FETCH_MAX_ATTEMPTS
        assert len(humandbs.sleeps) == HUMANDBS_FETCH_MAX_ATTEMPTS - 1

    def test_connect_error_on_every_attempt_raises_after_max_attempts(self, humandbs: _HumandbsStub) -> None:
        humandbs.serve(httpx.ConnectError("refused"))

        with pytest.raises(httpx.ConnectError):
            fetch_humandbs_pairs(HUMANDBS_URL, "jga-study")

        assert len(humandbs.requests) == HUMANDBS_FETCH_MAX_ATTEMPTS

    @pytest.mark.parametrize("status", [400, 404, 422])
    def test_client_error_raises_without_retry(self, humandbs: _HumandbsStub, status: int) -> None:
        humandbs.serve(httpx.Response(status))

        with pytest.raises(httpx.HTTPStatusError):
            fetch_humandbs_pairs(HUMANDBS_URL, "jga-study")

        assert len(humandbs.requests) == 1
        assert humandbs.sleeps == []

    def test_redirect_to_html_page_raises_response_error(self, humandbs: _HumandbsStub) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/api/dblink/jga-study":
                return httpx.Response(302, headers={"Location": "https://errors.example/404"})
            return httpx.Response(200, text="<!DOCTYPE html>\n<html></html>\n", headers={"Content-Type": "text/html"})

        humandbs.handler = handler

        with pytest.raises(HumandbsResponseError):
            fetch_humandbs_pairs(HUMANDBS_URL, "jga-study")


@pytest.mark.usefixtures("_setup_logger")
class TestUpdateJgaHumandbsFile:
    """Tests for update_jga_humandbs_file function."""

    def _saved_path(self, config: Config) -> Path:
        return config.const_dir.joinpath(JGA_STUDY_HUM_ID_REL_PATH)

    def test_fetched_pairs_are_saved_as_sorted_tsv(self, jga_config: Config, humandbs: _HumandbsStub) -> None:
        jga_config.humandbs_url = HUMANDBS_URL
        humandbs.serve(_ok(_ndjson({("JGAS000002", "hum0001"), ("JGAS000001", "hum0004")})))

        update_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert self._saved_path(jga_config).read_text(encoding="utf-8") == "JGAS000001\thum0004\nJGAS000002\thum0001\n"
        assert _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study") == {
            ("JGAS000001", "hum0004"),
            ("JGAS000002", "hum0001"),
        }

    def test_fetched_pairs_replace_saved_file_without_leaving_temp_file(
        self, jga_config: Config, humandbs: _HumandbsStub
    ) -> None:
        jga_config.humandbs_url = HUMANDBS_URL
        path = self._saved_path(jga_config)
        path.write_text("JGAS000009\thum0009\n", encoding="utf-8")
        humandbs.serve(_ok(_ndjson({("JGAS000001", "hum0004")})))

        update_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert path.read_text(encoding="utf-8") == "JGAS000001\thum0004\n"
        assert sorted(p.name for p in path.parent.iterdir()) == [path.name]

    def test_saved_directory_is_created_when_missing(self, tmp_path: Path, humandbs: _HumandbsStub) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path.joinpath("empty_const"), humandbs_url=HUMANDBS_URL)
        humandbs.serve(_ok(_ndjson({("JGAS000001", "hum0004")})))

        update_jga_humandbs_file(config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert self._saved_path(config).read_text(encoding="utf-8") == "JGAS000001\thum0004\n"

    @pytest.mark.parametrize(
        "response",
        [
            httpx.Response(500),
            httpx.Response(404),
            httpx.ConnectError("refused"),
            _ok(""),
            _ok("\n\n"),
            _ok(_dblinks_line("JGAS000001", []) + "\n"),
            _ok(_dblinks_line("INVALID", [("humandbs", "hum0004")]) + "\n"),
            _ok("{not json\n"),
            httpx.Response(200, text="<html></html>", headers={"Content-Type": "text/html"}),
        ],
    )
    def test_failed_or_empty_fetch_keeps_saved_file_and_warns(
        self, jga_config: Config, humandbs: _HumandbsStub, response: httpx.Response | Exception
    ) -> None:
        jga_config.humandbs_url = HUMANDBS_URL
        path = self._saved_path(jga_config)
        path.write_text("JGAS000009\thum0009\n", encoding="utf-8")
        humandbs.serve(response)

        update_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert path.read_text(encoding="utf-8") == "JGAS000009\thum0009\n"
        assert sorted(p.name for p in path.parent.iterdir()) == [path.name]
        warnings = [r for r in _read_log_records() if r["log_level"] == "WARNING"]
        assert any(r["extra"].get("url") == STUDY_URL and "using saved file" in r["message"] for r in warnings)

    def test_failed_fetch_without_saved_file_leaves_no_file_to_load(
        self, jga_config: Config, humandbs: _HumandbsStub
    ) -> None:
        jga_config.humandbs_url = HUMANDBS_URL
        humandbs.serve(httpx.Response(503))

        update_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        assert not self._saved_path(jga_config).exists()
        with pytest.raises(FileNotFoundError):
            _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

    @settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        saved=st.sets(st.tuples(st_jga_study(), st_humandbs_id()), min_size=1, max_size=10),
        fetched=st.sets(st.tuples(st_jga_study(), st_humandbs_id()), max_size=30),
    )
    def test_loaded_pairs_are_fetched_pairs_or_saved_pairs_when_fetch_is_empty(
        self,
        jga_config: Config,
        humandbs: _HumandbsStub,
        saved: set[tuple[str, str]],
        fetched: set[tuple[str, str]],
    ) -> None:
        jga_config.humandbs_url = HUMANDBS_URL
        self._saved_path(jga_config).write_text("".join(f"{a}\t{h}\n" for a, h in saved), encoding="utf-8")
        humandbs.serve(_ok(_ndjson(fetched)))

        update_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")

        loaded = _load_jga_humandbs_file(jga_config, JGA_STUDY_HUM_ID_REL_PATH, "jga-study")
        assert loaded == (fetched or saved)
