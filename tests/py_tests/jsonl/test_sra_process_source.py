"""jsonl/sra.py の process_source と worker の寿命管理のテスト。

worker は実際に fork させる。差し替えるのは DB (対象 submission の取得) と
batch の中身の処理だけで、executor・tar index・先読みの制御は本物を通す。
"""

import io
import multiprocessing
import os
import tarfile
import time
from pathlib import Path

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.jsonl import sra
from ddbj_search_converter.sra.tar_reader import TarXMLReader, get_ncbi_tar_path

WORKER_SLEEP_SEC = 0.15


def _fake_batch_worker(
    _config: Config,
    _source: str,
    batch_num: int,
    _total_batches: int,
    _batch_subs: list[str],
    _xml_data: dict[str, dict[str, bytes | None]],
    _blacklist: set[str],
    output_dir: Path,
    _is_ddbj_origin: bool,
    _include_dbxrefs: bool = False,
) -> dict[str, int]:
    output_dir.joinpath(f"pid_{batch_num}").write_text(str(os.getpid()))
    time.sleep(WORKER_SLEEP_SEC)
    output_dir.joinpath(f"done_{batch_num}").touch()
    return dict.fromkeys(sra.XML_TYPES, 0)


def _make_ncbi_tar(config: Config, submissions: list[str]) -> None:
    tar_path = get_ncbi_tar_path(config)
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "w") as tf:
        for sub in submissions:
            data = f"<SUBMISSION accession='{sub}'/>".encode()
            info = tarfile.TarInfo(f"{sub}/{sub}.submission.xml")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


@pytest.fixture
def source_env(test_config: Config, monkeypatch: pytest.MonkeyPatch) -> tuple[Config, Path, list[str]]:
    submissions = [f"SRA{i:06d}" for i in range(1, 13)]
    _make_ncbi_tar(test_config, submissions)
    output_dir = test_config.result_dir / "out"
    output_dir.mkdir()
    monkeypatch.setattr(sra, "iter_all_submissions", lambda _config, _source: iter(submissions))
    monkeypatch.setattr(sra, "_process_batch_worker", _fake_batch_worker)
    monkeypatch.setattr(sra, "DEFAULT_BATCH_SIZE", 1)
    return test_config, output_dir, submissions


@pytest.mark.usefixtures("with_logger_isolated")
class TestProcessSourceWorkerStartup:
    def test_all_workers_are_running_before_the_tar_index_is_built(
        self, source_env: tuple[Config, Path, list[str]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config, output_dir, submissions = source_env
        parallel_num = 3
        children_before = {p.pid for p in multiprocessing.active_children()}
        children_at_index_build: set[int | None] = set()
        original = TarXMLReader.get_submission_offsets

        def recording(self: TarXMLReader, subs: list[str]) -> dict[str, int]:
            children_at_index_build.update(p.pid for p in multiprocessing.active_children())
            return original(self, subs)

        monkeypatch.setattr(TarXMLReader, "get_submission_offsets", recording)

        sra.process_source(config, "sra", output_dir, set(), full=True, since=None, parallel_num=parallel_num)

        started_early = children_at_index_build - children_before
        assert len(started_early) == parallel_num
        batch_pids = {int(p.read_text()) for p in output_dir.glob("pid_*")}
        assert len(list(output_dir.glob("pid_*"))) == len(submissions)
        # index 構築後に fork された worker が batch を処理していないこと
        assert batch_pids <= started_early


@pytest.mark.usefixtures("with_logger_isolated")
class TestProcessSourceReadAhead:
    def test_parent_never_holds_more_than_in_flight_plus_two_batches(
        self, source_env: tuple[Config, Path, list[str]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        config, output_dir, submissions = source_env
        parallel_num = 2
        reads = 0
        max_outstanding = 0
        original = sra._read_batch_xml

        def counting(tar_reader: TarXMLReader, batch_subs: list[str]) -> dict[str, dict[str, bytes | None]]:
            nonlocal reads, max_outstanding
            reads += 1
            done = len(list(output_dir.glob("done_*")))
            max_outstanding = max(max_outstanding, reads - done)
            return original(tar_reader, batch_subs)  # type: ignore[return-value]

        monkeypatch.setattr(sra, "_read_batch_xml", counting)

        sra.process_source(config, "sra", output_dir, set(), full=True, since=None, parallel_num=parallel_num)

        assert reads == len(submissions)
        assert {p.name for p in output_dir.glob("done_*")} == {f"done_{i}" for i in range(1, len(submissions) + 1)}
        assert max_outstanding <= parallel_num + 2
