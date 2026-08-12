"""sra/dra_tar.py のテスト。

実 ``Config`` を使う方針 (MagicMock(config) は属性契約の検証を bypass する)。
"""

import tarfile
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.sra import dra_tar
from ddbj_search_converter.sra.dra_tar import (
    collect_tar_submissions,
    get_dra_accessions_db_path,
    get_dra_last_updated_path,
    get_dra_tar_path,
    get_dra_xml_dir_path,
    iter_updated_dra_submissions,
    repair_dra_tar,
)


class TestGetDraTarPath:
    def test_returns_result_dir_suffixed(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        result = get_dra_tar_path(config)
        assert result == tmp_path / "sra_tar" / "DRA_Metadata.tar"


class TestGetDraLastUpdatedPath:
    def test_returns_result_dir_suffixed(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path)
        result = get_dra_last_updated_path(config)
        assert result == tmp_path / "sra_tar" / "dra_last_updated.txt"


class TestGetDraAccessionsDbPath:
    def test_returns_const_dir_suffixed(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        result = get_dra_accessions_db_path(config)
        assert result == tmp_path / "const" / "sra" / "dra_accessions.duckdb"


class TestGetDraXmlDirPath:
    """get_dra_xml_dir_path は global 固定 path を返す (Config を取らない)。"""

    def test_returns_correct_path(self) -> None:
        result = get_dra_xml_dir_path("DRA000001")
        assert result == Path("/usr/local/resources/dra/fastq/DRA000/DRA000001")

    def test_different_submission(self) -> None:
        result = get_dra_xml_dir_path("DRA123456")
        assert result == Path("/usr/local/resources/dra/fastq/DRA123/DRA123456")


def _row(
    accession: str,
    submission: str | None,
    type_: str,
    updated: str | None,
    published: str | None,
) -> tuple[Any, ...]:
    """accessions テーブルの 1 行を組み立てる (差分判定に関係しない列は NULL)。"""
    return (
        accession,
        submission,
        None,
        None,
        None,
        None,
        None,
        type_,
        "public",
        "public",
        updated,
        published,
        None,
    )


def _setup_accessions_db(config: Config, rows: list[tuple[Any, ...]]) -> None:
    db_path = get_dra_accessions_db_path(config)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE accessions (
                Accession   TEXT,
                Submission  TEXT,
                BioSample   TEXT,
                BioProject  TEXT,
                Study       TEXT,
                Experiment  TEXT,
                Sample      TEXT,
                Type        TEXT,
                Status      TEXT,
                Visibility  TEXT,
                Updated     TIMESTAMPTZ,
                Published   TIMESTAMPTZ,
                Received    TIMESTAMPTZ
            )
        """)
        conn.executemany("INSERT INTO accessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)


class TestIterUpdatedDraSubmissions:
    """iter_updated_dra_submissions のテスト。

    tar 同期の対象選定。ここで漏れた submission は tar に XML が入らず、
    JSONL 生成側が対象にしても空振りする。
    """

    def test_raises_when_db_missing(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        with pytest.raises(FileNotFoundError):
            list(iter_updated_dra_submissions(config, date(2026, 1, 25)))

    def test_returns_submission_when_child_row_updated(self, tmp_path: Path) -> None:
        """SUBMISSION 行が動かなくても、配下 entry が更新されていれば対象になる。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        _setup_accessions_db(
            config,
            [
                _row("DRA000001", "DRA000001", "SUBMISSION", "2019-05-23 00:00:00", "2021-05-17 00:00:00"),
                _row("DRR000001", "DRA000001", "RUN", "2026-01-20 00:00:00", "2021-05-17 00:00:00"),
            ],
        )
        result = list(iter_updated_dra_submissions(config, date(2026, 1, 25), margin_days=30))
        assert result == ["DRA000001"]

    def test_returns_submission_when_recently_published(self, tmp_path: Path) -> None:
        """公開遅延明け (Updated は古いまま Published だけが動く) を拾う。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        _setup_accessions_db(
            config,
            [
                _row("DRA000001", "DRA000001", "SUBMISSION", "2022-06-17 00:00:00", "2026-01-20 00:00:00"),
                _row("DRR000001", "DRA000001", "RUN", "2022-06-17 00:00:00", "2026-01-20 00:00:00"),
            ],
        )
        result = list(iter_updated_dra_submissions(config, date(2026, 1, 25), margin_days=30))
        assert result == ["DRA000001"]

    def test_excludes_future_published(self, tmp_path: Path) -> None:
        """Published が未来日付 (公開予定日) の submission は対象にしない。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        _setup_accessions_db(
            config,
            [
                _row("DRA000001", "DRA000001", "SUBMISSION", "2000-01-01 00:00:00", "2999-12-31 00:00:00"),
            ],
        )
        result = list(iter_updated_dra_submissions(config, date(2026, 1, 25), margin_days=30))
        assert result == []

    def test_excludes_when_both_dates_are_old(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        _setup_accessions_db(
            config,
            [
                _row("DRA000001", "DRA000001", "SUBMISSION", "2020-01-01 00:00:00", "2020-06-01 00:00:00"),
            ],
        )
        result = list(iter_updated_dra_submissions(config, date(2026, 1, 25), margin_days=30))
        assert result == []

    def test_excludes_null_submission(self, tmp_path: Path) -> None:
        """Submission が NULL の行は返さない (tar のディレクトリ名にできない)。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        _setup_accessions_db(
            config,
            [
                _row("DRA000001", None, "SUBMISSION", "2026-01-20 00:00:00", None),
            ],
        )
        result = list(iter_updated_dra_submissions(config, date(2026, 1, 25), margin_days=30))
        assert result == []

    def test_margin_boundary(self, tmp_path: Path) -> None:
        """cutoff ちょうどの Updated は含まれ、その 1 日前は含まれない。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        _setup_accessions_db(
            config,
            [
                _row("DRA000001", "DRA000001", "SUBMISSION", "2026-01-15 00:00:00", None),
                _row("DRA000002", "DRA000002", "SUBMISSION", "2026-01-14 23:59:59", None),
            ],
        )
        # cutoff = 2026-01-25 - 10d = 2026-01-15
        result = list(iter_updated_dra_submissions(config, date(2026, 1, 25), margin_days=10))
        assert result == ["DRA000001"]

    def test_deduplicates_submission(self, tmp_path: Path) -> None:
        """同じ submission の複数行が該当しても 1 件にまとまる。"""
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        _setup_accessions_db(
            config,
            [
                _row("DRA000001", "DRA000001", "SUBMISSION", "2026-01-20 00:00:00", None),
                _row("DRR000001", "DRA000001", "RUN", "2026-01-21 00:00:00", None),
                _row("DRR000002", "DRA000001", "RUN", "2026-01-22 00:00:00", None),
            ],
        )
        result = list(iter_updated_dra_submissions(config, date(2026, 1, 25), margin_days=30))
        assert result == ["DRA000001"]


def _write_xml_tree(xml_root: Path, submissions: list[str]) -> None:
    """DRA_BASE_PATH と同じ配置 (fastq/{prefix}/{submission}/) で XML を作る。"""
    for sub in submissions:
        xml_dir = xml_root / "fastq" / sub[:6] / sub
        xml_dir.mkdir(parents=True, exist_ok=True)
        for xml_type in ("submission", "run"):
            xml_dir.joinpath(f"{sub}.{xml_type}.xml").write_text(f"<{xml_type.upper()}/>", encoding="utf-8")


def _write_tar(tar_path: Path, xml_root: Path, submissions: list[str]) -> None:
    """tar に submission の XML を NCBI 形式のメンバ名で詰める。"""
    tar_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tar_path, "w") as tar:
        for sub in submissions:
            for xml_type in ("submission", "run"):
                src = xml_root / "fastq" / sub[:6] / sub / f"{sub}.{xml_type}.xml"
                tar.add(src, arcname=f"{sub}/{sub}.{xml_type}.xml")


class TestCollectTarSubmissions:
    def test_extracts_submission_names(self, tmp_path: Path) -> None:
        xml_root = tmp_path / "resources"
        _write_xml_tree(xml_root, ["DRA000001", "DRA000002"])
        tar_path = tmp_path / "sra_tar" / "DRA_Metadata.tar"
        _write_tar(tar_path, xml_root, ["DRA000001", "DRA000002"])

        assert collect_tar_submissions(tar_path) == {"DRA000001", "DRA000002"}

    def test_empty_tar_returns_empty_set(self, tmp_path: Path) -> None:
        tar_path = tmp_path / "empty.tar"
        with tarfile.open(tar_path, "w"):
            pass

        assert collect_tar_submissions(tar_path) == set()


@pytest.mark.usefixtures("with_logger_isolated")
class TestRepairDraTar:
    """repair_dra_tar のテスト。

    差分同期が取りこぼした submission を集合差分で回収する。tar は追記でしか
    育たないので、ここで埋めない限り取りこぼしは以後の同期でも残り続ける。
    """

    def _prepare(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        in_tab: list[str],
        in_tar: list[str],
    ) -> Config:
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        xml_root = tmp_path / "resources"
        _write_xml_tree(xml_root, in_tab)
        monkeypatch.setattr(
            dra_tar,
            "get_dra_xml_dir_path",
            lambda sub: xml_root / "fastq" / sub[:6] / sub,
        )
        _setup_accessions_db(
            config,
            [_row(sub, sub, "SUBMISSION", "2020-01-01 00:00:00", "2020-06-01 00:00:00") for sub in in_tab],
        )
        _write_tar(get_dra_tar_path(config), xml_root, in_tar)

        return config

    def test_raises_when_tar_missing(self, tmp_path: Path) -> None:
        config = Config(result_dir=tmp_path, const_dir=tmp_path / "const")
        with pytest.raises(FileNotFoundError):
            repair_dra_tar(config)

    def test_appends_missing_submissions(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """tab にあって tar に無い submission が追記される。"""
        config = self._prepare(
            tmp_path,
            monkeypatch,
            in_tab=["DRA000001", "DRA000002", "DRA000003"],
            in_tar=["DRA000001"],
        )
        repair_dra_tar(config)

        assert collect_tar_submissions(get_dra_tar_path(config)) == {"DRA000001", "DRA000002", "DRA000003"}

    def test_appended_xml_is_readable(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """追記した XML が NCBI 形式のメンバ名で読み出せる。"""
        config = self._prepare(tmp_path, monkeypatch, in_tab=["DRA000001", "DRA000002"], in_tar=["DRA000001"])
        repair_dra_tar(config)

        with tarfile.open(get_dra_tar_path(config), "r") as tar:
            extracted = tar.extractfile("DRA000002/DRA000002.submission.xml")
            assert extracted is not None
            assert extracted.read() == b"<SUBMISSION/>"

    def test_noop_when_nothing_missing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """不足が無ければ tar を書き換えない。"""
        config = self._prepare(tmp_path, monkeypatch, in_tab=["DRA000001"], in_tar=["DRA000001"])
        tar_path = get_dra_tar_path(config)
        before = tar_path.read_bytes()

        repair_dra_tar(config)

        assert tar_path.read_bytes() == before

    def test_does_not_advance_last_updated(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """last_updated は進めない (通常の差分同期の起点を動かさない)。"""
        config = self._prepare(tmp_path, monkeypatch, in_tab=["DRA000001", "DRA000002"], in_tar=["DRA000001"])
        last_updated_path = get_dra_last_updated_path(config)
        last_updated_path.write_text("20200101")

        repair_dra_tar(config)

        assert last_updated_path.read_text() == "20200101"

    def test_skips_submission_without_xml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """XML が存在しない submission は黙って飛ばす (tab には載るが実体が無いことがある)。"""
        config = self._prepare(tmp_path, monkeypatch, in_tab=["DRA000001"], in_tar=["DRA000001"])
        # tab にだけ足す (XML の実体は作らない)
        with duckdb.connect(get_dra_accessions_db_path(config)) as conn:
            conn.execute(
                "INSERT INTO accessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                _row("DRA000009", "DRA000009", "SUBMISSION", "2020-01-01 00:00:00", "2020-06-01 00:00:00"),
            )

        repair_dra_tar(config)

        assert collect_tar_submissions(get_dra_tar_path(config)) == {"DRA000001"}
