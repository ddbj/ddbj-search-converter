"""Tests for logger module."""
import json
import sys
from io import StringIO
from pathlib import Path
from typing import Generator

import pytest

from ddbj_search_converter.config import Config
from ddbj_search_converter.logging.logger import (_ctx, init_logger, log_debug,
                                                  log_error, log_info,
                                                  log_warn, run_logger)


@pytest.fixture
def clean_ctx() -> Generator[None, None, None]:
    """Clean up logger context after each test."""
    yield
    _ctx.set(None)


class TestRunLogger:
    """Tests for run_logger context manager."""

    def test_run_logger_success(
        self,
        test_config: Config,
        clean_ctx: None,
    ) -> None:
        """Test run_logger logs start and end on success."""
        with run_logger(run_name="test_success", config=test_config):
            log_info("test message")

        # Check that log file was created
        log_dir = test_config.result_dir.joinpath("logs")
        log_files = list(log_dir.glob("*.log.jsonl"))
        assert len(log_files) == 1

        # Read and verify log records
        with log_files[0].open("r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]

        assert len(records) == 3  # start, test message, end

        # Check start record
        assert records[0]["log_level"] == "INFO"
        assert records[0]["extra"]["lifecycle"] == "start"
        assert "started" in records[0]["message"]

        # Check custom log
        assert records[1]["log_level"] == "INFO"
        assert records[1]["message"] == "test message"

        # Check end record
        assert records[2]["log_level"] == "INFO"
        assert records[2]["extra"]["lifecycle"] == "end"
        assert "completed" in records[2]["message"]

    def test_run_logger_failure(
        self,
        test_config: Config,
        clean_ctx: None,
    ) -> None:
        """Test run_logger logs start and failed on exception."""
        with pytest.raises(ValueError, match="Test error"):
            with run_logger(run_name="test_failure", config=test_config):
                raise ValueError("Test error")

        # Check log file
        log_dir = test_config.result_dir.joinpath("logs")
        log_files = list(log_dir.glob("*.log.jsonl"))
        assert len(log_files) == 1

        with log_files[0].open("r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]

        assert len(records) == 2  # start, failed

        # Check start record
        assert records[0]["extra"]["lifecycle"] == "start"

        # Check failed record
        assert records[1]["log_level"] == "CRITICAL"
        assert records[1]["extra"]["lifecycle"] == "failed"
        assert records[1]["error"] is not None
        assert records[1]["error"]["type"] == "ValueError"


class TestLogLevels:
    """Tests for different log levels."""

    def test_log_levels(
        self,
        test_config: Config,
        clean_ctx: None,
    ) -> None:
        """Test that all log levels work correctly."""
        init_logger(run_name="test_levels", config=test_config)

        log_debug("debug message")
        log_info("info message")
        log_warn("warning message")
        log_error("error message")

        # Read log file
        log_dir = test_config.result_dir.joinpath("logs")
        log_files = list(log_dir.glob("*.log.jsonl"))
        assert len(log_files) == 1

        with log_files[0].open("r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]

        assert len(records) == 4

        levels = [r["log_level"] for r in records]
        assert levels == ["DEBUG", "INFO", "WARNING", "ERROR"]


class TestLogWithExtra:
    """Tests for log functions with extra fields."""

    def test_log_with_extra(
        self,
        test_config: Config,
        clean_ctx: None,
    ) -> None:
        """Test logging with extra fields like file and accession."""
        init_logger(run_name="test_extra", config=test_config)

        log_info("processing file", file="/path/to/file.xml")
        log_info("processing accession", accession="PRJDB12345")
        log_info(
            "combined",
            file="/another/file.xml",
            accession="DRR000001",
        )

        # Read log file
        log_dir = test_config.result_dir.joinpath("logs")
        log_files = list(log_dir.glob("*.log.jsonl"))

        with log_files[0].open("r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]

        assert records[0]["extra"]["file"] == "/path/to/file.xml"
        assert records[1]["extra"]["accession"] == "PRJDB12345"
        assert records[2]["extra"]["file"] == "/another/file.xml"
        assert records[2]["extra"]["accession"] == "DRR000001"

    def test_log_with_path_object(
        self,
        test_config: Config,
        clean_ctx: None,
    ) -> None:
        """Test that Path objects are converted to strings."""
        init_logger(run_name="test_path", config=test_config)

        log_info("processing", file=Path("/path/to/file.xml"))

        log_dir = test_config.result_dir.joinpath("logs")
        log_files = list(log_dir.glob("*.log.jsonl"))

        with log_files[0].open("r", encoding="utf-8") as f:
            record = json.loads(f.readline())

        assert record["extra"]["file"] == "/path/to/file.xml"
        assert isinstance(record["extra"]["file"], str)


class TestStderrOutput:
    """Tests for stderr output."""

    def test_stderr_output_info(
        self,
        test_config: Config,
        clean_ctx: None,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that INFO and above are written to stderr."""
        init_logger(run_name="test_stderr", config=test_config)

        log_debug("debug message")  # Should NOT appear in stderr
        log_info("info message")  # Should appear
        log_warn("warning message")  # Should appear

        captured = capsys.readouterr()
        stderr = captured.err

        assert "debug message" not in stderr
        assert "info message" in stderr
        assert "warning message" in stderr

    def test_stderr_output_with_extra(
        self,
        test_config: Config,
        clean_ctx: None,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that file and accession are shown in stderr."""
        init_logger(run_name="test_stderr_extra", config=test_config)

        log_info("processing", file="/path/to/file.xml", accession="PRJDB12345")

        captured = capsys.readouterr()
        stderr = captured.err

        assert "file=/path/to/file.xml" in stderr
        assert "accession=PRJDB12345" in stderr


class TestInferRunName:
    """Tests for run_name inference."""

    def test_infer_run_name_from_context(
        self,
        test_config: Config,
        clean_ctx: None,
    ) -> None:
        """Test run_name inference when not provided."""
        # When run_name is not provided, it should be inferred
        with run_logger(config=test_config):
            log_info("test")

        log_dir = test_config.result_dir.joinpath("logs")
        log_files = list(log_dir.glob("*.log.jsonl"))

        with log_files[0].open("r", encoding="utf-8") as f:
            record = json.loads(f.readline())

        # run_name should be inferred (not empty)
        assert record["run_name"]
        assert len(record["run_name"]) > 0
