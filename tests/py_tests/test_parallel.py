"""parallel.py (worker プロセスの寿命管理) のテスト。"""

import ast
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import ddbj_search_converter


def _is_dead(pid: int) -> bool:
    """プロセスが存在しないか zombie なら True。"""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return True
    return stat.rsplit(")", 1)[1].split()[0] in ("Z", "X")


_PARENT_SCRIPT = """
import json, multiprocessing, os, sys, time
from concurrent.futures import ProcessPoolExecutor
from ddbj_search_converter.parallel import exit_with_parent

def pid_after_delay():
    time.sleep(0.3)
    return os.getpid()

if __name__ == "__main__":
    use_initializer = sys.argv[1] == "1"
    kwargs = {"initializer": exit_with_parent, "initargs": (os.getpid(),)} if use_initializer else {}
    with ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context("fork"), **kwargs) as executor:
        pids = sorted({f.result() for f in [executor.submit(pid_after_delay) for _ in range(2)]})
        for _ in range(2):
            executor.submit(time.sleep, 120)
        print(json.dumps(pids), flush=True)
        time.sleep(120)
"""


def _spawn_parent_and_get_worker_pids(
    tmp_path: Path, *, use_initializer: bool
) -> tuple[subprocess.Popen[str], list[int]]:
    script = tmp_path / "parent_script.py"
    script.write_text(_PARENT_SCRIPT)
    proc = subprocess.Popen(
        [sys.executable, str(script), "1" if use_initializer else "0"],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    worker_pids: list[int] = json.loads(proc.stdout.readline())
    return proc, worker_pids


def _kill_all(pids: list[int]) -> None:
    for pid in pids:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


class TestExitWithParent:
    def test_workers_die_when_the_parent_is_sigkilled(self, tmp_path: Path) -> None:
        proc, worker_pids = _spawn_parent_and_get_worker_pids(tmp_path, use_initializer=True)
        try:
            assert len(worker_pids) == 2
            assert not any(_is_dead(pid) for pid in worker_pids)

            proc.kill()
            proc.wait(timeout=10)

            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and not all(_is_dead(pid) for pid in worker_pids):
                time.sleep(0.05)
            assert all(_is_dead(pid) for pid in worker_pids)
        finally:
            proc.kill()
            _kill_all(worker_pids)

    def test_workers_without_the_initializer_outlive_the_parent(self, tmp_path: Path) -> None:
        # 上のテストが「親の SIGKILL では worker が自然には死なない」状況を本当に作れていることの確認
        proc, worker_pids = _spawn_parent_and_get_worker_pids(tmp_path, use_initializer=False)
        try:
            proc.kill()
            proc.wait(timeout=10)
            time.sleep(1.0)
            assert not any(_is_dead(pid) for pid in worker_pids)
        finally:
            proc.kill()
            _kill_all(worker_pids)

    def test_exits_immediately_when_the_parent_is_already_gone(self) -> None:
        # fork から prctl までの間に親が死んだ場合、シグナルは届かないので自分で気づく必要がある
        code = (
            "import os\n"
            "from ddbj_search_converter.parallel import exit_with_parent\n"
            "exit_with_parent(os.getppid() + 1)\n"
            "print('alive')\n"
        )
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30, check=False)

        assert result.returncode != 0
        assert "alive" not in result.stdout

    def test_keeps_running_while_the_parent_is_alive(self) -> None:
        code = (
            "import os\n"
            "from ddbj_search_converter.parallel import exit_with_parent\n"
            "exit_with_parent(os.getppid())\n"
            "print('alive')\n"
        )
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=30, check=False)

        assert result.returncode == 0
        assert "alive" in result.stdout


def _process_pool_calls() -> list[tuple[str, ast.Call]]:
    """パッケージ内の ``ProcessPoolExecutor(...)`` の呼び出しを (場所, node) で返す。"""
    package_dir = Path(ddbj_search_converter.__file__).parent
    calls: list[tuple[str, ast.Call]] = []
    for path in sorted(package_dir.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name == "ProcessPoolExecutor":
                calls.append((f"{path.relative_to(package_dir)}:{node.lineno}", node))
    return calls


def _keyword(node: ast.Call, arg: str) -> ast.expr | None:
    return next((kw.value for kw in node.keywords if kw.arg == arg), None)


def _is_fork_context(value: ast.expr | None) -> bool:
    """``multiprocessing.get_context("fork")`` なら True。"""
    if not isinstance(value, ast.Call):
        return False
    func = value.func
    is_get_context = (
        isinstance(func, ast.Attribute)
        and func.attr == "get_context"
        and isinstance(func.value, ast.Name)
        and func.value.id == "multiprocessing"
    )
    return (
        is_get_context
        and len(value.args) == 1
        and isinstance(value.args[0], ast.Constant)
        and value.args[0].value == "fork"
    )


class TestEveryProcessPoolUsesExitWithParent:
    def test_no_process_pool_is_created_without_the_initializer(self) -> None:
        calls = _process_pool_calls()
        offenders = [
            where for where, node in calls if getattr(_keyword(node, "initializer"), "id", None) != "exit_with_parent"
        ]

        assert len(calls) >= 5
        assert offenders == []

    def test_every_process_pool_forks_its_workers(self) -> None:
        # forkserver や spawn では worker の親が呼び出し元のプロセスにならず、exit_with_parent が worker を終了させる
        calls = _process_pool_calls()
        offenders = [where for where, node in calls if not _is_fork_context(_keyword(node, "mp_context"))]

        assert len(calls) >= 5
        assert offenders == []


class TestIsForkContext:
    def test_accepts_the_fork_context(self) -> None:
        assert _is_fork_context(ast.parse('multiprocessing.get_context("fork")', mode="eval").body)

    def test_rejects_other_start_methods_and_shapes(self) -> None:
        for source in [
            'multiprocessing.get_context("forkserver")',
            'multiprocessing.get_context("spawn")',
            "multiprocessing.get_context()",
            'get_context("fork")',
            'mp.get_context("fork")',
            '"fork"',
        ]:
            assert not _is_fork_context(ast.parse(source, mode="eval").body), source
        assert not _is_fork_context(None)
