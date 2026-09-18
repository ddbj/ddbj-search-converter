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
import json, os, sys, time
from concurrent.futures import ProcessPoolExecutor
from ddbj_search_converter.parallel import exit_with_parent

def pid_after_delay():
    time.sleep(0.3)
    return os.getpid()

if __name__ == "__main__":
    use_initializer = sys.argv[1] == "1"
    kwargs = {"initializer": exit_with_parent, "initargs": (os.getpid(),)} if use_initializer else {}
    with ProcessPoolExecutor(max_workers=2, **kwargs) as executor:
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


class TestEveryProcessPoolUsesExitWithParent:
    def test_no_process_pool_is_created_without_the_initializer(self) -> None:
        package_dir = Path(ddbj_search_converter.__file__).parent
        offenders: list[str] = []
        pools_found = 0
        for path in sorted(package_dir.rglob("*.py")):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
                if name != "ProcessPoolExecutor":
                    continue
                pools_found += 1
                initializer = next((kw.value for kw in node.keywords if kw.arg == "initializer"), None)
                if getattr(initializer, "id", None) != "exit_with_parent":
                    offenders.append(f"{path.relative_to(package_dir)}:{node.lineno}")

        assert pools_found >= 5
        assert offenders == []
