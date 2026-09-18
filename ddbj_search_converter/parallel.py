"""並列 worker プロセスの寿命管理。"""

import ctypes
import os
import signal
import sys

_PR_SET_PDEATHSIG = 1


def exit_with_parent(parent_pid: int) -> None:
    """ProcessPoolExecutor の initializer。親プロセスが終了したら worker も終了するようにする。

    ``initargs=(os.getpid(),)`` で親の pid を渡す。親だけが OOM などで SIGKILL されると、
    worker は次の仕事を待ったままメモリを保持して残る。PR_SET_PDEATHSIG は
    「worker を生成したスレッド」の終了で発火するので、executor への submit は親の main thread から行うこと。
    """
    if sys.platform != "linux":
        return
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_PDEATHSIG, signal.SIGKILL) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
    # prctl より前に親が終了していた場合はシグナルが届かない
    if os.getppid() != parent_pid:
        os._exit(1)
