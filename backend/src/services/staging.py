"""磁盘暂存与内存观测的共享工具。

每次研究独占一个暂存目录（``ARTIFACT_DIR/staging/run-<id>-<随机串>``）。
清理只删除本任务自己的目录，绝不扫描或删除同级的其他研究目录。
"""
import errno
import resource
import shutil
import tempfile
from pathlib import Path

from ..config.settings import ARTIFACT_DIR

STAGING_ROOT = ARTIFACT_DIR / 'staging'

DISK_FULL_MESSAGE = '磁盘暂存空间不足，本次研究已停止；本任务暂存已清理，请释放空间后重新创建任务'
STAGING_FAIL_MESSAGE = '磁盘暂存写入失败，本次研究已停止；本任务暂存已清理'


class MemoryLimitExceeded(RuntimeError):
    """分批处理时超出内存上限；保留可读原因而不是让进程被 OOM 杀掉。"""


def check_memory_limit(limit_mb, stage='处理'):
    peak = peak_memory_kb()
    if limit_mb and peak > limit_mb * 1024:
        raise MemoryLimitExceeded(
            '%s过程峰值内存约 %d MB，超过 %d MB 上限；已按批停止并清理本次暂存，'
            '可调小批次后重新创建任务' % (stage, round(peak / 1024), limit_mb))
    return peak


def ensure_staging_root() -> Path:
    STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    return STAGING_ROOT


def create_staging_dir(run_id: int) -> Path:
    """为一次研究创建唯一暂存目录。

    使用 mkdtemp 原子地创建带随机后缀的目录，因此并发研究不会共享或误删目录。
    目录名重复带上任务号，便于运维在暂存空间被定时清理前核对归属。
    """
    root = ensure_staging_root()
    return Path(tempfile.mkdtemp(prefix='run-%s-' % run_id, dir=str(root)))


def cleanup_staging(path) -> None:
    """只清理给定目录本身。调用方必须传入本次研究自己的目录。"""
    if not path:
        return
    try:
        shutil.rmtree(str(path), ignore_errors=True)
    except Exception:
        # 清理失败不应掩盖原始错误；保留目录里可由运维人工排查。
        pass


def is_disk_full(exc) -> bool:
    if isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
        return True
    text = str(exc).lower()
    return 'no space left' in text or 'disk is full' in text or 'database or disk is full' in text


def staging_error_message(exc) -> str:
    if is_disk_full(exc):
        return DISK_FULL_MESSAGE
    return STAGING_FAIL_MESSAGE + '：' + str(exc)[:300]


def peak_memory_kb() -> int:
    """当前进程生命周期内的峰值常驻内存（Linux 上 ru_maxrss 单位为 KiB）。"""
    try:
        value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        return max(value, 0)
    except Exception:
        return 0


def reset_peak_memory() -> None:
    """尽量把 Linux 的 VmHWM 归零，使峰值统计对应本次计算。

    clear_refs 不可用时静默忽略；此时报告的是进程生命周期峰值，仍可作为上界。
    """
    try:
        with open('/proc/self/clear_refs', 'w') as f:
            f.write('5')
    except Exception:
        pass


def process_rss_kb() -> int:
    """当前常驻内存，用于进度页面的即时观测（不可用时回退到峰值）。"""
    try:
        with open('/proc/self/status', 'r') as f:
            for line in f:
                if line.startswith('VmRSS:'):
                    return int(line.split()[1])
    except Exception:
        pass
    return peak_memory_kb()
