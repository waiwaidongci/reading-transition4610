"""独立 worker：从磁盘暂存分批读取冻结会话，磁盘聚合后写回结果。

内存边界：每批只读 ``COMPUTE_BATCH`` 条记录；边计数、会话去重、分母与排序
都在暂存 SQLite 文件里完成，因此百万会话/百万接续边的内存占用仍由批次大小
而不是文集规模决定。计算结束（成功或失败）后只清理本任务自己的暂存目录。
"""
import logging
import os
import tempfile
import time
from pathlib import Path

from redis import Redis

from .config.database import Session
from .config.settings import (ARTIFACT_DIR, COMPUTE_BATCH, MEMORY_LIMIT_MB,
                              REDIS_URL)
from .models.run import Run
from .services.algorithm import TransitionAggregator
from .services.audit import log
from .services.export import csv_response
from .services.intake import RECORDS_FILE, iter_frozen_records
from .services.staging import (MemoryLimitExceeded, cleanup_staging, is_disk_full,
                               peak_memory_kb, reset_peak_memory, staging_error_message)

queue = Redis.from_url(REDIS_URL)
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
START_PERCENT = 45
SPAN_PERCENT = 50
MISSING_STAGING_MESSAGE = '冻结的暂存文件已不存在，请重新创建任务'


def _batches(run):
    """产出冻结会话批次；优先读暂存文件，历史内联快照仍可计算。"""
    snapshot = run.snapshot or {}
    staging_dir = snapshot.get('staging_dir')
    if staging_dir and (Path(staging_dir) / snapshot.get('staging_file', RECORDS_FILE)).exists():
        return iter_frozen_records(staging_dir, COMPUTE_BATCH), 'staging'
    records = snapshot.get('records')
    if records is None:
        raise ValueError(MISSING_STAGING_MESSAGE)
    total = len(records)

    def inline_batches():
        consumed = 0
        for start in range(0, total, COMPUTE_BATCH):
            batch = records[start:start + COMPUTE_BATCH]
            consumed += len(batch)
            yield batch, consumed
    return inline_batches(), 'inline'


def execute(run_id):
    with Session() as db:
        run = db.get(Run, run_id)
        if not run or run.state != 'queued':
            return
        run.state = 'running'
        run.phase = 'compute'
        run.progress = START_PERCENT
        run.processed_count = 0
        db.commit()

        snapshot = dict(run.snapshot or {})
        staging_dir = snapshot.get('staging_dir')
        total = int(snapshot.get('frozen_count')
                    or len(snapshot.get('records') or []) or 0)
        aggregator = None
        temp_path = None
        try:
            reset_peak_memory()
            if staging_dir:
                aggregate_path = Path(staging_dir) / 'aggregate.sqlite3'
            else:
                handle, name = tempfile.mkstemp(prefix='transition-', suffix='.sqlite3')
                os.close(handle)
                temp_path = Path(name)
                aggregate_path = temp_path
            aggregator = TransitionAggregator(aggregate_path)
            batches, _ = _batches(run)
            processed = 0
            tick = max(COMPUTE_BATCH, total // 40) if total else COMPUTE_BATCH
            next_report = tick
            for batch, processed in batches:
                aggregator.add_batch(batch)
                if processed >= next_report or processed >= total:
                    peak = peak_memory_kb()
                    if peak > MEMORY_LIMIT_MB * 1024:
                        raise MemoryLimitExceeded(
                            '计算峰值内存约 %d MB，超过 %d MB 上限；'
                            '请调小 COMPUTE_BATCH 后重新创建任务'
                            % (round(peak / 1024), MEMORY_LIMIT_MB))
                    run.processed_count = processed
                    run.progress = START_PERCENT + min(
                        SPAN_PERCENT, int(SPAN_PERCENT * processed / total)
                        if total else SPAN_PERCENT)
                    run.compute_peak_kb = peak
                    db.commit()
                    next_report = processed + tick
            result = aggregator.finalize(run.parameters)
            peak = peak_memory_kb()
            run.result = result
            run.state = 'completed'
            run.phase = ''
            run.progress = 100
            run.processed_count = processed
            run.compute_peak_kb = peak
            run.peak_memory_kb = max(run.peak_memory_kb or 0, peak)
            log(db, 'worker', 'run.completed', run.id)
            db.commit()
            response = csv_response(result, 'run.csv')
            (ARTIFACT_DIR / ('run-%d.csv' % run.id)).write_bytes(response.body)
        except Exception as e:
            logging.exception('analysis failed')
            try:
                db.rollback()
                run = db.get(Run, run_id)
                if run:
                    run.state = 'failed'
                    run.phase = ''
                    run.error = (staging_error_message(e) if is_disk_full(e)
                                 else str(e)[:2000])
                    peak = peak_memory_kb()
                    run.compute_peak_kb = peak
                    run.peak_memory_kb = max(run.peak_memory_kb or 0, peak)
                    log(db, 'worker', 'run.failed', run.id, run.error)
                    db.commit()
            except Exception:
                logging.exception('failed to persist run failure')
        finally:
            # 先释放暂存库再清理目录：Windows 上不能删除已打开的文件。
            if aggregator is not None:
                aggregator.close()
            if temp_path is not None:
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            # 成功或失败都只清理本任务自己的暂存目录；并发研究的目录互不影响。
            cleanup_staging(staging_dir)


def main():
    from .services.recovery import recover_on_worker_start
    recover_on_worker_start()
    while True:
        (ARTIFACT_DIR / 'heartbeat').write_text(str(time.time()))
        value = queue.blpop('analysis:queue', timeout=2)
        if value:
            execute(int(value[1]))


if __name__ == '__main__':
    main()
