"""受理与计算的分层入口。

受理（API 进程后台线程）：冻结边界 → 按主键分批写磁盘 → 跨批会话去重 → 入队。
计算（worker 进程）：从磁盘按批读取 → 磁盘聚合 → 写结果与 CSV → 清理暂存。

两个进程各自把内存控制在 ``MEMORY_LIMIT_MB`` 附近：受理每批读 INTAKE_BATCH
条，计算每批读 COMPUTE_BATCH 条，其余状态都在暂存 SQLite 文件里。
"""
import concurrent.futures
import json
import logging
import time
from pathlib import Path

from fastapi import HTTPException
from redis import Redis
from sqlalchemy import func, select

from ..config.database import Session
from ..config.settings import (INLINE_SNAPSHOT_LIMIT, INTAKE_BATCH, INTAKE_WORKERS,
                               MEMORY_LIMIT_MB, REDIS_URL)
from ..models.record import Record
from ..models.run import Run
from ..utils.serialization import now, row
from .algorithm import DUPLICATE_SESSION_MESSAGE, SessionIndex
from .audit import log
from .domain_validation import validate_parameters
from .intake import RECORDS_FILE, frozen_boundary, frozen_total, record_payload
from .sources import source_or_404
from .staging import (MemoryLimitExceeded, cleanup_staging, create_staging_dir,
                      peak_memory_kb, reset_peak_memory, staging_error_message)

queue = Redis.from_url(REDIS_URL)

PUBLIC_SNAPSHOT_KEYS = ('source', 'frozen_count', 'boundary', 'batch_size',
                        'memory_limit_mb')
INTAKE_PERCENT = 45
# 受理进度按时间节流写入：百万行时避免每 500 行一次进度事务。
PROGRESS_INTERVAL = 0.5

# 有界的受理线程池：同时在途的受理数受限，受理内存不会随并发研究数线性增长。
_intake_pool = concurrent.futures.ThreadPoolExecutor(
    max_workers=max(1, INTAKE_WORKERS), thread_name_prefix='intake')


def get_run(db, id):
    r = db.get(Run, id)
    if not r:
        raise HTTPException(404, '未找到分析任务')
    return r


def public_run(run):
    """任务视图：不把内联样本或百万行快照发回浏览器。"""
    data = row(run)
    snapshot = data.get('snapshot') or {}
    data['snapshot'] = {key: snapshot[key] for key in PUBLIC_SNAPSHOT_KEYS if key in snapshot}
    data['snapshot']['records_inline'] = 'records' in snapshot
    known = snapshot.get('frozen_count') or len(snapshot.get('records') or [])
    if not data.get('frozen_count'):
        data['frozen_count'] = known
    return data


RUN_LIST_COLUMNS = (Run.id, Run.source_id, Run.name, Run.state, Run.phase,
                    Run.progress, Run.frozen_count, Run.processed_count,
                    Run.peak_memory_kb, Run.created_at)


def list_runs(db):
    """列表只取轻量列，避免把每个任务的内联快照/结果读进内存。"""
    return [dict(record._mapping) for record in
            db.execute(select(*RUN_LIST_COLUMNS).order_by(Run.id.desc()))]


# ---------------------------------------------------------------------------
# 受理
# ---------------------------------------------------------------------------
def _fail(db, run_id, staging_dir, message):
    try:
        run = db.get(Run, run_id)
        if run:
            run.state = 'failed'
            run.phase = ''
            run.error = message
            log(db, 'worker', 'run.failed', run_id, message)
            db.commit()
    except Exception:
        db.rollback()
    cleanup_staging(staging_dir)


def _freeze(db, run_id, source_id, boundary, total, staging_dir):
    """按主键升序分批冻结到磁盘，并跨批去重。

    整个冻结过程使用同一个只读事务：MySQL 的可重复读提供一致快照，受理开始
    之后并发提交的追加既不会混入，也不会因为逐批重新取快照而被漏读。进度与
    峰值内存用独立连接写入，因此不会打断这个读视图。

    返回 ``(count, inline_records, peak_kb)``。去重索引是暂存文件里的主键表，
    重复会话即使分处不同批次也会被拒绝。
    """
    index = SessionIndex(Path(staging_dir) / 'sessions.sqlite3')
    sink_path = Path(staging_dir) / RECORDS_FILE
    inline_records = [] if total <= INLINE_SNAPSHOT_LIMIT else None
    count = 0
    last = 0
    peak = 0
    last_progress = 0.0
    try:
        index.start()
        try:
            # 先取一次边界内的计数，建立本事务的一致读视图。
            db.execute(select(func.count()).select_from(Record)
                       .where(Record.source_id == source_id, Record.id <= boundary)).scalar()
            with sink_path.open('wb') as sink:
                while True:
                    # 只取列而不是 ORM 实体：百万行时不创建百万个对象，也不进身份映射。
                    rows = db.execute(
                        select(Record.id, Record.source_id, Record.reference, Record.values)
                        .where(Record.source_id == source_id,
                               Record.id <= boundary,
                               Record.id > last)
                        .order_by(Record.id)
                        .limit(INTAKE_BATCH)
                    ).all()
                    if not rows:
                        break
                    session_ids = []
                    for row in rows:
                        packed = record_payload(row)
                        sink.write(json.dumps(packed, ensure_ascii=False,
                                              sort_keys=True).encode('utf-8'))
                        sink.write(b'\n')
                        session_ids.append(packed['values']['session'])
                        if inline_records is not None:
                            inline_records.append(packed)
                    # 重复会话在这里抛出；暂存文件与索引随失败一起清理。
                    index.add(session_ids)
                    last = rows[-1].id
                    count += len(rows)
                    del rows
                    peak = max(peak, peak_memory_kb())
                    if peak > MEMORY_LIMIT_MB * 1024:
                        raise MemoryLimitExceeded(
                            '受理冻结峰值内存约 %d MB，超过 %d MB 上限；'
                            '请调小 INTAKE_BATCH 后重新创建任务'
                            % (round(peak / 1024), MEMORY_LIMIT_MB))
                    now_ts = time.monotonic()
                    if now_ts - last_progress >= PROGRESS_INTERVAL:
                        last_progress = now_ts
                        _write_intake_progress(run_id, count, total, peak)
            index.commit()
        except Exception:
            index.rollback()
            raise
    finally:
        index.close()
    _write_intake_progress(run_id, count, total, peak)
    return count, inline_records, peak


def _write_intake_progress(run_id, count, total, peak):
    """用独立会话写进度：避免干扰冻结事务的一致读视图；写失败不影响受理。"""
    try:
        with Session() as probe:
            run = probe.get(Run, run_id)
            if not run:
                return
            run.frozen_count = count
            run.intake_peak_kb = peak
            run.peak_memory_kb = max(run.peak_memory_kb or 0, peak)
            run.progress = int(count * INTAKE_PERCENT / total)
            probe.commit()
    except Exception:
        logging.debug('intake progress write skipped', exc_info=True)


def _intake(run_id, source_id, boundary, total, staging_dir):
    """后台受理：分批冻结、去重、入队；失败时保留原因并只清理本次暂存。"""
    reset_peak_memory()
    try:
        with Session() as db:
            try:
                count, inline_records, intake_peak = _freeze(
                    db, run_id, source_id, boundary, total, staging_dir)
            except ValueError as e:
                db.rollback()
                _fail(db, run_id, staging_dir, str(e) or DUPLICATE_SESSION_MESSAGE)
                return
            except MemoryLimitExceeded as e:
                db.rollback()
                _fail(db, run_id, staging_dir, str(e))
                return
            except Exception as e:
                logging.exception('intake failed')
                db.rollback()
                _fail(db, run_id, staging_dir, staging_error_message(e))
                return

            snapshot = {'source': dict(public_source(db, source_id)),
                        'frozen_count': count, 'boundary': boundary,
                        'staging_dir': str(staging_dir), 'staging_file': RECORDS_FILE,
                        'batch_size': INTAKE_BATCH, 'memory_limit_mb': MEMORY_LIMIT_MB}
            if inline_records is not None:
                snapshot['records'] = inline_records
            # 收尾前先结束读取事务，再在同一条连接上写入最终快照。
            db.commit()
            run = db.get(Run, run_id)
            run.snapshot = snapshot
            run.frozen_count = count
            run.intake_peak_kb = intake_peak
            run.peak_memory_kb = max(run.peak_memory_kb or 0, intake_peak)
            run.phase = 'queued'
            run.progress = INTAKE_PERCENT
            db.commit()
            try:
                queue.rpush('analysis:queue', run_id)
            except Exception:
                _fail(db, run_id, staging_dir, '队列暂不可用，请重新创建任务')
    except Exception:
        logging.exception('intake crashed')
        with Session() as db:
            _fail(db, run_id, staging_dir, '受理进程异常，本次暂存已清理，请重新创建任务')


def public_source(db, source_id):
    from ..models.source import Source
    source = db.get(Source, source_id)
    if not source:
        return {'id': source_id, 'name': '', 'filename': ''}
    return {'id': source.id, 'name': source.name, 'filename': source.filename}


def create_run(db, user, data):
    source = source_or_404(db, data.source_id)
    try:
        params = validate_parameters(data.parameters)
    except ValueError as e:
        raise HTTPException(422, str(e))

    # 受理起点在请求线程里一次确定：此后提交的追加不会进入本次冻结。
    boundary = frozen_boundary(db, source.id)
    total = frozen_total(db, source.id, boundary)
    if total == 0:
        raise HTTPException(422, '至少录入1个完整阅读会话后才能执行')

    run = Run(source_id=source.id, name=data.name, parameters=params,
              snapshot={'source': {'id': source.id, 'name': source.name,
                                   'filename': source.filename},
                        'frozen_count': 0, 'boundary': boundary,
                        'staging_file': RECORDS_FILE, 'batch_size': INTAKE_BATCH,
                        'memory_limit_mb': MEMORY_LIMIT_MB},
              created_at=now(), state='queued', phase='intake', progress=0,
              frozen_count=0, processed_count=0, peak_memory_kb=0,
              intake_peak_kb=0, compute_peak_kb=0)
    db.add(run)
    db.flush()
    run_id = run.id
    log(db, user, 'run.queued', run_id, data.name)
    db.commit()

    try:
        staging_dir = create_staging_dir(run_id)
    except OSError as e:
        message = staging_error_message(e)
        _fail(db, run_id, None, message)
        raise HTTPException(503, message)

    # 暂存目录在受理开始前即登记到快照，页面可据此核对，失败清理也有据可查。
    run = db.get(Run, run_id)
    snapshot = dict(run.snapshot or {})
    snapshot['staging_dir'] = str(staging_dir)
    snapshot['staging_file'] = RECORDS_FILE
    run.snapshot = snapshot
    db.commit()

    _intake_pool.submit(_intake, run_id, source.id, boundary, total, staging_dir)
    return public_run(run)
