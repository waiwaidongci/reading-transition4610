"""启动收尾：处理上次进程中断留下的任务。

* 受理在 API 进程的后台线程里执行；进程退出后 ``phase='intake'`` 的任务不会
  再前进，标为失败并保留原因，同时只清理它自己的暂存目录。
* 计算在独立 worker 里执行；worker 重启后 ``state='running'`` 的任务已无进程
  继续。若冻结文件仍在，就重新入队（受 ``attempts`` 上限保护），否则标为失败。

两类收尾都只接触任务快照里登记的暂存目录，绝不扫描或删除其他研究的目录。
"""
import logging

from sqlalchemy import select

from ..config.settings import REDIS_URL
from ..models.run import Run
from .staging import cleanup_staging

MAX_ATTEMPTS = 2
INTAKE_INTERRUPTED = '受理进程中断，冻结未完成；本次暂存已清理，请重新创建任务'
COMPUTE_INTERRUPTED = '计算进程中断，结果未写入；请重新创建任务'


def _staging_has_records(snapshot):
    from pathlib import Path
    from .intake import RECORDS_FILE
    staging_dir = (snapshot or {}).get('staging_dir')
    if not staging_dir:
        return False, None
    path = Path(staging_dir) / (snapshot or {}).get('staging_file', RECORDS_FILE)
    return path.exists(), staging_dir


def recover_on_api_start():
    from ..config.database import Session
    with Session() as db:
        for run in db.scalars(select(Run).where(Run.phase == 'intake')).all():
            staging_dir = (run.snapshot or {}).get('staging_dir')
            run.state = 'failed'
            run.phase = ''
            run.error = INTAKE_INTERRUPTED
            db.commit()
            cleanup_staging(staging_dir)
            logging.warning('recovered interrupted intake run %s', run.id)


def recover_on_worker_start():
    from redis import Redis
    from ..config.database import Session
    from pathlib import Path
    from .intake import RECORDS_FILE
    queue = Redis.from_url(REDIS_URL)
    requeue = []
    with Session() as db:
        for run in db.scalars(select(Run).where(Run.state == 'running')).all():
            present, staging_dir = _staging_has_records(run.snapshot)
            attempts = int(run.attempts or 0)
            if present and attempts < MAX_ATTEMPTS:
                run.attempts = attempts + 1
                run.state = 'queued'
                run.phase = 'queued'
                run.progress = 45
                run.processed_count = 0
                # 上次的聚合库可能只写了一半：删掉项目自己的聚合文件后重算。
                try:
                    (Path(staging_dir) / 'aggregate.sqlite3').unlink()
                except OSError:
                    pass
                requeue.append(run.id)
            else:
                run.state = 'failed'
                run.phase = ''
                run.error = COMPUTE_INTERRUPTED
                db.commit()
                cleanup_staging(staging_dir)
            db.commit()
    for run_id in requeue:
        try:
            queue.rpush('analysis:queue', run_id)
        except Exception:
            # 队列不可用时不要把任务留在 queued 假象里：保留可读原因并清理暂存。
            logging.exception('requeue failed for run %s', run_id)
            with Session() as db:
                run = db.get(Run, run_id)
                if run and run.state == 'queued':
                    staging_dir = (run.snapshot or {}).get('staging_dir')
                    run.state = 'failed'
                    run.phase = ''
                    run.error = COMPUTE_INTERRUPTED + '（队列暂不可用，无法重新入队）'
                    db.commit()
                    cleanup_staging(staging_dir)
