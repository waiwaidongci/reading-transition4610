"""端到端验证：HTTP 受理 → 后台冻结 → worker 计算 → 结果/CSV/说明/批准。

用 SQLite 与进程内假队列代替 MySQL/Redis，仅用于验证业务契约：
* 受理起点固定，受理开始后的追加不混入；
* 跨批重复会话被拒绝且原因可读；
* 磁盘暂存失败保留原因并只清理本次暂存；
* 冻结数量、进度、峰值内存可经 API 读取；
* 小研究结果与说明批准流程保持可读。
"""
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORK = Path(tempfile.mkdtemp(prefix='e2e-'))
os.environ['DATABASE_URL'] = 'sqlite:///' + str(WORK / 'test.db')
os.environ['ARTIFACT_DIR'] = str(WORK / 'artifacts')
os.environ['REDIS_URL'] = 'redis://127.0.0.1:6399/0'
os.environ['SEED_DEMO'] = 'false'
os.environ['INTAKE_BATCH'] = '7'
os.environ['COMPUTE_BATCH'] = '5'
sys.path.insert(0, str(ROOT / 'backend'))

from fastapi.testclient import TestClient  # noqa: E402

from src.config.database import Base, Session, engine  # noqa: E402
from src.main import app  # noqa: E402
from src.models.user import User  # noqa: E402
from src.models.run import Run  # noqa: E402
from src.services import runs as runs_service  # noqa: E402
from src.utils.security import hash_password  # noqa: E402
import src.worker as worker_module  # noqa: E402


class FakeQueue:
    def __init__(self):
        self.items = []

    def rpush(self, name, value):
        self.items.append(int(value))

    def blpop(self, name, timeout=0):
        return None

    def ping(self):
        return True


fake_queue = FakeQueue()
runs_service.queue = fake_queue
worker_module.queue = fake_queue


def wait_for(run_id, predicate, timeout=30):
    deadline = time.time() + timeout
    with Session() as db:
        while time.time() < deadline:
            run = db.get(Run, run_id)
            if run and predicate(run):
                return run
            db.expire_all()
            time.sleep(0.02)
    raise AssertionError('run %s did not reach expected state' % run_id)


def seed_user():
    Base.metadata.create_all(engine)
    with Session() as db:
        if not db.query(User).filter_by(email='admin@test.demo').first():
            db.add(User(email='admin@test.demo',
                        password_hash=hash_password('Research2026!'), role='admin'))
            db.commit()


def login(client):
    response = client.post('/auth/login',
                           json={'email': 'admin@test.demo', 'password': 'Research2026!'})
    assert response.status_code == 200, response.text
    return {'Authorization': 'Bearer ' + response.json()['token']}


def make_source(client, headers, key, records):
    source = client.post('/sources', headers=headers, json={
        'name': key + ' 大文集', 'filename': key + '.csv', 'description': '测试'}).json()
    for start in range(0, len(records), 500):
        response = client.post('/sources/%d/records' % source['id'], headers=headers,
                               json={'records': records[start:start + 500]})
        assert response.status_code == 200, response.text
    return source


def records(count, prefix='S', path_prefix='c'):
    out = []
    for index in range(count):
        out.append({'reference': '%s-%05d' % (prefix, index),
                    'values': {'session': '%s-%05d' % (prefix, index),
                               'path': '%s%02d>%s%02d' % (
                                   path_prefix, index % 5, path_prefix, (index % 5) + 1)}})
    return out


def main():
    seed_user()
    client = TestClient(app)
    with client:
        headers = login(client)
        source = make_source(client, headers, 'T-01', records(60))

        response = client.post('/runs', headers=headers, json={
            'source_id': source['id'], 'name': '大文集冻结测试',
            'parameters': {'min_count': 2, 'top_n': 20}})
        assert response.status_code == 200, response.text
        run_id = response.json()['id']

        # 受理起点之后追加：不得混入本次冻结。
        extra = client.post('/sources/%d/records' % source['id'], headers=headers,
                            json={'records': [{'reference': 'LATE-1',
                                              'values': {'session': 'LATE-1',
                                                         'path': 'c00>c01'}}]})
        assert extra.status_code == 200, extra.text

        run = wait_for(run_id, lambda r: r.phase != 'intake')
        assert run.state == 'queued', run.error
        assert run.frozen_count == 60, '并发追加混入了冻结：%s' % run.frozen_count
        frozen = run.frozen_count
        assert fake_queue.items[-1] == run_id

        worker_module.execute(run_id)
        run = wait_for(run_id, lambda r: r.state in ('completed', 'failed'))
        assert run.state == 'completed', run.error
        assert run.progress == 100
        assert run.processed_count == frozen
        assert run.peak_memory_kb > 0

        view = client.get('/runs/%d' % run_id, headers=headers).json()
        assert view['frozen_count'] == frozen
        assert view['progress'] == 100
        assert view['peak_memory_kb'] > 0
        assert view['result']['diagnostics']['frozen_sessions'] == frozen
        assert view['result']['diagnostics']['total_edges'] == frozen
        assert 'records' not in view['snapshot'], '大快照不应回传浏览器'

        listing = client.get('/runs', headers=headers).json()
        entry = [item for item in listing if item['id'] == run_id][0]
        assert entry['frozen_count'] == frozen and entry['processed_count'] == frozen

        export = client.get('/runs/%d/export' % run_id, headers=headers)
        assert export.status_code == 200 and 'from,to,count' in export.text

        # 说明批准快照仍可读。
        note = client.post('/notes', headers=headers, json={
            'run_id': run_id, 'title': '大文集复核意见', 'body': '核对冻结与分母。'})
        assert note.status_code == 200, note.text
        approved = client.post('/notes/%d/approve' % note.json()['id'], headers=headers)
        assert approved.status_code == 200, approved.text
        snapshot = client.get('/notes/%d/export' % note.json()['id'], headers=headers)
        assert snapshot.status_code == 200
        assert 'from,to,count' in snapshot.text

        # 跨批重复会话：INTAKE_BATCH=7，重复项分处不同批次。
        duplicate = records(20, prefix='D')
        duplicate.append({'reference': 'D-00000-DUP',
                          'values': {'session': 'D-00000', 'path': 'c01>c02'}})
        dup_source = make_source(client, headers, 'T-02', duplicate)
        response = client.post('/runs', headers=headers, json={
            'source_id': dup_source['id'], 'name': '跨批重复会话',
            'parameters': {'min_count': 1, 'top_n': 5}})
        dup_id = response.json()['id']
        dup_run = wait_for(dup_id, lambda r: r.state == 'failed')
        assert '会话编号重复' in dup_run.error, dup_run.error

        # 暂存目录归属：失败任务只清理自己的目录，不误删其他任务目录。
        from src.services import staging
        other_dir = staging.create_staging_dir(9999)
        (other_dir / 'records.jsonl').write_text('keep')
        with Session() as db:
            target = db.get(Run, dup_id)
            staging_dir = Path(target.snapshot['staging_dir'])
        assert not staging_dir.exists(), '失败任务的暂存应清理'
        assert other_dir.exists(), '并发研究的暂存不能被误删'
        staging.cleanup_staging(other_dir)
        print('PASS e2e intake/compute/progress/duplicate/cleanup')
    print('E2E tests passed')


if __name__ == '__main__':
    main()
