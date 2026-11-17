"""分层验证：oracle、离线字段级检查、端到端 HTTP、百万级内存。

三个脚本互相独立，均不需要 Docker：

    cd /workspace/backend
    ARTIFACT_DIR=/tmp/art python3 ../test-artifacts/gb-0043/test_large_corpus.py
    python3 ../test-artifacts/gb-0043/test_http_flow.py
    ARTIFACT_DIR=/tmp/art python3 -c "
import sys; sys.path.insert(0,'../test-artifacts/gb-0043')
from test_algorithm_oracle import oracle
from src.services.algorithm import calculate
records=[{'values':{'session':'s1','path':'A>B>C'}},{'values':{'session':'s2','path':'A>C'}}]
a=oracle(records,1,10); b=calculate(records,{'min_count':1,'top_n':10})
assert a['rows']==b['rows'] and a['incoming']==b['target_counts']
print('oracle matches')"

依赖：SQLAlchemy、FastAPI、httpx、redis、PyJWT（HTTP 测试用 SQLite 与进程内假队列，
不需要 MySQL/Redis）。

## 覆盖清单

| 要求 | 验证位置 |
|---|---|
| 百万会话、百万种不同接续边 | `test_memory_bounded_for_million_sessions_and_edges`（100 万会话，峰值 RSS ≈ 65 MB） |
| 受理与计算各在 256MB 内 | 同上 + 两处 `MEMORY_LIMIT_MB` 守护；超限保留可读原因 |
| 受理起点冻结已提交会话 | `test_frozen_intake_only_reads_boundary`、HTTP 流程中的 `LATE-1` 追加 |
| 并发追加不混入 | HTTP 流程：提交后立即追加，断言 `frozen_count == 60` |
| 读取顺序/分批大小不改变结果 | `test_batch_size_does_not_change_result`、`test_staged_read_order_does_not_change_result` |
| 全局分母含未达门槛与未入前列的边 | `test_global_denominators_include_below_threshold_and_off_list_edges` |
| 跨批重复会话仍拒绝 | `test_duplicate_session_rejected_across_batches`、HTTP 流程中 `INTAKE_BATCH=7` 的重复项 |
| 磁盘暂存耗尽保留原因并清理本次暂存 | `test_worker_failure_keeps_reason_and_cleans_only_own_staging` |
| 不误删并发研究暂存 | `test_staging_cleanup_is_isolated_and_reports_disk_failure`、HTTP 流程中的 `other_dir` |
| 页面可见冻结数量/进度/峰值内存 | HTTP 流程读取 `/runs/{id}` 的 `frozen_count/progress/peak_memory_kb`；前端结果页进度面板 |
| 原有小研究与批准建议可读 | HTTP 流程的说明创建、批准、批准快照 CSV 导出；小快照内联 `records` |
| 旧库升级 | `src/config/migrations.py` 幂等补列，历史任务列表/详情可读 |

## 手工烟测（Compose）

```bash
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
# 大文集：向某一来源追加 10 万条会话
docker compose exec -T backend python - <<'PY'
from src.config.database import Session
from src.models.source import Source
from src.models.record import Record
from sqlalchemy import select
with Session() as db:
    s=db.scalar(select(Source).order_by(Source.id))
    db.add_all([Record(source_id=s.id,reference='BULK-%06d'%i,
        values={'session':'BULK-%06d'%i,'path':'序章>材料>工艺>案例'}) for i in range(100000)])
    db.commit()
PY
```

随后在“分析配置”冻结该来源，观察结果页的冻结数量、进度条和峰值内存；
`/artifacts/staging/` 下每个任务一个独立目录，任务结束（成功或失败）后消失。
