import json, os
from pathlib import Path
CONFIG=json.loads((Path(__file__).parent / 'domain.json').read_text())
DATABASE_URL=os.getenv('DATABASE_URL','sqlite:///./dev.db')
REDIS_URL=os.getenv('REDIS_URL','redis://localhost:6379/0')
JWT_SECRET=os.getenv('JWT_SECRET','local-only-change-before-deployment')
ARTIFACT_DIR=Path(os.getenv('ARTIFACT_DIR','/artifacts'))
# 大文集分批参数：只影响吞吐，不影响任何结果。
MEMORY_LIMIT_MB=int(os.getenv('MEMORY_LIMIT_MB','256'))
INTAKE_BATCH=int(os.getenv('INTAKE_BATCH','500'))
COMPUTE_BATCH=int(os.getenv('COMPUTE_BATCH','500'))
INTAKE_WORKERS=int(os.getenv('INTAKE_WORKERS','2'))
# 小任务仍在快照中内联原始样本，便于旧页面/导出直接读取；大任务只保存冻结边界与磁盘暂存。
INLINE_SNAPSHOT_LIMIT=int(os.getenv('INLINE_SNAPSHOT_LIMIT','5000'))
# 分母核对表最多内联的章节数；超过后只列出结果规则涉及章节并给出全局分母摘要。
DIAGNOSTIC_NODE_LIMIT=int(os.getenv('DIAGNOSTIC_NODE_LIMIT','5000'))
