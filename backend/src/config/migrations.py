"""轻量幂等迁移：为既有数据库补上大文集所需的观测列与读取索引。

SQLAlchemy 的 ``create_all`` 只建表不改表，已有部署需要：
* 可空/默认列，用于展示冻结数量、处理进度与峰值内存；
* ``(source_id, id)`` 复合索引，让按主键游标的分批冻结走索引而不是全表扫描
  （百万级会话上实测从分钟级降到秒级）。

所有语句都先检查再执行，重复启动安全。
"""
from sqlalchemy import inspect, text

from .database import engine
from .settings import CONFIG

RUN_COLUMNS = {
    'phase': "VARCHAR(24) DEFAULT ''",
    'progress': 'INTEGER DEFAULT 0',
    'frozen_count': 'INTEGER DEFAULT 0',
    'processed_count': 'INTEGER DEFAULT 0',
    'peak_memory_kb': 'INTEGER DEFAULT 0',
    'intake_peak_kb': 'INTEGER DEFAULT 0',
    'compute_peak_kb': 'INTEGER DEFAULT 0',
    'attempts': 'INTEGER DEFAULT 0',
}

INDEXES = (
    (CONFIG['record_table'], 'ix_%s_source_id_id' % CONFIG['record_table'],
     ('source_id', 'id')),
)


def migrate():
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    run_table = CONFIG['run_table']
    if run_table in tables:
        existing = {column['name'] for column in inspector.get_columns(run_table)}
        with engine.begin() as connection:
            for name, definition in RUN_COLUMNS.items():
                if name not in existing:
                    connection.execute(text(
                        'ALTER TABLE %s ADD COLUMN %s %s' % (run_table, name, definition)))
    for table, index, columns in INDEXES:
        if table not in tables:
            continue
        known = {item['name'] for item in inspector.get_indexes(table)}
        if index in known:
            continue
        with engine.begin() as connection:
            connection.execute(text(
                'CREATE INDEX %s ON %s (%s)' % (index, table, ', '.join(columns))))
