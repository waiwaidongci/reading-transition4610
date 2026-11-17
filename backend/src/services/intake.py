"""冻结会话的磁盘暂存格式。

受理把会话写成一行一个 JSON 对象（``records.jsonl``），计算按行分批读回。
写入顺序固定为主键升序，因此读取顺序、批次大小都不改变内容与校验和。
"""
import json
from pathlib import Path

from sqlalchemy import func, select

from ..models.record import Record

RECORDS_FILE = 'records.jsonl'


def record_payload(record):
    """与 ``utils.serialization.row(Record)`` 完全一致的普通字典。

    也接受键值结果行（``select(Record.id, ...)``），避免为百万行创建 ORM 实体。
    """
    if hasattr(record, 'id'):
        return {'id': record.id, 'source_id': record.source_id,
                'reference': record.reference, 'values': record.values}
    return {'id': record[0], 'source_id': record[1],
            'reference': record[2], 'values': record[3]}


def frozen_boundary(db, source_id):
    """受理起点：此刻已提交的最大主键；之后的追加不会进入本次冻结。"""
    return int(db.scalar(
        select(func.max(Record.id)).where(Record.source_id == source_id)) or 0)


def frozen_total(db, source_id, boundary):
    return int(db.scalar(
        select(func.count()).select_from(Record)
        .where(Record.source_id == source_id, Record.id <= boundary)) or 0)


def iter_frozen_records(staging_dir, batch_size):
    """按写入顺序分批读取暂存记录，产出 ``(batch, consumed)``。"""
    path = Path(staging_dir) / RECORDS_FILE
    consumed = 0
    batch = []
    with path.open('rb') as handle:
        for line in handle:
            if not line.strip():
                continue
            batch.append(json.loads(line))
            if len(batch) >= batch_size:
                consumed += len(batch)
                yield batch, consumed
                batch = []
    if batch:
        consumed += len(batch)
        yield batch, consumed
