"""会话内一阶转移的磁盘暂存聚合。

百万级会话、百万种接续边无法全部装进内存，因此这里把去重、计数、分母与
排序全部交给一个临时 SQLite 暂存库完成：

* 会话编号写入带主键的 ``sessions`` 表，``INSERT OR IGNORE`` 的写入行数
  少于本批条数即说明出现了重复会话（同批或跨批都会被发现）。
* 每条相邻边写入 ``edges(from,to,count)``，按主键累加。
* 结束时从 **全部边** 生成 ``outgoing`` / ``incoming`` 两张分母表，
  未达门槛、未入前列的边同样进入分母。
* 最终候选按 (提升度降序, 次数降序, 起点, 终点) 排序；所有值都由整数
  计数确定，读取顺序与分批大小不改变结果。

Python 侧每个批次只持有一个批次大小的字典与集合，SQLite 的页缓存被压到
8MB 且排序临时文件落在磁盘上，因此进程内存不随文集规模增长。
"""
import hashlib
import json
import sqlite3

from ..config.settings import DIAGNOSTIC_NODE_LIMIT
from .domain_validation import validate_parameters

DUPLICATE_SESSION_MESSAGE = '会话编号重复，请将一个完整会话保存在同一行'
NO_EDGE_MESSAGE = '没有相邻章节转移；单章会话不形成接续边'

PRAGMAS = (
    'PRAGMA journal_mode=OFF',
    'PRAGMA synchronous=OFF',
    'PRAGMA temp_store=FILE',
    'PRAGMA cache_size=-8192',
    'PRAGMA mmap_size=0',
    'PRAGMA locking_mode=EXCLUSIVE',
)


class _SessionIndex:
    """磁盘上的会话去重索引，内存占用恒定。"""

    def __init__(self, conn):
        self.conn = conn
        self.conn.execute('CREATE TABLE sessions(name TEXT PRIMARY KEY) WITHOUT ROWID')
        self.total = 0

    def add(self, session_ids):
        """写入一批会话编号；出现重复时抛 ValueError。"""
        if not session_ids:
            return
        before = self.conn.total_changes
        self.conn.executemany('INSERT OR IGNORE INTO sessions(name) VALUES(?)',
                              [(sid,) for sid in session_ids])
        if self.conn.total_changes - before != len(session_ids):
            raise ValueError(DUPLICATE_SESSION_MESSAGE)
        self.total += len(session_ids)


class SessionIndex:
    """受理阶段使用的独立去重索引；调用方负责提交或回滚事务。"""

    def __init__(self, path):
        self.conn = sqlite3.connect(str(path), isolation_level=None)
        for pragma in PRAGMAS:
            self.conn.execute(pragma)
        self._index = _SessionIndex(self.conn)

    def start(self):
        self.conn.execute('BEGIN')

    def add(self, session_ids):
        self._index.add(session_ids)

    def count(self):
        return self._index.total

    def commit(self):
        self.conn.execute('COMMIT')

    def rollback(self):
        try:
            self.conn.execute('ROLLBACK')
        except Exception:
            pass

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


class TransitionAggregator:
    """计算阶段使用的磁盘聚合器；每个实例独占一个 SQLite 文件。"""

    def __init__(self, path):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, isolation_level=None)
        for pragma in PRAGMAS:
            self.conn.execute(pragma)
        self.conn.execute('CREATE TABLE nodes(name TEXT PRIMARY KEY) WITHOUT ROWID')
        self.conn.execute(
            'CREATE TABLE edges(a TEXT NOT NULL, b TEXT NOT NULL, n INTEGER NOT NULL, '
            'PRIMARY KEY(a,b)) WITHOUT ROWID')
        self.sessions = _SessionIndex(self.conn)
        self.processed = 0
        self._hasher = hashlib.sha256()
        self._hashed_any = False

    # ---- 分批写入 -------------------------------------------------------
    def _hash_record(self, record):
        payload = json.dumps(record, sort_keys=True)
        if self._hashed_any:
            self._hasher.update(b', ')
        else:
            self._hasher.update(b'[')
            self._hashed_any = True
        self._hasher.update(payload.encode('utf-8'))

    def add_batch(self, records):
        """写入一个批次并返回处理的记录数；会话重复时抛 ValueError。"""
        if not records:
            return 0
        session_ids = []
        node_names = set()
        edges = {}
        for record in records:
            values = record['values']
            session_ids.append(values['session'])
            path = [part.strip() for part in values['path'].split('>')]
            for a, b in zip(path, path[1:]):
                key = (a, b)
                edges[key] = edges.get(key, 0) + 1
                node_names.add(a)
                node_names.add(b)
            self._hash_record(record)

        self.conn.execute('BEGIN')
        try:
            self.sessions.add(session_ids)
            if node_names:
                self.conn.executemany('INSERT OR IGNORE INTO nodes(name) VALUES(?)',
                                      [(name,) for name in node_names])
            if edges:
                self.conn.executemany(
                    'INSERT INTO edges(a,b,n) VALUES(?,?,?) '
                    'ON CONFLICT(a,b) DO UPDATE SET n=n+excluded.n',
                    [(a, b, n) for (a, b), n in edges.items()])
            self.conn.execute('COMMIT')
        except Exception:
            try:
                self.conn.execute('ROLLBACK')
            except Exception:
                pass
            raise
        self.processed += len(records)
        return len(records)

    # ---- 汇总 -----------------------------------------------------------
    def _scalar(self, sql, params=()):
        found = self.conn.execute(sql, params).fetchone()
        return found[0] if found else None

    def _build_denominators(self):
        self.conn.execute('CREATE INDEX edges_by_target ON edges(b)')
        self.conn.execute('CREATE TABLE outgoing(a TEXT PRIMARY KEY, n INTEGER) WITHOUT ROWID')
        self.conn.execute('INSERT INTO outgoing(a,n) SELECT a,SUM(n) FROM edges GROUP BY a')
        self.conn.execute('CREATE TABLE incoming(b TEXT PRIMARY KEY, n INTEGER) WITHOUT ROWID')
        self.conn.execute('INSERT INTO incoming(b,n) SELECT b,SUM(n) FROM edges GROUP BY b')

    def finalize(self, parameters):
        params = validate_parameters(parameters)
        self.conn.execute('BEGIN')
        try:
            total = int(self._scalar('SELECT COALESCE(SUM(n),0) FROM edges') or 0)
            if total == 0:
                raise ValueError(NO_EDGE_MESSAGE)
            self._build_denominators()
            distinct_edges = int(self._scalar('SELECT COUNT(*) FROM edges'))
            chapters = int(self._scalar('SELECT COUNT(*) FROM nodes'))
            qualifying = int(self._scalar(
                'SELECT COUNT(*) FROM edges WHERE n>=?', (params['min_count'],)))
            rows = self._top_rows(params, total)
            self.conn.execute('COMMIT')
        except Exception:
            try:
                self.conn.execute('ROLLBACK')
            except Exception:
                pass
            raise
        payload = self._payload(params, rows, total, distinct_edges, chapters, qualifying)
        return payload

    def _top_rows(self, params, total):
        cursor = self.conn.execute(
            'SELECT e.a,e.b,e.n,o.n,i.n FROM edges e '
            'JOIN outgoing o ON o.a=e.a JOIN incoming i ON i.b=e.b '
            'WHERE e.n>=? '
            'ORDER BY (1.0*e.n/o.n)/(1.0*i.n/?) DESC, e.n DESC, e.a ASC, e.b ASC '
            'LIMIT ?',
            (params['min_count'], total, params['top_n']))
        rows = []
        for a, b, count, out_count, in_count in cursor:
            probability = count / out_count
            base = in_count / total
            rows.append({'from': a, 'to': b, 'count': count,
                         'conditional_probability': probability,
                         'target_base_rate': base,
                         'lift': probability / base,
                         'support': count / total})
        return rows

    def _denominator_rows(self, rows):
        names = []
        seen = set()
        for item in rows:
            for name in (item['from'], item['to']):
                if name not in seen:
                    seen.add(name)
                    names.append(name)
        if not names:
            return []
        marks = ','.join('?' * len(names))
        outgoing = dict(self.conn.execute(
            'SELECT a,n FROM outgoing WHERE a IN (%s)' % marks, names))
        incoming = dict(self.conn.execute(
            'SELECT b,n FROM incoming WHERE b IN (%s)' % marks, names))
        return [{'chapter': name, 'incoming': incoming.get(name, 0),
                 'outgoing': outgoing.get(name, 0)} for name in names]

    def _payload(self, params, rows, total, distinct_edges, chapters, qualifying):
        payload = {
            'summary': [
                {'label': '独立会话', 'value': self.processed},
                {'label': '有效接续边', 'value': total},
                {'label': '达标规则', 'value': len(rows)},
                {'label': '章节数', 'value': chapters},
            ],
            'rows': rows,
            'columns': ['from', 'to', 'count', 'conditional_probability',
                        'target_base_rate', 'lift', 'support'],
            'chart': [{'label': r['from'] + ' → ' + r['to'], 'value': r['lift']}
                      for r in rows[:12]],
            'explanation': ('P(下一章=b | 当前章=a)=边a→b次数/从a出发总次数；'
                            '目标基率=所有接续边中到达b次数/总边数；'
                            '提升度=条件概率/目标基率。无跨会话拼接，无因果推断。'),
            'checksum': self._checksum(),
            'seed': 42,
            'diagnostics': {
                'frozen_sessions': self.processed,
                'total_edges': total,
                'distinct_edges': distinct_edges,
                'chapters': chapters,
                'qualifying_edges': qualifying,
                'min_count': params['min_count'],
                'top_n': params['top_n'],
                'node_table_complete': chapters <= DIAGNOSTIC_NODE_LIMIT,
                'denominator_scope': 'all_edges',
            },
            # 分母始终来自全部边（含未达门槛、未入前列的边）。
            'denominators': self._denominator_rows(rows),
        }
        # 章节数不多时保留完整的到达/出发字典，旧页面与既有用法仍可直接读取。
        if chapters <= DIAGNOSTIC_NODE_LIMIT:
            payload['target_counts'] = dict(self.conn.execute('SELECT b,n FROM incoming'))
            payload['outgoing_counts'] = dict(self.conn.execute('SELECT a,n FROM outgoing'))
        return payload

    def _checksum(self):
        hasher = self._hasher.copy()
        hasher.update(b']' if self._hashed_any else b'[]')
        return hasher.hexdigest()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


def calculate(records, parameters):
    """内联输入（旧快照/小研究）的兼容入口；与分批路径共用同一聚合逻辑。"""
    import os
    import tempfile
    handle, path = tempfile.mkstemp(prefix='transition-', suffix='.sqlite3')
    os.close(handle)
    aggregator = TransitionAggregator(path)
    try:
        for start in range(0, len(records), 500):
            aggregator.add_batch(records[start:start + 500])
        return aggregator.finalize(parameters)
    finally:
        aggregator.close()
        try:
            os.unlink(path)
        except OSError:
            pass
