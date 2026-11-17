"""Oracle for 会话内一阶转移：独立实现用于交叉核对。

手算口径（与 README/oracle 一致）：
* 只统计同一会话内的相邻章节，A-B-C 产生 a→b、b→c 两条边；
* 单章会话不产生边；
* 目标基率的分母是**全部边**（含未达门槛、未入前列的边）；
* 提升度 = 条件概率 / 目标基率。
"""
from collections import Counter


def oracle(records, min_count=3, top_n=20):
    edges = Counter()
    outgoing = Counter()
    incoming = Counter()
    sessions = set()
    for record in records:
        values = record['values'] if 'values' in record else record
        session = values['session']
        if session in sessions:
            raise ValueError('duplicate')
        sessions.add(session)
        path = [part.strip() for part in values['path'].split('>')]
        for a, b in zip(path, path[1:]):
            edges[a, b] += 1
            outgoing[a] += 1
            incoming[b] += 1
    total = sum(edges.values())
    if total == 0:
        raise ValueError('no edge')
    rows = []
    for (a, b), count in edges.items():
        if count < min_count:
            continue
        probability = count / outgoing[a]
        base = incoming[b] / total
        rows.append({'from': a, 'to': b, 'count': count,
                     'conditional_probability': probability,
                     'target_base_rate': base,
                     'lift': probability / base,
                     'support': count / total})
    rows.sort(key=lambda r: (-r['lift'], -r['count'], r['from'], r['to']))
    return {'rows': rows[:top_n], 'total': total,
            'sessions': len(records), 'incoming': dict(incoming),
            'outgoing': dict(outgoing)}
