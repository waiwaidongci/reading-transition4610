"""离线验证：分批计算、全局分母、跨批重复、冻结边界、磁盘失败、并发隔离、内存。

运行：
    cd /workspace/backend
    ARTIFACT_DIR=/tmp/art python3 -m pytest ../test-artifacts/gb-0043/test_large_corpus.py -q
或直接：
    ARTIFACT_DIR=/tmp/art python3 ../test-artifacts/gb-0043/test_large_corpus.py
"""
import json
import os
import random
import resource
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'backend'))

from src.services.algorithm import (DUPLICATE_SESSION_MESSAGE, SessionIndex,
                                    TransitionAggregator, calculate)
from src.services import staging
from src.services.intake import iter_frozen_records

HERE = Path(__file__).resolve().parent


def make_records(count, seed=7, sessions=20, chapters=30):
    rng = random.Random(seed)
    records = []
    for index in range(count):
        length = rng.randint(1, 6)
        path = '>'.join('c%02d' % rng.randrange(chapters) for _ in range(length))
        records.append({'id': index + 1, 'source_id': 1,
                        'reference': 'r%d' % index,
                        'values': {'session': 's%06d' % index, 'path': path}})
    return records


def run_batched(records, batch_size, min_count=1, top_n=50):
    handle, path = tempfile.mkstemp(suffix='.sqlite3')
    os.close(handle)
    aggregator = TransitionAggregator(path)
    try:
        for start in range(0, len(records), batch_size):
            aggregator.add_batch(records[start:start + batch_size])
        return aggregator.finalize({'min_count': min_count, 'top_n': top_n})
    finally:
        aggregator.close()
        os.unlink(path)


def test_batch_size_does_not_change_result():
    records = make_records(4000)
    reference = run_batched(records, 10000)
    for size in (1, 7, 500, 999, 4000):
        candidate = run_batched(records, size)
        assert candidate['rows'] == reference['rows'], size
        assert candidate['summary'] == reference['summary'], size
        assert candidate['checksum'] == reference['checksum'], size
        assert candidate['diagnostics'] == reference['diagnostics'], size
        assert candidate['denominators'] == reference['denominators'], size


def test_read_order_does_not_change_result():
    records = make_records(1500)
    forward = run_batched(records, 137)
    # 先整体逆序再按同样批次写入，排序键决定结果应完全一致。
    reverse = run_batched(list(reversed(records)), 137)
    assert forward['rows'] == reverse['rows']
    # 校验和按写入顺序构造，顺序不同即视为不同输入（用于复算核对）。
    assert forward['checksum'] != reverse['checksum']


def test_staged_read_order_does_not_change_result():
    """暂存文件的读取顺序/批次大小不改变结果（写入顺序固定为主键升序）。"""
    from src.services.intake import RECORDS_FILE
    records = make_records(600)
    results = []
    for batch_size in (3, 41, 600):
        directory = Path(tempfile.mkdtemp())
        with (directory / RECORDS_FILE).open('wb') as sink:
            for record in records:
                sink.write(json.dumps(record, sort_keys=True).encode())
                sink.write(b'\n')
        handle, path = tempfile.mkstemp(suffix='.sqlite3')
        os.close(handle)
        aggregator = TransitionAggregator(path)
        try:
            for batch, _ in iter_frozen_records(directory, batch_size):
                aggregator.add_batch(batch)
            results.append(aggregator.finalize({'min_count': 1, 'top_n': 40}))
        finally:
            aggregator.close()
            os.unlink(path)
            staging.cleanup_staging(directory)
    assert results[0]['rows'] == results[1]['rows'] == results[2]['rows']
    assert results[0]['checksum'] == results[1]['checksum'] == results[2]['checksum']


def test_worker_failure_keeps_reason_and_cleans_only_own_staging(tmp_path=None):
    """模拟计算阶段磁盘写满：保留可读失败原因，且不误删其他研究暂存。"""
    import sqlite3
    from src.services.staging import is_disk_full
    error = sqlite3.OperationalError('database or disk is full')
    assert is_disk_full(error)
    message = staging.staging_error_message(error)
    assert '磁盘暂存空间不足' in message
    assert '已清理' in message
    mine = staging.create_staging_dir(201)
    other = staging.create_staging_dir(202)
    (mine / 'aggregate.sqlite3').write_text('partial')
    (other / 'records.jsonl').write_text('live')
    staging.cleanup_staging(mine)
    assert not mine.exists() and other.exists()
    staging.cleanup_staging(other)


def test_global_denominators_include_below_threshold_and_off_list_edges():
    records = [
        {'values': {'session': 'a', 'path': 'A>B>A'}},
        {'values': {'session': 'b', 'path': 'A>B'}},
        {'values': {'session': 'c', 'path': 'X>Y'}},
        {'values': {'session': 'd', 'path': 'Z>Y'}},
        {'values': {'session': 'e', 'path': 'Y>Q'}},
    ]
    result = calculate(records, {'min_count': 2, 'top_n': 1})
    # 边：A→B×2、B→A、X→Y、Z→Y、Y→Q 共 6 条；只有 A→B 达标。
    assert len(result['rows']) == 1
    row = result['rows'][0]
    assert (row['from'], row['to'], row['count']) == ('A', 'B', 2)
    assert result['diagnostics']['total_edges'] == 6
    assert result['diagnostics']['distinct_edges'] == 5
    assert result['diagnostics']['chapters'] == 6
    assert result['diagnostics']['qualifying_edges'] == 1
    assert abs(row['target_base_rate'] - 2 / 6) < 1e-12
    assert abs(row['conditional_probability'] - 2 / 2) < 1e-12
    assert abs(row['lift'] - 3.0) < 1e-12
    assert result['target_counts'] == {'B': 2, 'A': 1, 'Y': 2, 'Q': 1}
    assert result['outgoing_counts'] == {'A': 2, 'B': 1, 'X': 1, 'Z': 1, 'Y': 1}
    assert result['diagnostics']['denominator_scope'] == 'all_edges'


def test_single_chapter_sessions_produce_no_edges():
    result = calculate([{'values': {'session': 'a', 'path': 'A'}},
                        {'values': {'session': 'b', 'path': 'A>B'}}],
                       {'min_count': 1, 'top_n': 10})
    assert result['diagnostics']['total_edges'] == 1
    assert result['summary'][0]['value'] == 2
    try:
        calculate([{'values': {'session': 'a', 'path': 'A'}}], {'min_count': 1, 'top_n': 5})
    except ValueError as error:
        assert '单章会话' in str(error)
    else:
        raise AssertionError('expected no-edge failure')


def test_duplicate_session_rejected_across_batches():
    handle, path = tempfile.mkstemp(suffix='.sqlite3')
    os.close(handle)
    aggregator = TransitionAggregator(path)
    try:
        aggregator.add_batch([{'values': {'session': 's1', 'path': 'A>B'}}])
        aggregator.add_batch([{'values': {'session': 's2', 'path': 'A>B'}}])
        try:
            aggregator.add_batch([{'values': {'session': 's1', 'path': 'C>D'}}])
        except ValueError as error:
            assert str(error) == DUPLICATE_SESSION_MESSAGE
        else:
            raise AssertionError('cross-batch duplicate must be rejected')
        # 同一批内的重复同样拒绝。
        try:
            aggregator.add_batch([{'values': {'session': 's9', 'path': 'A>B'}},
                                  {'values': {'session': 's9', 'path': 'C>D'}}])
        except ValueError as error:
            assert str(error) == DUPLICATE_SESSION_MESSAGE
        else:
            raise AssertionError('in-batch duplicate must be rejected')
    finally:
        aggregator.close()
        os.unlink(path)


def test_session_index_is_independent_of_batch_size():
    sessions = ['s%04d' % index for index in range(500)]
    for size in (1, 37, 500):
        handle, path = tempfile.mkstemp(suffix='.sqlite3')
        os.close(handle)
        index = SessionIndex(path)
        try:
            index.start()
            for start in range(0, len(sessions), size):
                index.add(sessions[start:start + size])
            index.commit()
            assert index.count() == 500
        finally:
            index.close()
            os.unlink(path)


def test_staging_cleanup_is_isolated_and_reports_disk_failure(tmp_path=None):
    root = staging.ensure_staging_root()
    mine = staging.create_staging_dir(101)
    other = staging.create_staging_dir(102)
    (mine / 'records.jsonl').write_text('x')
    (other / 'records.jsonl').write_text('y')
    try:
        raise OSError(28, 'No space left on device')
    except OSError as error:
        assert staging.is_disk_full(error)
        message = staging.staging_error_message(error)
        assert '磁盘暂存空间不足' in message
    staging.cleanup_staging(mine)
    assert not mine.exists()
    assert other.exists(), '并发研究的暂存目录不能被误删'
    staging.cleanup_staging(other)
    assert not other.exists()


def test_frozen_intake_only_reads_boundary():
    """受理起点之后提交的追加不进入本次冻结。"""
    records = make_records(50)
    staged = Path(tempfile.mkdtemp())
    boundary = 50
    later = records + [{'id': 51, 'source_id': 1, 'reference': 'late',
                        'values': {'session': 'late', 'path': 'A>B'}}]
    with (staged / 'records.jsonl').open('wb') as sink:
        for record in later:
            if record['id'] <= boundary:
                sink.write(json.dumps(record, sort_keys=True).encode())
                sink.write(b'\n')
    batches = list(iter_frozen_records(staged, 7))
    consumed = [record['id'] for batch, _ in batches for record in batch]
    assert consumed == list(range(1, 51))
    assert 51 not in consumed


def test_memory_bounded_for_million_sessions_and_edges():
    """一百万会话、一百万种接续边在 256MB 内完成。"""
    handle, path = tempfile.mkstemp(suffix='.sqlite3')
    os.close(handle)
    aggregator = TransitionAggregator(path)
    batch_size = 5000
    sessions = 1_000_000
    try:
        for base in range(0, sessions, batch_size):
            batch = [{'values': {'session': 's%07d' % (base + offset),
                                 'path': 'a%07d>b%07d' % (base + offset, base + offset)}}
                     for offset in range(batch_size)]
            aggregator.add_batch(batch)
            del batch
        result = aggregator.finalize({'min_count': 1, 'top_n': 20})
        peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        assert result['diagnostics']['frozen_sessions'] == sessions
        assert result['diagnostics']['distinct_edges'] == sessions
        assert result['diagnostics']['total_edges'] == sessions
        assert peak_mb < 256, 'peak RSS %.1f MB' % peak_mb
        print('  million-session peak RSS: %.1f MB, staging %.1f MB' % (
            peak_mb, os.path.getsize(path) / 1048576))
    finally:
        aggregator.close()
        os.unlink(path)


def main():
    tests = [value for name, value in sorted(globals().items())
             if name.startswith('test_') and callable(value)]
    for test in tests:
        test()
        print('PASS', test.__name__)
    print('%d tests passed' % len(tests))


if __name__ == '__main__':
    main()
