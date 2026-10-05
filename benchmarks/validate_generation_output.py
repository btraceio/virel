#!/usr/bin/env python3
"""Independently validate finalized native benchmark chunks, one at a time."""
import contextlib
import functools
import io
from pathlib import Path
import struct
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'validators'))
from tcf_reader import Cursor, Reader, chunk_extent

EVENT_TYPE = 1 << 30
SAMPLE_TYPE = EVENT_TYPE + 1
METHOD_COUNT = 4096
MASK = (1 << 64) - 1


def stack_id(row, producer, seq):
    z = (seq * 0x9E3779B97F4A7C15 ^ (producer + 1) * 0xD1B54A32D192ED03) & MASK
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
    z ^= z >> 31
    u = z >> 32
    pick = (((((u * u) >> 32) * u) >> 32) * row['stacks']) >> 32
    return producer * row['stacks'] + pick if row['stack_sharing'] == 'disjoint' else pick


@functools.lru_cache(maxsize=None)
def expected_frames(stack, depth):
    """Recompute the benchmark's synthetic stack (method id, bci per frame)."""
    return tuple(((f * 7 + 1) % METHOD_COUNT if f < depth // 2 else
                  (((stack * 2654435761 + f * 40503) & MASK) >> 7) % METHOD_COUNT,
                  (stack + f * 3) % 1000) for f in range(depth))


def fixture(index):
    x = index + 1
    out = bytearray()
    for _ in range(64):
        x = (x * 6364136223846793005 + 1) & ((1 << 64) - 1)
        out.append(ord('!') + (x >> 32) % 90)
    return out.decode('ascii')


def validate(path, row):
    data = Path(path).read_bytes()
    pos = chunks = events = definitions = negatives = samples = stacks = methods = 0
    previous, sample_previous = {}, {}
    values = [fixture(i) for i in range(row['cardinality'])]
    streaming = row.get('streaming', False)
    while pos < len(data):
        size, last_snapshot, metadata = struct.unpack_from('>QQQ', data, pos + 8)
        if streaming:
            # Streaming chunk (spec §3.9.6): header written once, never patched; the extent
            # ends at END_OF_PARTITION and summaries are found in-band only.
            if size != 0 or last_snapshot != 0 or data[pos + 64] != 0 or data[pos + 66:pos + 68] != b'\x80\x05':
                raise RuntimeError('native streaming header')
            size, complete = chunk_extent(data, pos)
            if not complete:
                raise RuntimeError('streaming chunk without END_OF_PARTITION')
        chunk = data[pos:pos+size]
        if (len(chunk) != size or size < 76 or chunk[:4] != b'VIRL' or
            struct.unpack_from('>HH', chunk, 4) != (0, 1) or chunk[64] != 0 or
            (not streaming and chunk[66:68] != b'\x80\x01') or metadata != 76):
            raise RuntimeError('native finalized header')
        if not streaming and not 76 <= last_snapshot < size:
            raise RuntimeError('last snapshot offset')
        reader = Reader()
        reader.record = True
        # New reader per chunk: carryover pools must not mask missing definitions.
        with contextlib.redirect_stdout(io.StringIO()):
            reader.run(chunk)
        if reader.chunks != 1 or reader.foreign_spans or reader.stats['virel.eos'] != 1:
            raise RuntimeError('independent chunk attachment')
        count = reader.stats['virel.bench.event']
        sample_count = reader.stats['virel.bench.sample']
        if count + sample_count != len(reader.records):
            raise RuntimeError('event type/count')
        verified = {}  # stack key -> stack ids whose content matched, this chunk
        for tid, start, duration, fields in reader.records:
            seq, producer = fields['sequence'], fields['producer']
            if tid == SAMPLE_TYPE:
                if not row['signal_hz'] or not 0 <= producer < row['producers'] * row.get('nest_levels', 1):
                    raise RuntimeError('sample producer')
                if seq <= sample_previous.get(producer, -1):
                    raise RuntimeError('sample order')
                sample_previous[producer] = seq
                tag, key = fields['stackTrace']
                expected = stack_id(row, producer, seq)
                if expected in verified.get(key, ()):
                    continue
                stack = reader.pools[103].get(key) if tag == '@key' else None
                if stack is None:
                    raise RuntimeError('stack not independently resolvable')
                frames = []
                for frame in stack['frames']:
                    method = reader.pools[102].get(frame['method'][1])
                    if method is None or method['name'] != f"m{frame['method'][1] - 1}":
                        raise RuntimeError('method not independently resolvable')
                    frames.append((frame['method'][1] - 1, frame['bci']))
                if tuple(frames) != expected_frames(expected, row['stack_depth']):
                    raise RuntimeError('stack content')
                verified.setdefault(key, set()).add(expected)
                continue
            if tid != EVENT_TYPE or duration is not None or not 0 <= seq < row['events']:
                raise RuntimeError('record type/range')
            if not 0 <= producer < row['producers'] or seq <= previous.get(producer, -1):
                raise RuntimeError('producer order')
            previous[producer] = seq
            if fields['label'] != values[seq % row['cardinality']]:
                raise RuntimeError('pool value not independently resolvable')
            # Only this validator adopts accepted ZigZag starts; maintained reader
            # timestamp interpretation has not yet been changed.
            offset = (start >> 1) ^ -(start & 1)
            if row['verify'] and seq % 17 == 0 and offset != -1:
                raise RuntimeError('signed timestamp')
            negatives += offset < 0
        # Independently check backward snapshot links, flags, summaries, and
        # that the header points to the final summary before eos.
        cursor = Cursor(chunk, 76)
        previous_snapshot = 0
        event_count = sample_events = 0
        final_summary = None
        defined_methods, defined_stacks = set(), set()
        while cursor.pos < size:
            offset = cursor.pos
            length, tid = cursor.varint(), cursor.varint()
            end = offset + length
            if not offset < end <= size:
                raise RuntimeError('frame bounds')
            if tid == 1:
                cursor.varint(); cursor.varint()
                delta = cursor.s64()
                if delta != (previous_snapshot-offset if previous_snapshot else 0):
                    raise RuntimeError('snapshot delta chain')
                previous_snapshot = offset
                flags = cursor.u1()
                if flags & 0x7e:
                    raise RuntimeError('snapshot flags')
                if flags & 0x80:
                    version, entries = cursor.varint(), cursor.varint()
                    summary = [(cursor.varint(), cursor.varint()) for _ in range(entries)]
                    expected = ([(EVENT_TYPE, event_count)] if event_count else []) + \
                               ([(SAMPLE_TYPE, sample_events)] if sample_events else [])
                    if version != 1 or summary != expected or (not streaming and offset != last_snapshot):
                        raise RuntimeError('committed-prefix summary')
                    final_summary = offset
                # Definition order: a method before any stack using it, a stack
                # before any sample referencing it (same snapshot counts for methods).
                for _ in range(cursor.varint()):
                    pool_type, entries = cursor.varint(), cursor.varint()
                    for _ in range(entries):
                        key = cursor.varint()
                        if pool_type == 101:
                            if cursor.u1() != 3:
                                raise RuntimeError('string definition tag')
                            skip = cursor.varint(); cursor.pos += skip
                        elif pool_type == 102:
                            if cursor.u1() != 3:
                                raise RuntimeError('method name tag')
                            skip = cursor.varint(); cursor.pos += skip
                            defined_methods.add(key); methods += 1
                        elif pool_type == 103:
                            for _ in range(cursor.varint()):
                                if cursor.varint() not in defined_methods:
                                    raise RuntimeError('stack defined before its method')
                                cursor.varint()
                            defined_stacks.add(key); stacks += 1
                        else:
                            raise RuntimeError('unexpected pool type')
                if cursor.pos != end:
                    raise RuntimeError('snapshot pool bounds')
            elif tid == EVENT_TYPE:
                event_count += 1
            elif tid == SAMPLE_TYPE:
                cursor.varint(); cursor.varint(); cursor.varint()
                if cursor.varint() not in defined_stacks:
                    raise RuntimeError('sample before its stack definition')
                sample_events += 1
            elif tid == 8:
                if cursor.pos != end or end != size or final_summary != previous_snapshot:
                    raise RuntimeError('terminal marker')
            elif tid != 0:
                raise RuntimeError('undeclared unexpected type')
            cursor.pos = end
        if ((not streaming and previous_snapshot != last_snapshot) or final_summary is None or event_count != count
                or sample_events != sample_count):
            raise RuntimeError('summary discoverability')
        samples += sample_count
        definitions += sum(len(p['entries']) for s in reader.state_snapshot_models for p in s['pools'] if p['typeId'] == 101)
        events += count
        chunks += 1
        pos += size
    if (events, chunks, definitions, negatives) != (row['committed'], row['chunks'], row['definitions'], row['negative_offsets']):
        raise RuntimeError('native container accounting')
    if (samples, stacks, methods) != (row.get('samples_committed', 0), row.get('stack_definitions', 0),
                                      row.get('method_definitions', 0)):
        raise RuntimeError('sample accounting')
    if len(data) != row['wire_bytes']:
        raise RuntimeError('complete output byte accounting')
    return {'chunks': chunks, 'committed': events, 'definitions': definitions,
            'samples': samples, 'stack_definitions': stacks, 'method_definitions': methods,
            'negative_offsets': negatives, 'bytes': len(data)}
