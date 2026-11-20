# Tutorial 2 — reading a VIRL file

*What you will get: a native VIREL file you produced yourself, walked byte
by byte — header, first events, concatenation. Prerequisites: Python 3 and
a C++17 compiler (clang++ or g++). ~15 minutes.*

## Produce a real native stream

Build the prototype writer and record 300 events with one producer:

```sh
clang++ -std=c++17 -O3 -DNDEBUG -pthread \
  benchmarks/generation_writer.cpp -o /tmp/virel-writer
/tmp/virel-writer --mode generation --events 300 --producers 1 \
  --rotate-us 0 --signal-hz 0 --verify --output /tmp/demo.virl
```

`--verify` makes the writer validate its own output before exiting. You get
a single-chunk file of about 9 KB.

## Read it with the reference reader

```sh
python3 -B validators/tcf_reader.py /tmp/demo.virl
```

Output (values vary):

```
chunk @0: class native major 0.1 size 9245 meta@76 nanos 1795169199... ticks@782805912157333 freq 1000000000
  metadata: 2 event types of 4 declared, pool 23 strings
chunks parsed: 1; foreign spans: 0
--- event counts (type: count) ---
  virel.bench.event: 300
  <state-snapshot>: 2
  <metadata>: 1
  virel.eos: 1
parsed 9245 bytes in 0.00s; total events: 304
```

Read that as: one chunk, 76-byte native header, schema at offset 76, 300
user events, 2 state snapshots carrying the pools, one end-of-stream
marker.

## The header, field by field

```sh
xxd -l 76 /tmp/demo.virl
```

```
00000000  56 49 52 4c 00 00 00 01  00 00 00 00 00 00 24 1d   VIRL.....$.
...
00000040  00 00 80 01 56 49 52 45  4c 42 45 4e              ....VIRELBEN
```

| off | bytes | field | what to see |
|----:|---|---|---|
| 0..3 | `56 49 52 4c` | magic | `VIRL` — native chunk, not JFR (`FLR\0`) |
| 4..7 | `00 00 00 01` | version | major 0 (draft), minor 1 |
| 8..15 | | chunk size | 9245 — back-patched at finalize; readers skip the chunk in one jump |
| 24..31 | | metadata position | 76 — the schema event |
| 32..47 | | start nanos, duration | wall-clock anchor |
| 48..63 | | start ticks, ticks/second | the tick axis and its rate |
| 64 | `00` | state | 0x00 = COMPLETE; a live chunk shows 0xff |
| 66..67 | | flags | high byte bit 7 SET = VIREL-native signature; low byte = stream/final flags |
| 68..75 | | writer id | random u64 — what makes cross-writer concatenation safe |

The full table with every offset is spec §3.9.2.

## The first event

Every event is `[size:varint][typeId:varint][time words][fields]`, and
`size` counts its own varint width. The first event sits at offset 76:

```
9b 02     size = 0x1b | (0x02 << 7) = 283 bytes
00        typeId = 0 → metadata event: the schema for this chunk
...       metadata payload
```

Because the size is in-band, a reader that only wants user events skips the
metadata event — and every state snapshot — without parsing a byte of
payload. That skip-by-size property is the whole recovery story: a torn
event costs the reader at most one event, never the file.

## Concatenation

Two recordings of different writers concatenate into one valid file:

```sh
/tmp/virel-writer --mode generation --events 300 --producers 1 \
  --rotate-us 0 --signal-hz 0 --output /tmp/demo-b.virl
cat /tmp/demo.virl /tmp/demo-b.virl > /tmp/concat.virl
python3 -B validators/tcf_reader.py /tmp/concat.virl | grep 'chunk @'
```

```
chunk @0:    class native major 0.1 size 9245 ...
chunk @9245: class native major 0.1 size 9245 ...
```

The second chunk's own metadata and pools make it readable on its own —
that is what "self-contained" means. If the two writers had been different
processes, their writer ids would differ, and their type-id bindings would
not collide (spec §5.1, §5.3).

## What you verified

- a native producer writes a self-contained, concatenable stream;
- the reference reader parses it with no schema registry, no protoc, and no
  prior state.

Next: [03-writing-virl.md](03-writing-virl.md) — the writer's output modes.
Or jump to the container: [04-virc-container.md](04-virc-container.md).
