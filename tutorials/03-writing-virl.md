# Tutorial 3 — writing a VIRL file

*What you will get: the three output modes of the native writer, and how
each one's output is validated. Prerequisites: Python 3 and a C++17
compiler. ~10 minutes. Read [02-reading-virl.md](02-reading-virl.md)
first.*

## The writer

`benchmarks/generation_writer.cpp` is the native writer prototype: a
multi-producer recorder with producer-side encoding, pooled references and a
background drain (the design is in
[../doc/writer-design.md](../doc/writer-design.md)). It is also the tool the
gates use to produce real native input.

Build it once:

```sh
clang++ -std=c++17 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -pthread \
  benchmarks/generation_writer.cpp -o /tmp/virel-writer     # 0 warnings expected
```

## Mode 1 — finalized chunks (default)

```sh
/tmp/virel-writer --mode generation --events 300 --producers 1 \
  --rotate-us 0 --signal-hz 0 --verify --output /tmp/demo.virl
```

The writer back-patches each chunk header at finalize (size, duration,
snapshot offset) and prints a JSON row describing the run. `--verify`
re-reads its own output and checks event counts, pool accounting and
terminal markers before exiting — a correctness gate, not a benchmark.

## Mode 2 — streaming (never seeks)

```sh
/tmp/virel-writer --mode generation --events 300 --producers 1 \
  --streaming 1 --verify --output /tmp/demo-stream.virl
```

Streaming chunks write each header once and never patch it: size 0, a
STREAMING flag, and the chunk extent found by walking events to the
END_OF_PARTITION terminator. This is what makes pipes, sockets and
append-only uploads work. Compare the headers:

```sh
xxd -l 16 /tmp/demo.virl | head -1          # size = 9245 (back-patched)
xxd -l 16 /tmp/demo-stream.virl | head -1   # size = 0 (streaming)
```

The file itself is still a valid VIRL stream — the reference reader walks
both the same way.

## Mode 3 — finalize on close

```sh
/tmp/virel-writer --mode generation --events 300 --producers 1 \
  --streaming 1 --finalize-on-close 1 --verify --output /tmp/demo-foc.virl
```

A writer on a seekable sink can finalize each chunk itself when it closes
it: one header write per chunk instead of a rewrite per publication. On a
non-seekable sink (a pipe) the seek fails and the chunk simply stays
streaming — the same code path, no error.

## Validate output

The writer's `--verify` covers self-consistency. The independent validator
recomputes the expected event stream from the run parameters and checks the
bytes against it; the benchmark runner invokes it for every file it
produces:

```sh
python3 -B benchmarks/run_generation_writer.py --suite streaming \
  --events 20000 --repeats 1 --output /tmp/streaming.json
```

A non-zero exit means a correctness failure, not slowness. The JSON's
`native_output_checks` field counts validated files and chunks. The full
suite matrix and how results are recorded:
[README.md](../benchmarks/README.md) and [../AGENTS.md](../AGENTS.md).

## What you verified

- one writer, three output modes, all producing spec-valid VIRL;
- streaming costs no header rewriting; finalize-on-close collapses
  publication to one header write per chunk;
- independent validation exists and runs in one command.

Next: [04-virc-container.md](04-virc-container.md).
