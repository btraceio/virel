# AGENTS.md — VIREL

How to verify changes in this repository: which gates exist, what each needs,
how to run it, what "pass" looks like, and how the benchmarks are recorded.
Run everything from the repository root. Python scripts take `-B`, which keeps
`__pycache__` out of the tree.

## Orientation

- `doc/TraceContainerFormat.md`: the normative draft (v0.1, revised in place;
  do not bump the version). `doc/tcf-rfc.md` mirrors it and must be kept in
  sync. `doc/tcf-framing.ksy` (Kaitai), `doc/tcf-model.avsc` (Avro model,
  generated), `doc/m0-framing.proto` and `doc/tcf-schema.cddl` are the
  machine-readable artifacts.
- `validators/`: reference readers and gates. `tcf_reader.py` reads
  VIRL/JFR chunk streams; `virel_container.py` writes, reads, sniffs and
  finalizes; `tcf_model.py` maps files onto the Avro model.
- `benchmarks/`: the C++ native writer prototype (`generation_writer.cpp`),
  its runner and output validator, and the older capture/encoding pipeline
  experiment.
- `evidence/`: recorded benchmark results plus frozen sources.
- `WORK.md`: current decisions and open items. `.investigations/jfr-spec/`:
  the investigation memory graph. `.investigations/.active` is a
  session-local lock; never commit it.

## Prerequisites

| Tool | Needed by | Notes |
|---|---|---|
| Python 3 (3.14 used) | everything | standard library only for the gates |
| C++17 compiler (`clang++`; `CXX` overrides) | gates 6 and 7, benchmarks | writer builds must have 0 warnings with `-Wall -Wextra -Wpedantic` |
| Go (1.27 used) | gates 3, 4 and 6, fixture generation | `go tool trace` and `go tool pprof` are used as reference parsers |
| JDK with `java` and `jfr` (26 used) | gate 6 | records a multi-chunk JFR file; `jfr summary` is the reference parser |
| Node.js (24 used) | fixture generation (gate 5) | `node --cpu-prof` |
| Kaitai Struct compiler 0.11 + `kaitaistruct` runtime | gates 6 and 7, optional | without them the Kaitai step reports SKIP and the gate still passes |
| `fastavro` | optional strict Avro schema parse | the gates use their own model lint |
| `perfetto` Python package | optional Trace Processor cross-check of the Perfetto gate | downloads `trace_processor_shell` |

Optional tools go in a scratch virtualenv, never into the repository. Example:

```sh
python3 -m venv /tmp/virel-tools
/tmp/virel-tools/bin/pip install kaitaistruct fastavro perfetto
curl -sLO https://github.com/kaitai-io/kaitai_struct_compiler/releases/download/0.11/kaitai-struct-compiler-0.11.zip
unzip -q kaitai-struct-compiler-0.11.zip -d /tmp
export KSC=/tmp/kaitai-struct-compiler-0.11/bin/kaitai-struct-compiler
# run gates 6/7 with /tmp/virel-tools/bin/python so the Kaitai step runs
```

## Fixtures

Gates 3, 4 and 5 read fixtures from fixed `/tmp` paths. `/tmp` is not
persistent, so regenerate them with:

```sh
sh validators/fixtures/make_fixtures.sh
```

| Fixture | Producer | Read by |
|---|---|---|
| `/tmp/tcf-gotrace.out` | `validators/fixtures/gotrace` (Go `runtime/trace`) | gate 3 |
| `/tmp/tcf-pprof-cpu.pprof`, `/tmp/tcf-pprof-heap.pprof` | `validators/fixtures/pprof` (Go `runtime/pprof`) | gate 4 (a wire regression test; pprof is out of scope as a format) |
| `/tmp/tcf-cpuproffix/*.cpuprofile` | `validators/fixtures/cpuprofile/work.js` via `node --cpu-prof` | gate 5 |
| `/tmp/tcf-ctef-real.json` | a real Chrome DevTools trace export; **not reproducible here** | gate 5 (first argument overrides the path) |

Gates 6 and 7 build their own inputs in a temporary work directory and delete
it afterwards. Set `VIREL_GATE_WORK=<dir>` to keep and reuse the inputs; gate
6 then also leaves a fresh Go trace at `<dir>/go.trace`.

## Gates

Each gate prints a final pass line. Report results as
`command → key result line`.

| # | Command | Checks | Pass line | Time |
|---|---|---|---|---|
| — | `python3 -B validators/tcf_reader.py --selftest` | native chunk parsing, foreign bridging, summary flag | `selftest native chunks: 2, foreign: 1` | <1 s |
| 2 | `python3 -B validators/perfetto_gate.py` | Perfetto TracePacket stream → native VIREL, round trip against a golden model (synthetic input; `0x0A` packet framing, BOOTTIME→REALTIME via ClockSnapshot) | `GATE 2: PASSED` | <1 s |
| 3 | `python3 -B validators/go_gate.py /tmp/tcf-gotrace.out --full` | Go trace wire parse versus `go tool trace`, conversion to VIREL, reconstruction, Avro model lint | `MODEL-LINT: PASSED` and `GATE 3: PASSED` | seconds |
| 4 | `python3 -B validators/pprof_gate.py` | wire regression: pooled-reference split, Varint128, per-pool skip (parity versus `go tool pprof -raw`) | `GATE 4: PASSED` | ~1 s |
| 5 | `python3 -B validators/ctef_gate.py` | CTEF JSON and V8 cpuprofile → VIREL and back (Tier B) | `GATE 5: PASSED` | <1 s |
| 6 | `python3 -B validators/container_gate.py` | VIRC container on real inputs (9-chunk JFR, Go trace, native VIREL from the C++ writer, Perfetto, OTLP message framing, CTEF): extraction, decoding, resync, truncation/corruption recovery, live records, Avro model lint, Kaitai parser | `GATE 6: PASSED` (31 checks with Kaitai) | 1–3 min |
| 7 | `python3 -B validators/virl_stream_gate.py` | VIRL file kind and streaming chunks: FIFO output, validator on streaming and finalized output, live tail, concatenation, VIRC through a FIFO, sniffing, offline finalization, writer finalize-on-close incl. live views, Kaitai | `GATE 7: PASSED` (27 checks with Kaitai) | 1–3 min |
| — | `python3 -B validators/build_tcf_model_avsc.py` then `git diff --exit-code doc/tcf-model.avsc` | regenerates the Avro model; must not drift from the builder | `named types: 45 \| duplicates: none`, no diff | <1 s |
| — | `python3 -B benchmarks/gen_metadata.py` then `git diff --exit-code benchmarks/*.inc` | regenerates the writer's metadata blobs; checks the original byte for byte | `generation_metadata.inc reproduced; sample_metadata.inc written`, no diff | <1 s |

Also check spec cross-references after editing either spec document. Every
`§x.y` / "section x.y" must resolve to a heading:

```sh
python3 - <<'PY'
import re
for p in ['doc/TraceContainerFormat.md', 'doc/tcf-rfc.md']:
    s = open(p).read()
    heads = set(re.findall(r'^#+ (\d+(?:\.\d+)*)[ .]', s, re.M))
    refs = set(re.findall(r'(?:§|[Ss]ections? )(\d+(?:\.\d+)*)', s))
    print(p, 'missing refs:', sorted(r for r in refs if r not in heads))
PY
```

The RFC's single reference to 3.4.3 deliberately points at the parent draft.

Optional extra checks:
- the Kaitai compile to Java (`$KSC -t java`) followed by `javac` against
  `kaitai-struct-runtime-0.11.jar`;
- the C++ target (`-t cpp_stl`) followed by `clang++ -fsyntax-only` against the
  Kaitai C++ runtime;
- a fastavro parse of `doc/tcf-model.avsc`.

### Which gates for which change

| Change | Minimum |
|---|---|
| Spec/RFC text only | cross-reference check; keep RFC in sync |
| `.ksy` | Kaitai compile (python, java + javac); gates 6 and 7 with Kaitai |
| Avro model builder or `tcf_model.py` | build + no drift; gates 3 and 6 |
| `tcf_reader.py`, `virel_container.py` | selftest; all gates 2–7 |
| A single format gate | that gate (+ its fixture); selftest |
| `benchmarks/generation_writer.cpp` | release build 0 warnings; gate 7; gate 6; the benchmark suites below in verify mode; ASan/UBSan run for new I/O or signal paths |
| `benchmarks/validate_generation_output.py` or the runner | gate 7; one suite run |

## Writer builds and sanitizers

```sh
clang++ -std=c++17 -O3 -DNDEBUG -Wall -Wextra -Wpedantic -pthread \
  benchmarks/generation_writer.cpp -o /tmp/generation_writer      # must print no warnings
clang++ -std=c++17 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -pthread \
  benchmarks/generation_writer.cpp -o /tmp/generation_writer-asan
```

- **Useful verify run:**
  `--mode generation --events 20000 --producers 4 --rotate-us 20000 --signal-hz 1000 --verify --output out.virl`.
  Validate the output with `benchmarks/validate_generation_output.py`, passing
  the writer's JSON row (the runner does this).
- **Output modes:**
  - default: finalized chunks, header rewritten per publication; needs a
    seekable output;
  - `--streaming 1`: never seeks; works on a FIFO;
  - `--streaming 1 --finalize-on-close 1`: finalized at each chunk close when
    the output can seek, otherwise the chunk stays streaming and is counted
    in `finalize_skipped`.
- **TSan** defers signals, so it cannot exercise signal nesting; use
  ASan/UBSan for signal paths.

## Benchmarks

The runners build optimized binaries in a temporary directory, verify first,
then run a shuffled matrix. They write JSON plus an adjacent Markdown table.

```sh
python3 -B benchmarks/run_writer_pipeline.py --output /tmp/virel-pipeline.json        # capture/encoding experiment
python3 -B benchmarks/run_generation_writer.py --suite native      --output /tmp/native.json
python3 -B benchmarks/run_generation_writer.py --suite publication --output /tmp/publication.json
python3 -B benchmarks/run_generation_writer.py --suite signal      --output /tmp/signal.json
python3 -B benchmarks/run_generation_writer.py --suite streaming   --events 20000 --output /tmp/streaming.json
```

Arguments:
- `--repeats N`: default 5; use 1 for a regression check.
- `--events N`: per producer, default 100000. The streaming suite writes real
  output and indexes it in Python, so keep it at about 20000; the signal
  scenario otherwise produces files of about 650 MB.

Every suite's verification phase validates its outputs independently. A
non-zero exit means a correctness failure, not slowness. For a correctness
regression check, use one repeat and read the count of validated files and
chunks from `native_output_checks` in the JSON.

**Expected scale with one repeat** (2026-10-05):

| Suite | Runs | Validated files | Chunks | Samples | Time |
|---|---|---|---|---|---|
| native | 28 | 19 | ~2,000 | — | several minutes |
| signal | 24 | 9 | ~950 | ~600k | several minutes |
| streaming | 12 timed (60 with 5 repeats) | 11 | — | — | — |

The native and signal runs together exceeded 10 minutes on a loaded host, so
run long suites in the background.

**Recording evidence** (`evidence/`):

1. Run with the default 5 repeats into `evidence/generation-writer-<suite>.json`.
   The `.md` table is written next to it.
2. Freeze the measured source:
   `cp benchmarks/generation_writer.cpp evidence/generation-<experiment>-source.cpp`.
   Check that its SHA-256 equals `source_sha256` in the JSON.
3. Summarize in `benchmarks/generation-writer.md` with links to the table,
   JSON and source.
4. Do not run gates or other heavy work concurrently.
5. The runner records `load_average_before` and `load_average_after`. On a
   loaded host, CPU comparisons are noise: report them as such and rely on
   deterministic results (header writes, bytes, loss, validation).

## Commits

- Commit only when asked, with the gates green.
- One logical change per commit.
- The body says why the change was made and lists the verification as
  `command → key result line`.
- Never commit `.investigations/.active` or scratch artifacts.
