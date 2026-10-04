# Pooled generation writer benchmark

This prototype investigates producer encoding with bounded generation ownership,
pool references, background draining and cooperative rotation. It does not ratify
the proposed policy. The maintained protocol remains draft 0.1.

```sh
python3 -B benchmarks/run_generation_writer.py --events 40000 --repeats 5 \
  --output /tmp/virel-generation-native.json
```

## Comparisons

| Mode | Producer | Background writer |
|---|---|---|
| fixed | Encodes complete events against a fixed anchor; no per-call active-generation load | Emits definitions, copies events, publishes/finalizes one native chunk |
| generation | Checks current generation and encodes complete events against its anchor and keys | Drains old generations in order; emits definitions, copies events, publishes native chunks |
| staged | Resolves the same pools and captures absolute ticks, sequence and key; copies inline bytes only on fallback | Encodes frames and fields, emits definitions, publishes native chunks |

Staging already resolved keys avoids making the staged hit path copy strings.
It still does producer-side interning: this is a comparison of event encoding
placement, not all possible staging architectures. `fixed` shares the same
allocated two-slot storage and queue design to isolate the generation check;
it is an optimistic reference that cannot rotate. String values are 64 bytes;
normal events write a tag plus a small key, not the string bytes.

Registered handles avoid hashing and byte comparison. Their generation-specific
binding comes from a prebuilt registration map; producer caches refresh lazily.
Lookup hashes and probes existing values. Miss-then-hit begins with an empty pool
and subsequently reuses entries. Exhaustion uses 4096 distinct values with only
64 entry slots; after insertion/probe budgets run out, strings fall back to
inline UTF-8. Each lookup makes at most 16 probes. Values are immutable copies.

## Ownership and bounds

There are two fixed generation descriptors and eight 8-KiB buffers per producer.
Each descriptor owns fixed-capacity producer-local pool slices, with 256 entries
and 512 hash buckets per slice. Keys occupy disjoint ranges within one chunk's
string pool; no wire-level per-producer pool namespace is introduced. Slicing
avoids shared-table contention, but partitions capacity. This experiment does
not measure shared interning, cross-producer deduplication or composite stacks.

A producer retains one descriptor through a sequentially consistent hazard
slot. On handoff it seals its old partial buffer, clears its hazard, publishes
the new token, and rechecks the active token before touching the descriptor.
A failed recheck drops that call without retrying. Tokens increase monotonically;
reused slots never make an old token current again during the bounded run.
The single consumer retires a descriptor only after active-generation change,
a hazard scan, and checks of ready buffers. Pool definitions live in the
descriptor independently of the buffer that first used them.

When both generation slots are occupied, rotation is deferred. New active
buffers wait behind the old generation; full producer queues reject new calls.
Producers never wait for the consumer. A paused producer can therefore cause
loss in other producers: that consequence is measured, not hidden. Queue space
is checked before lookup/encoding. Pool exhaustion uses the defined string
fallback; queue exhaustion drops the entire event.

`--pause-ms` injects a pause inside producer 0's recording call while its
hazard is live. This models descheduling; its duration appears in latency
only if selected by sampling. It is not a recorder wait. `--stall-ms` initially suspends
consumer draining while rotation decisions continue. `--drop-buffer-every`
discards selected already accepted buffers to verify surviving references and
output-committed counts. Performance loss is reported separately from recording
call acceptance. LOSS/CLEAR records and their still-open semantics are not
implemented; loss is reported in benchmark metrics.

## Native output and publication

The current harness writes native `VIRL` draft-0.1 chunks with:

- The 76-byte big-endian header, one logical writer ID, stable tick/wall anchors
  and frequency, metadata and last-snapshot offsets, and final COMPLETE state.
- Complete XML-v2-envelope metadata: dynamic event type `2^30`, primitive long,
  string type 101, and fieldless end marker 8. Events contain signed ZigZag start
  offsets, sequence number, producer ID, and a tagged pooled/inline string.
- Per-buffer groups of needed string definitions in size-prefixed type-1
  snapshots. Backward links stay within the chunk. A buffer carries a fixed
  reference bitmap; introducing-buffer loss does not remove the underlying
  pool definitions. Snapshot groups emit each key once per chunk.
- A final cumulative summary of output-committed user events, followed by the
  end marker. As specified, the summary covers its prefix; the end marker is
  outside that prefix. Header offset 16 points to that summary.
- Header updates after metadata and drained buffers, with UPDATING=255 and
  intermediate generations 1..254 in a bounded atomic-byte header image; final
  state is zero. Body visibility advances only after definitions and events.

Timed runs use a bounded memory sink and two retained header images. Verification
runs additionally write files and backpatch their headers. File I/O is excluded
from performance runs. Total byte counts include headers, metadata, pools,
snapshots, summaries and end markers. Overwritten memory-sink bytes represent
consumed output; the harness does not retain the entire timed recording.

This is native chunk-output evidence, **not live-reader protocol conformance**.
The atomic header image models publication work, but there is no concurrent live
reader test, generation-wrap/ABA test or torn-file recovery test. The native
publication protocol is still open; visibility does not imply storage durability.
The prototype copies the entire header image on publication and still parses
frames in the background, even outside verification. These are implementation
costs of this prototype, not required costs of the proposed production writer.

## Measurements and checks

Calls make timing-clock reads even when their latency is not stored. Ordinary
success/drop and warm-hit samples use one in 127 calls; all successful misses
and generation handoffs are sampled separately. Initial acquisition counts as
a handoff. Tail percentiles with few samples are exploratory; consult raw sample
counts. The paused-producer scenario includes the deliberately injected pause.
The producer and consumer may sleep for benchmark pacing or idle polling outside
normal recording calls. Process CPU includes pacing, polling, pool seeding on
rotation, publication and drain; it is not an isolated encoder CPU cost.

The runner shuffles five repetitions at equal requested rates, preserving all
loss and ranges. Compare CPU only when all repetitions complete equal work.
Saturation and fault cases characterize overload, not efficiency at equal work.
OS scheduling and frequency are uncontrolled. Scheduling bursts can cause loss
at otherwise sustainable average rates. No throughput claim should omit drops.

Reported recorder storage counts fixed descriptors, pools, buffers and key
caches, excluding allocator bookkeeping. The memory sink (64 KiB), source
fixtures, thread stacks/control blocks and latency sample storage are separate
harness resources. Sample storage is preallocated for worst-case targeted
sampling, and reported separately. Queue occupancy is sampled; the allocation
limit is a hard bound. Pool slices never resize.

Global C++ `new`/`new[]`, including aligned forms, are instrumented by thread-local
recording/worker flags. Zero measured calls establishes no C++ allocation in
those paths; it does not intercept direct C `malloc`, OS allocation or library
internals. Setup, fixture creation and thread creation are excluded.

Before timing, verification checks all modes and lookup paths, frequent rotation,
pre-anchor starts, bounded exhaustion, discarded introducing buffers, paused
producers and consumer stalls. The independent validator creates a fresh existing
VIREL reader for every captured chunk, verifies values and producer ordering,
checks snapshot chains/header locators and exact summary counts, and reconciles
all output bytes and events. It separately decodes accepted ZigZag starts because
the maintained reader's native timestamp interpretation remains pending.

The first component-only run remains in
[generation-writer-initial.md](../evidence/generation-writer-initial.md) and
[raw JSON](../evidence/generation-writer-initial.json), with its source snapshot
in [evidence](../evidence/generation-component-source.cpp). It used an experimental
envelope, background per-entry definition emission and a different handle-binding
path, at 100k attempts/s. Compare architectures within the new native run;
comparing those two runs does not isolate the cost of native headers.

## Initial native results

[140 native runs](../evidence/generation-writer-native.md) and
[raw observations](../evidence/generation-writer-native.json) record five
repetitions, 40k attempts per producer at 50k attempts/s, plus saturation.
All ordinary paced cases completed without drops in every repetition. This
lower rate was chosen after some 100k/s component runs lost events; those
component results are not used to establish equal-work CPU comparisons.

The fixed/generation/staged registered paths had median successful-call p99 of
1.754/1.891/1.637 us. Their CPU medians were 7616/7798/7935 ns per committed event,
with broad overlapping ranges. Thus the generation check has no established
large penalty here, but these ranges also cannot prove it is free. Paced CPU
includes substantial harness costs: the corresponding saturation medians were
726/817/908 ns, all lossless in this run. Neither set establishes a CPU winner.

With four producers and 10-ms rotation, staging lowered median call p99 from
1.947 to 1.585 us (about 0.36 us); observed per-run p99 ranges were 1.708–2.024
and 1.441–1.654 us. CPU medians were 2853 and 2694 ns/committed, respectively,
with overlapping ranges. This comparison does not demonstrate the substantial
extra CPU from staging that would justify rejecting it on measured CPU grounds.
One-producer 1-ms rotation instead had a higher staging CPU median, again with
ranges overlapping. The current accepted architecture preference remains a
preference rather than a demonstrated performance ranking.

Rotation cadence had a clearer output-size cost. One-producer generation mode
used median 17.53 bytes/committed without rotation, 25.84 at 10 ms, and 87.72 at
1 ms. Pool definitions grew from 64 per run to 5120 and approximately 39k;
metadata/snapshots repeat too. The 1-ms case nearly redefined one string per event.
These figures describe this 64-value workload and producer-local pool layout.
Shared deduplication, more values or longer chunks would change them.

Both modes stayed within two generation slots, with approximately 115 KiB of
recorder storage for one producer and 458 KiB for four, plus the separately
reported sink/sample/fixture resources. In 50-ms pause and stall performance
cases, generation mode lost no records across five repetitions; staging lost
up to 0.966% and 0.797%, respectively. Staged records are larger, so the same
byte budget holds fewer events. These results are not a claim that staging is
slower at draining. Rotation was deferred by up to roughly 58 ms during producer
pauses, and the paused producer resumed recording in every run. The 50-ms pause
itself can be missed by selective latency sampling; recovery and defer metrics
are the evidence for that fault, not ordinary p99.

Recorded C++ allocation counts were zero in all measured recording/worker paths.
Nineteen file-producing verification cases independently decoded 1820 native
chunks. Six ASan/UBSan cases and three TSan cases passed; see
[validation details](../evidence/generation-writer-validation.md).

The candidate is viable within the tested scope. The clearest next improvement
is to decouple timely partial-buffer publication from chunk rotation, preserving
pool amortization. Shared-pool contention/deduplication, optimized draining,
thread registration churn and concurrent live-header readers still need evidence
before selecting a production implementation.

## Partial-buffer publication

```sh
python3 -B benchmarks/run_generation_writer.py --suite publication \
  --events 40000 --repeats 5 --output /tmp/virel-generation-publication.json
```

This suite publishes partial buffers into the open chunk without rotating the
generation or resetting its pools. It compares two mechanisms:

- `--flush-us N` (producer flush): a producer seals its partial buffer on its
  *next recording call* after the buffer's first event is N µs old. An idle
  producer therefore never flushes.
- `--drain-age-us N` (age-limited background drain): the producer
  release-publishes its committed byte count after each append. The background
  writer drains that committed prefix of the open buffer once the oldest
  undrained event is at least N µs old, keeping its own drain offset. The buffer
  is not sealed and no ring slot is consumed. A recently started buffer is left
  alone. Because the reference bitmap is still being written, keys for a prefix
  are parsed from its frames. The oldest event's age comes from its encoded
  start.

Publication latency is sampled capture time to header publication after the
buffer or prefix containing the event (`--observe-publication`, one call in 127,
fixed 16-byte sample slots per buffer). It excludes sink I/O and durability.
`bursty` writes 256 events per producer, then idles about 12.8 ms, leaving a
roughly half-full buffer. `slow` is 10k events/s in 64-event bursts. All runs
use four producers, the lookup workload, and 40k attempts per producer.

Verification ran each mode with producer flushes, age-limited drains (with
500-µs rotation where allowed), and discarded buffers whose prefixes had already
been drained or sampled. Every output file passed the independent per-chunk
validator: 12 files, 405 chunks.

### Results

[150 runs](../evidence/generation-writer-publication.md),
[raw JSON](../evidence/generation-writer-publication.json),
[source snapshot](../evidence/generation-publication-source.cpp).
Medians across five repeats:

| Case | No early publication p50 | Producer flush 1 ms p50 | Background age 1 ms p50 | Background age 100 µs p50 |
|---|---:|---:|---:|---:|
| paced 200k/s | 1.06 ms | 0.97 ms | 0.44 ms | 0.14 ms |
| bursty, idle gaps | 11.9 ms | 12.8 ms | 1.04 ms | 0.19 ms |
| slow 10k/s | 19.6 ms | 6.43 ms | 1.04 ms | — |

(generation mode; staged medians are similar.)

- The age-limited background drain bounds typical visibility close to the age
  limit even for idle producers. Producer-side flushing does not help an idle
  producer: in `bursty` it waits for the next burst.
- Producer flushes consumed ring slots. Paced runs with 1-ms/100-µs flushes lost
  up to 6.5% of events in some repeats, all as queue drops. Age-limited drains
  were lossless in every paced, bursty and slow repeat. One 10-ms-rotation
  generation repeat lost 0.71% (queue drops). Saturated staged loss (30%) is
  overload, not equal work.
- Output size did not change with publication cadence: 17.77 bytes/committed
  paced, 18.4 slow. Pool definitions are not repeated. Rotation adds its own
  cost, as before.
- p99 and max publication latency sometimes reached several milliseconds.
  The age limit is not a hard bound under macOS scheduling, with a 20-µs idle
  poll. Call-latency and CPU ranges overlap widely, so no producer-side cost of
  either mechanism is established or excluded.

Limitations: one drain thread polls all producers. The age decision decodes
one frame start per poll. Prefix drains parse frames for keys, a prototype cost.
No concurrent live reader, thread churn, or shared pools are covered.

## Signal-handler profiling samples

```sh
python3 -B benchmarks/gen_metadata.py   # regenerates sample_metadata.inc; checks the original blob
python3 -B benchmarks/run_generation_writer.py --suite signal \
  --events 100000 --repeats 5 --output /tmp/virel-generation-signal.json
```

Profiling samples may have to be recorded from async signal handlers, on Linux
and macOS. A handler runs on the thread it interrupts, at any instruction, so it
must never touch that thread's normal buffer, pool slice or hazard slot. Each
registered thread therefore gets a **signal lane**:

- Its own ring (4 × 16 KiB), committed cursor, SC hazard slot and generation
  token. Only that thread's `SIGPROF` handler writes it.
- A `busy` flag that rejects nested delivery (`SA_NODEFER` tests) with a counted
  drop. A lane-state check rejects signals before registration and after
  retirement. The lane pointer is in initial-exec TLS.
- The handler uses lock-free atomics, clock reads and fixed storage only. It
  saves and restores `errno`.
- At exit, the thread waits for the sampler to stop, blocks `SIGPROF`, and then
  finishes the lane as its last writer.

Samples carry a pooled stack trace whose frames reference pooled methods:
`virel.bench.sample` → `StackTrace` → `Frame[]` → `Method`. The schema is
generated by `gen_metadata.py`. A deterministic synthetic stack stands in for the
stack walk, and the validator recomputes it. Method definitions (symbolization)
are always emitted by the background, before the stacks that use them.

Two stack modes:

| Mode | Handler | Background |
|---|---|---|
| `intern` (S1) | hashes frames, probes and inserts into the lane's own stack-pool slice (256 entries per generation), release-publishes the pool count, writes a small event with the key | emits definitions up to the published count, copies events |
| `stage` (S2) | copies raw frames (8 bytes each) into the lane buffer | interns into one per-generation table (1024 entries), emits definitions, encodes events |

The total stack-entry budget is equal at four producers. Exhaustion drops the
sample: a pooled stack field has no inline fallback. A sampler thread sends
thread-directed `SIGPROF` at a fixed rate per producer. Meanwhile, the normal
lane records 50k lookup events/s per producer in generation mode, with
age-limited publication at 1 ms. Standard signals coalesce while pending, so
handler entries can be fewer than signals sent.

### Results

[60 runs](../evidence/generation-writer-signal.md),
[raw JSON](../evidence/generation-writer-signal.json),
[source snapshot](../evidence/generation-signal-source.cpp). Medians across five
repeats, 4 producers, depth 32 unless noted:

| Scenario | Handler p50 intern / stage | Output bytes/sample intern / stage | Lane bytes/sample intern / stage | Background ns/sample intern / stage | Loss intern / stage |
|---|---:|---:|---:|---:|---:|
| 256 shared stacks, 1 kHz | 1.57 / 1.04 µs | 32.3 / 21.0 | 18.7 / 282 | 666 / 1166 | 0 / 0 |
| 256 shared, 10 kHz | 1.11 / 0.65 µs | 18.4 / 17.0 | 19.0 / 282 | 215 / 649 | 0 / 0 |
| depth 64, 1 kHz | 2.00 / 1.17 µs | 48.1 / 25.0 | 18.7 / 538 | 687 / 1363 | 0 / 0 |
| 1024 shared stacks | 1.43 / 1.00 µs | 80.1 / 33.1 | 18.4 / 282 | 1119 / 1253 | **74.4%** / 0 |
| 128 per thread, disjoint | 1.52 / 1.01 µs | 25.3 / 25.3 | 18.6 / 282 | 639 / 1279 | 0 / 0 |
| 10-ms rotation | 1.46 / 0.97 µs | 159.1 / 72.7 | 17.6 / 282 | 2519 / 2553 | 0 / 0 |

- **Handler time:** staging was lower in every scenario; the per-repeat p50
  values do not overlap. Both include the synthetic stack generation and two
  clock reads, so the interning cost itself is roughly the 0.4–0.8 µs difference.
  p99 was also lower for staging; p99.9 (20–33 µs) is similar in both and
  dominated by scheduling.
- **Output bytes:** staging deduplicates stacks across threads, so a shared hot
  stack is defined once per chunk instead of once per lane. With disjoint stacks
  the outputs are equal.
- **Budget partitioning:** with 1024 stacks shared by all threads, each lane's
  256-entry slice had to hold all of them and lost 74%. The shared table at equal
  memory lost nothing.
- **Costs of staging:** 15–29× more signal-buffer bytes per sample, so the same
  lane budget holds proportionally fewer samples during a sink stall. Background
  work per sample is about 2× higher. At 4 kHz total samples that is about 2 ms of
  CPU per second; process CPU per event ranges overlap, so no total-CPU
  difference is measurable.
- Normal-lane event call p99 ranges overlap between modes. About 5% of samples
  interrupted a live normal-lane recording call; at 10 kHz, about 2.6%. Measured
  C++ allocations were zero, including in handlers.

Limitations: macOS x86-64 only; **Linux is not yet measured**. The stack walk is
synthetic, and real unwinding cost and its signal safety are out of scope. TSan
defers signal delivery, so it is weak evidence for handler paths; nesting is
exercised under ASan only. A global lock-free table (S3) was not tested. Reported
signal storage includes a 256 KiB per-lane harness array of handler timings.

### Global lock-free stack table (S3)

A third stack mode, `global`, has handlers intern into **one** table per
generation (1,024 entries, the same total budget), shared by all lanes:

- A handler that misses reserves a slot with `fetch_add`, writes the complete
  entry, and then publishes it into the hash bucket with a release
  compare-and-swap. Readers acquire-load bucket values, so they only ever see
  complete entries. Nobody waits. Signal-safe: lock-free atomics and fixed
  storage only.
- A handler that loses a race for a bucket re-examines the winner's entry. If it
  is the same stack, the handler uses the winner's key and its own slot is wasted
  (counted). The slot counter also covers in-progress and wasted slots, so the
  background defines exactly the keys that drained events reference.
- Reset happens only when the generation is unpinned, under the same hazard
  protocol as the rest of the prototype.

[105-run 3-way table](../evidence/generation-writer-signal-3way.md),
[raw JSON](../evidence/generation-writer-signal-3way.json),
[source](../evidence/generation-signal-3way-source.cpp). This run repeats S1/S2
alongside S3 and adds `churn-10khz` (1,024 shared stacks, 10 kHz, 2-ms
rotation) to force concurrent inserts. Medians:

| Scenario | Handler p50 intern / stage / global | Handler p99 intern / stage / global | Output bytes/sample i / s / g | Lane bytes/sample i / s / g | Background ns/sample i / s / g |
|---|---:|---:|---:|---:|---:|
| hot, 1 kHz | 1.47 / **0.79** / 1.36 µs | 6.3 / **1.9** / 5.7 µs | 32.3 / 21.1 / 21.1 | 18.7 / 282 / 18.4 | 695 / 1084 / 788 |
| hot, 10 kHz | 1.04 / **0.65** / 0.81 µs | 2.9 / **1.8** / 2.1 µs | 18.4 / 17.0 / 17.0 | 19.0 / 282 / 18.6 | 210 / 694 / 250 |
| depth 64 | 1.81 / **0.93** / 1.48 µs | 6.3 / **2.1** / 5.8 µs | 48.1 / 25.0 / 25.0 | 18.7 / 538 / 18.4 | 714 / 1264 / 632 |
| 1,024 shared stacks | 1.27 / **1.02** / 1.17 µs | 5.6 / **2.5** / 4.6 µs | 79.9 / 33.2 / 33.2 | 18.4 / 282 / 18.7 | 1307 / 1400 / 730 |
| disjoint | 1.20 / **0.92** / 1.41 µs | 4.7 / **2.1** / 6.1 µs | 25.2 / 25.2 / 25.3 | 18.6 / 282 / 18.6 | 583 / 1303 / 746 |
| 10-ms rotation | 1.26 / **0.90** / 1.39 µs | 5.1 / **1.9** / 6.3 µs | 159.0 / 72.8 / 72.8 | 17.6 / 282 / 16.8 | 2567 / 2596 / 2429 |
| churn, 10 kHz | 0.86 / **0.58** / 0.98 µs | 1.8 / **1.4** / 2.6 µs | 157.0 / 118.4 / 130.8 | 17.4 / 282 / 16.7 | 1553 / 2376 / 2495 |

- **Global gets staging's output and budget benefits with interning's small
  records.** It deduplicates across threads (bytes equal to stage), and with
  1,024 shared stacks it lost 0.2% (16 samples, at an exactly full table) where
  per-lane interning lost 74.4%. Lane records stay at about 18 bytes and
  background work per sample is near intern's.
- **It keeps the handler-side interning cost.** Handler p50 was 0.15–0.57 µs above
  staging in every scenario and p99 was 1.2–3.3× staging. The handler cost is
  hashing and comparing frames, whichever table is used.
- **Contention was negligible at four threads:** 0–5 lost CAS races per run, even
  under churn. Each wasted at most one slot.
- **No duplicate definitions:** a churn run checked every chunk, and no stack
  content was defined twice. Definitions equal the distinct stacks referenced.
  The churn bytes gap between global and stage reversed in that separate run, so
  it is run-to-run variance in how samples fall into 2-ms chunks.
- Process CPU per event and normal-lane call p99 ranges overlap across all three.

Untested: more threads (contention grows with concurrent misses), Linux, real
unwinding, and memory-ordering behaviour on ARM.

## Signal stacks at target scale: stage vs global vs hybrid

Targets (user, 2026-10-04): 512-frame stacks supported, 1024 must not degrade
terribly, 64 concurrently writing threads, Linux and macOS. The prototype was
restructured for these targets:

- One `StackTable` per generation with entries pointing into a fixed frame arena
  (4096 entries, 2M frames) and atomic buckets. Nothing resizes.
- Lane rings are 4 × 64 KiB per thread. The high bit of a record's 16-bit length
  marks raw frames.
- One background drain resolves (or interns) and defines the keys of a span,
  then writes its events.
- Stack choice is skewed and seeded per lane (cube of a uniform value; the
  hottest eighth of stacks get about half the samples). The validator mirrors it.
- Per-lane interning (S1) was removed; it was ruled out by the earlier runs.

Modes:

- `stage` (S2): the handler copies raw frames.
- `global` (S3): handlers intern concurrently, publishing with CAS.
- `hybrid` (S4): the handler hashes the frames and does a read-only lookup in
  the background-owned table. A hit records the key; a miss copies raw frames
  with the hash. Only the background inserts. A bucket can be hit only after the
  background published it, and the background writes that stack's definition
  before draining any other span.

[120-run table](../evidence/generation-writer-signal-scale.md),
[raw JSON](../evidence/generation-writer-signal-scale.json),
[source](../evidence/generation-signal-scale-source.cpp). The normal lane runs
at 5k events/s per thread. Medians over 5 repeats:

| Scenario | Handler p50 stage / global / hybrid (µs) | Handler p99 s / g / h (µs) | Lane bytes/sample s / g / h | Hybrid hit % | Background ns/sample s / g / h |
|---|---:|---:|---:|---:|---:|
| 4 threads, depth 512 | **2.7** / 4.9 / 6.7 | 18 / 24 / 28 | 4130 / 18 / 153 | 96.7 | 2963 / 844 / 1211 |
| 4 threads, depth 1024 | **4.7** / 9.0 / 7.2 | 21 / 33 / 25 | 8226 / 18 / 286 | 96.7 | 5195 / 1131 / 965 |
| 4 threads, depth 512, 10 kHz | **2.9** / 6.2 / 4.7 | 11 / 21 / 19 | 4130 / 18 / 33 | 99.6 | 2426 / 364 / 331 |
| 16 threads, depth 512 | **3.7** / 4.8 / 5.4 | 21 / 23 / 21 | 4130 / 19 / 152 | 96.8 | 3885 / 693 / 828 |
| 64 threads, depth 512 | **3.2** / 5.8 / 4.5 | 18 / 34 / 19 | 4130 / 19 / 54 | 99.1 | 3424 / 1100 / 532 |
| 64 threads, depth 1024 | **6.6** / 7.5 / 7.4 | 37 / 29 / 29 | 8226 / 19 / 90 | 99.1 | 7708 / 631 / 653 |
| 64 threads, 2048 stacks | **3.3** / 4.1 / 4.2 | 16 / 20 / 22 | 4130 / 19 / 87 | 98.3 | 3615 / 576 / 598 |
| 64 threads, 100-ms rotation | **2.7** / 4.7 / 5.2 | 16 / 18 / 23 | 4130 / 18 / 890 | 78.8 | 3521 / 1755 / 2188 |

- **No mode lost samples** in any scenario at native speed: 0.00% handler and
  table loss, with 4 × 64 KiB lanes even at depth 1024 and 64 threads.
- **Handler time:** staging is lowest at p50 everywhere. Hashing and comparing
  512–1024 frames costs global/hybrid 1.1–2.2× staging's p50. Global and hybrid
  handler times are statistically indistinguishable (ranges overlap; each wins
  some scenarios), as expected since both hash and compare on hits. At 64
  threads and depth 1024 the p50 gap narrows to ~0.9 µs and staging's p99 is the
  worst (37 vs 29 µs).
- **Background cost:** staging is 3–12× higher per sample (2.4–7.7 µs), because
  it hashes, compares and copies every sample; global and hybrid are 0.3–2.2 µs.
  With 64 threads at 1 kHz that is ~0.5 vs ~0.04–0.08 of one CPU spent draining.
  Process CPU per event ranges overlap.
- **Lane memory:** staging needs one raw sample (4–8 KiB) per record; global ~18
  B; hybrid 33–290 B in steady state.
- **Hybrid depends on the hit rate.** With 100-ms rotation each new table starts
  cold, the hit rate fell to 78.8% and lane bytes to 890 B/sample, and its
  background cost rose above global's. Global is insensitive to table age.
  CAS contention was zero lost races in timed runs, even at 64 threads.
- Output bytes/sample are the same for all three (all deduplicate across threads).

Caveats: the host has 12 logical CPUs, so 64 threads are oversubscribed:
handler p99/p99.9 include preemption, and real 64-way contention for global's
CAS is not exercised. The single sampler thread sometimes delivered fewer than
64 × 1 kHz signals (74k–128k of 128k per run; entries matched signals sent, so
this is sampling-rate variation, not loss). Synthetic stack walk; macOS x86-64
only; Linux and ARM untested.

### Benchmark defaults

With `--signal-hz` set, the benchmark now defaults to the accepted design:
`--stack-mode global --nest-levels 2`. The `--suite signal` comparison pins
`--nest-levels 1` and passes each stack mode explicitly, matching the conditions
of its recorded evidence.

### Re-entrant signal delivery

A handler can be interrupted by another signal on the same thread: another
profiling signal source, or `SA_NODEFER`. The global stack table tolerates
this by construction. Every operation is an atomic RMW or acquire load on shared
memory plus locals on the handler's stack, and nothing waits. If a nested
handler inserts the same stack while the outer holds a reserved, unpublished
slot, the nested handler publishes first. The outer's CAS fails, it reuses the
equal entry, and its slot is wasted; this is the same path as a cross-thread
race. The **lane** does not tolerate re-entry: its cursor, seal and hazard slot
are single-writer. Therefore:

- Each thread has `--nest-levels L` lanes (up to 4). A handler claims its level
  with an atomic increment of a per-thread depth counter. Nested handlers always
  finish before the one they interrupted resumes, so levels form a stack.
- Arrivals deeper than L are dropped and counted, never written to a shared lane.
- All levels share the generation's stack table. Each level has its own hazard
  slot, cursor and sequence, so per-lane order is preserved. Samples from
  different levels are ordered by timestamp only.

`--inject-every N` (test mode, requires `--nodefer 1`) deterministically raises a
nested `SIGPROF` at one of four points:

- handler start;
- inside a reserved, unpublished global insert;
- between the lane space check and the append;
- mid-append, before the committed count is published.

[Results](../evidence/signal-reentry-validation.md):

- With one level, every injected re-entry was dropped and counted.
- With two levels, every injected re-entry was recorded.
- In a single-stack, frequently rotated run, every same-stack re-entry inside a
  reserved insert produced exactly one lost CAS and one wasted slot, with no
  duplicate definitions.
- Release and ASan/UBSan outputs all validated.
- TSan defers signals, so it cannot exercise nesting.

The memory cost of extra levels is one more lane ring per thread per level.
With global's ~18-byte records a nested level can use a small ring; the
prototype uses equal sizes.
