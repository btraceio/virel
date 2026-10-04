# Writer pipeline experiment

The design priority is low application-thread latency without disproportionate
total CPU cost. This experiment compares where capture and encoding occur. It is
**not a complete native VIREL writer**. Following review, the preferred direction
is producer-side encoding with pooled references and background draining and
publication; full staging remains an experimental alternative. This is a design
decision, not a performance conclusion established by the initial measurements.

Run with Python 3 and a C++17 compiler:

```sh
python3 benchmarks/run_writer_pipeline.py --output /tmp/virel-pipeline.json
```

The runner builds an optimized executable in a temporary directory, validates
decoded records, warms up each mode, then runs a shuffled three-repeat matrix.
It writes raw observations and environment information to JSON and a median/range
table to the adjacent Markdown file. Increase `--events` and `--repeats` for
stronger evidence, especially tail percentiles. `CXX` or `--compiler` selects the
compiler. Each executable invocation is a separate process.

## Compared paths

| Mode | Producer | Consumer |
|---|---|---|
| `staged` | Captures absolute ticks, three fixed-width scalars, and inline bytes | Encodes fields, relative timestamp, and framing |
| `payload` | Captures absolute ticks and encodes payload fields | Encodes relative timestamp and framing; copies payload |
| `direct` | Encodes the entire event against one fixed anchor | Copies the completed event |

All paths copy records into the same bounded byte-buffer queue design and copy
wire bytes into the same bounded memory sink. Local scratch buffers are used in
all modes; this is not an optimized in-place reservation implementation. The
direct path is an optimistic fixed-anchor reference: it does not solve chunk
rotation, metadata, or pool ownership. Its internal queue prefix is still present.

Each producer owns eight preallocated 16 KiB buffers and an SPSC handoff to one
consumer. Publication occurs when the next record does not fit, and at producer
completion. The consumer releases complete buffers. A producer drops an attempted
record when its next buffer is unavailable; there is no retry, blocking wait, or
queue allocation inside the recording call. Capture currently happens before
checking queue capacity, so dropping still pays capture/encoding cost. Producers
may block in **benchmark pacing outside** the recording call. Producer/consumer
idle waits are harness choices, not proposed protocol requirements.

Events contain a sequence number, producer ID, a broad-width integer, and an
inline UTF-8 string of 0, 64, or 256 bytes. Values are preconstructed; generation
is excluded, but capture/copy is included. The broad-width integer exercises the
9-byte varint. This is one scalar distribution, not representative of every tiny
observability event. Strings come from one or 4096 fixtures; the diverse case
exercises capture locality, **not dictionary insertion or pool exhaustion**.
These inline-string cases do not represent events carrying an existing pool key.
Their observed latency reductions must not be presented as benefits for typical
pooled-string events. Even the zero-byte case lacks pool lookup and lifetime costs.

The experimental envelope uses a padded two-byte size, type ID 2, ZigZag start
offset, and payload. It has no metadata or chunk header and cannot be read as a
standalone recording. Rebase runs change the consumer's anchor every 4096 accepted
events, including starts before the anchor; they do **not** measure complete chunk
rotation. Direct mode rejects rebasing. Draft version 0.1 remains unchanged.

## Metrics and interpretation

- Sampled recording-call p50/p99/p99.9 and sampled maximum, separately from drop
  latency. One in 127 attempts is sampled, with a producer-dependent phase.
  Sampling includes two clock reads around the call; the call itself captures a
  timestamp. Timer overhead is not subtracted. Successful samples include buffer
  handoff when sampled, but do not isolate its tail. A fixed sampling interval can
  miss periodic effects; these are exploratory latency measurements.
- Process user + system CPU per attempt and per accepted event, total CPU seconds,
  and average cores used. This includes producer and consumer work, harness pacing,
  idle polling, and draining. Setup, fixture generation, allocation of queues and
  sample storage, thread creation, and post-run aggregation are excluded. Thread
  teardown is included. It is not an isolated encoder instruction-cost metric.
- Accepted throughput, attempted/accepted/dropped counts, and wire and intermediate
  bytes per accepted event. Different intermediate sizes change effective queue
  capacity; byte budgets, not record counts, are equal.
- Inline record bytes exclude all metadata, pool, snapshot, and chunk overhead.
  Logical intermediate bytes are not hardware memory-bandwidth measurements.

Compare CPU ratios first at **equal offered load with zero drops in every run**.
The paced cases issue batches of 64 at 200k attempts/s per producer. Scheduling
can create catch-up bursts. Saturated and initial-100-ms-stall cases characterize
bounded overload; dividing their CPU by accepted events includes work spent on
dropped events and is not an equal-work encoding comparison. Throughput includes
final drain and any remaining initial stall. Accepted latency alone must never
be used to rank an implementation that drops more events.

Runs use ordinary OS scheduling, without core affinity, frequency control, or
machine isolation. Repeat on an idle target machine and inspect ranges before
claiming a difference. Cross-run timestamps may have different varint widths.
There is no allocation instrumentation: the timed recording path uses fixed
storage by inspection, but this is not proof about runtime/library allocations.

## Verification and next steps

Before timing, the runner decodes every accepted frame in verification runs and
checks size, type, timestamps, scalar values, source-string bytes, strictly
increasing per-producer sequence numbers, and accepted/consumed accounting. It
exercises two producers, all three payload sizes, frequent rebasing, and forced
queue exhaustion. Each invocation also checks varint boundaries including the
raw ninth byte, ZigZag extrema, and a negative-offset frame. Verification decoding
is disabled during performance runs.

Before selecting a production design, add meaningful pool-key ownership and
bounded high-cardinality behavior, full chunk rotation/publication, allocation
instrumentation, realistic event mixes and stacks, thread churn, partial-buffer
visibility, and repeated sink stalls. Optimize scratch copies only with equivalent
correctness checks. This first experiment answers a narrower question: how much
latency and CPU differ for these concrete capture/encoding paths.

## Initial local run

The [60-run table](../evidence/writer-pipeline-initial.md) and
[raw observations](../evidence/writer-pipeline-initial.json) preserve three repeats
of the seven scenarios (no direct mode for rebasing), with 200k attempts per
producer per run, on the recorded macOS/x86-64 environment.

This run does not establish a CPU winner. For example, the tiny-event staged and
payload modes both completed all attempts in every repeat. Their median total
CPU costs were 2689 and 2618 ns/event respectively, but their ranges overlap:
2526–2918 and 2168–2738 ns/event. The direct baseline lost events in one repeat.
Several other paced cases also lost events. The table includes maximum drop rate
and the number of lossless repeats so a zero median cannot hide a lossy run.

Staging often had lower sampled producer p99 in this run, but the tiny-event
staged p99 ranged from 1452 to 5049 ns, so it does not establish a stable tail
latency advantage either. Paced CPU includes idle polling and timer/scheduler
costs; it must not be extrapolated linearly to predict CPU at higher event rates.
The next measurement should isolate the machine, calibrate harness/idle costs,
and establish a sustainable lossless offered rate before estimating incremental
CPU overhead. The results do not establish an architecture winner; the subsequent
design preference is recorded in [writer design notes](../doc/writer-design.md).

The next workload matrix must distinguish registered handles, pool hits and
bounded misses, including the cost of keeping keys valid across rotation. If a
staging alternative copies strings on hits while the direct path writes keys,
report that architectural difference explicitly instead of making all paths copy
inline strings. Count background pool/snapshot work in total CPU and bytes.

Validation also passed with AddressSanitizer and UndefinedBehaviorSanitizer for
all three modes, four producers, 256-byte diverse payloads, and saturation;
staged/payload modes exercised frequent rebasing, and direct mode a sink stall.

## Pooled generation investigation

The next experiment is implemented separately in [generation-writer.md](generation-writer.md).
It compares registered handles, lookup hits, bounded misses/fallbacks, generation
tracking, native chunk output, rotation and fault recovery. Use its native results
instead of the initial inline-string experiment for pooled-reference conclusions.
