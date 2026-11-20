# Native writer design

How the native VIREL writer is structured and why: the accepted decisions,
the constraints they rest on, and the design work still open. The working
implementation is the pooled native-output benchmark
([benchmarks/generation-writer.md](../benchmarks/generation-writer.md)); it
is not a production writer. The ratified constraints behind every decision
here are distilled in `.knowledgebase/memories/` (`find-writer-*`,
`find-signal-*`).

## Accepted decisions

**Producer-side encoding with pooled references.** Producers encode events
against a chunk's anchor and pools; background writers drain buffers,
publish chunks and perform sink I/O. A small potential application-thread
latency win does not justify potentially substantial extra CPU from
routinely staging and re-encoding every event. Full staging remains an
experimental alternative, not a rejected one — the initial benchmark did
not prove an extra-CPU penalty or a stable latency win (its inline strings
did not represent pool hits). Follow-up measurements must separate
registered handles, pool hits and misses.

**Partial buffers publish without rotation.** A partly filled buffer becomes
visible without rotating the chunk or resetting its pools. An idle
producer's buffer is published by the background writer under an age limit,
so a recently started buffer is left alone. Open: the limit's value, the
production mechanism (the benchmark's committed-prefix drain is one
candidate), and the interaction with live-header publication and loss
markers.

**Signal-handler recording uses per-thread lanes and the S3 stack table.**
Profiling samples may be recorded from async signal handlers (Linux, macOS;
not Windows). A handler runs on the thread it interrupts, so it must never
touch that thread's normal buffer, pool slice or hazard slot — making the
normal path re-entrant would tax every event. The working design:

- one signal lane per thread and nesting level: own buffers, committed
  cursor, hazard slot, nesting guard; initial-exec TLS; registration before
  the first signal; drop-and-count on any unavailable resource;
- stacks intern into one global lock-free table per generation (fixed frame
  arena, slot reservation, release-CAS publication; a lost race reuses the
  equal entry) — the handler writes only the key into its lane;
- two nesting levels per thread, selected by an atomic depth counter;
  deeper arrivals are dropped and counted. The S3 table itself tolerates
  re-entrant delivery; the lanes do not, which is why lanes are per level.

This beat the alternatives at the target scale (512/1024 frames, 64
threads): staging raw frames in the handler saved 1-4 us of handler p50 but
cost 3-12x background CPU per sample and 4-8 KiB of lane records; S3 has
~18-byte lane records and cheap background work. Re-entrant delivery was
validated with deterministic injection at four points, including inside an
unpublished table insert. Open: Linux many-core contention, ARM memory
ordering, a real stack walker, lane ring sizes.

**Signed native start times.** Native chunk-relative start offsets are
signed (ZigZag through the nine-byte varint); durations stay unsigned;
append order is independent of time order. Completion-time recording of
nested duration events can append an earlier start after a later one, and
starts may precede the chunk anchor. Propagating this to the protocol text,
schemas and converters is still pending; the JFR annex stays byte-frozen.

## Constraints for already encoded buffers

An encoded event depends on both a time anchor and a pool-key mapping.
Keeping pool keys stable does not let a writer copy an event into a chunk
with a new anchor; a valid timestamp does not make its pool references
valid.

- **Pool validity is chunk-scoped.** A key in another chunk is decodable
  only if that chunk supplies its definition; a reader cannot be required
  to retain an earlier chunk's pool.
- **Registration handles and wire pool keys are different concepts**, even
  where an implementation gives them the same numeric value. Registration
  alone does not make a value decodable in a new chunk; its definition must
  be emitted there.
- **Buffers are transport units, not chunks.** Several producer buffers may
  feed one logical writer's chunk; a sink may service several logical
  writers, but their pool namespaces and event destinations stay distinct.
  (Verified against OpenJDK: `JfrStorage::write_full()` drains multiple
  retired buffers through the existing chunk writer — JFR does not create a
  chunk per thread flush.)

## Open design: binding buffers to a chunk generation

Proposal, not implemented. A logical writer would publish a **generation
descriptor** — the chunk's immutable time anchor, generation identity and
bounded pool state. Producers would encode each buffer entirely against one
descriptor; the descriptor travels with the buffer, outside event bytes. On
a generation change, the producer seals its partial old-generation buffer
at an event boundary and acquires storage for the new generation; a
duration event picks its generation when encoded at completion. The drain
writer could then append accepted buffers without decoding or rewriting
payloads, and normal flushes would not repeat metadata and pool snapshots.

Two problems must be solved before this path is safe:

1. **Descriptor acquisition and reclamation.** A producer must observe
   generation changes and retain the descriptor safely. A plain load plus a
   reference-count increment is insufficient — retirement can race between
   the two. An acquisition/reclamation protocol must be established and
   shown bounded before this is claimed safe or inexpensive.
2. **Rotation versus unbounded producer pauses.** See the next section.

Pool rules the proposal would carry (all still to resolve):

- A `(generation, pool type, key)` binding stays immutable — no key reuse
  within a generation, including after a dropped event; exhaustion uses a
  defined fallback, never wraparound or resizing.
- Hits record keys without copying values; registered handles skip the
  hash/compare. Benchmark both paths separately, including small-key
  varint widths.
- Miss handling needs bounded probe, storage and synchronization budgets; a
  producer must not spin on another thread's pending insertion. Duplicate
  immutable entries may be preferable to waiting but consume capacity
  faster — measure.
- Definitions are retained independently of buffer delivery: if buffer A
  introduces a key and buffer B references it, dropping A must not discard
  B's only definition, including composite-pool dependencies.
- Reader visibility: make required definitions discoverable before event
  bytes are committed. This does not establish crash durability.

Strings have a defined inline encoding usable as a bounded miss fallback.
Non-string pooled fields are bare keys — substituting inline bytes would
corrupt framing — so their fallback is dropping the event or a declared
alternate representation. Null must never stand in for a lost value.

## Open design: rotation and paused producers

A producer paused halfway through encoding an old-generation record creates
a conflict among four goals: a fixed rotation deadline, preserving its
events, never re-encoding them, and bounded memory/open generations. The
normal path is cooperative handoff at event boundaries, with the old chunk
draining while producers switch to the new generation (its overlap counts
against the budget). For pathological pauses, the strict
one-generation/one-chunk assignment offers two unaccepted policies:

| Policy | Consequence |
|---|---|
| Defer closing while waiting for the producer | preserves its events, gives up the rotation deadline; resource exhaustion eventually needs a recording policy |
| Close admission after a bounded grace period | bounded finalization; late events count as loss |

A third option relaxes the assignment: emit a late buffer as a separate
self-contained chunk with its original anchor and key bindings — event
bytes unchanged, metadata and pools repeated. Overhead moves to the
background writer instead of rewriting every event. It needs an explicit
acceptance-order contract: per-writer order is chunk/append order, so an
already accepted record must never silently move behind a later record from
the same producer; a generation number or timestamp alone does not
establish acceptance order. Giving a late chunk a new writer identity would
change stream identity and is not a transparent solution.

Closing admission is not memory reclamation: a paused producer may still
read its descriptor, pool entries or buffer on resume. Cap retained
generations, pool bytes, buffers and producer slots explicitly; when the
budget is exhausted, defer rotation where allowed or enter the defined loss
path — never block application threads.

## Proposed starting policy

Start with cooperative rotation and a soft rotation deadline; bound retained
generations and storage; when a paused producer exhausts the budget, refuse
new work through the defined loss path rather than wait or allocate. Keep
draining accepted buffers whenever the sink permits it. A pool miss without
a valid bounded fallback follows the loss path. This is a proposal: the
soft-deadline option needs measurements of lost work and recovery after a
paused producer resumes, and promises neither lossless recording under
unbounded pauses nor behavior under a stalled sink.

## Measurement discipline

Before selecting a production implementation, compare exceptional late
chunks against deadline-triggered loss and deferred closure, settle safe
generation acquisition/reclamation, and benchmark with:

| Case | Include in measurements |
|---|---|
| Registered handle | generation check, key cache, actual small wire reference |
| Existing-pool lookup | hash/comparison, probe count, reference encoding |
| Miss | bounded insertion/capture, contention, independent definition publication |
| Rotation | cache invalidation, partial buffers, pool/metadata emission, old-generation drain |
| Paused producer / stalled sink | late-buffer treatment, bounded retained bytes, loss and recovery |

Report producer latency together with total CPU, allocation, accepted and
lost events, retained bytes and complete output bytes. Establish equal
offered load and lossless comparable runs before interpreting CPU ratios;
separate normal hits from misses and handoff tails. The existing experiment
is a capture/encoding baseline, not evidence that the proposals above hold.

One measured caution: frequent pool-generation rotation dominates output
bytes in the tested workload (17.5 bytes/event without rotation, 25.8 at a
10 ms rotation target, 87.7 at 1 ms) — chunk/pool reset cadence is a
first-order size knob; pooled references alone do not guarantee compact
output.
