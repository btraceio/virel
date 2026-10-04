# Native writer design notes

Status: draft 0.1 implementation discussion. The accepted direction below records
the user's decision; the generation protocol remains a proposal. A bounded
experimental implementation now exists in the
[pooled native-output benchmark](../benchmarks/generation-writer.md); it is not
a production writer or a ratified protocol change. Canonical agenda:
[WORK.md](../WORK.md).

## Accepted direction

Favor producer-side event encoding with pooled references. Background writers
drain buffers, publish chunks and perform sink I/O. A small potential improvement
in application-thread latency does not justify potentially substantial extra CPU
from routinely staging and re-encoding every event. Full staging remains available
for comparison, but is not the preferred architecture.

The initial benchmark does not prove an extra-CPU penalty or a stable latency win.
In particular, copying 64/256-byte inline strings is not representative of events
that reference existing pool keys. The next comparison must separate registered
handles, pool hits, and misses. Target allocation, contention, misses and rotation
as possible latency outliers rather than automatically deferring all encoding.

Partial buffers become visible without rotating the chunk or resetting its
pools. An idle producer's partly filled buffer is also published by the
background writer, but only once its oldest unpublished event exceeds an age
limit; a recently started buffer is left alone. Accepted in discussion; the age
limit's value, its interaction with any producer-side flush, and the production
mechanism remain open. The benchmark's committed-prefix drain is one candidate.

**Accepted (2026-10-04): S3 with two nesting levels.** Signal handlers intern
stack traces into one lock-free table per generation (fixed frame arena, slot
reservation, release-CAS publication, lost races reuse the equal entry) and
write only the key into their lane. Each thread has two signal lanes, one per
nesting level, selected by an atomic depth counter. Deeper arrivals are dropped
and counted. Validation on Linux many-core hosts and ARM is still pending.

Profiling samples may be recorded from async signal handlers (Linux, macOS).
A handler runs on the thread it interrupts, so it must not touch that thread's
normal buffer, pool slice or hazard slot. Making the normal path re-entrant
would tax every event and break committed-prefix ordering. Working design: a
per-thread signal lane with its own buffers, committed cursor, hazard slot and
nesting guard; initial-exec TLS; registration before the first signal; drop and
count on any unavailable resource. Measured in the
[signal-lane benchmark](../evidence/generation-writer-signal.md): the handler
copying raw frames and the background interning them into a chunk-wide stack
pool (S2) beat per-lane handler interning (S1) on handler time, output bytes and
budget use. It cost more lane-buffer bytes and background work. A global
lock-free table written from handlers (S3) matched S2's dedup and budget with
small lane records and cheap background work, but kept S1's handler interning
cost. S2 versus S3 trades handler time against lane memory, which grows with
stack depth, and background CPU. Open. At the target scale (512/1024 frames, 64 threads;
[results](../evidence/generation-writer-signal-scale.md)) a hybrid that looks
up stacks read-only in the handler and stages only misses matched S3's handler
time, but not its lane or background savings with cold tables.

Re-entrant delivery (another signal interrupting a handler on the same thread)
must never write into the interrupted handler's lane. The S3 stack table
tolerates re-entry (lock-free, no waiting, a lost race reuses the equal entry).
Lanes are therefore per nesting level: a per-thread atomic depth counter selects
the level's lane, and arrivals past the last level are dropped and counted.
Validated with deterministic same-thread re-entry at four points, including
inside an unpublished table insert
([details](../evidence/signal-reentry-validation.md)). How many levels the
writer provides is open: one level drops nested samples; two kept every
injected one.

Native start times may move backward and precede the chunk anchor, using signed
chunk-relative offsets and ZigZag encoding. Propagating that accepted decision to
the protocol and existing converters remains pending. JFR stays byte-frozen.

## Constraints for already encoded buffers

An encoded event depends on both a time anchor and a pool-key mapping. Merely
keeping pool keys stable does not let a writer copy it into a chunk with a new
anchor. Conversely, a valid timestamp does not make its pool references valid.

Pool validity remains chunk-scoped. Repeating the same numeric key in another
chunk is possible only if that chunk supplies its definition independently; a
reader cannot be required to retain an earlier chunk's pool. Internal registration
handles and wire pool keys are different concepts even if an implementation can
sometimes give them the same numeric value.

Buffers are transport units, not self-contained chunks. Multiple producer buffers
may feed one logical writer's chunk. A physical sink may service several logical
writers, but their pool namespaces and event destinations must stay distinct.

## Investigation evidence and reconciliation

The preserved [.investigations index](../.investigations/jfr-spec/INDEX.md)
is part of the design evidence. Historical recommendations need to be read
alongside their source evidence and the later decisions in [WORK.md](../WORK.md).

- [Writer/type-id decision](../.investigations/jfr-spec/nodes/find-type-id-model.md):
  retain one logical writer per chunk, chunk-scoped pools, complete in-chunk
  metadata, and container-global add-only type bindings. Its description of
  each thread sync producing a small chunk is being reconsidered here. The
  format ownership rule does not require a chunk per producer-buffer flush.
- Checking the original OpenJDK checkout at the recorded commit
  `b22ccb9af5fc7d097f5bf24e0e87a1c02c5810dd` confirms that distinction:
  `JfrStorage::write_full()` processes multiple retired buffers through the
  existing `_chunkwriter`; `JfrRecorderService::flush()` writes storage and
  pools through that same writer. The historical claim that JFR itself creates
  a chunk for every thread flush is therefore too broad. Sources:
  [storage](https://github.com/openjdk/jdk/blob/b22ccb9af5fc7d097f5bf24e0e87a1c02c5810dd/src/hotspot/share/jfr/recorder/storage/jfrStorage.cpp)
  and [recorder service](https://github.com/openjdk/jdk/blob/b22ccb9af5fc7d097f5bf24e0e87a1c02c5810dd/src/hotspot/share/jfr/recorder/service/jfrRecorderService.cpp).
- [Pool skipping decision](../.investigations/jfr-spec/nodes/find-pool-skip.md):
  retain independently size-skippable per-pool snapshots, with small pools
  allowed to co-locate. Definition publication can use multiple snapshots;
  neither a producer flush nor a pool update requires a new chunk. Its measured
  parsing savings concern readers, not producer latency or total writer CPU.
- [Parser dynamics](../.investigations/jfr-spec/nodes/ev-parser-dynamics.md):
  cross-chunk object caching is an optimization; it cannot replace the pool
  definitions required for independent chunk attachment. Repeated entries
  carry full payload even when a reader reuses an existing object.
- [Live-header evidence](../.investigations/jfr-spec/nodes/ev-live-header-protocol.md)
  and [header checkpoints](../.investigations/jfr-spec/nodes/ev-header-checkpoints.md):
  visibility updates and chunk finalization are separate operations. Checking
  `ChunkHeader.readFileState()` in the same checkout confirms that a reader
  waits while the first state read is `0xff`; equal reads alone are insufficient.
  The current native publication proposal still needs a complete protocol.
- [Go/Perfetto evidence](../.investigations/jfr-spec/nodes/ev-prior-art-fine-tracers.md):
  producer batches, generation-scoped tables, ordered writer sequences, and
  explicit loss/reset markers are useful precedents. They do not establish
  a safe VIREL generation-reclamation algorithm or its CPU cost.

The historical "snapshot cadence = durability window" wording also needs care:
these notes establish framing and reader visibility, not an I/O flush or storage
durability guarantee. Native signed timestamps supersede the historical native
monotonic-time recommendation; the evidence about absolute JFR ticks remains.

## Proposal: bind buffers to a chunk generation

A logical writer would publish a generation descriptor containing the chunk's
immutable time anchor, generation identity and bounded pool state. A producer
would encode each buffer entirely against one descriptor. The descriptor travels
with the buffer outside event bytes; it is not a per-event wire field.

On a generation change, the producer would seal its partial old-generation buffer
at an event boundary, then acquire storage for the new generation. A duration
event would choose a generation when it is encoded at completion. Its earlier
start time would not pin an old generation for the duration of the operation.

The drain writer could then append accepted buffers to their assigned chunks
without decoding and rewriting event payloads or timestamps. Normal buffer flushes
would not repeat metadata and pool snapshots as separate chunks.

The producer still needs a way to observe generation changes and safely retain
the descriptor. A per-event generation check and a buffer-level reference are
candidates, not assumed-free operations. A plain load followed by incrementing a
reference count is insufficient: retirement can race between those operations.
An implementation must establish an acquisition/reclamation protocol before
claiming this path is safe, bounded or inexpensive.

## Proposed pool rules and costs to resolve

- A `(generation, pool type, key)` binding would remain immutable. No key reuse
  within a generation, including after a dropped event or buffer. Exhaustion
  would use a defined fallback rather than wraparound or table resizing.
- Registered handles could cache their generation-specific keys. A generation
  mismatch would invalidate the cached key. Registration alone would not make a
  value decodable in a new chunk; its definition still has to be emitted there.
- Existing-value hits should record keys without copying the value again. A
  string lookup may still hash and compare bytes; registered handles avoid that
  lookup. Benchmark the two paths separately, including small-key varint widths.
- Miss handling needs bounded probe, storage and synchronization budgets. A
  producer must not spin indefinitely on another thread's pending insertion.
  Duplicate immutable entries with different keys may be preferable to waiting,
  but consume capacity faster and require measurement.
- Pool definitions would be retained independently of event-buffer delivery.
  If buffer A introduces a key and buffer B references it, dropping A cannot
  discard the only definition needed by B. Reachable composite-pool dependencies
  must be retained too; publishing an outer entry alone is insufficient.
- A conservative initial reader-visibility policy would make the required pool
  definitions discoverable before publishing event bytes as committed. That
  depends on the still-open live-header protocol. It does not establish crash
  durability. Broader forward-reference support remains a separate decision.

Strings have a defined inline encoding that could serve as a bounded miss
fallback. Non-string pooled fields are bare keys; substituting inline composite
bytes would corrupt framing. For those fields, fallback requires dropping the
event or an explicitly declared alternate event representation. Null must not
silently stand in for a value that was lost.

## Rotation exposes a policy choice

A producer can pause halfway through encoding an old-generation record. The
background writer cannot safely finish or reuse that producer's active buffer.
Under a strict one-generation/one-chunk assignment, there is a conflict among
all four of these goals when producer pauses are unbounded:

1. Rotate/finalize the old chunk within a fixed time.
2. Preserve every old-generation event.
3. Never re-encode or relocate events through a different representation.
4. Keep memory and the number of open generations bounded.

The proposed normal path is cooperative handoff at event boundaries, with the
old chunk draining while producers switch to the new generation. This introduces
some overlap; its memory must count against the configured budget.

For pathological pauses, the strict assignment offers two policies:

| Policy | Consequence |
|---|---|
| Defer closing/rotating while waiting for the producer | Preserves its events but gives up a strict rotation deadline; resource exhaustion eventually needs a recording policy |
| Close admission to the old generation after a bounded grace period | Permits bounded finalization by rejecting late buffers; those events must be counted as loss |

Neither policy has been accepted. A cutoff must arbitrate ownership with concurrent
buffer publication. A successfully sealed buffer is not necessarily committed to
the output: summaries count only output-committed events. Late-loss diagnostics
may need to be emitted in a later chunk because the affected chunk is already
closed. Their exact identity/range representation remains open.

A third option relaxes the one-generation/one-chunk assignment: a late buffer
could be emitted in a separate self-contained chunk using its original anchor
and key bindings. The event bytes could remain unchanged, while metadata and the
required pool definitions would be repeated. This moves exceptional overhead to
the background writer rather than rewriting every event. It requires checking
header time-range semantics, per-producer ordering, pool dependency closure, and
retention bounds; it is not yet specified or implemented. Small chunks would then
be an exceptional recovery cost, not the normal producer-flush policy. Coalescing
compatible late buffers could reduce that cost without waiting indefinitely.

In particular, the draft defines per-writer order through chunk/append order
([RFC §4.3](tcf-rfc.md)). Signed timestamps do not authorize reordering already
accepted producer records. Emitting a late old-generation buffer after a newer
chunk needs an explicit acceptance-order contract: an unfinished record may be
accepted later if that is the chosen contract, whereas an earlier accepted
record cannot silently move behind a later record from the same ordered producer.
An old generation number or timestamp alone does not establish acceptance order.
Holding newer chunks until all older buffers arrive restores generation order,
but reintroduces the bounded-wait/resource problem. Giving a late chunk a new
writer identity would change stream identity and is not a transparent solution.

Closing admission is not memory reclamation. A paused producer may still read its
descriptor, pool entries, or buffer when it resumes. Reuse must wait until a safe
handoff, including protection against generation-identity reuse. Cap retained
generations, pool bytes, buffers and registered producer slots explicitly. If
paused producers exhaust that budget, do not create generations indefinitely:
defer further rotation where allowed or enter the defined drop policy without
blocking application threads. Neither cutoff nor a timeout alone makes reuse safe.

## Proposed starting policy for discussion

Start with cooperative rotation and a soft rotation deadline. Bound the number
of retained generations and all associated storage. If an old producer prevents
reclamation and that budget is exhausted, refuse new recording work through the
defined loss path rather than allocate another generation or wait on the producer.
Keep draining already accepted buffers whenever the sink permits it. A pool miss
that cannot use a valid bounded fallback also follows the loss path.

This is a proposal, not an accepted policy. It prioritizes bounded memory and
nonblocking producers while allowing chunk closure to be delayed. A hard deadline
would require choosing cutoff loss or fully specifying late-chunk acceptance and
ordering. The soft-deadline option needs measurements of lost work and recovery
after a paused producer resumes; it does not promise lossless recording under
unbounded pauses or a stalled sink.

## Next evidence

The first pooled generation experiment now covers fixed-thread descriptor
acquisition, producer-local pool slices, native serialization, rotation and
finite stalls/pauses. [Its results](../evidence/generation-writer-native.md)
show bounded storage and lossless ordinary paced cases, but overlapping CPU
ranges. They do not establish a substantial CPU penalty for staging. Frequent
rotation sharply increases repeated metadata/pool bytes in this workload.
Partial-buffer publication without pool resets has since been measured
([results](../evidence/generation-writer-publication.md)): an age-limited
background drain of a buffer's committed prefix bounded typical visibility near
the limit, including for idle producers, without consuming ring slots or
repeating pool definitions. Producer-side flushes did neither reliably.
Shared interning, thread churn, full live-reader consistency and wire loss/reset
semantics remain open.

Before selecting a production implementation, compare exceptional late chunks against
deadline-triggered loss and deferred closure, and settle safe generation
acquisition/reclamation. Then benchmark producer-side encoding with:

| Case | Include in measurements |
|---|---|
| Registered handle | Generation check, key cache, actual small wire reference |
| Existing-pool lookup | Hash/comparison, probe count, reference encoding |
| Miss | Bounded insertion/capture, contention, independent definition publication |
| Rotation | Cache invalidation, partial buffers, pool/metadata emission, old-generation drain |
| Paused producer / stalled sink | Late-buffer treatment, bounded retained bytes, loss and recovery |

Report producer latency together with total CPU, allocation, accepted and lost
events, retained bytes, and complete output bytes. Establish equal offered load
and lossless comparable runs before interpreting CPU ratios. Separate measurements
of normal hits from misses and handoff tails. The existing experiment remains a
capture/encoding baseline, not evidence that the proposal has these properties.
