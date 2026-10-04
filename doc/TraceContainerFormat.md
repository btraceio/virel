# VIREL — Versioned In-band Runtime Event Ledger
### Specification draft v0.1 · native major 0 · majors 1/2 legacy annex (frozen) · 2026-10-03

> **Status:** DRAFT. VIREL is the selected protocol name. Native chunks use
> magic `VIRL` and wire type namespaces `virel.*`, `virel2.*`, and
> `virel3.*`. `FLR\0` remains exclusively JFR.
> Derived from the jfr-spec investigation graph (`.investigations/jfr-spec/`,
> 28 nodes); every normative claim about JFR v1/v2 carries a node id with
> file:line evidence. Numeric assignments marked PROPOSED were invented by
> this draft and are now RATIFIED by proceed (2026-10-03); all other content
> is decided. Corrections from the validator run are folded in as of
> 2026-10-03 (see ev-validator-jfr-annex).

---

## 1. Design goals

1. **Superset**: one format represents JFR (`.jfr`), Go execution trace v2,
   and Perfetto (TracePacket) as **sub-configurations** — plus OTLP
   Profiles/pprof (lossless per OTel's own mapping) [find-superset-goal,
   find-coverage-matrix, find-otlp-profile-matrix].
2. **One reader**: JFR-annex classes parse with the same implementation as
   native major-0 classes; "can read new ⇒ can read JFR" is executable via
   validators [find-subset-direction].
3. **Migration-first**: extreme ease of conversion from all popular trace
   formats — expressible, streaming single-pass bounded-memory, no
   infrastructure prerequisites (no protoc, no registry, no codec deps),
   bidirectional bootstrap emitters (CTEF JSON, pprof) [find-migration-
   strategy].
4. **Low overhead writes**: same cost model as JFR/Go batching — in-band
   varint sizes, padded back-patches, chunk-local interning [ev-content-
   encodings, ev-prior-art-fine-tracers].
5. **Convergent mechanics, not novelty**: chunked self-contained partitions,
   per-partition interning, back-patched batch lengths, in-band sync/loss
   markers = the baseline JFR, Go tracev2, and Perfetto share [ev-prior-art-
   fine-tracers].

Non-goals for v1.0: chunk-body compression, multi-clock domains, machine
federation, per-writer sub-chunk regions (all have declared extension
anchors, §8).

---

## 2. Conventions

- Words are bit-strings; **big-endian** byte order for all fixed-width and
  floating-point words in every major (JFR v2 is BE; Go/Perfetto variance is
  a converter concern, not a format one) [ev-chunk-header].
- `varint` = LEB128: groups of 7 low bits, little-endian order, high bit =
  continuation; **at most 9 bytes** (9th byte carries raw 8 bits). Identical
  family to protobuf varints and Go trace varints [ev-content-encodings,
  ev-prior-art-fine-tracers].
- `padded varint` = varint written at fixed width with zero-bit padding;
  readers MUST accept any legal width; used where a field's length must be
  back-patched after it is known (chunk size, event size, batch length).
  Proven in all three reference formats [ev-chunk-header, ev-prior-art].
- Integers are unsigned unless a declared type says signed (two's-
  complement, sign in the varint's raw bits).
- Pool reference = `u32` key into the type's constant pool; **0 = null**.
- Floating point = IEEE 754 binary32/binary64, BE.
- The terms *container* (a stream: file or datagram sequence),
  *chunk* (self-contained partition), *writer* (producer identity owning a
  chunk) are normative.

---

## 3. Container and chunk anatomy

A container is the concatenation of chunks. A chunk begins with its header
and spans exactly `size` bytes (header + content + pools). A reader MAY
attach to any chunk boundary without reading previous chunks, but MUST
resolve declared types and pools within the chunk's own scope.

Chunk classes are identified by their magic plus @66 signature bit (§3.2,
find-version-resilience): native = `VIRL` plus signature set (0 = draft,
1 = stable); JFR legacy annex = `FLR\0` plus majors 1/2 and signature clear
(§3.1); EVERYTHING ELSE = foreign chunk (§3.5) — readers bridge, never
interpret. `FLR\0` remains exclusively a JFR magic.

### 3.1 Legacy annex header — majors 1.x/2.x (BYTE-FROZEN, 68 bytes)

All big-endian [ev-chunk-header, ev-framing-constants]:

| off | len | field | notes |
|----:|----:|---|---|
| 0 | 4 | magic | `'F','L','R','\0'` |
| 4 | 2 | version major | writer emits 2; readers accept 1 and 2 |
| 6 | 2 | version minor | |
| 8 | 8 | chunk size | initially HEADER_SIZE (68); back-patched at finalize |
| 16 | 8 | last checkpoint offset | posthoc-updated |
| 24 | 8 | metadata position | posthoc-updated |
| 32 | 8 | chunk start, UNIX nanos | written at open; stable |
| 40 | 8 | chunk duration nanos | posthoc-updated |
| 48 | 8 | chunk start ticks | stable; conversion anchor |
| 56 | 8 | ticks per second | stable |
| 64 | 1 | file state | 0 = COMPLETE; 0xff = UPDATING (seqlock, §3.3) |
| 65 | 1 | pad | |
| 66 | 2 | flags (BE u2) | low byte @67: bit 0 = CompressedIntegers (varint regime) — writers of this codebase hardcode true; bit 1 = FINAL-CHUNK. High byte @66: **unused in v2** |

Posthoc-updated fields (size, checkpoint, metadata, duration, state, flags)
are reliable only under the §3.3 protocol; magic/version/start-nanos/
start-ticks/frequency are stable.

### 3.2 Native header — majors 0 (draft) and 1 (stable) — 76 bytes

**Version-number policy (normative)** [find-version-resilience]: native
chunks use the distinct `VIRL` magic; `FLR\0` remains exclusively JFR's.
The signature bit is a required class guard inside the shared header layout:

- `@66 bit 7 (0x80) = VIREL-NATIVE signature` — SET in every `VIRL` native
  chunk, ALWAYS clear in JFR-legacy chunks (v2 provably writes @66 = 0 and
  its readers never read byte 66 [ev-chunk-header]).
- `@66 bits 0..6` = metadata-default-encoding id (0..127).
- **Native majors**: 0 = this draft; **1 = first stable native** once the
  spec is ratified (graduation — majors do NOT stay at 0 forever); any
  later native major = an advertised spec event; readers of today skip it
  under §3.5 (foreign) until updated. Evolution within a native major is
  additive minors.
- **Annex majors 1/2** remain byte-frozen for legacy chunks (signature
  clear). `FLR\0` versus `VIRL` makes the JFR/native boundary explicit;
  the signature bit protects against malformed or mislabelled headers.
- **A future OpenJDK JFR major 3/4/…** cannot collide: unknown
  (major, minor) = foreign chunk (§3.5), bridged, never interpreted;
  real JFR-3 annex support later = pure additive spec revision.
- JFR readers fail fast on any native chunk (unknown native profile or an
  unexpected header extent) — migration-safe in both directions by design.

Native chunks use `VIRL` at offsets 0..3; offsets 4..67 retain the Annex
§3.1 layout and positions unchanged; the size is major-scoped
[ev-chunk-header]:

| off | len | field | notes |
|----:|----:|---|---|
| 0 | 4 | magic | `'V','I','R','L'` (native only; `FLR\0` is JFR-only) |
| 4..65 | | as annex | |
| 64 | 1 | file state | 0 = COMPLETE; 0xff = UPDATING |
| 65 | 1 | pad | |
| 66 | 2 | flags (BE u2) | low byte @67: bit 1 = FINAL-CHUNK (bit 0 reserved; the varint regime is MANDATORY in major 0) | high byte @66 (**NEW**): metadata-default-encoding id, §5.2 — advisory; envelope tag is authoritative |
| 68 | 8 | **writer id (NEW)** | `u64` BE, random per writer (Perfetto-TrackDescriptor-uuid style); discoverable at chunk attach; required by the one-writer-per-chunk rule [find-superset-goal] |

**Ratified**: writer-id = 64-bit random (zero = null/writer-unspecified,
e.g. single-writer converters that do not wish to mint ids).

### 3.3 Live-write protocol (seqlock)

The state byte at 64 transitions 0xff → 0x00 at finalize. A live reader
SHALL read state, then the posthoc fields, then state again; accept only on
equal reads. This reproduces the JDK protocol (ChunkHeader.java: FILE_
STATE_POSITION=64, UPDATING_CHUNK_HEADER=255) and gives single-chunk
streaming [ev-live-header-protocol]. The FINAL-CHUNK flag bit is the only
crash-vs-clean marker; normative truncation recovery (§3.4) covers the rest.

### 3.4 Truncation and recovery

A reader SHALL on structural failure seek forward for the next recognized
magic (`FLR\0` or `VIRL`), then re-attach at that chunk. The chunk whose header declared
size is absent (final chunk never finalized) ends at container end; its
content up to the last parseable event SHALL be consumed; pool/state
incompleteness follows §5's snapshot rules within the truncated range.
Sync points (§4.4) delimit the durability horizon a consumer may rely on
when the tail is torn.

### 3.5 Foreign chunks — resilience to JFR evolution (normative)

A chunk whose (major, minor) the reader does not implement (e.g. a future
OpenJDK JFR major 3, or an annex minor newer than the reader knows) is
FOREIGN:

1. The reader MUST NOT interpret, guess at, or parse it by v1/v2 (or any
   other) rules.
2. The reader bridges over it: locate the next chunk boundary by scanning
   for `FLR\0` or `VIRL` from the current position (§3.4 recovery semantics).
   A candidate is accepted only if its magic and signature-state identify an
   implemented class: `VIRL` + signature SET + implemented native major, or
   `FLR\0` + signature CLEAR + implemented annex (1/2) — otherwise keep
   scanning. Either 4-byte magic can occur inside foreign payloads; the
   signature bit is a header-octet guard read from the candidate header's
   flags.
3. The bridge is transparent to consumers (the chunk stream stays
   traversable); diagnostic layers SHOULD report foreign spans rather than
   silently dropping them.

This gives STRUCTURAL resilience to any JFR evolution — a future JFR major
3/4/10, or an unknown annex minor — and equally to future VIREL majors:
readers skip what they do not know and never clash with a version space
they cannot coordinate with. Supporting a future JFR annex (e.g. real JFR
major 3) is later a pure spec revision, additive in both directions.

---

## 4. Content layer

Identical wire grammar for annex and native major 0, except the tick-word
semantics (§4.2). Grammar [ev-content-encodings, ev-framing-constants]:

```
event      = size:pvarint        // includes its own varint width
             typeId:varint       // §5.3
             time words          // §4.2
             fields              // per declared type
```

### 4.1 Field encodings

- Primitive encodings (annex-verified, [ev-validator-jfr-annex;
  ParserFactory.java:186-276, RecordingInput.java]): `int`/`long`/`short`/
  `char` = varint (cast to declared width; readChar = varint code unit);
  `boolean`/`byte` = 1 raw byte; `float`/`double` = BE IEEE.
  No per-field codec negotiation exists — one varint regime for all
  16-bit+ integers.
- **Varint128 shape (NORMATIVE, verified by gates 3/4)**: little-endian
  7-bit groups, at most **9 bytes**. When a value needs more than 56
  bits, ALL 8 group bytes carry the continuation bit and the 9th byte is
  RAW 8 bits (RecordingInput.readLong semantics). A plain 10-byte LEB
  (e.g. for the 64-bit sentinel 2**64−1, Go's NoThread) is a VIOLATION:
  readers stop at the terminator bit and the residual byte drifts every
  later field [ev-go-gate-3]. Writers of 64-bit values MUST use the
  9-byte form.
- Strings — 1-byte kind tag then payload; kinds:
  | tag | kind | payload |
  |----:|---|---|
  | 0 | NULL | — |
  | 1 | EMPTY | — |
  | 2 | POOL-REF | u32 pool key (0 = null) |
  | 3 | UTF-8 | varint length + bytes |
  | 4 | char array | varint COUNT + count VARINT code units (readChar) |
  | 5 | LATIN-1 | varint length + bytes |
  | **6** | **BYTES (NEW, major 0)** | padded varint length + raw bytes |
- Composites: declared field list; arrays: varint count + elements.
- Poolability is a **schema attribute** (`constantPool` / `isConstantPool`)
  on the declared type; pools live in
  checkpoint events (§4.4). Chunk-valid per §5.4.
- **Pooled-ref encoding split (NORMATIVE, proven by gates 3/4)**:
  - a pooled field whose declared type is a STRING kind = **tag-encoded**
    exactly as above (tag 2 = pool-ref; inline UTF-8 = tag 3 allowed);
  - a pooled field whose declared type is NON-string (numeric primitive
    or declared composite, e.g. a typed pool entry ref) = **bare varint
    key with NO tag byte**; key 0 = null. A tag byte before a non-string
    pool key is a 1-byte-per-occurrence drift that corrupts all later
    reads (position-sequential pool entries) [ev-pprof-gate-4,
    ev-go-gate-3].
- **BYTES kind** (major 0 only): first-class binary field; carries
  attachment fragments (§6.4) and raw records. Skipping = consume length.

### 4.2 Time words

- Event words: `[startTicks:varint][durationTicks:varint if declared]`.
- **Annex (v2) semantics — BYTE-FROZEN**: ticks are ABSOLUTE on the writing
  process's elapsed-counter axis (JVM: `os::elapsed_counter()`/rdtsc,
  `FastUnorderedElapsedCounterSource` — deliberately NON-monotonic,
  "can jump backwards"). Conversion: `nanos = startNanos + (ticks −
  startTicks)/frequency` [ev-framing-constants, TimeConverter.java:36-40].
- **Major 0 (native) semantics**: ticks are CHUNK-RELATIVE (0 = chunk startTicks
  anchor); writers of a chunk MUST be non-decreasing within the chunk.
  Rationale: cheap varint deltas, writer-monotonicity restored, and Go/
  Perfetto converters avoid epoch rewrites [find-exec-trace-vocabulary].
- Durationless events omit the duration word (schema-driven).
- Nanosecond axis: chunk startNanos (UNIX epoch ns) + frequency is the only
  wall anchor in v1.0. Multi-clock = §8 extension.

### 4.3 Ordering, timebase-neutral records

In major 0, event order per writer = chunk order = append order; cross-
writer order is NOT chronological by default (matches Go's trace, JFR's
FastUnordered ticks). Timebase-neutral records (weights independent of any
clock — perf-style stack samples, pprof samples) are ordinary events whose
schema says so; their time words may be filler (e.g. uniform within the
chunk window) when the source had none — lossless (no information was
present) [find-otlp-profile-mapping].

### 4.4 State snapshots, pools, sync points

Bootstrap type ids — **fixed in ALL majors** [ev-framing-constants]:
- `0` = metadata event (§5.2) — must exist before any metadata is resolvable
- `1` = state-snapshot event — pools and writer state.  In the byte-frozen
  JFR annex this record is named a *checkpoint*; VIREL deliberately uses the
  more accurate native term *state snapshot*.

The annex checkpoint grammar is byte-frozen ([ev-validator-jfr-annex]):
`[size][1][timestamp][duration][delta][flush:u1][poolCount][entries...]`
where an entry = `[typeId][count][{key:varint, value: FULL payload} x count]`.
Native VIREL calls the record a **state snapshot**. Its existing
`flush:u1` byte is a native-only flag byte: bit 0 is `FLUSH`; bit 7 is
`TYPE_SUMMARY_PRESENT`; all other bits are zero. When the latter is set, the
record inserts a leading, cumulative event summary before the pool entries:

```
state-snapshot = [size] [1] [timestamp] [duration] [delta] [snapshotFlags:u1]
                 [summaryVersion] [summaryTypeCount]
                 [{typeId} {emittedCount} x summaryTypeCount]
                 [poolCount] [entries...]
```

`summaryVersion = 1` in this draft. The entries are
strictly ascending by `typeId`; each count is the exact cumulative number of
non-bootstrap records of that type preceding this snapshot in this chunk.
Type ids 0 (metadata) and 1 (the state snapshot itself) are excluded.  A
snapshot in a COMPLETE chunk immediately before `END_OF_PARTITION` is thus a
definitive type-presence/count index for the chunk; a live snapshot covers
only the prefix before its own offset.  A reader maps a symbolic type name to
an id using metadata, then binary-searches this small table without parsing
event bodies or pools.

The header's offset-16 field retains its JFR name **last checkpoint offset**
in annex chunks and means **last state snapshot offset** in native chunks.
It points at the record above; `delta` walks state snapshots backward.
Already-parsed pool entries carry full payload (skip = consume-and-discard;
dedup is object-level, not wire-level) [ev-parser-dynamics].

**Per-pool state snapshots (native draft, ratified find-pool-skip (b)+(c))**:
a chunk MAY carry any number of state snapshots; a state snapshot MAY
carry ANY subset of the chunk's pools.
- **Streaming shape**: one state-snapshot event PER POOL — each pool is then
  an independently size-prefixed unit, skippable by size WITHOUT
  parsing any entry bytes, exactly like an event (query readers bound
  parse cost to the pools they use; spiked on real pprof fixtures:
  +1.4..3.7% state-snapshot bytes, 27-29% less parsed for a strings-only
  query [find-pool-skip]). Writers of small pools MAY co-locate.
- **Reader contract**: pool materialization is LAZY by type id; entries
  always carry FULL payload, so skipping costs no later re-parse;
  pools resolve by type id from any state snapshot in the chunk, walking
  delta chains backward. Annex-compatible by construction: JFR v2
  already writes multiple delta-chained checkpoint events per chunk
  (in-band header checkpoints) [ev-header-checkpoints].
- Per-pool state snapshots double as sync points: partial-write recovery
  granularity improves (a mid-pool flush crash loses only up to the
  last pool event).

**Sync points** = registered declared event kinds (module m0) usable for
mid-chunk durability horizons and recovery. Registered kinds — ratified
numerics (all under m0; ids are the declared types' local ids, not magic):

| kind | payload | role |
|---|---|---|
| STATE_SNAPSHOT | pools/state + cumulative type summary (typeId 1; JFR alias `CHECKPOINT`) | pool validity, state resync, and cheap type presence/count |
| CLOCK | (clock-id, value, frequency) tuples | multi-clock extension table |
| LOSS | writer id, lost range | ring-buffer/flight-recorder loss (Perfetto previous_packet_dropped analog) |
| CLEAR | writer id | incremental-state cleared (Perfetto SEQ_INCREMENTAL_STATE_CLEARED analog) |
| END_OF_PARTITION | — zero-payload event (declared fieldless): Go EndOfGeneration / chunk finalize marker; verified = gates 3/5 |

Cadence: writers SHOULD write a state snapshot within a durability window (e.g. ≤ 1 s
of event time or ≤ a few MB); normative readers rely on sync points only up
to the last successfully parsed one [ev-header-checkpoints,
ev-parser-unknown-handling].

---

## 5. Metadata and type system

### 5.1 Model

- Declared types live **in-stream** (self-describing; no out-of-band schema,
  no registry dependency — the migration property #3 [find-migration-
  strategy]).
- **Type-id bindings are container-global and ADD-ONLY**: a binding made in
  any chunk holds for the whole container; bindings are never re-bound;
  redefinition = stream violation [find-type-id-model].
- **Metadata and state snapshots are separate with a one-way dependency.**
  Metadata declares `typeId` and field layouts and needs no pool state. A
  state snapshot materializes pool entries for those declared types, so a
  reader resolves metadata before decoding it. They MAY be emitted on
  different schedules; name-based summary queries read metadata only to map
  name to `typeId`, then inspect the state-snapshot summary.
- **Snapshot completeness** (normative): each chunk's metadata events carry
  the FULL declared set the writer's parser contract needs — no omissions
  (v2 semantics verified; mid-chunk checkpoints rebuild from snapshot,
  not diff) [STATE: resolved this phase, ev-parser-dynamics].
- Reader caching of metadata by (encoding, hash) = allowed optimization
  only; correctness never depends on cross-chunk carryover beyond add-only
  ids.

### 5.2 Metadata event

**Annex preamble (byte-frozen)**: `[size][typeId=0][startTime][duration]
[metadataId]` — the metadataId varint enables cross-chunk parse shortcuts
only (reader caching; identical payload = identical descriptor)
[ChunkHeader.java:224-239]. **Payload (binarized-XML v2, byte-frozen)**:
`[poolSize:varint][string x poolSize (tags 0-5)][root element]`; elements =
`[name:poolref][attrCount:{name,value}x][childCount:{element}x]`; the root
element carries children `metadata` (type elements; the XML element for a
type is literally `<class ...>`) and `region` (locale/gmtOffset/dst); a
field's referenced type id rides in the `class` attribute; event types
declare `startTime`, optionally `duration`, then `eventThread`/`stackTrace`
(constant-pool attrs) and user fields as ordinary field elements — the
event grammar needs no hidden words [ev-metadata-event,
ev-validator-jfr-annex, MetadataReader.java].

**Native (major >= 0, envelope)**: `[size][typeId=0][time words][envelope]`.

- **Envelope tag (authoritative)**: `varint encoding-id` + payload. Native
  chunks MAY omit the envelope when the header flags' high byte (@66)
  names the default encoding — then encoding = header default.
  **Permit-but-discourage**: changing encoding within one chunk
  [hyp-metadata-encoding].
- Encoding registry — ratified by proceed (2026-10-03): 0 = `xml-v2`
  (legacy, the ONLY encoding for annex chunks — binarized XML with string
  pool, gmtOffset/DST/locale, exact grammar above [ev-metadata-event]);
  1 = flat-binary; 2 = protobuf; 3 = CBOR; 4 = JSON. All express the SAME
  logical model (types, fields, attributes, units; consumers choose by
  capability).
- 5 = `avro` — ratified (2026-10-03): Avro's writer-schema-in-stream +
  resolution rules are the only mainstream native match for VIREL's add-only
  evolution [find-idl-codegen]. CORRECTED same day: the deeper ratified
  intent = VIREL is expressible IN-FULL as Avro (one complete protocol model,
  find-avro-full-model) — the metadata encoding id is the small part.

### 5.3 Type-id space — splits (ratified by proceed)

| range | use |
|---|---|
| 0 | metadata event (bootstrap) |
| 1 | checkpoint event (bootstrap) |
| [2, 2³⁰) | module-REGISTERED: ledger-assigned `(module u12) << 18 \| local u18` — 4096 modules × 262,144 types |
| [2³⁰, 2⁶²) | DYNAMIC: writer-assigned, per container, add-only |
| [2⁶², 2⁶⁴) | SYSTEM/RESERVED (format-internal, experiments) |

Annex chunks (v2): ids are as JFR v2 emits them — small first-come ids
assigned by the metadata itself; the reserved 0/1 bootstrap ids above hold
[ev-framing-constants].

### 5.4 Pools and pool kinds

- Pools = checkpoint-recorded key→value tables for pooled typeIds.
- Validity = chunk (writer-scoped). A chunk's first checkpoint (or its
  complete snapshot) bootstraps validity; writers MUST NOT assume validity
  beyond the chunk [find-type-id-model].
- Keys: u32; 0 = null (mirrors string POOL-REF and OTLP dictionary index-0
  nullability [find-otlp-profile-mapping]).
- No per-writer pool namespaces (ruled out); writer identity is the chunk
  header writer-id; partitioning happens at chunk granularity.

### 5.5 Module ledger — assignments (ratified by proceed)

| module id | name | content |
|---:|---|---|
| 0 | `framing` | sync-point kinds (§4.4), provenance + attachment-fragment types (§6.2, §6.3), header-adjacent state |
| 1 | `jfr-core` | JVM vocabulary (JDK event semantics as declared types) |
| 2 | `exec-trace` | execution-tracing vocabulary: task/executor (scheduling states, Go P/G model), slices (duration events; nesting = track+time convention), instants, counters (+ unit/temporality descriptors), STATE tracks, timebase-neutral stack samples, per-sequence numbers [find-exec-trace-vocabulary] |
| 3 | `otlp-profiles` | pprof/OTLP Profiles mapping vocabulary [find-otlp-profile-mapping] |
| 4+ | ledger | append-only additions (spec-adjacent registry file, community-tracked) |

---

## 6. Profiles and conversion

### 6.1 Profile roster

| profile | major | form of representation | conformance |
|---|---|---|---|
| `jfr-legacy` | 1.x/2.x | **byte-identical annex** | round-trip byte equality + JDK-reader parity suite |
| `standard` | **0 draft → 1 stable** (signature set) | native | writer/reader suites |
| `go-trace` | 0 | lossless representation (converter) | round-trip: Go gen → VIREL chunk → reconstruct Go trace |
| `perfetto` | 0 | lossless representation | round-trip TracePacket stream |
| `otlp-profiles` | 0 | lossless representation (subs pprof) | round-trip pprof/OTLP |

### 6.2 Conversion provenance (standardized) [find-conversion-provenance]

Declared metadata type `provenance` (module m0), emitted per chunk:

- `source_format` — STRING-first ("go-trace", "perfetto-pb", "pprof-gz",
  "linux", "etw", "ctef", "cpuprofile", …); no registry required; optional
  well-known ids MAY be added additively later.
- `source_format_version` — string.
- `converter` — identity/version string.
- `conversion_fidelity` — enum: `LOSSLESS` / `LOSSY_PARTIAL_ATTACHMENT`
  (standard lossy mode) / `LOSSY_FULL_ATTACHMENT` (discouraged; tiny
  sources) / `LOSSY_NO_ATTACHMENT`.
- `original_payload_hash` — u64 over attached bytes.

### 6.3 Attachment = only the non-recoded parts (ratified)

- Declared type `attachment-fragment` per lossy element:
  - `fragment` — BYTES field, **original encoding untouched**;
  - `source_locator` — u64 offset OR per-format coordinate string
    (documented with each Tier-B converter);
  - `reason` — enum: EXTENSION_UNMAPPED / SEMANTICS_UNMAPPED /
    TYPE_UNREPRESENTABLE.
- mapped portion + fragments = **complete original information**; a
  consumer with the source-format reader reassembles the original.
- Converter guidance (normative): fragments only; attachment budget SHOULD
  be a small fraction of stream size. FULL original attachment is allowed
  but discouraged (size-doubling ruled out).
- Blobs skip as ordinary size-prefixed events; **consumers MUST NOT
  auto-interpret attachment bytes** (normative).

### 6.4 Reference converters = validators

The round-trip suites and the shipped reference converters are the SAME
artifacts [find-converter-cost]: Go converter buffers one generation
(source format's own granularity, ~16 MiB ceiling); Perfetto converter
streams 1:1 via mid-chunk checkpoints; pprof converter is table-driven off
the OTLP mapping. Tier B (CTEF/cpuprofile, perf.data, ftrace, CTF, ETW):
shape-fold converters + CTEF emitter first (adoption wedge).

### 6.5 Fidelity classes and conformance ladder [find-migration-strategy]

Migration ease from currently-popular formats is THE product criterion;
the conformance ladder makes it testable:

- **Tier A — lossless round-trip suites (v1.0 REQUIRED for claimed
  profiles)**: JFR annex (byte-frozen; reader-implies-writer), Go
  tracev2 (per-(generation, M) chunking, exact recon: kinds/abs
  ticks/args/pools — ev-go-gate-3), Perfetto protobuf (pairs + folding
  = read-side — ev-validator-pfetto), pprof/OTLP profiles (m3; keyed
  pools = source identity — ev-pprof-gate-4).
- **Tier B — documented shape transform (vocabulary-level)**: all of
  the source's event-level semantics expressible and reconstructable
  modulo LISTED transforms; not byte identity. Verified for CTEF
  (X -> BEGIN/END pair; E names by nesting; cat/args delta) and V8
  cpuprofile (tree -> leaf-first pooled stacks; pc = 0; hitCount =
  aggregate) — ev-ctef-gate-5 — INCLUDING the reverse direction
  (VIREL -> CTEF JSON emitter = the adoption wedge: existing viewers
  consume VIREL data unchanged). perf.data/ftrace/CTF/ETW = declared
  types + BYTES fragments per §6.3.
- The reference converters ARE the conformance suites (§6.4); a profile
  claim ships its suite or it does not ship.

---

## 7. Evolution rules

- Majors [find-version-resilience]: native VIREL majors = signature-set
  chunks: **0 = draft (this spec); 1 = first stable (graduation upon
  ratification)**; later native majors = advertised spec events, additive
  preferred. JFR annex = majors 1/2, signature clear — frozen forever.
  Any unknown (major, minor) = foreign chunk (§3.5), bridged, never
  interpreted; number-space claims/reservations DO NOT EXIST (no
  coordination with OpenJDK or anyone else is assumed or needed).
- Minors (per native major): additive only.
- Type ids: add-only, never re-bound (any major).
- Module ledger: append-only; registered modules may version (additions
  only).
- Envelope encodings: registry append-only.

## 8. Declared extension directions (not in v1.0; anchors exist)

- **Multi-clock domains**: CLOCK sync-point kind carries (clock-id, value,
  frequency) tables; per-event clock = declared field (Perfetto model,
  sequence-scoped clocks = writer-scoped + CLEAR reset).
- **Chunk-body compression**: header stays clear; mid-chunk attach then
  requires streaming decompress — acceptable, opt-in flag.
- **Machine federation**: resource-level declared types + external clock
  sync packets.
- **Per-writer sub-regions**: re-open topic if converter evidence shows
  demand (currently ruled out).

## 9. Honest limits (v1.0)

- No native map/kv-associative type (composite arrays of kv pairs suffice —
  JFR/OTLP both model it this way in practice).
- One nanosecond wall axis + one tick axis per chunk (multi-clock = §8).
- Aggregated pprof shapes need a defined time-word filler (§4.3) — no loss,
  source had no timestamps.
- v2 annex event ticks are NON-monotonic by JVM design (§4.2); consumers of
  annex chunks must not assume intra-chunk order (verified TSC behavior).

## 10. References

Investigation nodes (authoritative for every verified claim):
`ev-chunk-header`, `ev-content-encodings`, `ev-metadata-event`,
`ev-header-checkpoints`, `ev-live-header-protocol`, `ev-parser-dynamics`,
`ev-parser-unknown-handling`, `ev-framing-constants`, `ev-prior-art-fine-
tracers`, `ev-external-ecosystem`, `find-subset-direction`, `find-type-id-
model`, `hyp-metadata-encoding`, `find-superset-goal`, `find-coverage-
matrix`, `find-otlp-profile-mapping`, `find-migration-strategy`, `find-
conversion-provenance`, `find-converter-cost`, `find-exec-trace-vocabulary`,
`find-format-sketch`, `find-streaming-primitives`, `find-consumer-matrix`,
`find-design-deltas`.

External primary sources: JDK sources in-repo (paths in node frontmatter);
Go runtime trace sources + design doc 60773; Perfetto trace-packet proto;
OTel profiles.proto v1development + Profiles data-format docs; async-
profiler OutputFormats/ProfilerOptions/JfrVisualization docs.

---

## 11. Machine-readable artifacts shipped with the spec [find-idl-codegen]

- `tcf-framing.ksy` — Kaitai Struct description of the chunk header/byte
  layer (SHIPPED, doc/tcf-framing.ksy; foreign-bridge algorithm stays
  normative prose §3.5 + reference reader by design).
- `m0-framing.proto` — canonical control-plane + m0 vocabulary proto
  (SHIPPED, doc/m0-framing.proto; records the Varint128 norm and the
  pooled-ref encoding split as comments).
- `tcf-schema.cddl` — CDDL contract for CBOR-encoded metadata (id 3)
  (SHIPPED, doc/tcf-schema.cddl).
- Encoding choice matrix: proto shops consume metadata as id 2; CBOR as
  id 3 (+CDDL); schema-resolution shops as id 5 (avro); XML tooling as
  id 0 (annex parity).
- The dynamic-content reader (runtime parser generation from in-stream
  metadata) remains the REQUIRED conformance baseline — machine-readable
  artifacts are conveniences, never substitutes.

*Draft ends. All numerics ratified by proceed (2026-10-03): sync-point
kind table (§4.4), encoding-registry ids (§5.2), type-id splits (§5.3),
module ledger (§5.5), writer-id=0 semantics (§3.2), VIREL-signature bit
@66 bit 7 + encoding-id cap 0..127 (§3.2), encoding id 5 = avro
(ratified 2026-10-03).*
