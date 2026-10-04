# VIREL — Versioned In-band Runtime Event Ledger

- Document: draft-virel-00
- Date: 2026-10-03
- Status: Experimental draft for review. No RFC number exists for this document.
- Native identity: magic `VIRL`; wire namespaces `virel.*`, `virel2.*`, and
  `virel3.*`; `FLR\0` remains JFR-only.

## Abstract

VIREL is a container format for event traces and profiles. One format represents JFR files, Go execution traces, and Perfetto traces as sub-configurations. OTLP Profiles and pprof form the fourth profile family. Native chunks use the distinct `VIRL` magic and carry a VIREL signature bit; byte-frozen JFR legacy chunks retain the exclusive JFR `FLR\0` magic. Readers bridge over unknown (magic, major, minor) chunks and never interpret them. Declared types and constant pools live inside the stream. The format needs no registry and no code-generation tools. Converters record provenance data and write attachment fragments for unmapped parts. This memo specifies the wire grammar, the chunk headers, the type system, and the conversion rules.

## Status of This Memo

This memo is an experimental draft for external review. Native major 0 remains
the active design version and is revised in place until ratification; protocol
design decisions do not consume new version numbers. The parent technical
draft is a normative reference (section 12). Its numeric assignments were
ratified on 2026-10-03.

## Terminology

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, MAY, and SHALL are RFC-2119 keywords. RFC 2119 defines them as normative requirement levels. This keyword use is an approved STE deviation. The definitions below satisfy both RFC-2119 use and plain reading.

- **container** — a stream of chunks; a file or a datagram sequence.
- **chunk** — a self-contained partition of a container. A chunk spans exactly its `size` bytes, header included.
- **pool** — the constant pool of one pooled type. It maps u32 keys to values.
- **state snapshot** — the bootstrap event with type id 1. It carries pool
  entries, writer state, and, when its native summary-present flag is set, a
  cumulative type summary.
  The JFR annex calls this byte-frozen record a *checkpoint*.
- **sync point** — a registered event kind. It marks a durability horizon, a loss, or a state reset.
- **envelope** — a varint tag in a metadata event. It names the payload encoding.
- **fragment** — the BYTES field of an `attachment-fragment` event. It carries original bytes, encoding untouched.
- **writer id** — the u64 producer name of one chunk. It lives in the native chunk header. Ratified: random; zero means writer-unspecified.

## 1. Design goals

1. **Superset.** One format represents JFR, Go tracev2, and Perfetto as sub-configurations. OTLP Profiles and pprof form the fourth profile, lossless per the OTel mapping.
2. **One reader.** JFR-annex classes parse with the same reader code as native major-0 classes. A reader that accepts new chunks also accepts JFR chunks. Validators verify this property.
3. **Migration-first.** Conversions from popular formats stay expressible, streaming, single-pass, and bounded in memory. No protoc, registry, or codec dependency exists. CTEF-JSON and pprof bootstrap emitters support both directions.
4. **Low overhead writes.** Writers use the JFR and Go cost model: in-band varint sizes, padded back-patches, chunk-local interning.
5. **Convergent mechanics.** VIREL keeps the mechanics that JFR, Go tracev2, and Perfetto share. Chunked partitions, per-partition interning, back-patched batch lengths, and in-band sync and loss markers.

Version 1.0 excludes chunk-body compression, multi-clock domains, machine federation, and per-writer sub-chunk regions. Section 8 holds their extension anchors.

Evidence: find-superset-goal, find-coverage-matrix, find-otlp-profile-matrix, find-subset-direction, find-migration-strategy, ev-content-encodings, ev-prior-art-fine-tracers.

## 2. Conventions

| convention | rule |
|---|---|
| words | bit-strings |
| byte order | big-endian for all fixed-width and floating-point words, in all majors; Go and Perfetto byte-order variance is a converter concern |
| `varint` | LEB128: 7 low bits per byte, little-endian group order, high bit = continuation; at most 9 bytes (the 9th byte carries raw 8 bits); same family as protobuf and Go trace varints |
| `padded varint` | varint at fixed width, zero-bit padded; readers MUST accept any legal width; used where a length needs a back-patch (chunk size, event size, batch length) |
| integers | unsigned unless a declared type says signed (two's complement, sign in the varint's raw bits) |
| pool reference | u32 key into the type's constant pool; 0 = null |
| floating point | IEEE 754 binary32/binary64, BE |
| terms | container, chunk, and writer are normative (Terminology) |

Evidence: ev-chunk-header, ev-content-encodings, ev-prior-art-fine-tracers, ev-prior-art.

## 3. Container and chunk anatomy

A container is the concatenation of its chunks. A chunk begins with its header and spans exactly its `size` bytes, pools included. A reader MAY attach at any chunk boundary without reading previous chunks. The reader MUST locate declared types and pools inside the chunk's own scope.

`VIRL` plus byte 66 bit 7 separates native chunks from JFR. Native VIREL chunks set this signature bit. JFR legacy annex chunks retain `FLR\0` and keep it clear. All other (magic, major, minor) values make a foreign chunk (section 3.5). Readers bridge such chunks and never interpret them. Evidence: find-version-resilience.

### 3.1 Legacy annex header — majors 1.x/2.x (BYTE-FROZEN, 68 bytes)

All fixed-width words here are big-endian.

| off | len | field | notes |
|----:|----:|---|---|
| 0 | 4 | magic | `VIRL` (native; `FLR\0` remains JFR-only) |
| 4 | 2 | version major | writer emits 2; readers accept 1 and 2 |
| 6 | 2 | version minor | |
| 8 | 8 | chunk size | initially HEADER_SIZE (68); back-patched at finalize |
| 16 | 8 | last checkpoint / state snapshot offset | posthoc-updated; JFR / VIREL-native meaning respectively |
| 24 | 8 | metadata position | posthoc-updated |
| 32 | 8 | chunk start, UNIX nanos | written at open; stable |
| 40 | 8 | chunk duration nanos | posthoc-updated |
| 48 | 8 | chunk start ticks | stable; conversion anchor |
| 56 | 8 | ticks per second | stable |
| 64 | 1 | file state | 0 = COMPLETE; 0xff = UPDATING (seqlock, §3.3) |
| 65 | 1 | pad | |
| 66 | 2 | flags (BE u2) | low byte @67: bit 0 = CompressedIntegers (varint regime) — writers of this codebase hardcode true; bit 1 = FINAL-CHUNK. High byte @66: **unused in v2** |

Writers update six fields posthoc: size, last checkpoint/state snapshot,
metadata, duration, state, and flags. These fields are reliable only under the section 3.3 protocol. The fields magic, version, start-nanos, start-ticks, and frequency are stable.

### 3.2 Native header — majors 0 (draft) and 1 (stable) — 76 bytes

`VIRL` is native-only; `FLR\0` remains JFR-only. The signature bit is a required class guard:

- Byte 66 bit 7 (0x80) is the VIREL-NATIVE signature. Native `VIRL` chunks set it; all JFR-legacy chunks keep it clear. Verified: JFR v2 writes byte 66 = 0. JFR v2 readers never read byte 66.
- Bits 0 to 6 of byte 66 carry the metadata-default-encoding id (0 to 127).
- Native major 0 is the active draft and evolves in place until ratification;
  this does not allocate a new major or minor. A future ratified successor is
  an advertised spec event. Readers of today skip an unsupported successor as
  foreign (section 3.5).
- Annex majors 1 and 2 stay byte-frozen (clear signature). `FLR\0` versus `VIRL` is the primary class test; the signature bit rejects malformed headers.
- A future OpenJDK JFR major 3 or later = a foreign chunk (section 3.5). Readers bridge it and never interpret it. Later real JFR-3 annex support is a pure additive spec revision.
- JFR readers fail fast on any native chunk or unexpected native header extent.
  This is migration-safe in both directions.

Offsets 0 to 67 keep the annex semantics and positions. The size is major-scoped.

| off | len | field | notes |
|----:|----:|---|---|
| 0..65 | | as annex | |
| 64 | 1 | file state | 0 = COMPLETE; 0xff = UPDATING |
| 65 | 1 | pad | |
| 66 | 2 | flags (BE u2) | low byte @67: bit 1 = FINAL-CHUNK (bit 0 reserved; the varint regime is MANDATORY in major 0) | high byte @66 (**NEW**): metadata-default-encoding id, §5.2 — advisory; envelope tag is authoritative |
| 68 | 8 | **writer id (NEW)** | `u64` BE, random per writer (Perfetto-TrackDescriptor-uuid style); discoverable at chunk attach; required by the one-writer-per-chunk rule [find-superset-goal] |

Ratified by proceed (2026-10-03): the writer id is a 64-bit random value. Zero means null (writer-unspecified). Single-writer converters MAY use zero instead of an id.

### 3.3 Live-write protocol (seqlock)

The state byte at offset 64 transitions from 0xff to 0x00 at finalize. A live reader SHALL read state, then posthoc fields, then state again, and SHALL accept only on equal reads. This reproduces the JDK protocol (ChunkHeader.java: FILE_STATE_POSITION=64, UPDATING_CHUNK_HEADER=255). The protocol supports single-chunk streaming. The FINAL-CHUNK flag bit is the only crash-vs-clean marker. Section 3.4 defines truncation recovery for the rest.

### 3.4 Truncation and recovery

A reader SHALL scan forward for the next recognized magic (`FLR\0` or `VIRL`) on structural failure and attach there. An unfinalized final chunk ends at the container end. The reader SHALL consume its content up to the last parseable event. Pool or state incompleteness follows the section 5 snapshot rules. Sync points (section 4.4) delimit the durability horizon of a torn tail.

### 3.5 Foreign chunks — resilience to JFR evolution (normative)

A chunk is FOREIGN when the reader does not support its (major, minor) pair. Examples are a future JFR major 3 and a newer annex minor.

1. The reader MUST NOT interpret, guess at, or parse the foreign chunk by v1/v2 or any other rules.
2. The reader bridges over it and scans for `FLR\0` or `VIRL` from the current position, per section 3.4. The reader accepts a candidate only when it supports that (magic, signature-state, major, minor) class. `VIRL` + signature set + a supported native major, or `FLR\0` + signature clear + a supported annex (1 or 2), is acceptable. Either 4-byte magic can occur inside foreign payloads. The signature bit is a header-octet test on the candidate header.
3. The bridge is transparent to consumers (the chunk stream stays traversable). Diagnostic layers SHOULD report foreign spans instead of dropping them silently.

This gives structural resilience to any JFR evolution and to future VIREL majors. Readers skip unknown chunks; they never clash with uncoordinated version spaces. The resilience is additive in both directions.

Evidence: find-version-resilience, ev-chunk-header, ev-framing-constants, ev-live-header-protocol, find-superset-goal.

## 4. Content layer

Annex chunks and native major-0 chunks share one wire grammar. Only the tick-word semantics differ (section 4.2).

```
event      = size:pvarint        // includes its own varint width
             typeId:varint       // §5.3
             time words          // §4.2
             fields              // per declared type
```

### 4.1 Field encodings

| primitives | encoding |
|---|---|
| `int` / `long` / `short` / `char` | varint, cast to declared width; `readChar` reads one varint code unit |
| `boolean` / `byte` | one raw byte |
| `float` / `double` | BE IEEE 754 |

Primitive encodings are annex-verified.

There is no per-field codec negotiation. One varint regime holds for all 16-bit+ integers.

A string field is a 1-byte kind tag, then the payload:

| tag | kind | payload |
|----:|---|---|
| 0 | NULL | — |
| 1 | EMPTY | — |
| 2 | POOL-REF | u32 pool key (0 = null) |
| 3 | UTF-8 | varint length + bytes |
| 4 | char array | varint COUNT + count VARINT code units (readChar) |
| 5 | LATIN-1 | varint length + bytes |
| **6** | **BYTES (NEW, major 0)** | padded varint length + raw bytes |

- Composites use a declared field list. Arrays use a varint count, then elements.
- Poolability is a schema attribute (`constantPool` / `isConstantPool`) on the declared type. Pooled fields carry u32 pool keys (sections 4.4 and 5.4).
- The BYTES kind is a first-class binary field, new in major 0. It carries attachment fragments (section 6.4) and raw records. To skip a BYTES event, the reader consumes its length.

### 4.2 Time words

- An event carries the time words `[startTicks:varint][durationTicks:varint if declared]`.
- The annex (v2) semantics are byte-frozen. Annex ticks are absolute on the elapsed-counter axis of the process that writes. On the JVM, ticks come from `os::elapsed_counter()`/rdtsc via `FastUnorderedElapsedCounterSource`. It is deliberately non-monotonic; ticks can jump backwards. The conversion formula is `nanos = startNanos + (ticks − startTicks)/frequency`.
- In major 0 (native), ticks are chunk-relative; the value 0 is the chunk `startTicks` anchor. A writer MUST keep its tick values non-decreasing within the chunk. This gives cheap varint deltas, writer monotonicity, and no epoch rewrites for Go and Perfetto converters.
- Durationless events omit the duration word. The schema drives this.
- The nanosecond axis is the chunk `startNanos` (UNIX epoch ns) plus the frequency, the only wall anchor in v1.0. Multi-clock support is a section 8 extension.

### 4.3 Ordering, timebase-neutral records

In major 0, per-writer event order equals chunk order and append order. Cross-writer order is not chronological by default (Go trace, JFR `FastUnordered`). Timebase-neutral records (perf-style stack samples, pprof samples) are ordinary events. Their weights do not depend on any clock. Their time words can be filler when the source had none, for example uniform within the chunk window. This is lossless. No information was present.

### 4.4 State snapshots, pools, sync points

Two bootstrap type ids are fixed in all majors. Type id 0 is the metadata event (section 5.2); it must exist first. Type id 1 is the state-snapshot event, with pools and state. JFR annex material calls type 1 a checkpoint.

Checkpoint grammar (annex-verified):

```
annex-checkpoint = [size] [1] [timestamp] [duration] [delta] [flush:u1]
                   [poolCount] [entries...]
native-state-snapshot = [size] [1] [timestamp] [duration] [delta] [snapshotFlags:u1]
                        [summaryVersion=1] [summaryTypeCount]
                        [{typeId} {emittedCount} x summaryTypeCount]
                        [poolCount] [entries...]
entry       = [typeId] [count]
              [{key:varint, value: FULL payload} x count]
```

The native `snapshotFlags` byte uses bit 0 for `FLUSH` and bit 7 for
`TYPE_SUMMARY_PRESENT` (all other bits are zero); without the latter, the
pool count immediately follows as in the annex shape. Its type-summary table is sorted by type id and
contains exact cumulative counts for all non-bootstrap records preceding the
snapshot. It excludes type ids 0 and 1. The header points at the last
checkpoint (annex) or state snapshot (native); delta walks go backward.
Already-parsed entries carry full payload too. Skip = consume-and-discard;
dedup is object-level, not wire-level.

Sync points are registered event kinds (module m0) for mid-chunk durability horizons and recovery. The numerics are ratified (proceed, 2026-10-03). The ids are the declared types' local ids, not magic numbers.

| kind | payload | role |
|---|---|---|
| STATE_SNAPSHOT | pools/state + cumulative type summary (typeId 1; JFR alias `CHECKPOINT`) | pool validity, state resync, and cheap type presence/count |
| CLOCK | (clock-id, value, frequency) tuples | multi-clock extension table |
| LOSS | writer id, lost range | ring-buffer/flight-recorder loss (Perfetto previous_packet_dropped analog) |
| CLEAR | writer id | incremental-state cleared (Perfetto SEQ_INCREMENTAL_STATE_CLEARED analog) |
| END_OF_PARTITION | — | Go EndOfGeneration / chunk finalize marker |

Writers SHOULD write a state snapshot inside a durability window. One example is one second of event time or a few MB. A final state snapshot immediately before `END_OF_PARTITION` makes presence/count definitive for a COMPLETE chunk. A live snapshot is authoritative only through its own offset. Normative readers use sync points only up to the last successfully parsed one.

Evidence: ev-content-encodings, ev-framing-constants, ev-validator-jfr-annex, ParserFactory.java:186-276, RecordingInput.java, TimeConverter.java:36-40, find-exec-trace-vocabulary, find-otlp-profile-mapping, ev-parser-dynamics, ev-header-checkpoints, ev-parser-unknown-handling.

## 5. Metadata and type system

### 5.1 Model

- Declared types live in-stream. The format is self-describing, with no out-of-band schema or registry. This is migration property 3 (section 1).
- Type-id bindings are container-global and add-only; a binding made in any chunk holds container-wide. Readers never re-bind them. A redefinition is a stream violation.
- Metadata and state snapshots are separate with a one-way dependency:
  metadata declares type ids and layouts without pool state; a state snapshot
  materializes pool values for those types. They may be emitted on different
  schedules. A name query reads metadata only to resolve the name to a type
  id, then uses the state-snapshot summary.
- Snapshot completeness is normative; omissions are forbidden. Each chunk's metadata carries the full declared set that the writer's parser contract needs. Verified v2 behavior: mid-chunk checkpoints rebuild from snapshots, not diffs.
- A reader MAY cache metadata by (encoding, hash) as an optimization only. Correctness never depends on cross-chunk carryover beyond the add-only ids.

### 5.2 Metadata event

```
preamble    = [size] [typeId=0] [startTime] [duration] [metadataId]
native      = [size] [typeId=0] [time words] [envelope]   // native major >= 0
payload     = [poolSize:varint] [string x poolSize (tags 0-5)] [root element]
element     = [name:poolref] [attrCount:{name,value} x] [childCount:{element} x]
```

- The annex preamble is byte-frozen. The metadataId varint supports cross-chunk parse shortcuts only; identical payload means identical descriptor.
- The annex payload uses binarized XML v2, byte-frozen. The root element carries the children `metadata` and `region`. The XML element for a type is literally `<class ...>`. The `region` child carries locale, gmtOffset, and dst. A field's referenced type id rides in the `class` attribute. Event types declare `startTime`, optionally `duration`, then `eventThread`/`stackTrace` (constant-pool attributes). User fields are ordinary field elements; the event grammar needs no hidden words.
- The envelope tag (a varint encoding-id, then the payload) is authoritative. Native chunks MAY omit it; the encoding is then the flag high byte (@66) default. A writer MAY change the encoding within one chunk (permitted but discouraged).
- The encoding registry was ratified by proceed (2026-10-03). Encoding id 0 is `xml-v2`, the only encoding for annex chunks. Encoding ids 1 to 4 are flat-binary, protobuf, CBOR, and JSON. All encodings express the same logical model: types, fields, attributes, units. Consumers choose an encoding by capability.

### 5.3 Type-id space — splits (ratified by proceed)

| range | use |
|---|---|
| 0 | metadata event (bootstrap) |
| 1 | checkpoint event (bootstrap) |
| [2, 2³⁰) | module-REGISTERED: ledger-assigned `(module u12) << 18 \| local u18` — 4096 modules × 262,144 types |
| [2³⁰, 2⁶²) | DYNAMIC: writer-assigned, per container, add-only |
| [2⁶², 2⁶⁴) | SYSTEM/RESERVED (format-internal, experiments) |

Annex chunks use ids as JFR v2 emits them (small first-come ids). The reserved bootstrap ids 0 and 1 hold.

### 5.4 Pools and pool kinds

- Pools are key-to-value tables for pooled typeIds, recorded in checkpoints.
- Pool validity is chunk-scoped (writer-scoped); a chunk's first checkpoint or complete snapshot bootstraps it. Writers MUST NOT assume validity beyond one chunk.
- Pool keys are u32; key 0 means null (string POOL-REF, OTLP dictionary index-0 nullability).
- There are no per-writer pool namespaces; writer identity is the chunk header writer-id. Pool partitioning happens at chunk granularity.

### 5.5 Module ledger — assignments (ratified by proceed)

| module id | name | content |
|---:|---|---|
| 0 | `framing` | sync-point kinds (§4.4), provenance + attachment-fragment types (§6.2, §6.3), header-adjacent state |
| 1 | `jfr-core` | JVM vocabulary (JDK event semantics as declared types) |
| 2 | `exec-trace` | execution-tracing vocabulary: task/executor (scheduling states, Go P/G model), slices (duration events; nesting = track+time convention), instants, counters (+ unit/temporality descriptors), STATE tracks, timebase-neutral stack samples, per-sequence numbers [find-exec-trace-vocabulary] |
| 3 | `otlp-profiles` | pprof/OTLP Profiles mapping vocabulary [find-otlp-profile-mapping] |
| 4+ | ledger | append-only additions (spec-adjacent registry file, community-tracked) |

Evidence: find-migration-strategy, find-type-id-model, ev-parser-dynamics, ChunkHeader.java:224-239, ev-metadata-event, ev-validator-jfr-annex, MetadataReader.java, hyp-metadata-encoding, ev-framing-constants, find-otlp-profile-mapping.

## 6. Profiles and conversion

### 6.1 Profile roster

The table defines the five profiles and their conformance suites.

| profile | major | form of representation | conformance |
|---|---|---|---|
| `jfr-legacy` | 1.x/2.x | **byte-identical annex** | round-trip byte equality + JDK-reader parity suite |
| `standard` | **0 draft → 1 stable** (signature set) | native | writer/reader suites |
| `go-trace` | 0 | lossless representation (converter) | round-trip: Go gen → VIREL chunk → reconstruct Go trace |
| `perfetto` | 0 | lossless representation | round-trip TracePacket stream |
| `otlp-profiles` | 0 | lossless representation (subs pprof) | round-trip pprof/OTLP |

### 6.2 Conversion provenance (standardized)

A writer emits the declared metadata type `provenance` (module m0) once per chunk.

| field | content |
|---|---|
| `source_format` | string-first; for example `go-trace`, `perfetto-pb`, or `cpuprofile`; no registry required; optional well-known ids MAY come later, additively |
| `source_format_version` | string |
| `converter` | identity/version string |
| `conversion_fidelity` | enum: `LOSSLESS` / `LOSSY_PARTIAL_ATTACHMENT` (standard lossy mode) / `LOSSY_FULL_ATTACHMENT` (discouraged; tiny sources) / `LOSSY_NO_ATTACHMENT` |
| `original_payload_hash` | u64 over attached bytes |

### 6.3 Attachment = only the non-recoded parts (ratified)

A converter writes one `attachment-fragment` per lossy element, with these fields:

- `fragment` — a BYTES field with the original encoding untouched.
- `source_locator` — a u64 offset or a per-format coordinate string. Each Tier-B converter documents its coordinate form.
- `reason` — an enum: EXTENSION_UNMAPPED, SEMANTICS_UNMAPPED, or TYPE_UNREPRESENTABLE.

The mapped portion plus the fragments contain the complete original information (reassembly with the source-format reader). Converter guidance (normative): converters write fragments only. The attachment budget SHOULD stay at a small fraction of the stream size. A full original attachment is allowed but discouraged (size-doubling ruled out). Blobs skip as ordinary size-prefixed events. Consumers MUST NOT auto-interpret attachment bytes.

### 6.4 Reference converters = validators

The round-trip suites and the shipped reference converters are the same artifacts. The Go converter buffers one generation, with a ~16 MiB ceiling. The Perfetto converter streams 1:1 with mid-chunk checkpoints. The pprof converter is table-driven off the OTLP mapping. Tier B (CTEF/cpuprofile, perf.data, ftrace, CTF, ETW) gets shape-fold converters and a CTEF emitter first.

Evidence: find-conversion-provenance, find-converter-cost.

## 7. Evolution rules

| aspect | rule |
|---|---|
| native majors | 0 = draft; 1 = first stable, on ratification; later majors = advertised spec events, additive preferred |
| JFR annex | majors 1 and 2, clear signature; frozen forever |
| foreign | any unknown (major, minor) = foreign chunk (section 3.5); number-space claims and reservations do not exist; no coordination is assumed or needed |
| minors | additive only, within one native major |
| type ids | add-only, never re-bound, in any major |
| module ledger | append-only; registered module versions add only |
| envelope registry | append-only |

Evidence: find-version-resilience.

## 8. Declared extension directions (not in v1.0; anchors exist)

| direction | anchor |
|---|---|
| multi-clock domains | CLOCK sync-point kind carries (clock-id, value, frequency) tables; per-event clock = declared field (Perfetto model, sequence-scoped clocks = writer-scoped + CLEAR reset) |
| chunk-body compression | header stays clear; mid-chunk attach then requires streaming decompress (acceptable, opt-in flag) |
| machine federation | resource-level declared types + external clock sync packets |
| per-writer sub-regions | re-open if converter evidence shows demand (currently ruled out) |

## 9. Honest limits (v1.0)

| limit | note |
|---|---|
| no native map/kv-associative type | composite arrays of kv pairs suffice; JFR and OTLP model it this way in practice |
| axes | one nanosecond wall axis and one tick axis per chunk (multi-clock = section 8) |
| aggregated pprof shapes | need a defined time-word filler (section 4.3); no loss, the source had no timestamps |
| v2 annex event ticks | non-monotonic by JVM design (section 4.2); consumers MUST NOT assume intra-chunk order (verified TSC behavior) |

## 10. Security Considerations

1. **Foreign chunks.** A reader MUST bridge over foreign chunks and MUST NOT interpret their content. The reader tests magic and signature on the candidate header, because `FLR\0` and `VIRL` can occur inside foreign payloads.
2. **Attachment bytes.** Consumers MUST NOT auto-interpret attachment bytes. Fragments keep original bytes in their original encoding.
3. **Truncation.** Recovery has defined bounds. The reader scans forward to the next chunk header and consumes up to the last parseable event. Its durability horizon ends at the last parsed sync point.
4. **Compression independence.** Version 1.0 defines no chunk-body compression (section 8 anchors it as opt-in); readers attach at any chunk without a decompressor.

## 11. IANA Considerations

This draft makes no request to IANA. The module ledger and the encoding registry are append-only files that ship with this spec. The community tracks the module ledger (section 5.5).

## 12. References

### 12.1 Normative

- VIREL technical draft: `TraceContainerFormat.md` (investigation `jfr-spec`), version 0.1, 2026-10-04.

### 12.2 Informative

- JDK sources in this repository; paths are recorded in the investigation node frontmatter.
- Go runtime trace sources; Go design document 60773.
- Perfetto trace-packet proto.
- OpenTelemetry `profiles.proto` v1development; OpenTelemetry Profiles data-format documents.
- async-profiler documents: OutputFormats, ProfilerOptions, JfrVisualization.

## Appendix A. Relation to existing formats

**JFR.** VIREL annex majors 1 and 2 are byte-identical JFR. An annex chunk is a JFR v1/v2 chunk, byte for byte. The section 3.1 header stays byte-frozen. The `jfr-legacy` profile requires round-trip byte equality and a JDK-reader parity suite. JFR readers fail fast on native chunks; VIREL readers bridge over future JFR majors (sections 3.2, 3.5).

**Go tracev2 and Perfetto.** The `go-trace` and `perfetto` profiles are lossless representations (converter in, validator back). The Go round trip restores a Go trace from one chunk. The Perfetto round trip restores the TracePacket stream.

**OTLP Profiles and pprof.** OTLP Profiles and pprof form the fourth profile, `otlp-profiles` (subsumes pprof). VIREL represents them without loss. pprof is a third independent convergence on pooled samples. The BYTES field kind is an additive result of the mapping. Module m3 carries the vocabulary.

**Conversion provenance and fragments.** A converter records its provenance in a declared metadata type (section 6.2). The record carries the source format and the source version. It carries the converter identity, the conversion fidelity, and the original payload hash. Attachment means only the non-recoded parts (section 6.3). Each unmapped element becomes an `attachment-fragment` event; mapped portion plus fragments contain the complete original information. Consumers MUST NOT auto-interpret attachment bytes.
