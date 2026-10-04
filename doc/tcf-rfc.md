# VIREL — Versioned In-band Runtime Event Ledger

- Document: draft-virel-00
- Date: 2026-10-04
- Status: Experimental draft for review. No RFC number exists for this document.
- Container signature: `\x89VRL\r\n\x1a\n`; native chunk magic `VIRL`; wire namespaces `virel.*`, `virel2.*`, and
  `virel3.*`; `FLR\0` remains JFR-only.

## Abstract

VIREL is a container format for event-oriented traces: timestamped event streams such as JFR, Go execution traces, Perfetto, trace-event JSON, and OTLP spans and logs. Every VIREL file is a format-agnostic container of framed streams. Native VIREL, JFR, Go execution traces, Perfetto traces, and other formats are payload formats on equal footing, and each stream is extractable byte-exactly. Readers MUST decode native VIREL; other formats decode through optional plugins. OTLP Profiles and pprof form the fourth profile family. Native chunks use the distinct `VIRL` magic and carry a VIREL signature bit; byte-frozen JFR legacy chunks retain the exclusive JFR `FLR\0` magic. Readers bridge over unknown (magic, major, minor) chunks and never interpret them. Declared types and constant pools live inside the stream. The format needs no registry and no code-generation tools. Converters record provenance data and write attachment fragments for unmapped parts. This memo specifies the wire grammar, the chunk headers, the type system, and the conversion rules.

## Status of This Memo

Synced with the parent draft on 2026-10-04, including the format-agnostic
container (section 3).

This memo is an experimental draft for external review. Native major 0 remains
the active design version and is revised in place until ratification; protocol
design decisions do not consume new version numbers. The parent technical
draft is a normative reference (section 12). Its numeric assignments were
ratified on 2026-10-03.

## Terminology

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, MAY, and SHALL are RFC-2119 keywords. RFC 2119 defines them as normative requirement levels. This keyword use is an approved STE deviation. The definitions below satisfy both RFC-2119 use and plain reading.

- **container** — a VIREL file: a container header followed by records (section 3).
- **record** — one framed unit of a container: a header plus payload bytes.
- **stream** — the byte stream of one payload format inside a container, reconstructed byte-exactly from its DATA records.
- **payload format** — what a stream's bytes are, named by a format id (`virel`, `jfr`, `go-trace`, ...).
- **payload decoder** — an optional reader plugin that decodes one non-`virel` payload format into the VIREL model.
- **chunk** — a self-contained partition of a `virel` or `jfr` stream. A chunk spans exactly its `size` bytes, header included.
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

0. **Event-oriented scope.** VIREL records and carries event streams only. Formats without timestamped events are out of scope: aggregated profiles (pprof, OTLP Profiles), heap snapshots (HPROF, V8 heap profiles), and folded stacks.
1. **Superset.** One format represents JFR, Go tracev2, and Perfetto as sub-configurations.
2. **Agnostic container.** The container frames streams of any format identically and never interprets payloads. Readers MUST decode `virel`; decoding other formats is OPTIONAL, through plugins. JFR-annex chunks share the native chunk grammar, so a `jfr` decoder is cheap, but it is not mandatory.
3. **Migration-first.** Existing producer output becomes VIREL data by being framed as a stream, with no encoder change. Conversions from popular formats stay expressible, streaming, single-pass, and bounded in memory. No protoc, registry, or codec dependency exists. A CTEF-JSON bootstrap emitter supports the reverse direction.
4. **Low overhead writes.** Writers use the JFR and Go cost model: in-band varint sizes, padded back-patches, chunk-local interning.
5. **Convergent mechanics.** VIREL keeps the mechanics that JFR, Go tracev2, and Perfetto share. Chunked partitions, per-partition interning, back-patched batch lengths, and in-band sync and loss markers.

Version 1.0 excludes formats without timestamped events, replacing OTLP as a telemetry protocol (OTLP traces and logs are carried byte-exactly, never converted into a competing vocabulary), payload compression inside the container (whole-stream `content-encoding` aside), cross-stream references, multi-clock domains, machine federation, and per-writer sub-chunk regions. Section 8 holds their extension anchors.

Evidence: find-superset-goal, find-coverage-matrix, find-event-scope, find-subset-direction, find-agnostic-container, find-migration-strategy, ev-content-encodings, ev-prior-art-fine-tracers.

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
| terms | container, record, stream, payload format, chunk, and writer are normative (Terminology) |

Evidence: ev-chunk-header, ev-content-encodings, ev-prior-art-fine-tracers, ev-prior-art.

## 3. Container

Every VIREL file is a container: a container header followed by records. The container is format-agnostic. It frames streams of any payload format the same way and never parses payload bytes. Native VIREL and JFR get no special treatment. Container numerics in this section are PROPOSED (2026-10-04). Evidence: find-agnostic-container.

### 3.1 Model

- A stream is the byte stream of one payload format. Concatenating the payloads of a stream's DATA records in sequence order yields the stream's bytes exactly. The writer chooses where to split a stream into records; the split has no meaning for the payload format. Exception: under `record-framing: message` (section 3.4) each DATA record is one whole document, and the stream extracts as the document sequence. Writers SHOULD split at the format's natural resync unit (section 3.4.1).
- A container has one writer at a time. A typical writer is a collector that assembles finished inputs, for example JFR from a host JVM, VIREL from a profiler, and a Go trace from an embedded library. Runtimes do not share a container writer. Records of different streams MAY interleave.
- A native profiler that writes live output writes a container with one `virel` stream.
- Conformance has three levels:
  - Container conformance (every reader and writer): header, records, descriptors, manifest, recovery, and byte-exact extraction of every stream.
  - Native decoding (every reader): decoding `virel` streams (sections 3.9, 4, 5).
  - Payload decoders (OPTIONAL): decoding other formats into the VIREL model, provided as plugins selected by the `format` id. A reader without a decoder for a stream still lists, indexes, and extracts that stream. A reader states which payload decoders it supports.

### 3.2 Container header (32 bytes)

| off | len | field | notes |
|----:|----:|---|---|
| 0 | 8 | signature | `89 56 52 4C 0D 0A 1A 0A` (`\x89VRL\r\n\x1a\n`); detects 7-bit, CR/LF and DOS-EOF damage |
| 8 | 2 | container major | 0 = this draft; readers reject unknown majors |
| 10 | 2 | container minor | additive only |
| 12 | 2 | header size | 32 in major 0 |
| 14 | 2 | flags | reserved, 0 |
| 16 | 8 | container id | random u64; 0 = unspecified |
| 24 | 4 | reserved | 0 |
| 28 | 4 | header CRC-32C | over all preceding header bytes; always the last 4 header bytes |

### 3.3 Records

A record is a 56-byte header (major 0) followed by `payload length` payload bytes.

| off | len | field | notes |
|----:|----:|---|---|
| 0 | 4 | record magic | `VREC` (static part, written at open) |
| 4 | 2 | kind | PAD 0, STREAM 1, DATA 2, MANIFEST 3, END 4; append-only registry |
| 6 | 2 | header size | 56 in major 0 |
| 8 | 4 | stream id | u32; 0 = container-level record |
| 12 | 4 | sequence | DATA: 0, 1, 2, ... per stream in file order; other kinds: 0 |
| 16 | 4 | static CRC-32C | over bytes 0..15 |
| 20 | 1 | state | 0xFF = OPEN, 0x00 = CLOSED (dynamic part, final once CLOSED) |
| 21 | 1 | flags | bit 0 STREAM_END; bit 1 PAYLOAD_CRC; bit 2 PREAMBLE; bit 3 RESYNC |
| 22 | 2 | reserved | 0 |
| 24 | 8 | payload length | all ones while OPEN |
| 32 | 8 | time start | UNIX epoch ns, writer-supplied, advisory; 0 = unknown |
| 40 | 8 | time end | as time start |
| 48 | 4 | payload CRC-32C | valid iff PAYLOAD_CRC |
| 52 | 4 | dynamic CRC-32C | over bytes 20..51; valid iff CLOSED |

Readers skip unknown kinds by length. A stream's first STREAM record precedes its first DATA record. Stream ids are unique and never reused. A stream with no data ends with a zero-length DATA record carrying STREAM_END. END is the last record of a cleanly closed container and is 64 bytes long; its payload is the u64 file offset of the MANIFEST record (0 = none). MANIFEST, when present, immediately precedes END.

PREAMBLE and RESYNC are writer-asserted seek hints. PREAMBLE marks leading DATA records holding the format's stream preamble (for example the 16-byte Go trace header). RESYNC marks a DATA record where a decoder may start: it reads the PREAMBLE records, then that record and every later record of the stream in order. The container does not verify them.

### 3.4 Stream descriptors

A STREAM record carries `count:varint` key/value pairs; each key is a varint-length UTF-8 string and each value a varint-length byte string. Unknown keys are ignored; `x-` keys are private. A later STREAM record for the same stream MAY add keys; it MUST NOT change or remove a value already given.

| key | value | notes |
|---|---|---|
| `format` | UTF-8 | REQUIRED format id (section 3.4.1) |
| `format-version` | UTF-8 | as the format names its versions |
| `content-encoding` | UTF-8 | `identity` (default), `gzip`, or `zstd` |
| `record-framing` | UTF-8 | `stream` (default): records concatenate into one byte stream; `message`: each DATA record is one complete document and extraction yields the document sequence (OTLP export requests); fixed in the first STREAM record |
| `source` | UTF-8 | where the bytes came from |
| `producer` | UTF-8 | software that produced the payload, with version |
| `writer-id` | u64 BE | the payload's native writer id, when it has one |
| `clock-anchor` | anchor | repeatable (section 3.4.2) |
| `derived-from` | varint stream id | repeatable; this stream was converted from that stream |
| `input-truncated` | empty | the writer knows its input was cut off |
| `label` | UTF-8 | human-readable name |

#### 3.4.1 Format ids

The registry is string-first and append-only: `virel` (native chunk stream), `jfr` (JFR chunk stream, annex majors 1 and 2), `go-trace`, `perfetto`, `otlp-traces`, `otlp-logs`, `otlp-traces-json`, `otlp-logs-json`, `ctef`, `v8-cpuprofile`, `v8-log`, and `x-...` for private formats. The parent draft (section 3.4.3) gives informative split units, resync points, and clock anchors per format. A writer MUST NOT append a second Perfetto session to a `perfetto` stream. Only `virel` decoding is mandatory. Every stream is extractable byte-exactly whatever its format.

#### 3.4.2 Clock anchors

An anchor is `clock:UTF-8, ticks, frequency, unixNanos, uncertaintyNanos` (varints, with a varint-length clock name). When the payload clock read `ticks` (units of 1/`frequency` s), the container timeline (UNIX epoch ns) read `unixNanos` within `uncertaintyNanos`. Writers SHOULD anchor every stream whose payload lacks its own wall-clock anchor. Payload-internal anchors (the `virel` and `jfr` chunk headers) remain authoritative for decoding. Cross-stream alignment uses these anchors. Registered clock names: `virel-ticks`, `jfr-ticks`, `go-ticks`, `perfetto:<builtin clock id>`, `v8-timeticks`, and `uv-hrtime`; the last two are different clocks on macOS.

### 3.5 Manifest

The MANIFEST payload lists, per stream: stream id, offset of its first STREAM record, DATA record count, payload bytes, time range, and an ENDED flag. It MAY add a record index of every DATA record (offset, stream id, sequence, time range, flags). The manifest MUST agree with the records; when they disagree, the records win. Fast path: read the last 64 bytes, validate END, then read the manifest.

### 3.6 Live writing

A writer opens a record with state OPEN and an all-ones length, then appends payload. It closes the record by writing the length, time range, flags, and CRCs, and stores state CLOSED last. At most one record is OPEN, and it is the last record; its payload extends to end of file. A live reader decodes an OPEN payload under the format's own live rules (`virel`: the chunk seqlock, section 3.9.3). A CLOSED record with a mismatched dynamic CRC is a torn read and is read again.

### 3.7 Truncation and recovery

A container is cleanly closed iff a valid END record is present. A reader walks records by length. On an invalid record it scans forward for `VREC`, resumes at the first candidate that passes the static and dynamic checks, and reports the skipped bytes. A stream is complete when its DATA sequences run 0..n without gaps and record n has STREAM_END; only a complete stream is byte-exact. An incomplete stream extracts as its surviving payloads in order, with gaps reported.

### 3.8 Bare legacy files

A reader MAY open a non-container file as an implicit one-stream container: `FLR\0` opens as `jfr`, `VIRL` as `virel`, and a Go trace header as `go-trace`. This is a reader feature, not container grammar.

### 3.9 Chunk-stream payloads: `virel` and `jfr`

A `virel` or `jfr` stream is the concatenation of its chunks. A chunk begins with its header and spans exactly its `size` bytes, pools included. A decoder MAY attach at any chunk boundary without reading previous chunks, and MUST locate declared types and pools inside the chunk's own scope. In a container, writers SHOULD place one chunk per DATA record.

`VIRL` plus byte 66 bit 7 separates native chunks from JFR. Native VIREL chunks set this signature bit. JFR legacy annex chunks retain `FLR\0` and keep it clear. All other (magic, major, minor) values make a foreign chunk (section 3.9.5). Decoders bridge such chunks and never interpret them. Evidence: find-version-resilience.

#### 3.9.1 Legacy annex header — majors 1.x/2.x (BYTE-FROZEN, 68 bytes)

All fixed-width words here are big-endian.

| off | len | field | notes |
|----:|----:|---|---|
| 0 | 4 | magic | `FLR\0` (JFR-only; native chunks use `VIRL`) |
| 4 | 2 | version major | writer emits 2; readers accept 1 and 2 |
| 6 | 2 | version minor | |
| 8 | 8 | chunk size | initially HEADER_SIZE (68); back-patched at finalize |
| 16 | 8 | last checkpoint / state snapshot offset | posthoc-updated; JFR / VIREL-native meaning respectively |
| 24 | 8 | metadata position | posthoc-updated |
| 32 | 8 | chunk start, UNIX nanos | written at open; stable |
| 40 | 8 | chunk duration nanos | posthoc-updated |
| 48 | 8 | chunk start ticks | stable; conversion anchor |
| 56 | 8 | ticks per second | stable |
| 64 | 1 | file state | 0 = COMPLETE; 0xff = UPDATING (seqlock, §3.9.3) |
| 65 | 1 | pad | |
| 66 | 2 | flags (BE u2) | low byte @67: bit 0 = CompressedIntegers (varint regime) — writers of this codebase hardcode true; bit 1 = FINAL-CHUNK. High byte @66: **unused in v2** |

Writers update six fields posthoc: size, last checkpoint/state snapshot,
metadata, duration, state, and flags. These fields are reliable only under the section 3.9.3 protocol. The fields magic, version, start-nanos, start-ticks, and frequency are stable.

#### 3.9.2 Native header — majors 0 (draft) and 1 (stable) — 76 bytes

`VIRL` is native-only; `FLR\0` remains JFR-only. The signature bit is a required class guard:

- Byte 66 bit 7 (0x80) is the VIREL-NATIVE signature. Native `VIRL` chunks set it; all JFR-legacy chunks keep it clear. Verified: JFR v2 writes byte 66 = 0. JFR v2 readers never read byte 66.
- Bits 0 to 6 of byte 66 carry the metadata-default-encoding id (0 to 127).
- Native major 0 is the active draft and evolves in place until ratification;
  this does not allocate a new major or minor. A future ratified successor is
  an advertised spec event. Readers of today skip an unsupported successor as
  foreign (section 3.9.5).
- Annex majors 1 and 2 stay byte-frozen (clear signature). `FLR\0` versus `VIRL` is the primary class test; the signature bit rejects malformed headers.
- A future OpenJDK JFR major 3 or later = a foreign chunk (section 3.9.5). Readers bridge it and never interpret it. Later real JFR-3 annex support is a pure additive spec revision.
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

#### 3.9.3 Chunk live-write protocol (seqlock)

The state byte at offset 64 transitions from 0xff to 0x00 at finalize. A live reader SHALL read state, then posthoc fields, then state again, and SHALL accept only on equal reads. This reproduces the JDK protocol (ChunkHeader.java: FILE_STATE_POSITION=64, UPDATING_CHUNK_HEADER=255). The protocol supports single-chunk streaming. The FINAL-CHUNK flag bit is the only crash-vs-clean marker within the stream. Section 3.9.4 defines truncation recovery for the rest. In a container, record state and the END record add container-level crash detection.

#### 3.9.4 Chunk truncation and recovery

A decoder SHALL scan forward within the stream for the next recognized magic (`FLR\0` or `VIRL`) on structural failure and attach there; in a container written one chunk per record, the next record boundary is the preferred attach point. An unfinalized final chunk ends at the stream end. The reader SHALL consume its content up to the last parseable event. Pool or state incompleteness follows the section 5 snapshot rules. Sync points (section 4.4) delimit the durability horizon of a torn tail.

#### 3.9.5 Foreign chunks — resilience to JFR evolution (normative)

Within a `virel` or `jfr` stream, a chunk is FOREIGN when the decoder does not support its (major, minor) pair. The container is unaffected; the stream stays extractable. Examples are a future JFR major 3 and a newer annex minor.

1. The reader MUST NOT interpret, guess at, or parse the foreign chunk by v1/v2 or any other rules.
2. The reader bridges over it and scans for `FLR\0` or `VIRL` from the current position, per section 3.9.4. The reader accepts a candidate only when it supports that (magic, signature-state, major, minor) class. `VIRL` + signature set + a supported native major, or `FLR\0` + signature clear + a supported annex (1 or 2), is acceptable. Either 4-byte magic can occur inside foreign payloads. The signature bit is a header-octet test on the candidate header.
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

### 4.3 Ordering

In major 0, per-writer event order equals chunk order and append order. Cross-writer order is not chronological by default (Go trace, JFR `FastUnordered`). Every event carries real time words. The former filler time words for timebase-neutral records (aggregated pprof samples) are withdrawn with the event-oriented scope.

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

Evidence: ev-content-encodings, ev-framing-constants, ev-validator-jfr-annex, ParserFactory.java:186-276, RecordingInput.java, TimeConverter.java:36-40, find-exec-trace-vocabulary, ev-parser-dynamics, ev-header-checkpoints, ev-parser-unknown-handling.

## 5. Metadata and type system

### 5.1 Model

- Declared types live in-stream. The format is self-describing, with no out-of-band schema or registry. This is migration property 3 (section 1).
- Type-id bindings are stream-global and add-only; a binding made in any chunk holds for the whole `virel` stream. Readers never re-bind them. A redefinition is a stream violation.
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
| [2³⁰, 2⁶²) | DYNAMIC: writer-assigned, per stream, add-only |
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
| 2 | `exec-trace` | execution-tracing vocabulary: task/executor (scheduling states, Go P/G model), slices (duration events; nesting = track+time convention), instants, counters (+ unit/temporality descriptors), STATE tracks, timestamped stack samples, per-sequence numbers [find-exec-trace-vocabulary] |
| 3+ | ledger | append-only additions (spec-adjacent registry file, community-tracked) |

Evidence: find-migration-strategy, find-type-id-model, ev-parser-dynamics, ChunkHeader.java:224-239, ev-metadata-event, ev-validator-jfr-annex, MetadataReader.java, hyp-metadata-encoding, ev-framing-constants.

## 6. Profiles and conversion

### 6.1 Profile roster

Every profile has two forms. Embedded: the original bytes as a stream of their own format id, byte-exact by construction. Converted: a `virel` stream produced by the profile's converter. A container MAY carry both, linked by `derived-from`. Decoding the embedded form is an optional payload decoder; only `standard` decoding is mandatory.

| profile | format id | major | converted form | conformance |
|---|---|---|---|---|
| `jfr-legacy` | `jfr` | 1.x/2.x | none needed: annex chunks decode directly | round-trip byte equality + JDK-reader parity suite |
| `standard` | `virel` | **0 draft → 1 stable** (signature set) | — (native) | writer/reader suites |
| `go-trace` | `go-trace` | 0 | lossless representation (converter) | round-trip: Go gen → VIREL chunk → reconstruct Go trace |
| `perfetto` | `perfetto` | 0 | lossless representation | round-trip TracePacket stream |

### 6.2 Conversion provenance (standardized)

A writer emits the declared metadata type `provenance` (module m0) once per chunk. When the source is embedded in the same container, the converted stream's descriptor SHOULD also name it with `derived-from`.

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

The mapped portion plus the fragments contain the complete original information (reassembly with the source-format reader). Converter guidance (normative): converters write fragments only. The attachment budget SHOULD stay at a small fraction of the stream size. A full original attachment is allowed but discouraged (size-doubling ruled out). Blobs skip as ordinary size-prefixed events. Consumers MUST NOT auto-interpret attachment bytes. When the source is embedded in the same container (`derived-from`), converters MAY omit fragments.

### 6.4 Reference converters = validators

The round-trip suites and the shipped reference converters are the same artifacts. The Go converter buffers one generation, with a ~16 MiB ceiling. The Perfetto converter streams 1:1 with mid-chunk checkpoints. Tier B (CTEF/cpuprofile, perf.data, ftrace, CTF, ETW) gets shape-fold converters and a CTEF emitter first.

Evidence: find-conversion-provenance, find-converter-cost.

## 7. Evolution rules

| aspect | rule |
|---|---|
| container | major 0 = draft; readers reject unknown container majors; minors additive; record kinds, descriptor keys, and format ids append-only; unknown format ids are carried and extracted, never decoded |
| native majors | 0 = draft; 1 = first stable, on ratification; later majors = advertised spec events, additive preferred |
| JFR annex | majors 1 and 2, clear signature; frozen forever |
| foreign | within a chunk stream, any unknown (major, minor) = foreign chunk (section 3.9.5); number-space claims and reservations do not exist; no coordination is assumed or needed |
| minors | additive only, within one native major |
| type ids | add-only, never re-bound, in any major |
| module ledger | append-only; registered module versions add only |
| envelope registry | append-only |

Evidence: find-version-resilience.

## 8. Declared extension directions (not in v1.0; anchors exist)

| direction | anchor |
|---|---|
| multi-clock domains | CLOCK sync-point kind carries (clock-id, value, frequency) tables; per-event clock = declared field (Perfetto model, sequence-scoped clocks = writer-scoped + CLEAR reset) |
| record payload compression | a record flag plus codec id; until then, `content-encoding` covers whole-stream encodings |
| cross-stream references | native events referring to entities inside another stream; out of scope for v1.0 |
| machine federation | resource-level declared types + external clock sync packets |
| per-writer sub-regions | re-open if converter evidence shows demand (currently ruled out) |

## 9. Honest limits (v1.0)

| limit | note |
|---|---|
| no native map/kv-associative type | composite arrays of kv pairs suffice; JFR and OTLP model it this way in practice |
| axes | one nanosecond wall axis and one tick axis per chunk (multi-clock = section 8) |
| v2 annex event ticks | non-monotonic by JVM design (section 4.2); consumers MUST NOT assume intra-chunk order (verified TSC behavior) |
| container time data | record time ranges and clock anchors are writer-supplied; the container does not verify them |
| byte-exactness | holds only for complete streams; recovery reports gaps, it does not fill them |

## 10. Security Considerations

1. **Untrusted container input.** Readers MUST check every length and offset against the file size, SHOULD enforce a configurable maximum record payload, and MUST validate the record at a manifest offset before trusting it. Payload decoders, including plugins, run on untrusted bytes.
2. **Foreign chunks.** A decoder MUST bridge over foreign chunks and MUST NOT interpret their content. The reader tests magic and signature on the candidate header, because `FLR\0` and `VIRL` can occur inside foreign payloads.
3. **Attachment bytes.** Consumers MUST NOT auto-interpret attachment bytes. Fragments keep original bytes in their original encoding.
4. **Truncation.** Recovery has defined bounds. The reader rescans for a valid record header (section 3.7); inside a chunk stream, the decoder scans forward to the next chunk header and consumes up to the last parseable event. Its durability horizon ends at the last parsed sync point.
5. **Compression independence.** Version 1.0 defines no record payload compression (section 8 anchors it as opt-in); readers attach at any record without a decompressor.

## 11. IANA Considerations

This draft makes no request to IANA. The module ledger, the encoding registry, the record kind registry, and the format id registry are append-only files that ship with this spec. The community tracks the module ledger (section 5.5).

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

**JFR.** A `jfr` stream is JFR, byte for byte; extracting it yields the original file. VIREL annex majors 1 and 2 are byte-identical JFR. An annex chunk is a JFR v1/v2 chunk, byte for byte. The section 3.9.1 header stays byte-frozen. The `jfr-legacy` profile requires round-trip byte equality and a JDK-reader parity suite. JFR readers fail fast on native chunks; VIREL readers bridge over future JFR majors (sections 3.9.2, 3.9.5).

**Go tracev2 and Perfetto.** Both embed unchanged as `go-trace` and `perfetto` streams. Their converted forms, the `go-trace` and `perfetto` profiles, are lossless representations (converter in, validator back). The Go round trip restores a Go trace from one chunk. The Perfetto round trip restores the TracePacket stream.

**OTLP.** OTLP traces and logs embed unchanged as `otlp-*` streams with message framing; VIREL does not convert them or define a competing span/log vocabulary. Aggregated profiles (pprof, OTLP Profiles) are out of scope.

**Conversion provenance and fragments.** A converter records its provenance in a declared metadata type (section 6.2). The record carries the source format and the source version. It carries the converter identity, the conversion fidelity, and the original payload hash. Attachment means only the non-recoded parts (section 6.3). Each unmapped element becomes an `attachment-fragment` event; mapped portion plus fragments contain the complete original information. Consumers MUST NOT auto-interpret attachment bytes.
