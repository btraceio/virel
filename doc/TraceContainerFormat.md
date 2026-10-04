# VIREL — Versioned In-band Runtime Event Ledger
### Specification draft v0.1 · container major 0 · native payload major 0 · JFR payload majors 1/2 (frozen) · 2026-10-04

> **Status:** DRAFT. VIREL is the selected protocol name. Native chunks use
> magic `VIRL` and wire type namespaces `virel.*`, `virel2.*`, and
> `virel3.*`. `FLR\0` remains exclusively JFR.
> Derived from the jfr-spec investigation graph (`.investigations/jfr-spec/`,
> 28 nodes); every normative claim about JFR v1/v2 carries a node id with
> file:line evidence. Numeric assignments marked PROPOSED were invented by
> this draft and are now RATIFIED by proceed (2026-10-03); all other content
> is decided. Corrections from the validator run are folded in as of
> 2026-10-03 (see ev-validator-jfr-annex).
> **2026-10-04:** every VIREL file is a format-agnostic container of framed
> streams (§3). Native VIREL and JFR are payload formats on equal footing with
> Go trace, Perfetto and others [find-agnostic-container]. Container numerics
> are PROPOSED.

---

## 1. Design goals

0. **Event-oriented scope** (2026-10-04) [find-event-scope]: VIREL records
   and carries *event* streams, i.e. timestamped records such as JFR events,
   Go trace events, Perfetto packets, trace-event JSON, timestamped CPU
   samples, and OTLP spans and logs. Formats without timestamped events are
   out of scope: aggregated profiles (pprof, OTLP Profiles), heap snapshots
   (HPROF, V8 heap profiles), folded stacks.
1. **Superset**: one format represents JFR (`.jfr`), Go execution trace v2,
   and Perfetto (TracePacket) as **sub-configurations** [find-superset-goal,
   find-coverage-matrix].
2. **Agnostic container**: the container (§3) frames streams of any format
   (native VIREL, JFR, Go trace, Perfetto, OTLP traces and logs) identically
   and never
   interprets payloads [find-agnostic-container]. A conforming reader MUST
   decode `virel` streams. Decoding any other format is OPTIONAL and supported
   through payload decoders (plugins) (§3.1). JFR-annex chunks share the
   native chunk grammar (§3.9), so a `jfr` decoder is cheap to provide, but it
   is not mandatory [find-subset-direction, superseded in part].
3. **Migration-first**: extreme ease of conversion from all popular trace
   formats — expressible, streaming single-pass bounded-memory, no
   infrastructure prerequisites (no protoc, no registry, no codec deps),
   bootstrap emitter (CTEF JSON) [find-migration-
   strategy]. Zero-conversion first step: an existing producer's output
   becomes VIREL data by being framed as a stream (§3), with no encoder
   change.
4. **Low overhead writes**: same cost model as JFR/Go batching — in-band
   varint sizes, padded back-patches, chunk-local interning [ev-content-
   encodings, ev-prior-art-fine-tracers].
5. **Convergent mechanics, not novelty**: chunked self-contained partitions,
   per-partition interning, back-patched batch lengths, in-band sync/loss
   markers = the baseline JFR, Go tracev2, and Perfetto share [ev-prior-art-
   fine-tracers].

Non-goals for v1.0: formats without timestamped events (goal 0); replacing
OTLP as a telemetry protocol (OTLP traces and logs are carried byte-exactly as
streams, never converted into a competing VIREL vocabulary) [find-event-scope];
payload compression inside the container (whole-stream
`content-encoding` aside), cross-stream references, multi-clock domains, machine
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
- The following terms are normative:
  - *container*: a VIREL file, i.e. a header plus records (§3);
  - *record*: one framed unit of a container (§3.3);
  - *stream*: the byte stream of one payload format inside a container,
    reconstructed byte-exactly from its DATA records (§3.1);
  - *payload format*: what a stream's bytes are (`virel`, `jfr`,
    `go-trace`, …; §3.4.1);
  - *chunk*: a self-contained partition of a `virel` or `jfr` stream (§3.9);
  - *writer*: the producer identity owning a chunk.

---

## 3. Container (format-agnostic)

> Decided 2026-10-04 [find-agnostic-container]. Container numerics in this
> section (signature, record layout, kinds, descriptor keys, format ids) are
> PROPOSED and not yet ratified. The former §3.1–§3.5 (chunk headers, chunk
> live-write, chunk recovery, foreign-chunk bridging) are now §3.9.1–§3.9.5
> and apply only inside chunk-stream payloads.

### 3.1 Model

- **Every VIREL file is a container**: a container header (§3.2) followed by
  records (§3.3). This includes a file written live by one native profiler,
  which is a container with a single `virel` stream.
- A **stream** is a byte stream in one payload format (§3.4). Native VIREL,
  JFR, Go trace, Perfetto, OTLP and every other format are framed the same
  way. The container MUST NOT treat any format specially, and it never parses
  payload bytes.
- **Reconstruction rule (normative)**: concatenating the payloads of a
  stream's DATA records in ascending sequence order yields the stream's bytes
  exactly. Where a writer splits a stream into records is its choice and has
  no meaning for the payload format. Exception: a stream whose descriptor
  says `record-framing: message` (§3.4) is a sequence of whole documents, one
  per DATA record. It extracts as that sequence, and its record boundaries
  are significant. This is for formats whose documents do not concatenate,
  such as OTLP export requests. Writers SHOULD split at the format's
  natural resync unit so the time index and recovery work per unit: one chunk
  per record for `virel` and `jfr`, one generation for `go-trace`.
- **Single writer**: a container has exactly one writer at a time. A typical
  writer is a collector that assembles finished inputs (JFR from a host JVM,
  VIREL from a profiler, a Go trace from an embedded library). Runtimes do not
  share one container writer. Records of different streams MAY interleave.
- **Conformance levels**:
  - *Container conformance* (every reader and writer): header, records,
    descriptors, manifest, recovery, and byte-exact extraction of any stream.
  - *Native decoding* (every reader): decoding `virel` streams (§3.9, §4–§5).
  - *Payload decoders* (OPTIONAL, per format, §6.1): decoding another
    format's stream into the VIREL model. Readers provide them as plugins
    selected by the descriptor's `format` id (and `format-version`). A reader
    without a decoder for a stream's format still lists, indexes and extracts
    that stream (§3.4.1). A reader states which payload decoders it supports.

### 3.2 Container header — 32 bytes

All fixed-width fields are big-endian (§2).

| off | len | field | notes |
|----:|----:|---|---|
| 0 | 8 | signature | `89 56 52 4C 0D 0A 1A 0A` (`\x89VRL\r\n\x1a\n`). PNG-style: detects 7-bit, CR/LF and DOS-EOF damage. Distinct from the chunk magics `VIRL` and `FLR\0`. |
| 8 | 2 | container major | 0 = this draft |
| 10 | 2 | container minor | additive only |
| 12 | 2 | header size | 32 in major 0 |
| 14 | 2 | flags | reserved, 0 |
| 16 | 8 | container id | random u64; 0 = unspecified |
| 24 | 4 | reserved | 0 |
| 28 | 4 | header CRC-32C | over all preceding header bytes; always the last 4 header bytes |

Records start at offset `header size`. A reader MUST reject a container major
it does not implement. It MUST accept unknown minors and skip header bytes
it does not know.

### 3.3 Records

Every record has a fixed header followed by `payload length` payload bytes.
Record header, 56 bytes in container major 0:

| off | len | field | notes |
|----:|----:|---|---|
| | | ***static part*** | written once when the record is opened |
| 0 | 4 | record magic | `'V','R','E','C'` |
| 4 | 2 | kind | §3.3.1 |
| 6 | 2 | header size | 56 in major 0; payload starts at record offset + header size |
| 8 | 4 | stream id | u32; 0 = container-level record |
| 12 | 4 | sequence | u32; DATA records: 0, 1, 2, … per stream in file order. Other kinds: 0 |
| 16 | 4 | static CRC-32C | over bytes 0..15 |
| | | ***dynamic part*** | final once state = CLOSED |
| 20 | 1 | state | 0xFF = OPEN, 0x00 = CLOSED |
| 21 | 1 | flags | bit 0 STREAM_END: last DATA record of its stream. Bit 1 PAYLOAD_CRC: payload CRC is valid. Bit 2 PREAMBLE and bit 3 RESYNC: see below. Other bits 0 |
| 22 | 2 | reserved | 0 |
| 24 | 8 | payload length | u64; all ones while OPEN |
| 32 | 8 | time start | UNIX epoch ns; 0 = unknown |
| 40 | 8 | time end | UNIX epoch ns; 0 = unknown |
| 48 | 4 | payload CRC-32C | valid iff PAYLOAD_CRC |
| 52 | 4 | dynamic CRC-32C | over bytes 20..51; valid iff CLOSED |

- The time range is supplied by the writer, which knows the payload. It is a
  superset of the payload's event times on the container timeline and is
  used only for indexing. Payload timestamps are always authoritative.
- A stream with no data still ends with a DATA record: a zero-length one with
  STREAM_END set.
- **PREAMBLE and RESYNC (writer-asserted, format-neutral)**: some formats can
  be decoded from the middle only if their stream preamble is prepended (a Go
  trace needs its 16-byte header). A writer sets PREAMBLE on the leading DATA
  records that hold such a preamble, and RESYNC on a DATA record at which a
  decoder may start. A decoder that starts at a RESYNC record first reads the
  stream's PREAMBLE records, then that record and every later record of the
  stream in order. How complete the first decoded unit is depends on the
  format (§3.4.3). The container does not verify these flags; they only
  direct seeking. The first record after the preamble is always a valid
  start.

#### 3.3.1 Record kinds (registry, append-only)

| kind | name | stream id | payload |
|---:|---|---|---|
| 0 | PAD | 0 | ignored bytes (reservation, alignment) |
| 1 | STREAM | the declared stream, ≠ 0 | stream descriptor (§3.4) |
| 2 | DATA | ≠ 0 | the next bytes of that stream |
| 3 | MANIFEST | 0 | stream summary and record index (§3.5) |
| 4 | END | 0 | u64 BE file offset of the MANIFEST record; 0 = no manifest |

- Readers skip unknown kinds by `payload length`.
- A stream's first STREAM record precedes its first DATA record. Stream ids
  are unique within a container and never reused.
- END is the last record of a cleanly closed container; nothing follows it.
  Its size is fixed (56 + 8 = 64 bytes), so a reader finds it at end of file
  − 64. MANIFEST, when present, immediately precedes END.

### 3.4 Streams and stream descriptors

A STREAM record's payload is a key/value list (varints per §2):

```
descriptor = count:varint
             { keyLen:varint key:UTF-8  valueLen:varint value:bytes } x count
```

- Keys are case-sensitive. Unknown keys are ignored. Keys starting with `x-`
  are private.
- **Add-only updates**: a later STREAM record for the same stream MAY add
  keys. It MUST NOT change or remove a value already given; repeatable keys
  MAY gain more values. Readers merge all STREAM records of a stream.

Registered keys:

| key | value | notes |
|---|---|---|
| `format` | UTF-8 | REQUIRED. Payload format id (§3.4.1) |
| `format-version` | UTF-8 | the version as the format names it, e.g. `2.1`, `go1.23` |
| `content-encoding` | UTF-8 | `identity` (default), `gzip` or `zstd`: the stream bytes are this encoding of the format's bytes (per document under `message` framing) |
| `record-framing` | UTF-8 | `stream` (default): records concatenate into one byte stream. `message`: each DATA record is one complete document; extraction yields the document sequence. Fixed at stream creation (MUST be in the first STREAM record) |
| `source` | UTF-8 | where the bytes came from, e.g. process, host or path |
| `producer` | UTF-8 | software that produced the payload, with its version |
| `writer-id` | u64 BE | the payload's native writer id, when it has one |
| `clock-anchor` | anchor (§3.4.2) | repeatable |
| `derived-from` | varint stream id | repeatable. This stream was converted from that stream (§6.2) |
| `input-truncated` | empty | the writer knows its input was cut off, e.g. a Go trace that ends mid-generation |
| `label` | UTF-8 | human-readable name |

#### 3.4.1 Format ids (registry, string-first, append-only)

| id | stream bytes | profile (§6.1) |
|---|---|---|
| `virel` | native VIREL chunk stream (§3.9, §4–§5) | `standard` |
| `jfr` | JFR chunk stream, annex majors 1/2 (§3.9.1) | `jfr-legacy` |
| `go-trace` | Go `runtime/trace` output (Go 1.22+ format) | `go-trace` |
| `perfetto` | `perfetto.protos.Trace` (TracePacket stream) | `perfetto` |
| `otlp-traces` | one OTLP `ExportTraceServiceRequest` (binary protobuf) per record; `record-framing: message` | none: carried only |
| `otlp-logs` | one OTLP `ExportLogsServiceRequest` (binary protobuf) per record; `record-framing: message` | none: carried only |
| `otlp-traces-json`, `otlp-logs-json` | the same requests in the OTLP JSON encoding, one per record; `record-framing: message` | none: carried only |
| `ctef` | Chrome trace event JSON (Node.js `--trace-events`, Chrome) | Tier B (§6.5) |
| `v8-cpuprofile` | V8 `.cpuprofile` JSON (Node.js `--cpu-prof`, inspector `Profiler`) | Tier B (§6.5) |
| `v8-log` | V8 `--prof` log (`isolate-*-v8.log`, CSV lines) | — |
| `x-…` | private formats | — |

A reader carries and extracts every stream byte-exactly. It decodes a stream
only when it has a payload decoder for that format id; only `virel` is
mandatory (§3.1).

#### 3.4.2 Clock anchors

```
anchor = clockLen:varint clock:UTF-8  ticks:varint  frequency:varint
         unixNanos:varint  uncertaintyNanos:varint
```

At the moment the payload clock `clock` read `ticks` (in units of
1/`frequency` s), the container timeline (UNIX epoch ns) read `unixNanos`,
± `uncertaintyNanos`. Clock names are defined per format, e.g. `jfr-ticks`,
`go-ticks`, or `perfetto:<builtin clock id>`. Two anchors on the same clock
give its drift.

- Writers SHOULD anchor every stream whose payload has no wall-clock anchor
  of its own.
- When the payload has one (the `virel` and `jfr` chunk headers' start
  nanos/ticks), that anchor is authoritative for decoding and the descriptor
  anchor MAY be omitted.
- Cross-stream alignment uses these anchors. VIREL does not correct one
  stream's clock by another.
- Registered clock names: `virel-ticks`, `jfr-ticks` (chunk tick axis),
  `go-ticks` (Go trace ticks), `perfetto:<builtin clock id>`,
  `v8-timeticks` (V8 `TimeTicks`), `uv-hrtime` (libuv `uv_hrtime`). Two clocks
  that coincide on one platform keep separate names: on macOS,
  `v8-timeticks` (`mach_absolute_time`) stops during sleep while `uv-hrtime`
  (`mach_continuous_time`) does not [ev-node-split-clock].
- A collector that records its own anchor SHOULD read the payload's clock and
  the wall clock back to back on the producing host, and set
  `uncertaintyNanos` to the measured bracket.

#### 3.4.3 Per-format guidance (informative)

Split units, resync points and clock anchors for the registered formats.
"Preamble" names the bytes a writer marks PREAMBLE; "resync" names where it
sets RESYNC. Evidence: ev-go-split-clock, ev-perfetto-split-clock,
ev-node-split-clock.

| format | split unit (one record each) | preamble | resync | clock in payload | collector anchor |
|---|---|---|---|---|---|
| `virel`, `jfr` | one chunk | none | every chunk | chunk header start nanos + ticks (authoritative) | optional |
| `go-trace` | one generation, ending after its `EvEndOfGeneration` byte (format 1.26); pre-1.26 formats: where the batch `gen` changes. Oversized generations MAY split further at batch boundaries (not resync) | the 16-byte header `go 1.NN trace\0\0\0` | first record of each generation | per-generation `EvFrequency`; `EvClockSnapshot` (formats 1.25+) gives wall + mono at a tick value | `go-ticks`, frequency = `EvFrequency`. From 1.25: re-anchor per generation from `EvClockSnapshot`. Formats 1.22/1.23: collector's own pair at trace start |
| `perfetto` | runs of whole outer `TracePacket`s (`0x0A` + length + packet); prefer ending a record at a traced write pass (`read_tracing_buffers_completed`) | none | the first packet after a write pass (or before a `synchronization_marker`) | `ClockSnapshot` packets (session start, then about every 10 s); REALTIME only on Linux/Android | `perfetto:<primary_trace_clock>`, normally `perfetto:6` (BOOTTIME). Collector's own pair SHOULD be recorded: snapshots may be disabled, absent from hand-built traces, or lack REALTIME (Apple/Windows) |
| `ctef` (Node.js) | the whole file, or runs of whole events (after an event's `}`, before the following `,`) | `{"traceEvents":[` | none (JSON document) | none; `ts` in µs of `uv_hrtime` | `uv-hrtime`, frequency 1e6, from a `process.hrtime`/`Date.now` pair |
| `v8-cpuprofile` | the whole document | none | none | none; `startTime`/`timeDeltas` in µs of V8 `TimeTicks` | `v8-timeticks`, ticks = `startTime`, frequency 1e6, from a wall-clock read bracketing `Profiler.start`. Not `process.hrtime` (different clock on macOS) |
| `otlp-*` | one export request (`message` framing) | none | every record | `time_unix_nano` fields are UNIX epoch ns: the payload is wall-clock | none needed |
| `v8-log` | runs of whole lines (split at `\n`) | none | none | µs since the V8 logger started | coarse: process start; uncertainty ms or more |

Format-specific rules that affect containers:

- **Perfetto: one session per stream.** Two Perfetto traces concatenated
  parse as protobuf but are not one trace: sequence ids are only unique
  within a session. A writer MUST NOT append a second session to a
  `perfetto` stream. Starting at a resync point recovers packet boundaries,
  not full state: a decoder drops `SEQ_NEEDS_INCREMENTAL_STATE` packets until
  each sequence's next clear, waits for the next `ClockSnapshot`, and lacks
  the one-time config packets of the first pass.
- **Go: skip only leading generations.** A decoder may start at any
  generation (with the preamble) but MUST then read the following generations
  contiguously; dropping a middle generation breaks decoding.
- **Rotated Node.js trace files are separate streams**: each rotated
  `node_trace.N.log` is a complete JSON document. A trace file from a crashed
  process lacks its closing `]}`; writers SHOULD set `input-truncated`.
- **Truncation signals the collector can check**: Go 1.26 streams end with
  `EvEndOfGeneration` at a batch boundary; Perfetto has no end marker (a clean
  stop ends with service events and `trace_stats`; absence is only a hint);
  JSON formats end with their closing bracket.

### 3.5 Manifest

```
manifest = version:varint (= 1)
           streamCount:varint
           { streamId:varint  firstStreamRecord:varint (file offset)
             dataRecords:varint  payloadBytes:varint
             timeStart:varint  timeEnd:varint (0 = unknown)
             flags:varint (bit 0 = ENDED) } x streamCount
           indexCount:varint (0, or the number of DATA records)
           { recordOffset:varint  streamId:varint  sequence:varint
             timeStart:varint  timeEnd:varint
             flags:varint (the record's flags byte) } x indexCount   // ascending offset
```

The manifest only accelerates reading. It MUST agree with the records. A
reader MAY ignore it and walk the records. When they disagree, the records
win and the reader reports the difference. Fast path: read the last 64 bytes,
validate the END record, then read the manifest at the offset it gives.

### 3.6 Live writing

- To open a record, the writer writes the static part, `state = OPEN` and
  `payload length = all ones`, then appends payload bytes.
- To close it, the writer writes the payload length, time range, flags,
  payload CRC and dynamic CRC (computed with `state = 0x00`). It stores
  `state = 0x00` last, after all payload bytes are written.
- At most one record is OPEN, and it is the last record in the file. Its
  payload extends to the current end of file.
- A live reader that finds an OPEN record decodes its payload under that
  format's own live rules (for `virel`: the chunk seqlock, §3.9.3). A CLOSED
  record whose dynamic CRC does not match is a torn read and is read again.
- A native profiler writes one record per chunk. The chunk keeps its own
  header seqlock inside the payload.

### 3.7 Truncation and recovery

- A container is cleanly closed iff a valid END record is present. Without
  one, it is live or was cut off.
- A reader walks records from the header using `header size` and
  `payload length`. A record is valid when its magic and static CRC match and
  either it is CLOSED with a matching dynamic CRC, or it is OPEN and is the
  last record.
- On an invalid record, the reader scans forward for `VREC` at any byte
  offset and resumes at the first candidate that is valid by the same test.
  It reports skipped bytes as a damaged span.
- A stream is **complete** when its DATA sequences run 0..n without gaps and
  record n has STREAM_END. Only a complete stream is byte-exact. An incomplete
  stream extracts as the surviving records' payloads in order, with its gaps
  reported. A final record cut short (OPEN, or CLOSED with a length past end
  of file) contributes its bytes up to end of file and leaves its stream
  incomplete.
- Recovery inside a recovered payload follows the payload format (for
  `virel`/`jfr`: §3.9.4).

**Untrusted input**: readers MUST check every length and offset against the
file size before use. They SHOULD enforce a configurable maximum record
payload. They MUST validate the record at a manifest offset before trusting
it. Payload decoders run on untrusted bytes.

### 3.8 Bare legacy files (reader convenience)

A reader MAY open a file that is not a container as a container with one
implicit stream, detected by its leading bytes:

- `FLR\0` → `jfr`;
- `VIRL` → `virel` (pre-container drafts and stream extracts);
- `go 1.` followed by a Go trace header → `go-trace`.

Formats without a reliable signature, such as Perfetto protobuf, need the
format named explicitly. This is a reader feature, not part of the container
grammar. Magic scanning (§3.9.5) belongs to the `virel` and `jfr` decoders.

### 3.9 Chunk-stream payloads: `virel` and `jfr`

The `virel` and `jfr` payload formats are each a concatenation of chunks. A
chunk begins with its header and spans exactly `size` bytes (header, content
and pools). A decoder MAY attach at any chunk boundary without reading earlier
chunks, but MUST resolve declared types and pools within the chunk's own
scope. In a container, writers SHOULD put exactly one chunk per DATA record,
so a decoder can attach at record boundaries without scanning.

Chunk classes are identified by their magic plus the @66 signature bit
(§3.9.2, find-version-resilience). Native = `VIRL` plus signature set
(0 = draft, 1 = stable). JFR legacy annex = `FLR\0` plus majors 1/2 and
signature clear (§3.9.1). Any other chunk in a chunk stream is foreign
(§3.9.5): decoders bridge over it and never interpret it. `FLR\0` remains
exclusively a JFR magic.

#### 3.9.1 Legacy annex header — majors 1.x/2.x (BYTE-FROZEN, 68 bytes)

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
| 64 | 1 | file state | 0 = COMPLETE; 0xff = UPDATING (seqlock, §3.9.3) |
| 65 | 1 | pad | |
| 66 | 2 | flags (BE u2) | low byte @67: bit 0 = CompressedIntegers (varint regime) — writers of this codebase hardcode true; bit 1 = FINAL-CHUNK. High byte @66: **unused in v2** |

Posthoc-updated fields (size, checkpoint, metadata, duration, state, flags)
are reliable only under the §3.9.3 protocol; magic/version/start-nanos/
start-ticks/frequency are stable.

#### 3.9.2 Native header — majors 0 (draft) and 1 (stable) — 76 bytes

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
  under §3.9.5 (foreign) until updated. Evolution within a native major is
  additive minors.
- **Annex majors 1/2** remain byte-frozen for legacy chunks (signature
  clear). `FLR\0` versus `VIRL` makes the JFR/native boundary explicit;
  the signature bit protects against malformed or mislabelled headers.
- **A future OpenJDK JFR major 3/4/…** cannot collide: unknown
  (major, minor) = foreign chunk (§3.9.5), bridged, never interpreted;
  real JFR-3 annex support later = pure additive spec revision.
- JFR readers fail fast on any native chunk (unknown native profile or an
  unexpected header extent) — migration-safe in both directions by design.

Native chunks use `VIRL` at offsets 0..3; offsets 4..67 retain the Annex
§3.9.1 layout and positions unchanged; the size is major-scoped
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

**Container overlap (open, 2026-10-04)**: inside a container, the magic,
signature bit, size and live seqlock partly duplicate record framing (§3.3,
§3.6). The native header stays unchanged for now. Slimming it is a separate,
later step [find-agnostic-container].

#### 3.9.3 Chunk live-write protocol (seqlock)

The state byte at 64 transitions 0xff → 0x00 at finalize. A live reader
SHALL read state, then the posthoc fields, then state again; accept only on
equal reads. This reproduces the JDK protocol (ChunkHeader.java: FILE_
STATE_POSITION=64, UPDATING_CHUNK_HEADER=255) and gives single-chunk
streaming [ev-live-header-protocol]. The FINAL-CHUNK flag bit is the only
crash-vs-clean marker within the stream; normative truncation recovery
(§3.9.4) covers the rest. In a container, record state (§3.6) and the END
record (§3.7) add container-level crash detection; the chunk seqlock still
governs reads of an open chunk.

#### 3.9.4 Chunk truncation and recovery

A decoder SHALL on structural failure seek forward within the stream for the
next recognized magic (`FLR\0` or `VIRL`), then re-attach at that chunk. In a
container written one chunk per record, the next record boundary is the
preferred re-attach point. The chunk whose header declared
size is absent (final chunk never finalized) ends at stream end; its
content up to the last parseable event SHALL be consumed; pool/state
incompleteness follows §5's snapshot rules within the truncated range.
Sync points (§4.4) delimit the durability horizon a consumer may rely on
when the tail is torn.

#### 3.9.5 Foreign chunks — resilience to JFR evolution (normative)

Within a `virel` or `jfr` stream, a chunk whose (major, minor) the decoder
does not implement (e.g. a future OpenJDK JFR major 3, or an annex minor newer
than the decoder knows) is FOREIGN. The container is unaffected: the stream
stays extractable byte-exactly (§3.1). For the decoder:

1. The reader MUST NOT interpret, guess at, or parse it by v1/v2 (or any
   other) rules.
2. The reader bridges over it: locate the next chunk boundary by scanning
   for `FLR\0` or `VIRL` from the current position (§3.9.4 recovery semantics).
   A candidate is accepted only if its magic and signature-state identify an
   implemented class: `VIRL` + signature SET + implemented native major, or
   `FLR\0` + signature CLEAR + implemented annex (1/2) — otherwise keep
   scanning. Either 4-byte magic can occur inside foreign payloads; the
   signature bit is a header-octet guard read from the candidate header's
   flags.
3. The bridge is transparent to consumers (the chunk stream stays
   traversable); diagnostic layers SHOULD report foreign spans rather than
   silently dropping them.

This gives the decoder STRUCTURAL resilience to any JFR evolution — a future JFR major
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

### 4.3 Ordering

In major 0, event order per writer = chunk order = append order; cross-
writer order is NOT chronological by default (matches Go's trace, JFR's
FastUnordered ticks). Every event carries real time words. The former
"timebase-neutral records with filler time words" rule, which existed only
for aggregated pprof samples, was withdrawn on 2026-10-04 with the
event-oriented scope (§1 goal 0) [find-event-scope].

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
- **Type-id bindings are stream-global and ADD-ONLY**: a binding made in
  any chunk holds for the whole `virel` stream (before 2026-10-04: the whole
  container); bindings are never re-bound;
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
| [2³⁰, 2⁶²) | DYNAMIC: writer-assigned, per stream, add-only |
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
| 2 | `exec-trace` | execution-tracing vocabulary: task/executor (scheduling states, Go P/G model), slices (duration events; nesting = track+time convention), instants, counters (+ unit/temporality descriptors), STATE tracks, timestamped stack samples, per-sequence numbers [find-exec-trace-vocabulary] |
| 3+ | ledger | append-only additions (spec-adjacent registry file, community-tracked) |

---

## 6. Profiles and conversion

### 6.1 Profile roster

Every profile has two forms. **Embedded**: the original bytes as a stream of
their own format id (§3.4.1). This is byte-exact by the reconstruction rule
(§3.1) and needs no converter. **Converted**: a `virel` stream produced by
the profile's converter. A container MAY carry both, linked by `derived-from`.
Decoding the embedded form is an OPTIONAL payload decoder (plugin, §3.1);
only `standard` (`virel`) decoding is mandatory for readers. A converted
stream is readable by every conforming reader.

| profile | format id | major | converted form | conformance |
|---|---|---|---|---|
| `jfr-legacy` | `jfr` | 1.x/2.x | none needed: annex chunks decode directly (§3.9) | round-trip byte equality + JDK-reader parity suite |
| `standard` | `virel` | **0 draft → 1 stable** (signature set) | — (native) | writer/reader suites |
| `go-trace` | `go-trace` | 0 | lossless representation (converter) | round-trip: Go gen → VIREL chunk → reconstruct Go trace |
| `perfetto` | `perfetto` | 0 | lossless representation | round-trip TracePacket stream |

### 6.2 Conversion provenance (standardized) [find-conversion-provenance]

Declared metadata type `provenance` (module m0), emitted per chunk. When the
source is embedded in the same container, the converted stream's descriptor
SHOULD also name it with `derived-from` (§3.4):

- `source_format` — STRING-first ("go-trace", "perfetto-pb",
  "linux", "etw", "ctef", "v8-cpuprofile", …; container format ids, §3.4.1,
  SHOULD be used where one exists); no registry required; optional
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
- When the converted stream's source is embedded in the same container
  (`derived-from`, §3.4), converters MAY omit fragments: the embedded stream
  is already the complete original.

### 6.4 Reference converters = validators

The round-trip suites and the shipped reference converters are the SAME
artifacts [find-converter-cost]: Go converter buffers one generation
(source format's own granularity, ~16 MiB ceiling); Perfetto converter
streams 1:1 via mid-chunk checkpoints. Tier B (CTEF/cpuprofile, perf.data, ftrace, CTF, ETW):
shape-fold converters + CTEF emitter first (adoption wedge).

### 6.5 Fidelity classes and conformance ladder [find-migration-strategy]

Migration ease from currently-popular formats is THE product criterion;
the conformance ladder makes it testable:

- **Tier A — lossless round-trip suites (v1.0 REQUIRED for claimed
  profiles)**: JFR annex (byte-frozen; reader-implies-writer), Go
  tracev2 (per-(generation, M) chunking, exact recon: kinds/abs
  ticks/args/pools — ev-go-gate-3), Perfetto protobuf (pairs + folding
  = read-side — ev-validator-pfetto). The former pprof/OTLP-profiles suite
  (gate 4) is no longer a profile suite; it stays as a wire regression test
  for pooled references, Varint128 and pool skipping [ev-pprof-gate-4,
  find-event-scope].
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

- Container [find-agnostic-container]: container major 0 = this draft;
  readers reject unknown container majors; minors are additive. The record
  kind registry, descriptor keys and format ids are append-only. A stream
  with an unknown format id is carried and extracted byte-exactly, never
  decoded.
- Majors [find-version-resilience]: native VIREL majors = signature-set
  chunks: **0 = draft (this spec); 1 = first stable (graduation upon
  ratification)**; later native majors = advertised spec events, additive
  preferred. JFR annex = majors 1/2, signature clear — frozen forever.
  Within a chunk stream, any unknown (major, minor) = foreign chunk
  (§3.9.5), bridged, never
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
- **Record payload compression**: a record flag plus codec id, so the
  container stays traversable while payloads are compressed. Until then,
  `content-encoding` (§3.4) covers whole-stream encodings.
- **Cross-stream references**: native events referring to entities
  (threads, tracks, stacks) inside another stream. Out of scope for v1.0.
- **Machine federation**: resource-level declared types + external clock
  sync packets.
- **Per-writer sub-regions**: re-open topic if converter evidence shows
  demand (currently ruled out).

## 9. Honest limits (v1.0)

- No native map/kv-associative type (composite arrays of kv pairs suffice —
  JFR/OTLP both model it this way in practice).
- One nanosecond wall axis + one tick axis per chunk (multi-clock = §8).
- v2 annex event ticks are NON-monotonic by JVM design (§4.2); consumers of
  annex chunks must not assume intra-chunk order (verified TSC behavior).
- Container record time ranges and clock anchors are supplied by the writer
  (§3.3, §3.4.2). The container does not verify them against payloads.
- A stream is byte-exact only when complete (§3.7). Recovery reports gaps; it
  does not fill them.

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
`find-design-deltas`, `find-agnostic-container`, `q-composite-container`,
`ev-go-split-clock`, `ev-perfetto-split-clock`, `ev-node-split-clock`,
`find-event-scope`.

External primary sources: JDK sources in-repo (paths in node frontmatter);
Go runtime trace sources + design doc 60773; Perfetto trace-packet proto;
OTel profiles.proto v1development + Profiles data-format docs; async-
profiler OutputFormats/ProfilerOptions/JfrVisualization docs.

---

## 11. Machine-readable artifacts shipped with the spec [find-idl-codegen]

- `tcf-framing.ksy` — Kaitai Struct description of the container records and
  of the `virel`/`jfr` chunk header layer (SHIPPED, doc/tcf-framing.ksy;
  recovery scanning and the foreign-bridge algorithm stay normative prose
  §3.7/§3.9.5 + reference reader by design). The container part is a DRAFT:
  no validator cross-checks it yet.
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
module ledger (§5.5), writer-id=0 semantics (§3.9.2), VIREL-signature bit
@66 bit 7 + encoding-id cap 0..127 (§3.9.2), encoding id 5 = avro
(ratified 2026-10-03). Container numerics (§3: signature, record
layout, record kinds, descriptor keys, format ids, manifest) are PROPOSED
(2026-10-04), not ratified.*
