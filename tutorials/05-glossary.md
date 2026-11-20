# Tutorial 5 — glossary

One line per term, defined the way the specification uses it. Section
references point into [../doc/TraceContainerFormat.md](../doc/TraceContainerFormat.md).

## File kinds

| Term | Meaning |
|---|---|
| **VIREL** | the format family: one native event stream format plus one format-agnostic container (§1) |
| **VIRL file** | one native event stream — a concatenation of native chunks; concatenable and streamable (§3.1, §3.9) |
| **VIRC file** | the container: a 32-byte header followed by records framing streams of any payload format (§3.2) |
| **sniffing** | identifying a file kind from its leading bytes: `VIRC\r\n\x1a\n`, `VIRL`, or a legacy signature (§3.8) |

## Chunk layer (native streams)

| Term | Meaning |
|---|---|
| **chunk** | a self-contained partition of an event stream: header + events + pools; readers attach at any chunk boundary (§3.9) |
| **native chunk** | a chunk with magic `VIRL`, the VIREL signature bit set, and a native major (§3.9.2) |
| **annex chunk** | a byte-frozen JFR chunk (magic `FLR\0`, majors 1/2) — carried as a payload format, not re-encoded (§3.9.1) |
| **foreign chunk** | a chunk whose version a decoder does not implement; bridged over, never interpreted (§3.9.5) |
| **chunk header** | the 76-byte native (or 68-byte annex) fixed header: version, size, clock anchor, writer id, state, flags |
| **signature bit** | flags bit @66 bit 7: set in every native chunk, always clear in JFR chunks — the class guard (§3.9.2) |
| **writer id** | u64 random identity at header offset 68; one writer per chunk; makes cross-writer concatenation safe (§3.9.2) |
| **seqlock** | the live-write protocol on the header state byte (0xff = UPDATING, 0x00 = COMPLETE): read, read fields, re-read, accept only on equal states (§3.9.3) |
| **finalized chunk** | a chunk whose header size field is valid; readers skip it in one jump (§3.9.6) |
| **streaming chunk** | a chunk written once with size 0 and the STREAMING flag; its extent is found by walking events to END_OF_PARTITION (§3.9.6) |
| **finalization** | turning a streaming chunk into a finalized one in place: write the posthoc header fields, clear STREAMING last; event bytes unchanged (§3.9.6) |

## Event and schema layer

| Term | Meaning |
|---|---|
| **event** | one timestamped record: `[size:varint][typeId:varint][time words][fields]`; size counts its own width (§4) |
| **type id** | the numeric identity of an event type; stream-global, add-only — a binding is never re-bound (§5.3) |
| **metadata event** | the in-stream schema: declares event types, fields and poolable types; the first event of a chunk (§5.2) |
| **state snapshot** | bootstrap type 1: carries pool data and writer state; optionally leads with a cumulative type summary (§4.4) |
| **pool** | a chunk-scoped intern table (strings, stacks, ...); events refer to entries by 4-byte key, 0 = null (§5.4) |
| **sync point** | a registered event kind that marks recoverable state (state snapshots, END_OF_PARTITION); the durability horizon for truncation recovery (§4.4) |
| **varint** | LEB128, at most 9 bytes; the integer encoding of the whole content layer (§2) |
| **padded varint** | a varint written at fixed width with zero-bit padding, so it can be back-patched (§2) |
| **back-patch** | rewriting a length field in place once its final value is known (chunk size, event count) (§2) |

## Container layer

| Term | Meaning |
|---|---|
| **record** | one framed container unit: fixed header (kind, stream id, sequence, CRCs, state) + payload (§3.3) |
| **stream** | one byte stream in one payload format, reconstructed by concatenating its DATA records in sequence order — byte-exactly (§3.1) |
| **payload format** | what a stream's bytes are: `virel`, `jfr`, `go-trace`, `perfetto`, `otlp-traces`, ... (§3.4.1) |
| **payload decoder** | an optional plugin that decodes a non-native stream into the VIREL model; only `virel` decoding is mandatory (§3.1) |
| **stream descriptor** | the STREAM record's key/value list: `format`, `format-version`, `content-encoding`, `clock-anchor`, `record-framing`, ... (§3.4) |
| **record-framing** | descriptor key: `stream` (records concatenate into one byte stream, default) or `message` (each DATA record is one whole document — OTLP) (§3.4) |
| **manifest** | an optional accelerating index of streams and DATA records; records win when they disagree (§3.5) |
| **PREAMBLE / RESYNC** | record flags marking where a decoder must prepend a stream's preamble and where it may start mid-stream (§3.3) |
| **clock anchor** | a binding between a payload's tick clock and UNIX epoch ns, with uncertainty; how streams align (§3.4.2) |

## Ecosystem

| Term | Meaning |
|---|---|
| **CTEF** | Chrome Trace Event Format — the JSON trace format of Chrome DevTools and Node.js; a Tier B conversion profile (§6) |
| **conformance ladder** | Tier A = lossless round-trip suites (jfr, go-trace, perfetto); Tier B = documented shape transform (§6.5) |
| **gate** | one of the seven verification programs in `validators/` — see [../AGENTS.md](../AGENTS.md) for the matrix |
