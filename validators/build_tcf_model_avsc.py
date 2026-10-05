#!/usr/bin/env python3
"""build_tcf_model_avsc.py — regenerates doc/tcf-model.avsc from python structure (guaranteed valid)."""
import json, sys
from pathlib import Path

# ---------------- base definitions (dependency order) ----------------
nfv_named = {
    "type": "record", "name": "NamedFieldValue",
    "fields": [{"name": "name", "type": "string"},
               {"name": "value", "type": "virel.model.FieldValue"}]}
FieldValue = {
    "type": "record", "name": "FieldValue",
    "doc": "Tagged union of all content-layer values; union order = the content-layer kind register: 0 null, 1 long (varint raw pattern), 2 double, 3 string UTF-8 inline, 4 PoolRef (strings: POOL-REF tag / objects: bare u32 key), 5 char-array (varint code units), 6 bool (raw byte), 7 bytes (BYTES tag, major 0), 8 Composite (values by field name), 9 Array (constantPool array items = PoolRef).",
    "fields": [{
        "name": "v", "type": [
            "null",
            "long",
            "double",
            "string",
            {"type": "record", "name": "PoolRef", "fields": [
                {"name": "typeId", "type": "long", "doc": "the pooled type (the string type for tag-2 refs)"},
                {"name": "key", "type": "long"}]},
            {"type": "record", "name": "CharArray", "fields": [
                {"name": "units", "type": {"type": "array", "items": "int"},
                 "doc": "code units; readChar = varint"}]},
            "boolean",
            "bytes",
            {"type": "record", "name": "Composite", "fields": [
                {"name": "fields", "type": {"type": "array", "items": nfv_named}}]},
            {"type": "record", "name": "Array", "fields": [
                {"name": "items", "type": {"type": "array", "items": "virel.model.FieldValue"}}]}
        ]}]
}


header = {
    "type": "record", "name": "Header",
    "doc": "byte-positioned chunk header (Kaitai-expressible; the .ksy artifact checks this against binaries). Annex: 68 B, magic=FLR\\0, majors 1/2, BYTE-FROZEN (JFR). Native: magic=VIRL, same offsets 4..67, majors 0 (draft) and 1 (stable). Flags @66 bit 7 (0x80) = VIREL-NATIVE signature (set for VIRL, always clear in the FLR annex - JFR v2 writes @66=0 and never reads byte 66, verified); bits 0..6 = metadata-default encoding id (0..127); writerId at offset 68 = u64 random (0 = writer-unspecified). Annex low flags byte @67: bit 0 = varint regime, bit 1 = FINAL-CHUNK. State @64: 0=COMPLETE, 255=UPDATING (seqlock double-read before trusting posthoc fields).",
    "fields": [
        {"name": "magic", "type": "bytes", "doc": "FLR\\0 for the JFR annex; VIRL for native chunks"},
        {"name": "major", "type": "int"},
        {"name": "minor", "type": "int"},
        {"name": "chunkSize", "type": "long", "doc": "back-patched at finalize (annex initial = 68); 0 in a native streaming chunk (flags @67 bit 2 STREAMING), whose extent ends at its END_OF_PARTITION event (type id 8)"},
        {"name": "stateSnapshotOffset", "type": "long", "doc": "chunk-relative; JFR annex = LAST checkpoint, native = LAST state snapshot; delta walks backward; 0 = none"},
        {"name": "metadataOffset", "type": "long", "doc": "chunk-relative; MAY move mid-chunk"},
        {"name": "startNanos", "type": "long", "doc": "UTC ns; written at open; stable"},
        {"name": "durationNanos", "type": "long", "doc": "posthoc at finalize; 0 while live and in streaming chunks"},
        {"name": "startTicks", "type": "long", "doc": "anchor: annex event ticks ABSOLUTE (FastUnordered, non-monotonic by design); native event ticks CHUNK-RELATIVE, per-writer non-decreasing"},
        {"name": "ticksPerSecond", "type": "long"},
        {"name": "state", "type": "int", "doc": "0 = COMPLETE, 255 = UPDATING"},
        {"name": "flags", "type": "int"},
        {"name": "writerId", "type": ["null", "long"], "default": None, "doc": "native only (76-byte header); null for annex"}
    ]
}

attr = {
    "type": "record", "name": "Attribute",
    "fields": [{"name": "name", "type": "int"}, {"name": "value", "type": "int"}]
}

element = {
    "type": "record", "name": "Element",
    "doc": "binarized-XML element tree (xml-v2 grammar). BARE pool refs, no tags.",
    "fields": [
        {"name": "name", "type": "int"},
        {"name": "attributes", "type": {"type": "array", "items": "virel.model.Attribute"}},
        {"name": "children", "type": {"type": "array", "items": "virel.model.Element"}}
    ]
}

entry = {
    "type": "record", "name": "Entry",
    "doc": "key: u32 in the wire, 0 = null; value: FULL payload always written (dedup = reader-level only).",
    "fields": [{"name": "key", "type": "long"}, {"name": "value", "type": "virel.model.FieldValue"}]
}

pool = {
    "type": "record", "name": "Pool",
    "fields": [
        {"name": "typeId", "type": "long"},
        {"name": "entries", "type": {"type": "array", "items": "virel.model.Entry"}}]
}

state_snapshot = {
    "type": "record", "name": "StateSnapshot",
    "doc": "[typeId=1][startTime][duration][delta][snapshotFlags:u1] then, if native snapshotFlags bit 7 TYPE_SUMMARY_PRESENT is set, [summaryVersion=1][summaryTypeCount][typeId,emittedCount]* before [poolCount][pools...]. Bit 0 is FLUSH; all other flag bits are zero. The summary is cumulative within the chunk, sorted by type id, and excludes bootstrap ids 0/1. Pool validity = THIS chunk (writer-scoped); delta = previous state snapshot offset (backward chain; 0 = first).",
    "fields": [
        {"name": "startTime", "type": "long"},
        {"name": "duration", "type": "long"},
        {"name": "delta", "type": "long"},
        {"name": "flush", "type": "boolean"},
        {"name": "summaryVersion", "type": "int", "default": 0},
        {"name": "hasTypeSummary", "type": "boolean", "default": False},
        {"name": "typeSummary", "type": {"type": "array", "items": {
            "type": "record", "name": "TypeCount",
            "fields": [
                {"name": "typeId", "type": "long"},
                {"name": "emittedCount", "type": "long"}
            ]}}, "default": []},
        {"name": "pools", "type": {"type": "array", "items": "virel.model.Pool"}}
    ]
}

metadata = {
    "type": "record", "name": "Metadata",
    "doc": "annex preamble = [startTime][duration][metadataId]; native adds the envelope: [encodingId varint][payload]. Encoding-0 payload = [poolSize][strings][root element]. The root carries 'metadata' and 'region' children; the XML element for a type is literally 'class'; a field's linked type id rides in the 'class' attribute; event types declare startTime, optionally duration, then eventThread/stackTrace (constantPool attr) and user fields - no hidden event words (validated in gate 1).",
    "fields": [
        {"name": "startTime", "type": "long"},
        {"name": "duration", "type": "long"},
        {"name": "metadataId", "type": "long", "doc": "reader-caching affordance only"},
        {"name": "encodingId", "type": "int", "doc": "annex: implicitly 0"},
        {"name": "stringPool", "type": {"type": "array", "items": ["null", "string"]}},
        {"name": "root", "type": "virel.model.Element"}
    ]
}

event = {
    "type": "record", "name": "Event",
    "doc": "wire = [sizeInclOwnWidth][typeId][startTime][durationTicks iff 'duration' declared][fields in declared order]; unknown types ride GenericDyn: body FieldValues against the chunk's metadata; skip-by-size for foreign/unknown bodies is a native-wire property (retained there, not in Avro renders).",
    "fields": [
        {"name": "typeId", "type": "long"},
        {"name": "startTicks", "type": ["null", "long"], "default": None,
         "doc": "null for bridge-internal uses without time words"},
        {"name": "durationTicks", "type": ["null", "long"], "default": None},
        {"name": "values", "type": {"type": "array", "items": "virel.model.NamedFieldValue"}, "default": []}
    ]
}

# ---------------- container layer (spec §3, format-agnostic; numerics PROPOSED 2026-10-04) ----------------
container_header = {
    "type": "record", "name": "ContainerHeader",
    "doc": "32-byte container header (spec §3.2): signature VIRC\\r\\n\\x1a\\n, major/minor, header size, flags, container id, CRC-32C as the last 4 header bytes. Readers reject unknown majors; minors are additive.",
    "fields": [
        {"name": "major", "type": "int", "doc": "0 = draft"},
        {"name": "minor", "type": "int"},
        {"name": "headerSize", "type": "int", "doc": "32 in major 0"},
        {"name": "flags", "type": "int"},
        {"name": "containerId", "type": "long", "doc": "random u64; 0 = unspecified"}
    ]
}

record_header = {
    "type": "record", "name": "RecordHeader",
    "doc": "56-byte record header (spec §3.3). Static part (magic VREC, kind, header size, stream id, sequence, static CRC-32C over bytes 0..15) is written once at open; dynamic part (state, flags, length, time range, payload CRC, dynamic CRC-32C over bytes 20..51) is final once CLOSED. At most one record is OPEN and it is the last record; its payload runs to end of file.",
    "fields": [
        {"name": "offset", "type": "long", "doc": "file offset of the record (model only)"},
        {"name": "kind", "type": {"type": "enum", "name": "RecordKind",
            "doc": "append-only registry: PAD (ignored bytes), STREAM (descriptor), DATA (next stream bytes), MANIFEST (summary + index), END (64-byte trailer with the manifest offset). Unknown kinds are skipped by length.",
            "symbols": ["PAD", "STREAM", "DATA", "MANIFEST", "END"]}},
        {"name": "headerSize", "type": "int", "doc": "56 in major 0"},
        {"name": "streamId", "type": "long", "doc": "u32; 0 = container-level"},
        {"name": "sequence", "type": "long", "doc": "DATA: 0,1,2,... per stream in file order; other kinds 0"},
        {"name": "open", "type": "boolean", "doc": "state byte 0xFF = OPEN, 0x00 = CLOSED"},
        {"name": "streamEnd", "type": "boolean", "doc": "flags bit 0: last DATA record of its stream"},
        {"name": "preamble", "type": "boolean", "default": False, "doc": "flags bit 2: holds the format's stream preamble, prepended by a decoder that starts at a RESYNC record (writer-asserted)"},
        {"name": "resync", "type": "boolean", "default": False, "doc": "flags bit 3: a decoder may start here after the PREAMBLE records, then reads every later record in order (writer-asserted; spec §3.4.3 per format)"},
        {"name": "payloadLength", "type": ["null", "long"], "default": None, "doc": "null while OPEN (all ones on the wire)"},
        {"name": "timeStartNanos", "type": "long", "doc": "UNIX epoch ns, writer-supplied, advisory; 0 = unknown"},
        {"name": "timeEndNanos", "type": "long"},
        {"name": "payloadCrc", "type": ["null", "long"], "default": None, "doc": "CRC-32C when flags bit 1 PAYLOAD_CRC is set"}
    ]
}

clock_anchor = {
    "type": "record", "name": "ClockAnchor",
    "doc": "descriptor key clock-anchor (spec §3.4.2): when the payload clock read ticks (units of 1/frequency s), the container timeline (UNIX epoch ns) read unixNanos, +/- uncertaintyNanos. Payload-internal anchors (virel/jfr chunk headers) stay authoritative for decoding.",
    "fields": [
        {"name": "clock", "type": "string", "doc": "registered: virel-ticks, jfr-ticks, go-ticks, perfetto:<builtin clock id>, v8-timeticks, uv-hrtime (distinct clocks on macOS)"},
        {"name": "ticks", "type": "long"},
        {"name": "frequency", "type": "long"},
        {"name": "unixNanos", "type": "long"},
        {"name": "uncertaintyNanos", "type": "long"}
    ]
}

descriptor = {
    "type": "record", "name": "StreamDescriptor",
    "doc": "merged STREAM-record key/value list (spec §3.4); keys are add-only across STREAM records of a stream. Typed fields mirror the registered keys; 'entries' keeps every key verbatim, including unknown and x- keys.",
    "fields": [
        {"name": "format", "type": "string", "doc": "REQUIRED format id (registry: virel, jfr, go-trace, perfetto, otlp-traces, otlp-logs, otlp-traces-json, otlp-logs-json, ctef, v8-cpuprofile, v8-log, x-...)"},
        {"name": "formatVersion", "type": ["null", "string"], "default": None},
        {"name": "contentEncoding", "type": "string", "default": "identity", "doc": "identity, gzip or zstd"},
        {"name": "recordFraming", "type": {"type": "enum", "name": "RecordFraming",
            "doc": "STREAM (default): DATA records concatenate into one byte stream. MESSAGE: each DATA record is one complete document and the stream extracts as the document sequence (OTLP export requests). Fixed in the first STREAM record.",
            "symbols": ["STREAM", "MESSAGE"]}, "default": "STREAM"},
        {"name": "source", "type": ["null", "string"], "default": None},
        {"name": "producer", "type": ["null", "string"], "default": None},
        {"name": "writerId", "type": ["null", "long"], "default": None},
        {"name": "clockAnchors", "type": {"type": "array", "items": clock_anchor}, "default": []},
        {"name": "derivedFrom", "type": {"type": "array", "items": "long"}, "default": [], "doc": "source stream ids of a converted stream"},
        {"name": "inputTruncated", "type": "boolean", "default": False},
        {"name": "label", "type": ["null", "string"], "default": None},
        {"name": "entries", "type": {"type": "array", "items": {
            "type": "record", "name": "DescriptorEntry",
            "fields": [{"name": "key", "type": "string"}, {"name": "value", "type": "bytes"}]}}, "default": []}
    ]
}

manifest = {
    "type": "record", "name": "Manifest",
    "doc": "MANIFEST record payload (spec §3.5), immediately before END; an accelerator that MUST agree with the records (records win on disagreement).",
    "fields": [
        {"name": "version", "type": "int", "default": 1},
        {"name": "streams", "type": {"type": "array", "items": {
            "type": "record", "name": "ManifestStream",
            "fields": [
                {"name": "streamId", "type": "long"},
                {"name": "firstStreamRecord", "type": "long", "doc": "file offset"},
                {"name": "dataRecords", "type": "long"},
                {"name": "payloadBytes", "type": "long"},
                {"name": "timeStartNanos", "type": "long"},
                {"name": "timeEndNanos", "type": "long"},
                {"name": "ended", "type": "boolean"}]}}},
        {"name": "index", "type": {"type": "array", "items": {
            "type": "record", "name": "ManifestIndexEntry",
            "fields": [
                {"name": "recordOffset", "type": "long"},
                {"name": "streamId", "type": "long"},
                {"name": "sequence", "type": "long"},
                {"name": "timeStartNanos", "type": "long"},
                {"name": "timeEndNanos", "type": "long"},
                {"name": "flags", "type": "int", "doc": "the record's flags byte (STREAM_END, PAYLOAD_CRC, PREAMBLE, RESYNC)"}]}}, "default": []}
    ]
}

damaged_span = {
    "type": "record", "name": "DamagedSpan",
    "doc": "container recovery (spec §3.7): bytes skipped while rescanning for a valid VREC record header.",
    "fields": [{"name": "firstOffset", "type": "long"}, {"name": "nextOffset", "type": "long"}]
}

registry = {
    "type": "record", "name": "Registry",
    "doc": "spec-level registries as model records (append-only in the spec text).",
    "fields": [
        {"name": "valueModel", "type": FieldValue},
        {"name": "containerModel", "type": {
            "type": "record", "name": "ContainerModel",
            "doc": "anchored container-layer types (spec §3), referenced by name from the container root.",
            "fields": [
                {"name": "header", "type": container_header},
                {"name": "record", "type": record_header},
                {"name": "descriptor", "type": descriptor},
                {"name": "manifest", "type": manifest},
                {"name": "damagedSpan", "type": damaged_span}]}},
        {"name": "formatIds", "type": {"type": "array", "items": {
            "type": "record", "name": "FormatId",
            "doc": "append-only, string-first payload format registry (spec §3.4.1). Only 'virel' decoding is mandatory for readers; every other format is decoded, if at all, by an OPTIONAL payload decoder (plugin). Every stream is extractable byte-exactly regardless.",
            "fields": [
                {"name": "id", "type": "string", "doc": "virel, jfr, go-trace, perfetto, otlp-traces, otlp-logs, otlp-traces-json, otlp-logs-json, ctef, v8-cpuprofile, v8-log, x-..."},
                {"name": "profile", "type": ["null", "string"], "default": None, "doc": "conformance profile of its decoder (spec §6.1)"},
                {"name": "decodeMandatory", "type": "boolean", "default": False, "doc": "true only for virel"}]}}},
        {"name": "wireModel", "type": {
            "type": "record", "name": "WireModel",
            "doc": "anchored wire-layer types (dependency order; referenced by name from the chunk tree).",
            "fields": [
                {"name": "header", "type": header},
                {"name": "attribute", "type": attr},
                {"name": "element", "type": element},
                {"name": "entry", "type": entry},
                {"name": "pool", "type": pool},
                {"name": "stateSnapshot", "type": state_snapshot},
                {"name": "metadata", "type": metadata},
                {"name": "event", "type": event},
                {"name": "namedFieldValue", "type": "virel.model.NamedFieldValue"}]}},
        {"name": "typeIdSpace", "type": {
            "type": "record", "name": "TypeIdSpace",
            "doc": "Bindings: stream-global (per virel stream; container-global before 2026-10-04), ADD-ONLY, never re-bound; redefinition = violation. 0=metadata event, 1=state snapshot (JFR checkpoint; bootstrap, fixed in ALL majors; v2-verified). [2,2^30): module-registered, ledger-assigned (moduleId u12)<<18|(local u18) = 4096 modules x 262144 types. [2^30,2^62): dynamic, writer-assigned per stream, add-only. [2^62,2^64): system/reserved. Snapshot completeness: normative, no omissions (mid-chunk state snapshots rebuild from snapshots, not diffs).",
            "fields": [
                {"name": "moduleIdBits", "type": "int", "default": 12},
                {"name": "localIdBits", "type": "int", "default": 18}]}},
        {"name": "moduleLedger", "type": {"type": "array", "items": {
            "type": "record", "name": "Module",
            "doc": "append-only module registry: 0 framing (sync kinds, provenance, attachment-fragment); 1 jfr-core; 2 exec-trace (Go/Perfetto-modeled execution vocabulary: task/executor states, slices, instants, counters + units, STATE tracks, timestamped stack samples, per-sequence numbers); 3+ community ledger.",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}]}}},
        {"name": "syncKinds", "type": {"type": "array", "items": {
            "type": "enum", "name": "SyncKind",
            "doc": "registered sync-point kinds (module m0; numerics ratified): STATE_SNAPSHOT = typeId 1 (pools/state plus an optional native cumulative type summary; JFR alias CHECKPOINT); CLOCK = multi-clock extension table (clock-id, value, frequency) + per-event clock ref = declared field (extension, not v1.0); LOSS = ring-buffer lost ranges (Perfetto previous_packet_dropped analog); CLEAR = incremental-state-cleared (SEQ_INCREMENTAL_STATE_CLEARED analog); END_OF_PARTITION = Go EndOfGeneration / finalize marker. Cadence: writers SHOULD write state snapshots within a durability window (<= 1s event time or <= a few MB).",
            "symbols": ["STATE_SNAPSHOT", "CLOCK", "LOSS", "CLEAR", "END_OF_PARTITION"]}}},
        {"name": "metadataEncodings", "type": {"type": "array", "items": {
            "type": "record", "name": "Encoding",
            "doc": "envelope encoding registry (append-only; envelope tag authoritative; header @66 bits0..6 = advisory default; within-chunk changes permit-but-discourage): 0=xml-v2 (byte-frozen annex grammar; ALSO legal for native chunks - verified in gate 2), 1=flat, 2=protobuf, 3=CBOR, 4=JSON, 5=avro. All express the SAME logical model.",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}]}}},
        {"name": "provenance", "type": {
            "type": "record", "name": "Provenance",
            "doc": "standardized converted-stream provenance (declared metadata type, module m0; emitted per chunk) - ratified 2026-10-03.",
            "fields": [
                {"name": "sourceFormat", "type": "string", "doc": "STRING-first ('go-trace','perfetto-pb','linux','etw','ctef','v8-cpuprofile',...); no registry required; optional well-known ids later, additive"},
                {"name": "sourceFormatVersion", "type": "string"},
                {"name": "converter", "type": "string", "doc": "converter identity/version"},
                {"name": "fidelity", "type": {"type": "enum", "name": "Fidelity",
                    "doc": "LOSSY_PARTIAL_ATTACHMENT = the standard lossy mode (fragments only); FULL discouraged (tiny sources)",
                    "symbols": ["LOSSLESS", "LOSSY_PARTIAL_ATTACHMENT", "LOSSY_FULL_ATTACHMENT", "LOSSY_NO_ATTACHMENT"]}},
                {"name": "originalPayloadHash", "type": "long", "doc": "u64 over attached fragment bytes; 0 = none"}]}},
        {"name": "attachmentFragment", "type": {
            "type": "record", "name": "AttachmentFragment",
            "doc": "'only the non-recoded parts' (ratified): one per lossy element; fragment = ORIGINAL encoding untouched; attachment budget SHOULD stay a small fraction of stream size; consumers MUST NOT auto-interpret attachment bytes.",
            "fields": [
                {"name": "sourceLocator", "type": "long", "doc": "u64 offset (source file/batch/message)"},
                {"name": "coordString", "type": ["null", "string"], "default": None,
                 "doc": "per-format coordinate (perf.data feature offset, CTF type ref, CTEF object path, ETW buffer/record offset) documented with each Tier-B converter"},
                {"name": "reason", "type": {"type": "enum", "name": "FragmentReason",
                    "symbols": ["EXTENSION_UNMAPPED", "SEMANTICS_UNMAPPED", "TYPE_UNREPRESENTABLE"]}},
                {"name": "fragment", "type": "bytes"}]}},
        {"name": "conversion", "type": {
            "type": "record", "name": "Conversion",
            "doc": "converter = validator (same artifact, ratified). Go = bounded per-generation (~16 MiB ceiling, the source's own granularity); Perfetto = streaming 1:1 via mid-chunk checkpoints; Tier B = shape-folds (CTEF/cpuprofile, perf.data, ftrace, CTF, ETW) + CTEF emitter first (adoption wedge). SLICE_BEGIN/END stay PAIRS (a streaming writer cannot back-patch a duration into a written position; folding = read-side, as CTEF B/E).",
            "fields": [
                {"name": "tier", "type": {"type": "enum", "name": "Tier",
                    "symbols": ["A_LOSSLESS_ROUNDTRIP", "B_SHAPE_FOLD"]}},
                {"name": "sourceFormat", "type": "string"}]}},
        {"name": "foreignChunk", "type": {
            "type": "record", "name": "ForeignChunk",
            "doc": "inside virel/jfr chunk streams only (spec §3.9.5; the container never parses payloads): bridged, never interpreted: skip via FLR or VIRL magic re-scan; candidate accepted only for an implemented (magic, signature, major, minor) class; diagnostic layers SHOULD report foreign spans; support for future formats = pure additive spec revision.",
            "fields": [
                {"name": "firstOffset", "type": "long"},
                {"name": "nextOffset", "type": "long"}]}}
    ]
}

chunk = {
    "type": "record", "name": "Chunk",
    "doc": "self-contained partition; chunk = ONE writer (one-writer-per-chunk, ratified).",
    "fields": [
        {"name": "index", "type": "int"},
        {"name": "class", "type": {"type": "enum", "name": "ChunkClass",
            "doc": "annex = byte-frozen JFR FLR majors 1/2 (signature clear); native = VIREL VIRL 0 (draft) / 1 (stable) (signature set); foreign = NOT parsed, bridged (virel.model.ForeignChunk).",
            "symbols": ["ANNEX", "NATIVE", "FOREIGN"]}, "default": "NATIVE"},
        {"name": "major", "type": "int"},
        {"name": "minor", "type": "int"},
        {"name": "header", "type": "virel.model.Header"},
        {"name": "metadata", "type": ["null", "virel.model.Metadata"], "default": None},
        {"name": "stateSnapshots", "type": {"type": "array", "items": "virel.model.StateSnapshot"}, "default": []},
        {"name": "events", "type": {"type": "array", "items": "virel.model.Event"}, "default": []},
        {"name": "foreignSpans", "type": {"type": "array", "items": "virel.model.ForeignChunk"}, "default": []}
    ]
}

stream = {
    "type": "record", "name": "Stream",
    "doc": "one framed stream (spec §3.1): its bytes = the concatenated DATA payloads in sequence order (byte-exact when complete). 'chunks' holds the decoded VIREL-model content: native for virel, or produced by an optional payload decoder (plugin) for other formats; null when not decoded.",
    "fields": [
        {"name": "streamId", "type": "long"},
        {"name": "descriptor", "type": "virel.model.StreamDescriptor"},
        {"name": "records", "type": {"type": "array", "items": "virel.model.RecordHeader"}, "doc": "DATA records in sequence order"},
        {"name": "complete", "type": "boolean", "doc": "sequences 0..n without gaps and STREAM_END on n"},
        {"name": "gaps", "type": {"type": "array", "items": "long"}, "default": [], "doc": "missing DATA sequence numbers"},
        {"name": "decoder", "type": ["null", "string"], "default": None, "doc": "payload decoder used (format id / plugin name); null = not decoded"},
        {"name": "chunks", "type": ["null", {"type": "array", "items": chunk}], "default": None}
    ]
}

container = {
    "type": "record", "name": "Container", "namespace": "virel.model",
    "doc": "VIREL protocol model, FULL: a format-agnostic container of framed streams (spec §3, 2026-10-04) whose virel streams carry chunks. TCF protocol model, FULL. Ratified OPTION C (2026-10-03, find-avro-full-model): machine-readable model artifact (documentation + validator model-lint + codegen source). The NATIVE wire (TraceContainerFormat.md) is the sole normative byte encoding; an Avro-wire profile is deliberately NOT registered (an Avro container cannot express per-event sizes/skip, live seqlock, chunk resync/foreign bridging, mid-stream type introduction). The reference bridge library carries native<->model duty; fidelity = its test suite, not this text. Named types defined at first use, referenced by name after (backward-only resolution).",
    "fields": [
        {"name": "registry", "type": registry},
        {"name": "header", "type": "virel.model.ContainerHeader"},
        {"name": "streams", "type": {"type": "array", "items": stream}},
        {"name": "manifest", "type": ["null", "virel.model.Manifest"], "default": None},
        {"name": "cleanlyClosed", "type": "boolean", "doc": "valid END record present"},
        {"name": "implicit", "type": "boolean", "default": False, "doc": "opened from a VIRL file (or a bare legacy file) as an implicit one-stream container (spec §3.8)"},
        {"name": "damagedSpans", "type": {"type": "array", "items": "virel.model.DamagedSpan"}, "default": []}
    ]
}

out = json.dumps(container, indent=2, ensure_ascii=False)
path = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).resolve().parent.parent / "doc" / "tcf-model.avsc")
open(path, "w").write(out + "\n")
doc = json.load(open(path))

seen, missing = set(), []
def walk(node):
    if isinstance(node, dict):
        nm = node.get("name")
        if isinstance(nm, str) and nm[:1].isupper() and isinstance(node.get("type"), (dict, str)):
            seen.add(nm)
        t = node.get("type")
        refs = [t] + (node.get("items") if isinstance(node.get("items"), list) else [node.get("items")])
        for r in refs:
            if isinstance(r, str) and r.startswith("virel.model."):
                r = r[len("virel.model."):]
            if isinstance(r, str) and r[:1].isupper() and r not in seen:
                missing.append(r)
        for v in node.values(): walk(v)
    elif isinstance(node, list):
        for v in node: walk(v)
walk(doc)
types_ = []
def collect(node, acc):
    if isinstance(node, dict):
        t = node.get("type")
        nm = node.get("name")
        if isinstance(nm, str) and nm[:1].isupper() and (t in ("record", "enum", "fixed") or (isinstance(t, dict) and t.get("type") in ("record", "enum", "fixed"))):
            acc.append(nm)
        for v in node.values(): collect(v, acc)
    elif isinstance(node, list):
        for v in node: collect(v, acc)
collect(doc, types_)
from collections import Counter
dups = [n for n, c in Counter(types_).items() if c > 1]
print(f"avsc written: {path} ({len(out.splitlines())} lines)")
print("root:", doc["name"], "| registry fields:", [f["name"] for f in registry["fields"]])
print("forward refs (must be none):", sorted(set(missing)) or "none")
print("named types:", len(types_), "| duplicates:", dups or "none")
