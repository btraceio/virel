# Tutorial 1 — the mental model

*What you will get: a working picture of what VIREL is, what a chunk is, and
why the format can carry JFR, Go traces and Perfetto without converting
them. No code required. ~10 minutes.*

## The problem

Every runtime records traces in its own binary format: the JVM writes
`.jfr`, Go writes `runtime/trace` output, Perfetto writes protobuf packets,
browsers and Node.js write Chrome trace-event JSON. The formats disagree on
framing, timestamps, and how strings and stacks are stored, so:

- every toolchain writes a reader per format;
- combining traces from a JVM, a Go service and a browser session means
  converting them — and conversion usually loses something;
- a new language runtime has no low-overhead recording format to adopt and
  must reinvent one.

VIREL attacks this from two ends at once.

## One native format, one container

VIREL defines a **native event stream** — a low-overhead binary format a
runtime can write directly — and a **container** that carries streams of
*any* format side by side.

```
VIRL file (one native stream)         VIRC file (container)
┌───────┬───────┬───────┬─────┐      ┌──────────┬───────┬───────┬───────┬────┐
│ chunk │ chunk │ chunk │ ... │      │ 32-byte  │ STREAM│ DATA  │ DATA  │... │
└───────┴───────┴───────┴─────┘      │ header   │ descr │ chunk │ chunk │    │
 concatenable, streamable,           └──────────┴───────┴───────┴───────┴────┘
 written directly by a runtime        one record frames one payload stream:
                                      virel, jfr, go-trace, perfetto, otlp-*
```

- A **VIRL file** holds one native event stream. A profiler or runtime
  writes it straight to disk or a pipe. Files concatenate: `cat a.virl
  b.virl` is a valid VIRL file, even when `a` and `b` come from different
  writers.
- A **VIRC file** is what a collector writes: a header, then records, each
  framing one stream of one payload format. The container *never parses*
  payload bytes. It indexes, timestamps and extracts streams exactly as
  they were written.

The asymmetry is deliberate and small: a conforming reader MUST read VIRL
files and `virel` streams. Reading JFR, Go traces or Perfetto is optional —
each is a plugin selected by the stream's `format` descriptor.

## A chunk is self-contained

The native stream is a concatenation of **chunks**. Each chunk carries:

1. a fixed header — magic, version, size, clock anchor, writer identity;
2. a **metadata event** — the schema: which event types exist, which fields
   they have, which types are pooled;
3. **events** — `[size][typeId][time words][fields]`, each self-sized;
4. **state snapshots** — the pool data: interned strings, stacks, the
   constant data events refer to by key.

Because the metadata and pools ride *inside* the chunk, a reader can attach
at any chunk boundary without reading anything earlier. That is what makes
chunks concatenable, truncation recoverable (you only lose the torn chunk,
not the file), and streaming possible.

## The byte-level bargain

Three mechanisms give the format its low write cost — all three are
inherited from what JFR, Go traces and Perfetto already do in production:

| Mechanism | What it buys |
|---|---|
| size-prefixed events (varint) | readers skip unknown or unwanted events without parsing them |
| chunk-local interning (pools) | a repeated string or stack costs one 4-byte reference after its first occurrence |
| padded back-patching | sizes and lengths are rewritten in place at finalize — no lookahead while writing |

Streaming mode removes the one constraint back-patching implies (seeking):
a streaming chunk is written once, never patched, and its extent is found
by walking events to a terminator. Pipes, sockets and append-only uploads
work.

## Scope, stated plainly

VIREL records **timestamped events**. That is the "E" in the name. Formats
without timestamped events — aggregated profiles (pprof), heap snapshots
(HPROF, V8 heap profiles), folded stacks — are out of scope. OTLP traces
and logs are *carried* byte-exactly as streams; VIREL does not compete with
OTLP as an export protocol.

## Where to next

- [02-reading-virl.md](02-reading-virl.md) — produce a real VIRL file and
  walk it byte by byte.
- The normative overview: [../doc/TraceContainerFormat.md](../doc/TraceContainerFormat.md)
  §0-§2.
- [05-glossary.md](05-glossary.md) — every term used above, one line each.
