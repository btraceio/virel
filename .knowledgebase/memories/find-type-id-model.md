---
id: find-type-id-model
title: One writer per chunk; type-id bindings are add-only and never re-bound
kind: finding
tags: [type-ids, pools, writers, design, invariant]
applies_to: [doc/TraceContainerFormat.md, validators/tcf_reader.py]
source: jfr-spec/find-type-id-model
check: python3 validators/tcf_reader.py --selftest
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Two binding invariants of the native format:

1. **One writer per chunk.** Chunks are both the writer boundary and the
   pool boundary. Internally multi-threaded producers follow the JFR
   architecture: thread-local writers/pools sync as small chunks whose pools
   are valid for exactly that chunk. (JFR itself bounds the checkpoint delta
   walk per chunk — ChunkParser.fillConstantPools — and resets the
   checkpoint offset per chunk.)
2. **Type ids are stream-global with add-only evolution.** A binding, once
   declared in any chunk's metadata, is never re-bound; redefinition is a
   spec violation and readers fail fast. Each chunk's metadata is an
   authoritative, complete snapshot (omissions forbidden), so readers may
   assume completeness; metadataId dedup across chunks is a reader-caching
   optimization, not a contract. Cross-chunk pool dedup is safe exactly
   because bindings are add-only.

Correction 2026-10-04: multiple JFR buffers do drain into one existing chunk
writer — "thread sync creates a chunk" is the sync-boundary model, not the
only buffer-drain shape (see find-age-limited-drain).
