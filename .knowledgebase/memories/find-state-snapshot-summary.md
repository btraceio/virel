---
id: find-state-snapshot-summary
title: Native bootstrap type 1 is the State Snapshot; snapshotFlags bit 7 carries a cumulative type summary
kind: finding
tags: [pools, index, native, design]
applies_to: [doc/TraceContainerFormat.md, validators/tcf_reader.py]
source: jfr-spec/find-state-snapshot-summary
check: python3 validators/tcf_reader.py --selftest
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


VIREL reserves bootstrap type id 1 for a **State Snapshot** (JFR annex keeps
its byte-frozen name, *checkpoint*). The record carries constant-pool/writer
state, not execution recovery. The native draft reuses the type-1 record's
`flush:u1` byte as `snapshotFlags`: bit 0 = FLUSH, bit 7 =
TYPE_SUMMARY_PRESENT. A summary-present snapshot leads with a cumulative,
typeId-ordered summary `[typeId, emittedCount]` (bootstrap types 0 and 1
excluded) — exact cumulative counts of non-bootstrap records preceding the
snapshot in its chunk. A reader binary-searches this bounded table instead
of parsing event bodies. Offset 16 in the header stays the
last-state-snapshot locator; the summary is written before that posthoc
pointer advances under the seqlock protocol, so a torn newest record leaves
the previous one usable. Metadata and state snapshots are independent
records with a one-way dependency: metadata declares types; snapshots
materialize typed pool data.
