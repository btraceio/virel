---
id: find-pool-skip
title: One state snapshot per pool (streaming shape): pools are size-skippable like events
kind: finding
tags: [pools, streaming, query, design]
applies_to: [doc/TraceContainerFormat.md, validators/go_gate.py]
source: jfr-spec/find-pool-skip
check: sh validators/fixtures/make_fixtures.sh && python3 validators/go_gate.py /tmp/tcf-gotrace.out --full
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Ratified shape: native draft writers emit one state snapshot event PER POOL
(or pool group); each snapshot is an independently size-prefixed, skippable
unit exactly like any event. A query reader that needs only, say, strings
skips the other pools by size without parsing a single entry — measured 27-29%
less parsed on real pprof-derived pools for a strings-only query, with ~7 B
per-pool overhead. Writers MAY still co-locate small pools in one snapshot.
The JFR annex needs no grammar change (v2 already writes multiple
delta-chained checkpoint events per chunk). Reader contract: pool
materialization is lazy by type id, entries carry full payload (skip costs
no later re-parse), delta chains walk backward within the chunk. Reference
emitters (go_gate, pprof_gate) use the per-pool shape; the co-located shape
is covered by `tcf_reader --selftest`. Side benefit: per-pool snapshots are
themselves sync points, improving partial-write recovery granularity.
