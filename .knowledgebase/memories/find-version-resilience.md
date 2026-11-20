---
id: find-version-resilience
title: Version policy: native majors graduate 0 to 1; no version-number claims anywhere
kind: finding
tags: [versioning, framing, header, design]
applies_to: [doc/TraceContainerFormat.md]
source: jfr-spec/find-version-resilience
check: python3 validators/tcf_reader.py --selftest
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Coexistence with JFR (and any future JFR majors) is decided by a class guard,
never by reserving or claiming version numbers — no coordination with OpenJDK
exists. Disambiguation is exact: `VIRL` + signature bit set + implemented
native major = native; `FLR\0` + signature clear + major 1/2 = byte-frozen
JFR annex; anything else = foreign, bridged (spec §3.9.5). Native major 0 is
the draft; 1 is the first stable native after ratification; later native
majors are advertised spec events that today's readers skip as foreign.
JFR v2 provably never writes the flags high byte @66 (verified in this
repo's writer/parser), which is what makes the @66 signature bit safe.
