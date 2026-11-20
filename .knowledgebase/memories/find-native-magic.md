---
id: find-native-magic
title: Native chunk identity is `VIRL` magic + signature bit; `FLR\0` stays exclusively JFR
kind: finding
tags: [framing, magic, jfr-boundary, design]
applies_to: [doc/**, validators/tcf_reader.py, validators/virel_container.py]
source: jfr-spec/find-native-magic
check: python3 validators/tcf_reader.py --selftest
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


The JFR magic `FLR\0` is a property of JFR and must never be reused by native
chunks. Native identity: magic `VIRL` + VIREL-NATIVE signature bit (@66 bit 7
SET) + an implemented native major. JFR-legacy identity: `FLR\0` + signature
CLEAR + annex major 1 or 2. Any other candidate in a chunk stream is foreign
and is bridged over, never interpreted. Native chunks keep the verified JFR
field layout at offsets 4..67 (plus the writer id at 68..75); layout
compatibility is not format identity.

Proven: `tcf_reader.py --selftest` (native chunks with an intervening foreign
span bridged), gate 1 (real JDK 26 annex), gates 2-5 (native `VIRL` output
re-read), model lint clean.
