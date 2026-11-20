---
id: find-idl-codegen
title: IDL layering: Kaitai = framing, CDDL/proto = vocabulary, Avro = model resolution; no single IDL covers the format
kind: finding
tags: [idl, codegen, avro, kaitai, design]
applies_to: [doc/, validators/build_tcf_model_avsc.py]
source: jfr-spec/find-idl-codegen
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Layered machine-readable artifacts (spec §11), each doing the one job its
system is good at — never one monolithic IDL:

- **Kaitai .ksy** for the framing/head grammar: read-side DSL that can
  express magic/offsets/varint; the gates can check .ksy against the binary.
- **CDDL+CBOR and proto** for control-plane vocabulary contracts.
- **Avro** for the full protocol MODEL: the only mainstream system whose
  read path natively encodes add-only evolution (writer schema in-stream +
  reader-schema resolution). The Avro artifact is the model of record;
  `validators/build_tcf_model_avsc.py` regenerates it and must not drift.
- SBE/FlatBuffers-class systems: possible later fast-path bindings for
  closed worlds; no spec role today. ASN.1 academically closest, too heavy.

Ruled out: auto-generated readers/writers for the whole format from one IDL
(the dynamic content layer + streaming back-patch make it impossible without
giving up self-description or streaming); schema registries as a codegen
prerequisite.
