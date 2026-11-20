---
id: find-conversion-provenance
title: Converted streams carry provenance; lossy converters attach only non-recoded fragments
kind: finding
tags: [conversion, provenance, migration, design]
applies_to: [doc/TraceContainerFormat.md]
source: jfr-spec/find-conversion-provenance
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


Two ratified rules for converters:

1. **Provenance is standardized in-stream** (a declared metadata type, not a
   header-only field): `source_format` (string-first — zero registry
   coordination), `source_format_version`, `converter` identity/version, and
   a `conversion_fidelity` enum. In-stream provenance survives chunk
   splitting and is visible at attach time by streaming readers.
2. **Lossy conversions attach ONLY the non-recoded parts (fragments) —
   partial attachment is the v1.0 standard; FULL attachment of the original
   is discouraged** (it doubles data size). A fragment is a size-prefixed,
   skippable event holding untouched ORIGINAL bytes plus a `source_locator`
   (u64 offset or format-specific coordinate string documented per
   converter) and an optional `reason` enum. Mapped portion + fragments =
   complete original information; a consumer owning the source format's
   reader reassembles the original without having collected it. Opaque
   attachments need no bulk channel: size-prefixed events skip by size-jump.
   Precedent: OTel profiles standardized the same escape hatch
   (original_payload_format/original_payload).
