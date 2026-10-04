# VIREL framing — Kaitai Struct (normative for the byte layer)
# Cross-checked by validators/: tcf_reader.py (annex + native accept/reject,
# foreign bridging), go_gate.py / perfetto_gate.py / pprof_gate.py /
# ctef_gate.py (native emit). Conveniences, never substitutes (spec §11).
# NOTE: the foreign-chunk bridge algorithm (resync to the next FLR or VIRL
# magic, §3.5)
# is intentionally NOT modeled here — it is normative prose plus the
# reference reader; a .ksy can only describe WELL-FORMED streams.
meta:
  id: tcf-framing
  title: VIREL — Versioned In-band Runtime Event Ledger — chunk framing, majors 0..2
  applications-source: doc/TraceContainerFormat.md sections 3 and 4
seq:
  - id: chunks
    type: chunk
    repeat: eos
types:
  chunk:
    seq:
      - id: header
        type: tcf_header
      - id: body
        size: header.native ? header.chunk_size - 76 : header.chunk_size - 68
  tcf_header:
    seq:
      - id: magic
        type: u4
        valid: magic == 0x464c5200 or magic == 0x5649524c # "FLR\0" or "VIRL"
      - id: version_major
        type: u2
      - id: version_minor
        type: u2
      - id: chunk_size
        type: u8
      - id: last_state_snapshot_offset
        type: u8
        # JFR annex: last checkpoint. Native VIREL: last state snapshot.
      - id: metadata_offset
        type: u8            # 0 = none; native: in-chunk offset of metadata event
      - id: start_nanos
        type: u8            # UNIX epoch ns of chunk anchor (writer's clock)
      - id: duration_nanos
        type: u8
      - id: start_ticks
        type: u8
      - id: ticks_per_second
        type: u8
      - id: state
        type: u1            # 0xff = UPDATING (live seqlock, spec §3.3)
      - id: flags_word
        type: u2            # @66: bit15 = VIREL-SIGNATURE (set = native VIRL; always clear
                            # in the JFR 1/2 FLR annex, spec §3.5). LOW byte @67 = final-chunk flag.
                            # HIGH byte bits 0..6 = default metadata-encoding id, 0..127.
      - if: native
        id: writer_id
        type: u8            # RANDOM per chunk writer, != 0 (native 76-byte header only)
    instances:
      signature_set:
        value: (flags_word & 0x8000) != 0
      native:
        value: magic == 0x5649524c and signature_set
# Content-layer grammar = DYNAMIC (in-stream declared types; spec §4–5):
# per-module .proto (doc/m0-framing.proto, …) and tcf-schema.cddl are
# codegen conveniences; the declared-types reader remains the conformance
# baseline.
