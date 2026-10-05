# VIREL framing — Kaitai Struct (normative for the byte layer)
# Two layers (spec §3):
#   - the format-agnostic container: header + records (top level of this file);
#   - the `virel`/`jfr` chunk-stream payload formats (type `chunk_stream`),
#     parsed from a stream reconstructed by concatenating its DATA payloads.
# Cross-checked by validators/: tcf_reader.py (annex + native accept/reject,
# foreign bridging), go_gate.py / perfetto_gate.py / pprof_gate.py /
# ctef_gate.py (native emit). Compiled with kaitai-struct-compiler 0.11 (python, java,
# cpp_stl, go); container_gate.py step 7 checks the generated parser against
# virel_container.py on a real multi-format container and its virel/jfr chunk streams.
# Conveniences, never substitutes (spec §11).
# NOTE: recovery scanning (§3.7) and the foreign-chunk bridge (resync to the
# next FLR or VIRL magic, §3.9.5) are intentionally NOT modeled — they are
# normative prose plus the reference reader; a .ksy can only describe
# WELL-FORMED, CLOSED containers (no OPEN record).
meta:
  id: tcf_framing
  title: VIREL — Versioned In-band Runtime Event Ledger — container (major 0) and chunk framing (majors 0..2)
  endian: be
  # normative source: doc/TraceContainerFormat.md sections 3 and 4
seq:
  - id: header
    type: container_header
  - id: records
    type: record
    repeat: eos
types:
  container_header:
    seq:
      - id: signature
        contents: [0x89, 0x56, 0x52, 0x4c, 0x0d, 0x0a, 0x1a, 0x0a]   # "\x89VRL\r\n\x1a\n"
      - id: major
        type: u2            # 0 = draft; readers reject unknown majors
      - id: minor
        type: u2
      - id: header_size
        type: u2            # 32 in major 0
      - id: flags
        type: u2
      - id: container_id
        type: u8            # random; 0 = unspecified
      - id: extension
        size: header_size - 24   # reserved + unknown future fields, then CRC
    instances:
      header_crc:
        pos: header_size - 4
        type: u4            # CRC-32C over all preceding header bytes
  record:
    seq:
      - id: magic
        contents: "VREC"
      - id: kind
        type: u2
        enum: record_kind
      - id: header_size
        type: u2            # 56 in major 0
      - id: stream_id
        type: u4            # 0 = container-level
      - id: sequence
        type: u4            # DATA: 0,1,2,... per stream; other kinds: 0
      - id: static_crc
        type: u4            # CRC-32C over record bytes 0..15
      - id: state
        type: u1            # 0x00 CLOSED; 0xff OPEN (not modeled)
      - id: flags
        type: u1            # bit0 STREAM_END, bit1 PAYLOAD_CRC, bit2 PREAMBLE, bit3 RESYNC
      - id: reserved
        type: u2
      - id: payload_length
        type: u8
      - id: time_start
        type: u8            # UNIX epoch ns; 0 = unknown
      - id: time_end
        type: u8
      - id: payload_crc
        type: u4
      - id: dynamic_crc
        type: u4            # CRC-32C over record bytes 20..51
      - id: header_extension
        size: header_size - 56
      - id: payload
        size: payload_length
        type:
          switch-on: kind
          cases:
            'record_kind::stream': descriptor
            'record_kind::manifest': manifest
            'record_kind::end': end_payload
        # DATA and PAD stay raw bytes: the container never parses stream bytes.
  descriptor:
    seq:
      - id: count
        type: varint
      - id: entries
        type: descriptor_entry
        repeat: expr
        repeat-expr: count.value
  descriptor_entry:
    seq:
      - id: key_len
        type: varint
      - id: key
        type: str
        encoding: UTF-8
        size: key_len.value
      - id: value_len
        type: varint
      - id: value
        size: value_len.value   # encoding per key, spec §3.4
  manifest:
    seq:
      - id: version
        type: varint          # 1
      - id: stream_count
        type: varint
      - id: streams
        type: manifest_stream
        repeat: expr
        repeat-expr: stream_count.value
      - id: index_count
        type: varint
      - id: index
        type: manifest_index_entry
        repeat: expr
        repeat-expr: index_count.value
  manifest_stream:
    seq:
      - id: stream_id
        type: varint
      - id: first_stream_record
        type: varint
      - id: data_records
        type: varint
      - id: payload_bytes
        type: varint
      - id: time_start
        type: varint
      - id: time_end
        type: varint
      - id: flags
        type: varint          # bit0 ENDED
  manifest_index_entry:
    seq:
      - id: record_offset
        type: varint
      - id: stream_id
        type: varint
      - id: sequence
        type: varint
      - id: time_start
        type: varint
      - id: time_end
        type: varint
      - id: flags
        type: varint          # the record's flags byte
  end_payload:
    seq:
      - id: manifest_offset
        type: u8              # 0 = no manifest
  varint:
    # VIREL Varint128 (spec §2, §4.1): LE 7-bit groups, at most 9 bytes; the
    # 9th byte, when present, carries raw 8 bits.
    seq:
      - id: groups
        type: u1
        repeat: until
        repeat-until: (_ & 0x80) == 0 or _index == 8
    instances:
      value:
        value: >-
          (groups[0] & 0x7f)
          | (groups.size > 1 ? (groups[1] & 0x7f).as<u8> << 7 : 0)
          | (groups.size > 2 ? (groups[2] & 0x7f).as<u8> << 14 : 0)
          | (groups.size > 3 ? (groups[3] & 0x7f).as<u8> << 21 : 0)
          | (groups.size > 4 ? (groups[4] & 0x7f).as<u8> << 28 : 0)
          | (groups.size > 5 ? (groups[5] & 0x7f).as<u8> << 35 : 0)
          | (groups.size > 6 ? (groups[6] & 0x7f).as<u8> << 42 : 0)
          | (groups.size > 7 ? (groups[7] & 0x7f).as<u8> << 49 : 0)
          | (groups.size > 8 ? groups[8].as<u8> << 56 : 0)
  # --- `virel` / `jfr` chunk-stream payload formats (spec §3.9) -------------
  chunk_stream:
    seq:
      - id: chunks
        type: chunk
        repeat: eos
  chunk:
    seq:
      - id: header
        type: tcf_header
      - id: body
        size: 'header.is_native ? header.chunk_size - 76 : header.chunk_size - 68'
  tcf_header:
    seq:
      - id: magic
        type: u4
        valid:
          expr: _ == 0x464c5200 or _ == 0x5649524c # "FLR\0" or "VIRL"
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
        type: u1            # 0xff = UPDATING (chunk seqlock, spec §3.9.3)
      - id: pad
        type: u1
      - id: flags_word
        type: u2            # @66: bit15 = VIREL-SIGNATURE (set = native VIRL; always clear
                            # in the JFR 1/2 FLR annex, spec §3.9.5). LOW byte @67 = final-chunk flag.
                            # HIGH byte bits 0..6 = default metadata-encoding id, 0..127.
      - id: writer_id
        type: u8            # RANDOM per chunk writer, != 0 (native 76-byte header only)
        if: is_native
    instances:
      signature_set:
        value: (flags_word & 0x8000) != 0
      is_native:
        value: magic == 0x5649524c and signature_set
enums:
  record_kind:
    0: pad
    1: stream
    2: data
    3: manifest
    4: end
# Content-layer grammar = DYNAMIC (in-stream declared types; spec §4–5):
# per-module .proto (doc/m0-framing.proto, …) and tcf-schema.cddl are
# codegen conveniences; the declared-types reader remains the conformance
# baseline.
