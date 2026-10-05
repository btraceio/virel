#!/usr/bin/env python3
"""tcf_model.py — REFERENCE BRIDGE LIBRARY (ratified Option C).

native wire <-> model duty; fidelity = THIS file's test suite (not spec text).
  * to_model(data)  : native/annex bytes -> the full VIREL model dict (virel.model.Container shape)
  * lint(model)     : model-lint against doc/tcf-model.avsc (recursive type check)
  * --check FILE    : parse + lint + report

The model carries the DYNAMIC layer as data (GenericDyn -> Event.values as FieldValues),
exactly as ratified: model = documentation + lint + codegen source; NOT a second wire.
"""
import json, struct, sys
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from tcf_reader import Reader

AVSC = __file__.rsplit("/", 1)[0] + "/../doc/tcf-model.avsc"

COMPOSITE = __import__("tcf_reader").ModelComposite

def fv(v):
    """python value -> virel.model.FieldValue form (union alt selected by python type)"""
    if isinstance(v, bool):
        return {"v": v}
    if isinstance(v, str):
        return {"v": v}
    if isinstance(v, bytes):
        return {"v": v}
    if isinstance(v, int):
        if v < -(1 << 62) or v > (1 << 62):        # u64-pattern guard
            return {"v": {"__u64raw__": v & ((1 << 64) - 1)}} if False else {"v": v & ((1 << 64) - 1) - (1 << 64) if v & (1 << 63) else v}
        return {"v": v}
    if isinstance(v, float):
        return {"v": v}
    if isinstance(v, tuple) and len(v) == 2 and v[0] == "@key":
        return {"v": {"typeId": -1, "key": v[1]}}          # bare pool key without type context
    if isinstance(v, list):
        return {"v": {"items": [fv(x) for x in v]}}
    if isinstance(v, COMPOSITE):
        return {"v": {"fields": list(v.model_fields)}}
    if isinstance(v, dict):
        return {"v": {"fields": [{"name": k, "value": fv(x)} for k, x in v.items()]}}
    return {"v": None}

def chunk_models(data: bytes) -> list:
    """a virel/jfr chunk stream -> list of avsc Chunk records"""
    import io, contextlib
    r = Reader(); r.record = True
    with contextlib.redirect_stdout(io.StringIO()):
        r.run(data)
    chunks = []
    for cm in r.chunk_models:
        chunk = {k: v for k, v in cm.items() if k != "major_minor"}
        # normalize model shapes -> avsc forms
        chunk["header"]["flags"] = int(chunk["header"]["flags"])
        meta = chunk.get("metadata")
        if meta is not None:
            root = meta["root"]
            meta["root"] = model_element(root)
        evs = []
        for rec in chunk["events"]:
            tid, st, dt, vals = rec
            evs.append({"typeId": tid, "startTicks": st, "durationTicks": dt,
                        "values": [{"name": k, "value": fv(v)} for k, v in vals.items()]})
        chunk["events"] = evs
        cps = []
        for cp in chunk["stateSnapshots"]:
            pools = []
            for p in cp["pools"]:
                pools.append({"typeId": p["typeId"],
                              "entries": [{"key": e["key"], "value": fv(e["value"])} for e in p["entries"]]})
            cps.append({"startTime": cp["startTime"], "duration": cp["duration"],
                        "delta": cp["delta"], "flush": cp["flush"],
                        "summaryVersion": cp["summaryVersion"],
                        "hasTypeSummary": cp["hasTypeSummary"],
                        "typeSummary": cp["typeSummary"], "pools": pools})
        chunk["stateSnapshots"] = cps
        chunks.append(chunk)
    return chunks

def container_model(data: bytes) -> dict:
    """a real container (spec §3) -> virel.model.Container; virel and jfr streams are decoded"""
    import virel_container as vc
    cr = vc.ContainerReader(data)
    typed = {"format-version": "formatVersion", "source": "source", "producer": "producer", "label": "label"}
    streams = []
    for sid, st in sorted(cr.streams.items()):
        desc = {"format": st.fmt or "", "contentEncoding": st.get("content-encoding", "identity"),
                "recordFraming": "MESSAGE" if st.framing == "message" else "STREAM",
                "inputTruncated": any(k == "input-truncated" for k, _ in st.keys),
                "entries": [{"key": k, "value": v} for k, v in st.keys],
                "clockAnchors": [vc.decode_anchor(v) for k, v in st.keys if k == "clock-anchor"],
                "derivedFrom": [vc.dec_varint(v, 0)[0] for k, v in st.keys if k == "derived-from"]}
        for k, field in typed.items():
            if st.get(k) is not None:
                desc[field] = st.get(k)
        if st.get("writer-id") is not None:
            desc["writerId"] = struct.unpack(">q", dict(st.keys)["writer-id"])[0]
        recs = [{"offset": r.offset, "kind": vc.KIND_NAMES[r.kind], "headerSize": r.header_size,
                 "streamId": r.stream_id, "sequence": r.sequence, "open": r.state == vc.STATE_OPEN,
                 "streamEnd": bool(r.flags & vc.F_STREAM_END), "preamble": bool(r.flags & vc.F_PREAMBLE),
                 "resync": bool(r.flags & vc.F_RESYNC),
                 "payloadLength": None if r.state == vc.STATE_OPEN else r.length,
                 "timeStartNanos": r.t0, "timeEndNanos": r.t1,
                 "payloadCrc": r.payload_crc if r.flags & vc.F_PAYLOAD_CRC else None}
                for r in st.records_by_seq()]
        decoded = st.fmt in ("virel", "jfr") and st.complete()
        streams.append({"streamId": sid, "descriptor": desc, "records": recs, "complete": st.complete(),
                        "gaps": st.gaps(), "decoder": st.fmt if decoded else None,
                        "chunks": chunk_models(cr.extract(sid)) if decoded else None})
    m = None
    if cr.manifest:
        m = {"version": cr.manifest["version"],
             "streams": [dict(x, ended=bool(x["ended"])) for x in cr.manifest["streams"]],
             "index": cr.manifest["index"]}
    return {"registry": avsc_registry(),
            "header": {"major": cr.major, "minor": cr.minor, "headerSize": cr.header_size,
                       "flags": cr.flags, "containerId": struct.unpack(">q", struct.pack(">Q", cr.container_id))[0]},
            "streams": streams, "manifest": m, "cleanlyClosed": cr.clean, "implicit": False,
            "damagedSpans": [{"firstOffset": a, "nextOffset": b} for a, b in cr.damaged]}

def to_model(data: bytes) -> dict:
    import virel_container as vc
    if data[:8] == vc.SIGNATURE:
        return container_model(data)
    chunks = chunk_models(data)
    # Inputs are bare chunk streams; spec §3.8 opens them as an implicit container
    # with one stream whose format follows the leading magic.
    fmt = "jfr" if data[:4] == b"FLR\0" else "virel"
    stream = {"streamId": 1, "descriptor": {"format": fmt}, "records": [],
              "complete": True, "decoder": fmt, "chunks": chunks}
    return {"registry": avsc_registry(),
            "header": {"major": 0, "minor": 0, "headerSize": 32, "flags": 0, "containerId": 0},
            "streams": [stream], "cleanlyClosed": False, "implicit": True}

def model_chunks(model) -> list:
    return [c for st in model["streams"] for c in (st.get("chunks") or [])]

def model_element(el) -> dict:
    return {"name": el["n"],
            "attributes": [{"name": a, "value": b} for (a, b) in el["a"]],
            "children": [model_element(c) for c in el["children"]]}

_REG = None
def avsc_registry() -> dict:
    """the registry part of the model comes FROM the avsc itself (single source of truth)"""
    global _REG
    if _REG is None:
        doc = json.load(open(AVSC))
        named = {}
        def collect(node):
            if isinstance(node, dict):
                nm = node.get("name")
                t = node.get("type")
                if isinstance(nm, str) and nm[:1].isupper() and (t in ("record", "enum", "fixed") or (isinstance(t, dict) and t.get("type") in ("record", "enum", "fixed"))):
                    named["virel.model." + nm] = node
                for v in node.values(): collect(v)
            elif isinstance(node, list):
                for v in node: collect(v)
        collect(doc)
        _REG = next(f for f in doc["fields"] if f["name"] == "registry")["type"]
        _REG = {f["name"]: fv_stub(f["type"], named) for f in _REG["fields"]}
    return _REG

def fv_stub(t, named):
    if t in ("int", "long"): return 0
    if t == "string": return ""
    if t == "bytes": return b""
    if t == "boolean": return False
    if t in ("double", "float"): return 0.0
    if isinstance(t, str):
        if t in named: return fv_stub(named[t], named)   # named-type reference
        return None
    if isinstance(t, list):
        return None                       # union default = the null alt (exists in our unions)
    if isinstance(t, dict) and t.get("type") == "array":
        return []
    if isinstance(t, dict) and t.get("type") == "enum":
        return t["symbols"][0]
    if isinstance(t, dict) and t.get("type") == "fixed":
        return b"\x00" * t["size"]
    if isinstance(t, dict) and t.get("type") == "record":
        return {ff["name"]: fv_stub(ff["type"], named) for ff in t["fields"]}
    return None

# ---------------- lint ----------------
PYT = {"long": int, "int": int, "string": str, "boolean": bool, "bytes": (bytes, bytearray), "double": float, "float": float}

def lint(model, doc=None, errors=None, path="Container"):
    doc = doc or json.load(open(AVSC))
    errors = errors if errors is not None else []
    named = {}
    def collect(node):
        if isinstance(node, dict):
            nm = node.get("name")
            t = node.get("type")
            if isinstance(nm, str) and nm[:1].isupper() and (t in ("record", "enum", "fixed") or (isinstance(t, dict) and t.get("type") in ("record", "enum", "fixed"))):
                named["virel.model." + nm] = node
            for v in node.values(): collect(v)
        elif isinstance(node, list):
            for v in node: collect(v)
    collect(doc)
    def union_alts(t):
        return t if isinstance(t, list) else None
    def check(node, schema, where, collect_errs=True):
        def err(m):
            if collect_errs: errors.append(m)
        # union?
        if isinstance(schema, list):
            errs_before = len(errors)
            matched = any(check(node, a, where, collect_errs=False) for a in schema)
            if not matched:
                del errors[errs_before:]
                err(f"{where}: value {type(node).__name__}={node!r:.60} matches no union alternative")
            return matched
        if isinstance(schema, str):
            if schema in PYT:
                ok = isinstance(node, PYT[schema]) and not (schema in ("long", "int") and isinstance(node, bool))
                if not ok and schema != "null": err(f"{where}: expected {schema}, got {type(node).__name__}")
                return ok
            if schema == "null":
                return node is None
            ref = named.get(schema)
            if ref is None:
                err(f"{where}: unknown named type {schema}")
                return False
            return check(node, ref, where, collect_errs=True)
        if isinstance(schema, dict):
            st = schema["type"]
            if st == "record":
                if not isinstance(node, dict):
                    err(f"{where}: expected record {schema.get('name')}, got {type(node).__name__}")
                    return False
                fm = {f["name"]: f for f in schema["fields"]}
                for k, v in node.items():
                    if k not in fm:
                        err(f"{where}.{k}: not a field of {schema.get('name')}")
                    else:
                        check(v, fm[k]["type"], f"{where}.{k}")
                for f in schema["fields"]:
                    if f["name"] not in node and "default" not in f:
                        err(f"{where}: missing required field {f['name']}")
                return True
            if st == "array":
                if not isinstance(node, list):
                    err(f"{where}: expected array")
                    return False
                for i, v in enumerate(node): check(v, schema["items"], f"{where}[{i}]")
                return True
            if st == "enum":
                if node not in schema["symbols"]:
                    err(f"{where}: {node!r} not in enum {schema['name']}")
                    return False
                return True
            if st == "fixed":
                if not (isinstance(node, (bytes, str)) and len(node) == schema["size"]):
                    err(f"{where}: expected fixed({schema['size']})")
                return True
            return check(node, st, where, collect_errs=True)
        return True
    check(model, doc, path)
    return errors

def main():
    for path in sys.argv[1:]:
        data = open(path, "rb").read()
        model = to_model(data)
        errs = lint(model)
        print(f"{path}: streams={len(model['streams'])} chunks={len(model_chunks(model))} lint-errors={len(errs)}")
        for e in errs[:12]: print("   !", e)
        print("   MODEL-LINT:", "PASSED" if not errs else "FAILED")
    return 0

if __name__ == "__main__":
    sys.exit(main())
