#!/usr/bin/env python3
"""Generate the synthetic CTEF fixture for gate 5.

Gate 5's preferred input is a real Chrome DevTools trace export
(/tmp/tcf-ctef-real.json), which is not reproducible here. This generator
writes an equivalent-shape stand-in (/tmp/tcf-ctef-synthetic.json) so the
gate runs in CI: 4 processes, several threads per process, nested B/E
slices (E names match the innermost open slice, the rule the back-emit
uses to recover them) and i instants. The output is deterministic.
"""
import json
import os
import sys

NAMES = ["RenderFrame::UpdateLayout", "V8.CompileCode", "cc::Scheduler::BeginFrame",
         "ScriptController::Evaluate", "Decoder::DecodeBuffer", "EventDispatcher::DispatchEvent"]
PIDS = [1204, 1520, 1723, 1988]
TIDS = [7751, 7752, 8813]
SLICES_PER_THREAD = 155   # 4 pids x 3 tids x 155 -> 1860 B/E events
MAX_DEPTH = 3
START_US = 20_000_000     # arbitrary absolute us anchor, like a real boot-relative clock


def gen_thread(pid, tid):
    evs = []
    t = START_US
    opens = []
    for k in range(SLICES_PER_THREAD):
        if len(opens) >= MAX_DEPTH or (opens and k % 3 == 2):
            nm = opens.pop()
            evs.append({"ph": "E", "pid": pid, "tid": tid, "ts": t,
                        "name": nm, "cat": "benchmark", "tts": t, "args": {}})
            t += 7
        nm = NAMES[(k + tid) % len(NAMES)]
        evs.append({"ph": "B", "pid": pid, "tid": tid, "ts": t,
                    "name": nm, "cat": "benchmark", "tts": t, "args": {}})
        t += 11
        opens.append(nm)
        if k % 5 == 0:
            evs.append({"ph": "i", "pid": pid, "tid": tid, "ts": t,
                        "name": "TraceEventMarker", "cat": "benchmark", "s": "t"})
            t += 3
    while opens:
        nm = opens.pop()
        evs.append({"ph": "E", "pid": pid, "tid": tid, "ts": t,
                    "name": nm, "cat": "benchmark", "tts": t, "args": {}})
        t += 7
    return evs


def main(out):
    events = []
    for pid in PIDS:
        for tid in TIDS:
            events.extend(gen_thread(pid, tid))
    doc = {
        "traceEvents": events,
        "metadata": {"clock-domain": "LINUX_CLOCK_MONOTONIC", "benchmarks": ["virel-gate5"]},
    }
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(doc, f)
    nb = sum(1 for e in events if e["ph"] == "B")
    ne = sum(1 for e in events if e["ph"] == "E")
    ni = sum(1 for e in events if e["ph"] == "i")
    print(f"{out}: {len(events)} events (B={nb} E={ne} i={ni}), "
          f"{len(PIDS)} processes x {len(TIDS)} threads")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/tcf-ctef-synthetic.json"))
