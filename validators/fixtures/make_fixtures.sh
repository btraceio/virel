#!/bin/sh
# Regenerate the /tmp fixtures read by gates 3, 4 and 5 (see AGENTS.md).
# The programs write fixed /tmp paths; the gates' defaults read them.
#   /tmp/tcf-gotrace.out                         go_gate.py   (Go runtime/trace)
#   /tmp/tcf-pprof-cpu.pprof, tcf-pprof-heap.pprof pprof_gate.py (Go runtime/pprof)
#   /tmp/tcf-cpuproffix/*.cpuprofile              ctef_gate.py (node --cpu-prof)
# Not regenerable here: /tmp/tcf-ctef-real.json, a real Chrome DevTools trace export
# (ctef_gate.py's first input); provide your own or pass a path as its first argument.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
(cd "$here/gotrace" && go run .)
(cd "$here/pprof" && go run .)
mkdir -p /tmp/tcf-cpuproffix
rm -f /tmp/tcf-cpuproffix/*.cpuprofile
cp "$here/cpuprofile/work.js" /tmp/tcf-cpuproffix/work.js
(cd /tmp/tcf-cpuproffix && node --cpu-prof --cpu-prof-dir=/tmp/tcf-cpuproffix work.js)
ls -l /tmp/tcf-gotrace.out /tmp/tcf-pprof-cpu.pprof /tmp/tcf-pprof-heap.pprof /tmp/tcf-cpuproffix/*.cpuprofile
