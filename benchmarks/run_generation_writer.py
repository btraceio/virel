#!/usr/bin/env python3
"""Build, verify and measure bounded pooled generation recording."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import tempfile
from validate_generation_output import validate

ROOT = Path(__file__).resolve().parent
MODES = ('fixed', 'generation', 'staged')
SCENARIOS = {
    'registered': {},
    'lookup': {'workload': 'lookup'},
    'miss-then-hit': {'workload': 'miss'},
    'exhaustion': {'workload': 'miss', 'cardinality': 4096, 'capacity': 64},
    'rotation-10ms': {'rotate-us': 10000},
    'rotation-1ms': {'rotate-us': 1000},
    'four-producers': {'producers': 4},
    'four-rotation': {'producers': 4, 'rotate-us': 10000},
    'paused-producer': {'producers': 4, 'rotate-us': 1000, 'pause-ms': 50},
    'stalled-sink': {'rotate-us': 1000, 'stall-ms': 50},
    'saturated': {'rate': 0},
}
# Partial-buffer publication without resetting the open generation or its pools.
# flush-us: producer seals its partial buffer on its next call after the deadline.
# drain-age-us: background drains an open buffer's committed prefix once its
# oldest undrained event is at least that old, covering idle producers.
# Publication latency is capture-to-header-publication, sampled.
PUBLICATION_BASE = {'producers': 4, 'workload': 'lookup', 'observe-publication': 1}
BURSTY = {'rate': 20000, 'burst': 256}  # 256 events, then about 12.8 ms idle
PUBLICATION_SCENARIOS = {
    'paced-none': {'rate': 200000},
    'paced-flush-1ms': {'rate': 200000, 'flush-us': 1000},
    'paced-flush-100us': {'rate': 200000, 'flush-us': 100},
    'paced-flush-100us-clock': {'rate': 200000, 'flush-us': 100, 'flush-clock': 1},
    'paced-age-1ms': {'rate': 200000, 'drain-age-us': 1000},
    'paced-age-100us': {'rate': 200000, 'drain-age-us': 100},
    'bursty-none': dict(BURSTY),
    'bursty-flush-1ms': dict(BURSTY, **{'flush-us': 1000}),
    'bursty-age-1ms': dict(BURSTY, **{'drain-age-us': 1000}),
    'bursty-age-100us': dict(BURSTY, **{'drain-age-us': 100}),
    'slow-none': {'rate': 10000},
    'slow-flush-1ms': {'rate': 10000, 'flush-us': 1000},
    'slow-age-1ms': {'rate': 10000, 'drain-age-us': 1000},
    'rotation-10ms-age-1ms': {'rate': 200000, 'drain-age-us': 1000, 'rotate-us': 10000},
    'saturated-age-100us': {'rate': 0, 'drain-age-us': 100},
}
PUBLICATION_MODES = ('generation', 'staged')
# Profiling samples from SIGPROF handlers into per-thread signal lanes while the
# normal lane records 5k events/s per thread. Targets: 512-frame stacks (1024
# tolerable), up to 64 writing threads. Stack modes (one table per generation,
# frames in a fixed arena): stage = handler copies raw frames, background interns;
# global = handlers intern concurrently (CAS-published); hybrid = handler does a
# read-only lookup, records the key on a hit and raw frames on a miss.
# Pinned to the conditions of the recorded comparison (one nesting level), not
# the benchmark defaults (global, two levels).
SIGNAL_BASE = {'workload': 'lookup', 'rate': 5000, 'drain-age-us': 1000, 'nest-levels': 1}
SIGNAL_SCENARIOS = {
    't4-d512': {'producers': 4, 'signal-hz': 1000, 'stacks': 256, 'stack-depth': 512},
    't4-d1024': {'producers': 4, 'signal-hz': 1000, 'stacks': 256, 'stack-depth': 1024},
    't4-d512-10khz': {'producers': 4, 'signal-hz': 10000, 'stacks': 256, 'stack-depth': 512},
    't16-d512': {'producers': 16, 'signal-hz': 1000, 'stacks': 1024, 'stack-depth': 512},
    't64-d512': {'producers': 64, 'signal-hz': 1000, 'stacks': 1024, 'stack-depth': 512},
    't64-d1024': {'producers': 64, 'signal-hz': 1000, 'stacks': 1024, 'stack-depth': 1024},
    't64-d512-wide': {'producers': 64, 'signal-hz': 1000, 'stacks': 2048, 'stack-depth': 512},
    # Each 100-ms generation starts with an empty table: bursts of concurrent misses.
    't64-d512-rotation': {'producers': 64, 'signal-hz': 1000, 'stacks': 1024, 'stack-depth': 512, 'rotate-us': 100000},
}
SIGNAL_MODES = ('stage', 'global', 'hybrid')


def run(binary, mode, options, events, verify=False):
    command = [str(binary), '--mode', mode, '--events', str(events)]
    for k, v in options.items():
        command += ['--' + k, str(v)]
    if verify:
        command += ['--verify']
    result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=90)
    row = json.loads(result.stdout)
    if row['accepted'] + row['dropped'] != events * row['producers']:
        raise RuntimeError('attempt accounting')
    if row['committed'] + row['discarded_after_acceptance'] != row['accepted']:
        raise RuntimeError('commit accounting')
    if row['recording_new_calls'] or row['worker_new_calls']:
        raise RuntimeError('unexpected measured C++ allocation')
    if row['max_live_generations'] > 2 or row['max_queued_buffers'] > 8 * row['producers']:
        raise RuntimeError('storage bounds')
    if row['observe_publication'] and row['committed'] and not row['publication_samples']:
        raise RuntimeError('publication sampling')
    if row['flush_us'] and row['rate'] and row['rate'] <= 10000 and not row['periodic_flushes']:
        raise RuntimeError('periodic flush not exercised')
    if row['drain_age_us'] and row['rate'] and row['rate'] <= 20000 and not row['partial_drains']:
        raise RuntimeError('age-limited drain not exercised')
    if row['signal_hz'] and (not row['handler_entries'] or not row['samples_committed']):
        raise RuntimeError('signal samples not exercised')
    return row


def report(rows):
    lines = ['# Pooled generation writer experiment', '',
             'Five-repeat local exploration by default; medians [min–max]. Successful-call latency',
             'includes clock reads. CPU includes all workers, pacing, polling and drain.',
             'Output includes native headers, metadata, pool snapshots and final summaries.',
             'Drop cases are overload evidence, not equal-work performance comparisons.', '',
             '| Scenario | Mode | CPU ns/committed [range] | Call p99 ns [range] | Hit p99 ns | Handoff p99 ns | Loss % max | Lossless | Bytes/committed | Chunks median |',
             '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for scenario in SCENARIOS:
        for mode in MODES:
            group = [r for r in rows if r['scenario'] == scenario and r['mode'] == mode]
            if not group:
                continue
            def med(key):
                return statistics.median(r[key] for r in group)
            def dist(key):
                vs = [r[key] for r in group]
                return f'{statistics.median(vs):.0f} [{min(vs):.0f}–{max(vs):.0f}]'
            loss = max(100*(r['dropped']+r['discarded_after_acceptance'])/(r['events']*r['producers']) for r in group)
            lossless = sum(r['dropped']+r['discarded_after_acceptance']==0 for r in group)
            wire = statistics.median(r['wire_bytes']/r['committed'] for r in group if r['committed'])
            lines.append(f"| {scenario} | {mode} | {dist('cpu_ns_per_committed')} | {dist('producer_p99_ns')} | "
                         f"{med('hit_p99_ns'):.0f} | {med('handoff_p99_ns'):.0f} | {loss:.3f} | {lossless}/{len(group)} | {wire:.2f} | {med('chunks'):.0f} |")
    return '\n'.join(lines)+'\n'


def publication_report(rows):
    lines = ['# Partial-buffer publication experiment', '',
             'Four producers, lookup workload; medians [min–max] over repeats. Publication',
             'latency is capture to the background writer\'s header publication after the',
             'containing buffer or prefix; it excludes sink durability. Producer flushes are',
             'checked only on that producer\'s next recording call; age-limited prefix drains',
             'are background decisions on committed bytes. Bursty: 256 events, then ~12.8 ms idle.', '',
             '| Scenario | Mode | Pub p50 µs [range] | Pub p99 µs [range] | Pub max µs | Flushes | Prefix drains | Call p99 ns [range] | CPU ns/committed [range] | Loss % max | Bytes/committed | Chunks |',
             '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for scenario in PUBLICATION_SCENARIOS:
        for mode in PUBLICATION_MODES:
            group = [r for r in rows if r['scenario'] == scenario and r['mode'] == mode]
            if not group:
                continue
            def med(key):
                return statistics.median(r[key] for r in group)
            def dist(key, scale=1, digits=0):
                vs = [r[key]/scale for r in group]
                return f'{statistics.median(vs):.{digits}f} [{min(vs):.{digits}f}–{max(vs):.{digits}f}]'
            loss = max(100*(r['dropped']+r['discarded_after_acceptance'])/(r['events']*r['producers']) for r in group)
            wire = statistics.median(r['wire_bytes']/r['committed'] for r in group if r['committed'])
            lines.append(f"| {scenario} | {mode} | {dist('publication_p50_ns', 1000, 1)} | {dist('publication_p99_ns', 1000, 1)} | "
                         f"{med('publication_max_ns')/1000:.1f} | {med('periodic_flushes'):.0f} | {med('partial_drains'):.0f} | {dist('producer_p99_ns')} | "
                         f"{dist('cpu_ns_per_committed')} | {loss:.3f} | {wire:.2f} | {med('chunks'):.0f} |")
    return '\n'.join(lines)+'\n'


def signal_report(rows):
    lines = ['# Signal-handler profiling samples at target scale', '',
             'Producers record normal events at 5k/s each (generation mode, lookup) while a sampler',
             'sends thread-directed SIGPROF. Medians [min–max] over repeats. Handler time includes the',
             'synthetic stack walk common to all modes. Loss = handler drops plus background table',
             'drops over handler entries. Bytes/sample = sample events plus method/stack definitions.',
             'Lane bytes = signal-buffer bytes per accepted sample. Hit % = hybrid read-only lookups that',
             'found the stack. Storage: lane rings (4 x 64 KiB per thread) and two generation tables',
             '(4096 entries, 2M-frame arena each). The host has 12 logical CPUs.', '',
             '| Scenario | Mode | Handler p50 ns [range] | Handler p99 ns [range] | Handler p99.9 ns | Samples | Loss % max | Bytes/sample | Lane bytes/sample | Hit % | Drain ns/sample [range] | Event call p99 ns | CPU ns/event [range] | CAS lost | Interrupted |',
             '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    for scenario in SIGNAL_SCENARIOS:
        for mode in SIGNAL_MODES:
            group = [r for r in rows if r['scenario'] == scenario and r['stack_mode'] == mode]
            if not group:
                continue
            def med(key):
                return statistics.median(r[key] for r in group)
            def dist(key):
                vs = [r[key] for r in group]
                return f'{statistics.median(vs):.0f} [{min(vs):.0f}–{max(vs):.0f}]'
            loss = max(100*(r['handler_entries']-r['samples_committed'])/r['handler_entries'] for r in group)
            hit = statistics.median(100*r['hybrid_hits']/max(1, r['hybrid_hits']+r['hybrid_misses']) for r in group)
            lines.append(f"| {scenario} | {mode} | {dist('handler_p50_ns')} | {dist('handler_p99_ns')} | "
                         f"{med('handler_p999_ns'):.0f} | {med('samples_committed'):.0f} | {loss:.2f} | "
                         f"{med('sample_bytes_per_committed'):.1f} | {med('lane_intermediate_bytes_per_accepted'):.1f} | "
                         f"{hit:.1f} | {dist('sample_drain_ns_per_committed')} | {med('producer_p99_ns'):.0f} | "
                         f"{dist('cpu_ns_per_committed')} | {med('stack_cas_retries'):.0f} | {med('interrupted_recording'):.0f} |")
    return '\n'.join(lines)+'\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', type=int, default=100000)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--compiler', default=os.environ.get('CXX', 'clang++'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--suite', choices=('native', 'publication', 'signal'), default='native')
    args = parser.parse_args()
    if not 10000 <= args.events <= 10000000 or args.repeats < 1:
        parser.error('events must be 10000..10000000; repeats positive')
    source = ROOT / 'generation_writer.cpp'
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    metadata_sha = hashlib.sha256((ROOT / 'generation_metadata.inc').read_bytes()).hexdigest()
    runner_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    rows, verification = [], []
    native_checks = []
    with tempfile.TemporaryDirectory(prefix='virel-generation-') as directory:
        binary = Path(directory) / 'generation_writer'
        flags = ['-std=c++17','-O3','-DNDEBUG','-Wall','-Wextra','-Wpedantic','-pthread']
        subprocess.run([args.compiler, *flags, str(source), '-o', str(binary)], check=True)
        compiler = subprocess.run([args.compiler, '--version'], check=True, capture_output=True, text=True).stdout
        def verified(mode, options, events):
            options = dict(options, output=Path(directory) / 'verify.virel')
            row = run(binary, mode, options, events, True)
            native_checks.append(validate(options['output'], row))
            return row
        if args.suite == 'publication':
            for mode in MODES:
                opts = dict(PUBLICATION_BASE, rate=10000, **{'flush-us': 100})
                if mode != 'fixed':
                    opts['rotate-us'] = 500
                row = verified(mode, opts, 10000)
                if not row['periodic_flushes'] or not row['publication_samples']:
                    raise RuntimeError('must verify periodic partial publication')
                verification.append(row)
                row = verified(mode, dict(PUBLICATION_BASE, rate=10000, **{'flush-us': 100,
                                          'drop-buffer-every': 7, 'visibility-every': 1}), 10000)
                if not row['publication_discarded_samples'] or not row['committed']:
                    raise RuntimeError('must verify discarded publication samples')
                verification.append(row)
                opts = dict(PUBLICATION_BASE, workload='miss', **BURSTY, **{'drain-age-us': 100})
                if mode != 'fixed':
                    opts['rotate-us'] = 500
                row = verified(mode, opts, 10000)
                if not row['partial_drains']:
                    raise RuntimeError('must verify age-limited prefix drains')
                verification.append(row)
                row = verified(mode, dict(PUBLICATION_BASE, workload='miss', **BURSTY, **{'drain-age-us': 100,
                                          'drop-buffer-every': 7, 'visibility-every': 1}), 10000)
                if not row['partial_drains'] or not row['publication_discarded_samples']:
                    raise RuntimeError('must verify discarded buffers drained in prefixes')
                verification.append(row)
            scenarios, modes, render = PUBLICATION_SCENARIOS, PUBLICATION_MODES, publication_report
            scenario_options = lambda name: dict(PUBLICATION_BASE, **PUBLICATION_SCENARIOS[name])
        elif args.suite == 'signal':
            for stack_mode in SIGNAL_MODES:
                base = dict(SIGNAL_BASE, workload='miss', **{'stack-mode': stack_mode})
                cases = [
                    ('generation', {'producers': 4, 'signal-hz': 5000, 'stack-depth': 1024, 'rotate-us': 500,
                                    'drain-age-us': 100, 'pause-ms': 10}),
                    ('staged', {'producers': 16, 'signal-hz': 10000, 'stack-depth': 512, 'nodefer': 1,
                                'rotate-us': 2000, 'stall-ms': 10}),
                    ('fixed', {'producers': 64, 'signal-hz': 1000, 'stack-depth': 512, 'stacks': 4096,
                               'stack-sharing': 'disjoint'}),
                ]
                for mode, extra in cases:
                    row = verified(mode, dict(base, **extra), 10000)
                    if not row['interrupted_recording'] or (mode == 'fixed' and row['samples_committed'] == row['handler_entries']):
                        raise RuntimeError('must verify interrupted recording and stack-table exhaustion')
                    verification.append(row)
            scenarios, modes, render = SIGNAL_SCENARIOS, SIGNAL_MODES, signal_report
            scenario_options = lambda name: dict(SIGNAL_BASE, **SIGNAL_SCENARIOS[name])
        else:
            scenarios, modes, render = SCENARIOS, MODES, report
            scenario_options = lambda name: SCENARIOS[name]
        for mode in (MODES if args.suite == 'native' else ()):
            for workload in ('handle','lookup','miss'):
                opts = {'workload': workload, 'producers': 4}
                if mode != 'fixed':
                    opts['rotate-us'] = 500
                verification.append(verified(mode, opts, 10000))
            verification.append(verified(mode, {'workload': 'miss', 'cardinality': 4096,
                                                   'capacity': 16, 'rate': 0}, 20000))
            row = verified(mode, {'workload': 'miss', 'drop-buffer-every': 7}, 20000)
            if not row['discarded_after_acceptance'] or not row['committed'] or not row['definitions']:
                raise RuntimeError('must verify dropped introducer buffers and surviving definitions')
            verification.append(row)
            if mode != 'fixed':
                for fault in ('stall-ms', 'pause-ms'):
                    row = verified(mode, {'producers': 4, 'rotate-us': 500, fault: 50}, 20000)
                    if row['rotations'] < 2 or not row['rotation_deferred_polls']:
                        raise RuntimeError('must exercise deferred rotation and recovery')
                    if fault == 'pause-ms' and not row['post_pause_accepted']:
                        raise RuntimeError('paused producer failed to recover')
                    verification.append(row)
            run(binary, mode, {'rate': 0}, 50000)
        jobs = [(repeat, name, mode) for repeat in range(args.repeats)
                for name, opts in scenarios.items() for mode in modes
                if not (mode=='fixed' and opts.get('rotate-us'))]
        if args.suite == 'signal':  # stack mode varies; the normal lane stays generation
            run_mode = lambda mode, name: ('generation', dict(scenario_options(name), **{'stack-mode': mode}))
        else:
            run_mode = lambda mode, name: (mode, scenario_options(name))
        random.Random(20261004).shuffle(jobs)
        for index, (repeat, name, mode) in enumerate(jobs):
            print(f'{index+1}/{len(jobs)}: {name} {mode} repeat {repeat+1}', flush=True)
            row = run(binary, *run_mode(mode, name), args.events)
            row.update(scenario=name, repeat=repeat)
            rows.append(row)
    mode_key = 'stack_mode' if args.suite == 'signal' else 'mode'
    rows.sort(key=lambda r:(list(scenarios).index(r['scenario']),modes.index(r[mode_key]),r['repeat']))
    output = {'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'platform':platform.platform(),'logical_cpus':os.cpu_count(),'compiler':compiler,
              'source_sha256':source_sha,'metadata_sha256':metadata_sha,
              'runner_sha256':runner_sha,'suite':args.suite,
              'build_flags':flags,'events':args.events,'repeats':args.repeats,'shuffle_seed':20261004,
              'verification_rows':verification,'native_output_checks':native_checks,'rows':rows}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(output,indent=2)+'\n')
    args.output.with_suffix('.md').write_text(render(rows))
    print(render(rows))

if __name__ == '__main__':
    main()
