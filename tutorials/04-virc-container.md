# Tutorial 4 — the VIRC container

*What you will get: a VIRC file holding a native stream and a foreign
stream side by side, extracted byte-exactly — plus the truncation and
corruption behavior. Prerequisites: Python 3 and the writer build from
[03-writing-virl.md](03-writing-virl.md). ~15 minutes.*

## Pack streams into a container

The reference container implementation is `validators/virel_container.py`.
Collect two streams — a native VIRL file and, to show that the container is
format-agnostic, a plain text stream under a private `x-` format id:

```sh
printf 'hello from a private stream\n' > /tmp/notes.txt
python3 -B validators/virel_container.py collect /tmp/demo.virc \
  virel:/tmp/demo.virl x-notes:/tmp/notes.txt
```

`collect` accepts any `FORMAT:PATH` pairs; format ids are the registry of
spec §3.4.1 (`virel`, `jfr`, `go-trace`, `perfetto`, `otlp-traces`, ...),
and `x-...` ids are private. Real collectors pack a JVM's `.jfr`, a Go
trace and native streams the same way — see gate 6, which does exactly that
with real inputs.

## Look inside

```sh
python3 -B validators/virel_container.py ls /tmp/demo.virc
```

```
container id 0xb71fbdb7b0902c7 clean=True records=6 damaged=[]
  stream 1: format=virel framing=stream records=1 bytes=9245 complete=True gaps=[] errors=[]
  stream 2: format=x-notes framing=stream records=1 bytes=28 complete=True gaps=[] errors=[]
```

Each stream got an id, a format label and a completeness verdict. The
container did not parse a single payload byte to produce this.

## Extract byte-exactly

```sh
python3 -B validators/virel_container.py extract /tmp/demo.virc 2 /tmp/out.txt
cmp /tmp/notes.txt /tmp/out.txt && echo byte-exact
```

```
byte-exact
```

Extraction of stream 1 returns the original `.virl` file, byte for byte —
which means the container is a zero-conversion migration path: an existing
producer's output becomes VIREL data by being framed, with no encoder
change. Extract stream 1 and re-read it with the chunk reader to confirm:

```sh
python3 -B validators/virel_container.py extract /tmp/demo.virc 1 /tmp/back.virl
python3 -B validators/tcf_reader.py /tmp/back.virl | head -2
```

## What damage looks like

Truncate the container mid-stream:

```sh
head -c 6000 /tmp/demo.virc > /tmp/trunc.virc
python3 -B validators/virel_container.py ls /tmp/trunc.virc
```

```
container id ... clean=False records=3 damaged=[]
  stream 1: format=virel framing=stream records=1 bytes=5717 complete=False gaps=[] errors=[]
  stream 2: format=x-notes framing=stream records=0 bytes=0 complete=False gaps=[] errors=[]
```

Corrupt two payload bytes:

```sh
python3 - <<'PY'
d = bytearray(open('/tmp/demo.virc','rb').read()); d[2000:2002] = b'XX'
open('/tmp/corrupt.virc','wb').write(bytes(d))
PY
python3 -B validators/virel_container.py ls /tmp/corrupt.virc
```

```
  ! payload CRC mismatch: record @227 stream 1 seq 0
  stream 1: ... complete=False ... errors=[]
```

The verdicts are precise: a truncated container is not clean and its
streams are incomplete; a CRC mismatch does not hide the record — it is
reported and the stream is marked not byte-exact. The container-level
recovery scan (finding the next valid record after damage) is exercised by
gate 6.

## What you verified

- one container, heterogeneous streams, no format special-casing;
- byte-exact extraction, verified with `cmp`;
- damage is detected and reported, never silently dropped.

Next: [05-glossary.md](05-glossary.md), or the spec's container chapter
([../doc/TraceContainerFormat.md](../doc/TraceContainerFormat.md) §3).
