---
id: find-age-limited-drain
title: Idle producers' partial buffers are published by the background drain under an age limit
kind: finding
tags: [writer, publication, buffers]
applies_to: [benchmarks/, doc/writer-design.md]
source: jfr-spec/find-age-limited-drain
status: active
recorded: 2026-11-20
valid_at: 2026-11-20
---


An idle producer's partly filled buffer must still become visible: the
background writer publishes it, with an age limit so a recently started
buffer is not flushed prematurely. Partial publication goes into the open
chunk without rotation or pool resets. Evidence: producer-side opportunistic
flushing cannot serve idle producers and consumes ring slots; a
committed-prefix background drain bounds visibility near the age limit
without repeating definitions. Open: the limit's value/configurability,
whether any producer-side flush remains, the production mechanism
(committed-prefix reads vs background sealing), and the interaction with
live-header publication and LOSS/CLEAR markers.
