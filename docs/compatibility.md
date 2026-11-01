# Candidate 0.1.0 compatibility

The code supports Python 3.11–3.14. Local verification was performed on
CPython 3.14.7, macOS arm64. CI defines a 3.11–3.14 matrix on Linux;
the presence of a workflow does not mean that this matrix has passed.

Application dependencies are optional. The main runtime and sync facade use
only the stdlib. Extension code imports Kafka, Protobuf, and Allure lazily.
The passive pytest plugin does not open connections or change the asyncio mode.

The specific development environment is pinned in `uv.lock`. The declared
lower bounds for extras are still candidates: full verification of minimum
versions, all platforms, and generated-code/runtime combinations remains a
release gate. A broad metadata range should not be treated as the result of
such verification.

Current local versions and the status of real Kafka tests are recorded in
[status.md](status.md). Additional Kafka conditions are described in [kafka.md](kafka.md).

`max_bytes` is a deterministic estimate of retained data, not an RSS limit.
It includes raw payload/key/UTF-8 source lengths, 128 bytes for the envelope,
32 bytes per header/metadata pair, encoded metadata size, a decoded payload
estimate, and 128 bytes per record. The Protobuf estimate uses an adjusted
wire size and is not equivalent to native heap size. Native queues, snapshots,
and user results are accounted for separately from the retained journal.

One active sync watcher has one runtime thread; Kafka adds one serialized
polling worker. An arbitrary blocking callback cannot be forcibly interrupted.
If cleanup hangs, an incomplete/abandoned outcome is recorded; the library
does not claim to have forcibly stopped the thread.

Temporal assertions, reconnect/checkpoint recovery, and other brokers are
deferred. `wait_for` checks do not prove ordering, the absence of future events,
exactly-once delivery, or a global order across Kafka partitions.
