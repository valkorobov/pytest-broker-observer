# Security

This project is an MVP candidate. Supported and actually verified environments
are listed in the [compatibility matrix](docs/compatibility.md); release readiness
is described in the [status document](docs/status.md).

## Report a vulnerability

Use the repository's private security advisory feature, if enabled, or an existing
private contact channel with the maintainer. Public issues are suitable for reports
that omit credentials, personal information, and exploitation details for an
unresolved vulnerability. Include Python and dependency versions, a minimal
synthetic example, and safe diagnostics. A dedicated security-reporting address
has not yet been established.

## Credentials and reports

Pass transport configuration programmatically; obtain secrets from your test
project's environment or secret store. Do not include credentials, production
messages, or private schemas in fixtures, examples, or attachments.

Diagnostic export uses `METADATA_ONLY` by default: payload, key, headers,
expected/actual values, arbitrary descriptions, and trace IDs are not disclosed.
Additional disclosure requires explicit settings. A redactor works on a copy;
failure does not permit falling back to the original content. See the
[diagnostics guide](docs/diagnostics.md).

`watcher.snapshot()` is an event API that contains payload copies; it is not a
metadata-only export. Python exception chaining and third-party loggers can also
contain original client errors. The library's policy does not control external
tracebacks or logging handlers.

## Protection boundaries

Journal, queue, expectation, and diagnostic budgets bound the library's own
accounted state. They do not cap process RSS or the client's native heap. The hard
single-event limit is checked before decoding.

User decoders, matchers, predicates, and reporters are trusted Python code.
Payload isolation protects canonical history from ordinary mutation; it does not
sandbox hostile code in a separate process. A blocking callback cannot be forcibly
stopped at its deadline.

The Kafka observer uses an independent reader and does not commit to the
application's consumer group. Infrastructure owners configure permissions,
TLS/SASL, and test-broker isolation. Real-broker tests create and delete test topics.
Trace context is extracted passively; the library does not create spans or
propagate baggage automatically.
