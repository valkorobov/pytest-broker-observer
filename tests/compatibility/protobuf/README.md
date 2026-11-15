# Generated Protobuf compatibility fixtures

These fictional schemas are test inputs. Their `*_pb2.py` files are unmodified
output from real `grpc_tools.protoc` invocations; they are not hand-built dynamic
messages. Separate filenames and Protobuf package names prevent descriptor-pool
collisions when multiple generations load in one process.

| Fixture | Generator | protoc | Python gencode | Expected runtime combinations |
| --- | --- | --- | --- | --- |
| `v526` | `grpcio-tools==1.64.1` | 26.1 | 5.26.1 | Minimum 5.29 and locked 7.36.2 |
| `v528` | `grpcio-tools==1.68.1` | 28.1 | 5.28.1 | Minimum 5.29 and locked 7.36.2 |
| `current` | `grpcio-tools==1.85.1` | 35.1 | 7.35.1 | Accepted by locked 7.36.2; explicitly rejected by minimum 5.29 |

`test_protobuf_generated.py` checks content hashes and provenance, real decoder
presence/invalid-payload behavior, explicit type resolution, and independent
copies through both blocking and async facades. The newer-code/older-runtime
cell asserts `VersionError`; it does not skip, xfail, alter generated code, or
disable the upstream version guard. The other two generations run on every
minimum/current profile.

The selected pairs follow Protobuf's documented
[cross-version runtime contract](https://protobuf.dev/support/cross-version-runtime-guarantee/).
This is a representative matrix, not a claim that every generator/runtime pair
works. Runtime and generated-code versions are distinct from protoc's release
number; see [version support](https://protobuf.dev/support/version-support/).

## Reproduction

On November 8, 2026, generation ran in separate tool environments on macOS arm64.
The two older generators used CPython 3.9.6 because their published wheels predate
Python 3.14; the current generator used CPython 3.14.7. These are generation tools,
not a change to the library's supported Python 3.11–3.14 runtime range. Each JSON
sidecar records the actual generator interpreter/platform, compiler, generation
runtime, command, and source/output SHA-256. The generator packages came from PyPI.
CI tests the checked-in bytes and does not install code-generation tools into the
candidate's runtime environment.

Create an isolated environment with an interpreter supported by the chosen tool,
then install and invoke the pinned generator. For example:

```sh
uv venv --python 3.11 /tmp/observer-protoc526
uv pip install --python /tmp/observer-protoc526/bin/python 'grpcio-tools==1.64.1'
/tmp/observer-protoc526/bin/python tools/regenerate_protobuf_fixtures.py v526
```

Repeat in separate environments with `grpcio-tools==1.68.1` / `v528` and
`grpcio-tools==1.85.1` / `current`. Select an interpreter supported by the pinned
tool (3.11 works for all three). On Windows, use the environment's
`Scripts/python.exe` path. The generator script rejects a different generator
version. Regeneration may change the recorded tool environment; review both the
sidecar and byte-for-byte generated output before committing. Do not format or
edit generated Python files.
