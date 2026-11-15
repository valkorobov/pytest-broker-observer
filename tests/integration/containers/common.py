"""Docker fixtures for isolated integration runs."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
import time
from typing import TypeVar
from uuid import uuid4


_LOOPBACK = "127.0.0.1"
_OWNER_LABEL = "org.pytest-broker-observer.fixture-owner"
_LOG_LIMIT = 32 * 1024
_Ready = TypeVar("_Ready")


@dataclass(frozen=True)
class BrokerService:
    """Test process environment and publishable evidence for one owned broker."""

    env: dict[str, str] = field(repr=False)
    metadata: dict[str, object]


def loopback_ports(container, *ports: int) -> None:
    """Ask Docker to allocate host ports bound only to IPv4 loopback."""
    for port in ports:
        # Docker SDK accepts (address, None) for an ephemeral host binding.
        # Testcontainers' with_bind_ports() only accepts a host port number.
        container.ports[f"{port}/tcp"] = (_LOOPBACK, None)


def mapped_port(container, port: int) -> int:
    """Read an assigned loopback port and reject exposure on other interfaces."""
    wrapped = container.get_wrapped_container()
    wrapped.reload()
    bindings = wrapped.attrs["NetworkSettings"]["Ports"].get(f"{port}/tcp")
    if not bindings or len(bindings) != 1 or bindings[0]["HostIp"] != _LOOPBACK:
        raise RuntimeError(f"Container port {port} has no unique IPv4 loopback binding")
    result = int(bindings[0]["HostPort"])
    if not 0 < result <= 65535:
        raise RuntimeError(f"Container port {port} has no assigned host port")
    return result


def container_metadata(container, image: str) -> dict[str, object]:
    """Record image identity from Docker without dumping container configuration."""
    wrapped = container.get_wrapped_container()
    wrapped.reload()
    actual = wrapped.image
    manifest = wrapped.attrs.get("ImageManifestDescriptor") or {}
    return {
        "image": image,
        "image_id": actual.id,
        "os": actual.attrs["Os"],
        "architecture": actual.attrs["Architecture"],
        "manifest_digest": manifest.get("Digest"),
    }


def wait_until_ready(
    description: str,
    probe: Callable[[float], _Ready | None],
    *,
    timeout: float = 90,
) -> _Ready:
    """Retry expected startup failures until a finite deadline."""
    deadline = time.monotonic() + timeout
    while (remaining := deadline - time.monotonic()) > 0:
        result = probe(min(remaining, 2))
        if result is not None:
            return result
        time.sleep(min(0.25, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"{description} did not become ready within {timeout:g} seconds")


def redact_text(text: str, redactions: tuple[str, ...]) -> str:
    """Remove known synthetic credentials before retaining logs."""
    for value in sorted(set(redactions) - {""}, key=len, reverse=True):
        text = text.replace(value, "[redacted]")
    return text


def _write_log(wrapped, path: Path, redactions: tuple[str, ...]) -> None:
    """Keep a bounded excerpt of the daemon's container log tail."""
    stream = wrapped.logs(stream=True, follow=False, tail=120, timestamps=True)
    bounded = False
    try:
        excerpt = bytearray()
        for chunk in stream:
            excerpt.extend(chunk[: _LOG_LIMIT + 1024 - len(excerpt)])
            if len(excerpt) >= _LOG_LIMIT + 1024:
                bounded = True
                break
    finally:
        stream.close()
    sanitized = redact_text(excerpt.decode("utf-8", errors="replace"), redactions).encode("utf-8")
    truncated = bounded or len(sanitized) > _LOG_LIMIT
    marker = b"\n[log excerpt truncated]\n" if truncated else b""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(sanitized[: _LOG_LIMIT - len(marker)] + marker)


def _find_owned_container(container, name: str, owner: str):
    """Find a container created before Testcontainers could attach its wrapper."""
    from docker.errors import NotFound
    from testcontainers.core.exceptions import ContainerStartException

    try:
        wrapped = container.get_wrapped_container()
    except ContainerStartException:
        wrapped = None
    if wrapped is not None:
        return wrapped
    # Testcontainers 4.9 uses Docker's create-and-start helper. That helper
    # can raise on start before Testcontainers records the created object.
    try:
        wrapped = container.get_docker_client().client.containers.get(name)
    except NotFound:
        return None
    return wrapped if wrapped.labels.get(_OWNER_LABEL) == owner else None


@contextmanager
def running_container(
    container,
    *,
    log_path: Path,
    redactions: tuple[str, ...] = (),
) -> Iterator[None]:
    """Start, capture logs, and remove the owned container after partial or full startup."""
    owner = uuid4().hex
    name = f"pbo-stage3-{owner}"
    # with_kwargs replaces the mapping in both supported Testcontainers releases.
    # Preserve existing Docker arguments, including explicit platform selection.
    options = dict(container._kwargs)
    options["labels"] = {**options.get("labels", {}), _OWNER_LABEL: owner}
    container.with_name(name).with_kwargs(**options)
    primary = None
    try:
        try:
            container.start()
        except Exception as error:
            # Docker errors can contain the full container environment or command.
            raise RuntimeError(
                f"Could not start {log_path.stem} container ({type(error).__name__})"
            ) from None
        yield
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            wrapped = _find_owned_container(container, name, owner)
            if wrapped is not None:
                try:
                    _write_log(wrapped, log_path, redactions)
                finally:
                    wrapped.remove(force=True, v=True)
        except Exception as error:
            failure = (
                f"Could not capture logs or remove owned {log_path.stem} container ({type(error).__name__})"
            )
            if primary is None:
                raise RuntimeError(failure) from None
            primary.add_note(failure)
        finally:
            container.get_docker_client().client.close()
