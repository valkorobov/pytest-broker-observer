"""Docker fixtures for isolated integration runs."""

from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass, field
import os
from pathlib import Path
import re
import socket
import time
from typing import TypeVar
from uuid import uuid4


_LOOPBACK = "127.0.0.1"
_OWNER_LABEL = "org.pytest-broker-observer.fixture-owner"
_BROKER_LABEL = "org.pytest-broker-observer.fixture-broker"
_LOG_LIMIT = 32 * 1024
_Ready = TypeVar("_Ready")


@dataclass(frozen=True)
class BrokerService:
    """Test process environment and publishable evidence for one owned broker."""

    env: dict[str, str] = field(repr=False)
    metadata: dict[str, object]


def loopback_ports(container, *ports: int, restartable: bool = False) -> None:
    """Ask Docker to allocate host ports bound only to IPv4 loopback."""
    if restartable:
        # Docker can reassign daemon-selected ports on restart. Request explicit
        # available ports for the destructive profiles that require stable endpoints.
        with ExitStack() as reservations:
            for port in ports:
                endpoint = reservations.enter_context(socket.socket())
                endpoint.bind((_LOOPBACK, 0))
                container.ports[f"{port}/tcp"] = (_LOOPBACK, endpoint.getsockname()[1])
        return
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


def fault_container_env(container, broker: str) -> dict[str, str]:
    """Pass the exact identity of the isolated fixture to its fault-test process."""
    wrapped = container.get_wrapped_container()
    wrapped.reload()
    owner = wrapped.labels.get(_OWNER_LABEL, "")
    if wrapped.labels.get(_BROKER_LABEL) != broker or wrapped.name != f"pbo-stage3-{owner}":
        raise RuntimeError("Fault controls require a uniquely owned broker fixture")
    return {
        "EVENT_OBSERVER_FAULT_CONTAINER_ID": wrapped.id,
        "EVENT_OBSERVER_FAULT_CONTAINER_TOKEN": owner,
    }


@contextmanager
def _owned_broker(broker: str):
    """Limit destructive fault operations to the container started for this profile."""
    import docker

    container_id = os.environ.get("EVENT_OBSERVER_FAULT_CONTAINER_ID", "")
    owner = os.environ.get("EVENT_OBSERVER_FAULT_CONTAINER_TOKEN", "")
    if (
        broker not in {"kafka", "rabbitmq", "redis-streams"}
        or os.environ.get("EVENT_OBSERVER_FAULTS") != broker
        or not re.fullmatch(r"[a-f0-9]{64}", container_id)
        or not re.fullmatch(r"[a-f0-9]{32}", owner)
    ):
        raise RuntimeError("Fault controls require an explicit isolated broker profile")
    with closing(docker.from_env(timeout=30)) as client:
        wrapped = client.containers.get(container_id)
        if (
            wrapped.id != container_id
            or wrapped.name != f"pbo-stage3-{owner}"
            or wrapped.labels.get(_OWNER_LABEL) != owner
            or wrapped.labels.get(_BROKER_LABEL) != broker
            or wrapped.status != "running"
        ):
            raise RuntimeError("Fault controls could not verify the owned running broker")
        yield wrapped


def restart_owned_broker(broker: str) -> None:
    """Abruptly restart the isolated server while preserving its container identity."""
    with _owned_broker(broker) as wrapped:
        wrapped.restart(timeout=0)


def verify_owned_broker_ports(broker: str, ports: dict[int, int]) -> None:
    """Require native/admin test endpoints to belong to the labeled Docker fixture."""
    with _owned_broker(broker) as wrapped:
        exposed = wrapped.attrs["NetworkSettings"]["Ports"]
        for container_port, host_port in ports.items():
            if exposed.get(f"{container_port}/tcp") != [{"HostIp": _LOOPBACK, "HostPort": str(host_port)}]:
                raise RuntimeError("Fault endpoint does not belong to the owned broker")


@contextmanager
def paused_owned_broker(broker: str) -> Iterator[None]:
    """Blackhole protocol replies by pausing one owned process and always resume it."""
    with _owned_broker(broker) as wrapped:
        wrapped.pause()
        primary = None
        try:
            yield
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                wrapped.unpause()
            except Exception as error:
                failure = f"Could not resume the owned broker ({type(error).__name__})"
                if primary is None:
                    raise RuntimeError(failure) from None
                primary.add_note(failure)


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
    broker: str | None = None,
) -> Iterator[None]:
    """Start, capture logs, and remove the owned container after partial or full startup."""
    owner = uuid4().hex
    name = f"pbo-stage3-{owner}"
    # with_kwargs replaces the mapping in both supported Testcontainers releases.
    # Preserve existing Docker arguments, including explicit platform selection.
    options = dict(container._kwargs)
    options["labels"] = {**options.get("labels", {}), _OWNER_LABEL: owner}
    if broker is not None:
        options["labels"][_BROKER_LABEL] = broker
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
