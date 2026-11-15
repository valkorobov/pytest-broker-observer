"""Isolated RabbitMQ and Redis fixtures using native readiness probes."""

import base64
from collections.abc import Iterator
from contextlib import contextmanager
import json
from pathlib import Path
from secrets import token_hex
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from tests.integration.containers.common import (
    BrokerService,
    container_metadata,
    loopback_ports,
    mapped_port,
    running_container,
    wait_until_ready,
)


@contextmanager
def rabbitmq_service(image: str, *, log_dir: Path, timeout: float = 90) -> Iterator[BrokerService]:
    """Start authenticated AMQP and management listeners for the same RabbitMQ node."""
    import pika
    from testcontainers.core.container import DockerContainer

    username = f"observer_{token_hex(8)}"
    password = token_hex(16)
    token = base64.b64encode(f"{username}:{password}".encode("ascii")).decode("ascii")
    container = DockerContainer(image, docker_client_kw={"timeout": 30})
    container.with_env("RABBITMQ_DEFAULT_USER", username)
    container.with_env("RABBITMQ_DEFAULT_PASS", password)
    loopback_ports(container, 5672, 15672)
    with running_container(
        container, log_path=log_dir / "rabbitmq.log", redactions=(username, password, token)
    ):
        amqp_port, management_port = (mapped_port(container, port) for port in (5672, 15672))
        management = f"http://127.0.0.1:{management_port}"
        opener = build_opener(ProxyHandler({}))
        request = Request(f"{management}/api/overview", headers={"Authorization": f"Basic {token}"})

        def probe(budget: float) -> str | None:
            try:
                with opener.open(request, timeout=budget / 2) as response:
                    observed = json.load(response)
            except HTTPError as error:
                if error.code in (401, 403):
                    raise RuntimeError("RabbitMQ management rejected fixture authentication") from None
                return None
            except (URLError, OSError):
                return None
            parameters = pika.ConnectionParameters(
                host="127.0.0.1",
                port=amqp_port,
                credentials=pika.PlainCredentials(username, password),
                connection_attempts=1,
                socket_timeout=budget / 2,
                stack_timeout=budget / 2,
            )
            try:
                with pika.BlockingConnection(parameters):
                    return observed["rabbitmq_version"]
            except (pika.exceptions.ProbableAuthenticationError, pika.exceptions.ProbableAccessDeniedError):
                raise RuntimeError("RabbitMQ AMQP rejected fixture authentication") from None
            except (pika.exceptions.AMQPConnectionError, OSError):
                return None

        version = wait_until_ready("RabbitMQ AMQP and management", probe, timeout=timeout)
        yield BrokerService(
            env={
                "RABBITMQ_URL": f"amqp://{username}:{password}@127.0.0.1:{amqp_port}/",
                "RABBITMQ_MANAGEMENT_URL": management,
            },
            metadata={**container_metadata(container, image), "version": version},
        )


@contextmanager
def redis_service(image: str, *, log_dir: Path, timeout: float = 90) -> Iterator[BrokerService]:
    """Start authenticated ephemeral Redis, including its actual server version."""
    import redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry
    from testcontainers.core.container import DockerContainer

    password = token_hex(16)
    container = DockerContainer(image, docker_client_kw={"timeout": 30})
    container.with_command(["redis-server", "--save", "", "--appendonly", "no", "--requirepass", password])
    loopback_ports(container, 6379)
    with running_container(container, log_path=log_dir / "redis.log", redactions=(password,)):
        port = mapped_port(container, 6379)

        def probe(budget: float) -> str | None:
            try:
                with redis.Redis(
                    host="127.0.0.1",
                    port=port,
                    password=password,
                    protocol=2,
                    socket_connect_timeout=budget / 3,
                    socket_timeout=budget / 3,
                    retry=Retry(NoBackoff(), 0),
                ) as client:
                    if client.ping():
                        return client.info("server")["redis_version"]
            except redis.exceptions.AuthenticationError:
                raise RuntimeError("Redis rejected fixture authentication") from None
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                return None
            return None

        version = wait_until_ready("Redis RESP", probe, timeout=timeout)
        yield BrokerService(
            env={"REDIS_URL": f"redis://:{password}@127.0.0.1:{port}/0"},
            metadata={**container_metadata(container, image), "version": version},
        )
