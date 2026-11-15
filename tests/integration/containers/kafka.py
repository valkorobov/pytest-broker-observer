"""Start a private Kafka KRaft broker with one host route through Toxiproxy."""

from base64 import urlsafe_b64encode
from contextlib import contextmanager
import json
from pathlib import Path
import re
from secrets import token_hex
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

from .common import (
    BrokerService,
    container_metadata,
    fault_container_env,
    loopback_ports,
    mapped_port,
    running_container,
    wait_until_ready,
)


_CONTROL_PORT = 8474
_PROXY_PORT = 8666
_EXTERNAL_PORT = 9092
_KAFKA_ALIAS = "kafka"


def _proxy_request(control_url, path, timeout, data=None):
    """Call the loopback control plane with a bounded response."""
    body = json.dumps(data).encode() if data is not None else None
    request = Request(control_url + path, data=body, headers={"Content-Type": "application/json"})
    with build_opener(ProxyHandler({})).open(request, timeout=timeout) as response:
        return json.loads(response.read(16384))


def _proxy_version(control_url, timeout):
    """Return the reported version once the local control socket is listening."""
    try:
        version = _proxy_request(control_url, "/version", timeout)["version"]
    except HTTPError:
        raise RuntimeError("Toxiproxy returned an unexpected readiness status") from None
    except (TimeoutError, URLError):
        return None
    return version


def _kraft_environment(servers):
    """Configure the Apache image's own combined broker/controller startup scripts."""
    return {
        "CLUSTER_ID": urlsafe_b64encode(uuid4().bytes).decode("ascii").rstrip("="),
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": f"INTERNAL://:29092,EXTERNAL://:{_EXTERNAL_PORT},CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"INTERNAL://{_KAFKA_ALIAS}:29092,EXTERNAL://{servers}",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@{_KAFKA_ALIAS}:9093",
        "KAFKA_LOG_DIRS": "/tmp/kraft-combined-logs",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_HEAP_OPTS": "-Xms128m -Xmx512m",
    }


def _wait_for_advertised_route(servers, options):
    """Validate real metadata through the proxy before allowing contract tests."""
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": servers, "log_level": 0, **options})

    def probe(timeout):
        try:
            metadata = admin.list_topics(timeout=timeout)
        except KafkaException:
            return None
        return metadata if metadata.controller_id in metadata.brokers else None

    metadata = wait_until_ready("Kafka metadata through Toxiproxy", probe, timeout=100)
    endpoints = {f"{broker.host}:{broker.port}" for broker in metadata.brokers.values()}
    if endpoints != {servers}:
        raise RuntimeError("Kafka metadata advertised a route other than the isolated proxy")


def _broker_version(container):
    """Read the actual Kafka runtime version from its packaged CLI."""
    result = container.exec_run(["/opt/kafka/bin/kafka-topics.sh", "--version"])
    version = re.search(rb"^([0-9]+\.[0-9]+\.[0-9]+)(?:\s|$)", result.output, re.MULTILINE)
    if result.exit_code or version is None:
        raise RuntimeError("Could not determine the isolated Kafka runtime version")
    return version[1].decode("ascii")


def _security_settings(admin_password, reader_password):
    """Configure SASL and broker ACLs on the private external listener."""
    return {
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "INTERNAL:PLAINTEXT,EXTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_LISTENER_NAME_EXTERNAL_SASL_ENABLED_MECHANISMS": "PLAIN",
        "KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'user_observer_admin="{admin_password}" user_observer_reader="{reader_password}";'
        ),
        "KAFKA_AUTHORIZER_CLASS_NAME": "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
        "KAFKA_SUPER_USERS": "User:ANONYMOUS;User:observer_admin",
    }


@contextmanager
def kafka_service(
    image: str, toxiproxy_image: str, *, log_dir: Path, faults: bool = False, secure: bool = False
):
    """Expose just the proxy control and data ports and own the Docker resources."""
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.network import Network

    with Network() as network:
        proxy = DockerContainer(toxiproxy_image).with_network(network)
        loopback_ports(proxy, _CONTROL_PORT, _PROXY_PORT)
        with running_container(proxy, log_path=log_dir / "toxiproxy.log"):
            control_url = f"http://127.0.0.1:{mapped_port(proxy, _CONTROL_PORT)}"
            servers = f"127.0.0.1:{mapped_port(proxy, _PROXY_PORT)}"
            proxy_version = wait_until_ready(
                "Toxiproxy control API", lambda timeout: _proxy_version(control_url, timeout)
            )
            proxy_name = f"observer-kafka-{uuid4().hex}"
            upstream = f"{_KAFKA_ALIAS}:{_EXTERNAL_PORT}"
            listen = f"0.0.0.0:{_PROXY_PORT}"
            route = {"name": proxy_name, "listen": listen, "upstream": upstream, "enabled": True}
            try:
                created = _proxy_request(control_url, "/proxies", 5, route)
            except (TimeoutError, URLError):
                raise RuntimeError("Could not create the private Kafka proxy route") from None
            if (
                created.get("name") != proxy_name
                or created.get("upstream") != upstream
                or created.get("listen") not in {listen, f"[::]:{_PROXY_PORT}"}
                or created.get("enabled") is not True
            ):
                raise RuntimeError("Toxiproxy created an unexpected Kafka route")

            # KafkaContainer uses distribution-specific startup scripts. The
            # generic container preserves the pinned Apache image's entrypoint.
            kafka = DockerContainer(image).with_network(network).with_network_aliases(_KAFKA_ALIAS)
            environment = _kraft_environment(servers)
            credentials = {}
            admin_options = {}
            if secure:
                admin_password, reader_password = token_hex(16), token_hex(16)
                environment.update(_security_settings(admin_password, reader_password))
                admin_options = {
                    "security.protocol": "SASL_PLAINTEXT",
                    "sasl.mechanism": "PLAIN",
                    "sasl.username": "observer_admin",
                    "sasl.password": admin_password,
                }
                credentials = {
                    "EVENT_OBSERVER_KAFKA_ADMIN_PASSWORD": admin_password,
                    "EVENT_OBSERVER_KAFKA_READER_PASSWORD": reader_password,
                    "EVENT_OBSERVER_FAULT_PASSWORD": token_hex(16),
                }
            for key, value in environment.items():
                kafka.with_env(key, value)
            with running_container(
                kafka,
                log_path=log_dir / "kafka.log",
                broker="kafka" if faults else None,
                redactions=tuple(credentials.values()),
            ):
                wrapped = kafka.get_wrapped_container()
                wrapped.reload()
                if any(wrapped.attrs["NetworkSettings"]["Ports"].values()):
                    raise RuntimeError("The isolated Kafka broker has unexpected published ports")
                _wait_for_advertised_route(servers, admin_options)
                metadata = container_metadata(kafka, image)
                metadata.update(
                    {
                        "version": _broker_version(wrapped),
                        "kraft": True,
                        "published_kafka_ports": [],
                        "advertised_endpoint": servers,
                        "external_security_protocol": admin_options.get("security.protocol", "PLAINTEXT"),
                        "toxiproxy": {**container_metadata(proxy, toxiproxy_image), "version": proxy_version},
                    }
                )
                yield BrokerService(
                    {
                        "KAFKA_BOOTSTRAP_SERVERS": servers,
                        "EVENT_OBSERVER_TOXIPROXY_URL": control_url,
                        "EVENT_OBSERVER_TOXIPROXY_PROXY": proxy_name,
                        **credentials,
                        **(fault_container_env(kafka, "kafka") if faults else {}),
                    },
                    metadata,
                )
