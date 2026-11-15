"""Broker-free Kafka harness contracts for receipts, ownership, and failed cleanup."""

import asyncio
from dataclasses import dataclass
import importlib
import re
import threading
from types import SimpleNamespace

import pytest

from event_observer.errors import ConfigurationError, MissingOptionalDependencyError, TransportError
from event_observer.models import TransportSource
from event_observer.testing import KafkaAdapterHarness


@pytest.fixture
def client(monkeypatch):
    state = SimpleNamespace(
        imports=[],
        admin_configs=[],
        producer_configs=[],
        topics=set(),
        created=[],
        delete_requests=[],
        deleted=[],
        publications=[],
        native_threads=[],
        future_timeouts=[],
        metadata_timeouts=[],
        metadata_steps=[],
        flush_timeouts=[],
        create_error=None,
        create_callback=None,
        delete_error=None,
        delete_errors={},
        producer_error=None,
        produce_error=None,
        flush_error=None,
        pending=0,
        delivery_count=1,
        delivery_error=None,
        missing_client=False,
    )

    class NativeError:
        UNKNOWN_TOPIC_OR_PART = 3
        LEADER_NOT_AVAILABLE = 5

        def __init__(self, code):
            self._code = code

        def code(self):
            return self._code

    class Future:
        def __init__(self, error=None):
            self.error = error

        def result(self, *, timeout):
            state.native_threads.append(threading.get_ident())
            state.future_timeouts.append(timeout)
            if self.error is not None:
                raise self.error

    @dataclass
    class NewTopic:
        topic: str
        num_partitions: int
        replication_factor: int

    class Admin:
        def __init__(self, config):
            state.native_threads.append(threading.get_ident())
            state.admin_configs.append(dict(config))

        def create_topics(self, topics, **options):
            state.native_threads.append(threading.get_ident())
            state.created.append((list(topics), options))
            # An acknowledgement can fail after the broker creates the topic.
            state.topics.update(topic.topic for topic in topics)
            if state.create_callback is not None:
                state.create_callback()
            return {topic.topic: Future(state.create_error) for topic in topics}

        def list_topics(self, *, timeout):
            state.native_threads.append(threading.get_ident())
            state.metadata_timeouts.append(timeout)
            topics = {
                topic.topic: SimpleNamespace(
                    error=None,
                    partitions={
                        pid: SimpleNamespace(error=None, leader=1) for pid in range(topic.num_partitions)
                    },
                )
                for specs, _ in state.created
                for topic in specs
                if topic.topic in state.topics
            }
            if state.metadata_steps:
                topics = state.metadata_steps.pop(0)(topics)
            return SimpleNamespace(topics=topics)

        def delete_topics(self, topics, **options):
            state.native_threads.append(threading.get_ident())
            state.delete_requests.append((list(topics), options))
            if state.delete_error is not None:
                raise state.delete_error
            futures = {}
            for topic in topics:
                error = state.delete_errors.get(topic)
                if error is None:
                    if topic in state.topics:
                        state.topics.remove(topic)
                        state.deleted.append(topic)
                    else:
                        error = RuntimeError(NativeError(NativeError.UNKNOWN_TOPIC_OR_PART))
                futures[topic] = Future(error)
            return futures

    class Delivery:
        def __init__(self, partition, offset):
            self._partition, self._offset = partition, offset

        def partition(self):
            return self._partition

        def offset(self):
            return self._offset

    class Producer:
        def __init__(self, config):
            state.native_threads.append(threading.get_ident())
            state.producer_configs.append(dict(config))
            if state.producer_error is not None:
                raise state.producer_error
            self.pending = []

        def produce(self, topic, *, value, key, headers, partition, on_delivery):
            state.native_threads.append(threading.get_ident())
            if state.produce_error is not None:
                raise state.produce_error
            state.publications.append((topic, value, key, headers, partition))
            self.pending.append((on_delivery, Delivery(partition, 40 + len(state.publications))))

        def flush(self, timeout):
            state.native_threads.append(threading.get_ident())
            state.flush_timeouts.append(timeout)
            if state.flush_error is not None:
                raise state.flush_error
            pending, self.pending = self.pending, []
            for callback, delivery in pending:
                for _ in range(state.delivery_count):
                    callback(state.delivery_error, delivery)
            return state.pending

    native = SimpleNamespace(Producer=Producer, KafkaError=NativeError, KafkaException=RuntimeError)
    admin = SimpleNamespace(AdminClient=Admin, NewTopic=NewTopic)
    original_import = importlib.import_module

    def import_client(name, package=None):
        if name in {"confluent_kafka", "confluent_kafka.admin"}:
            state.imports.append(name)
            if state.missing_client:
                raise ImportError("SECRET_OPTIONAL_CLIENT")
            return native if name == "confluent_kafka" else admin
        return original_import(name, package)

    monkeypatch.setattr("event_observer.testing.kafka.importlib.import_module", import_client)
    state.native_error = NativeError
    return state


def test_construction_and_empty_cleanup_do_not_import_or_connect_a_client(client):
    harness = KafkaAdapterHarness(("first:9092", "second:9092"))
    assert harness.adapter.config.bootstrap_servers == ("first:9092", "second:9092")
    assert not client.imports
    asyncio.run(harness.cleanup())
    asyncio.run(harness.cleanup())
    assert not client.imports
    assert not client.admin_configs
    assert not client.producer_configs


@pytest.mark.parametrize(
    "options",
    [
        {"partitions": False},
        {"partitions": 0},
        {"partitions": -1},
        {"partitions": 1.5},
        {"timeout": False},
        {"timeout": 0},
        {"timeout": -1},
        {"timeout": float("inf")},
        {"timeout": float("nan")},
        {"timeout": "15"},
    ],
)
def test_invalid_configuration_is_rejected_before_client_import(client, options):
    with pytest.raises(ConfigurationError):
        KafkaAdapterHarness("broker:9092", **options)
    assert not client.imports


@pytest.mark.asyncio
async def test_sources_are_unique_valid_owned_topics_with_bounded_admin_requests(client):
    harness = KafkaAdapterHarness(("first:9092", "second:9092"), partitions=2, timeout=0.25)
    first = await harness.create_source(case_id="a test / case: " + "x" * 300)
    second = await harness.create_source(case_id="a test / case: " + "x" * 300)
    assert first != second
    for source in (first, second):
        assert re.fullmatch(r"[A-Za-z0-9._-]+", source.name)
        assert source.name.startswith("observer-contract-")
        assert len(source.name.encode()) <= 249
    assert client.admin_configs == [{"bootstrap.servers": "first:9092,second:9092"}]
    assert client.imports == ["confluent_kafka", "confluent_kafka.admin"]
    for specs, options in client.created:
        assert specs[0].num_partitions == 2
        assert specs[0].replication_factor == 1
        assert options == {"operation_timeout": 0.25, "request_timeout": 0.25}
    assert len(client.future_timeouts) == len(client.metadata_timeouts) == 2
    assert all(0 < timeout <= 0.25 for timeout in client.future_timeouts + client.metadata_timeouts)
    await harness.cleanup()
    assert set(client.deleted) == {first.name, second.name}
    assert not client.topics
    assert set(client.native_threads).isdisjoint({threading.get_ident()})


@pytest.mark.asyncio
async def test_creation_waits_for_metadata_and_every_partition_leader(client, monkeypatch):
    def unknown_topic(topics):
        next(iter(topics.values())).error = client.native_error(3)
        return topics

    def leader_pending(topics):
        next(iter(topics.values())).partitions[1].leader = -1
        return topics

    monkeypatch.setattr("event_observer.testing.kafka.sleep", lambda seconds: None)
    client.metadata_steps = [lambda _: {}, unknown_topic, leader_pending]
    harness = KafkaAdapterHarness("broker:9092", partitions=2)
    source = await harness.create_source(case_id="metadata-propagation")
    assert source.name in client.topics
    assert len(client.created) == 1
    assert len(client.metadata_timeouts) == 4
    assert client.metadata_timeouts == sorted(client.metadata_timeouts, reverse=True)
    await harness.cleanup()


@pytest.mark.asyncio
async def test_post_creation_metadata_wait_shares_the_creation_deadline(client, monkeypatch):
    now = 0.0

    def sleep(seconds):
        nonlocal now
        now += seconds

    def missing_metadata(_):
        nonlocal now
        now += 0.03
        return {}

    monkeypatch.setattr("event_observer.testing.kafka.monotonic", lambda: now)
    monkeypatch.setattr("event_observer.testing.kafka.sleep", sleep)
    client.create_callback = lambda: sleep(0.06)
    client.metadata_steps = [missing_metadata]
    harness = KafkaAdapterHarness("broker:9092", timeout=0.1)
    with pytest.raises(TransportError, match="could not create") as caught:
        await harness.create_source(case_id="metadata-timeout")
    assert isinstance(caught.value.__cause__, TimeoutError)
    assert client.future_timeouts == pytest.approx([0.04])
    assert client.metadata_timeouts == pytest.approx([0.04])
    await harness.cleanup()
    assert not client.topics


@pytest.mark.asyncio
async def test_post_creation_metadata_refuses_nontransient_errors(client):
    denied = client.native_error(29)

    def denied_metadata(topics):
        next(iter(topics.values())).error = denied
        return topics

    client.metadata_steps = [denied_metadata]
    harness = KafkaAdapterHarness("broker:9092")
    with pytest.raises(TransportError, match="could not create") as caught:
        await harness.create_source(case_id="metadata-denied")
    assert caught.value.__cause__.args == (denied,)
    assert len(client.metadata_timeouts) == 1
    await harness.cleanup()


@pytest.mark.parametrize("case_id", ["", None, 42])
@pytest.mark.asyncio
async def test_invalid_case_id_cannot_start_an_admin_request(client, case_id):
    harness = KafkaAdapterHarness("broker:9092")
    with pytest.raises(ConfigurationError):
        await harness.create_source(case_id=case_id)
    assert not client.imports
    await harness.cleanup()


@pytest.mark.parametrize("payload", [None, b"", b"\x00\xff"])
def test_broker_confirmed_receipt_preserves_envelope_across_independent_loops(client, payload):
    harness = KafkaAdapterHarness("broker:9092", timeout=0.25)
    source = asyncio.run(harness.create_source(case_id="publisher"))
    headers = (("duplicate", b"first"), ("duplicate", None), ("binary", b"\x00\xff"))
    receipt = asyncio.run(harness.publish(source, payload, key=b"\x00key", headers=headers))
    assert receipt.accepted
    assert receipt.metadata == {"kafka.partition": 0, "kafka.offset": 41}
    assert client.publications == [(source.name, payload, b"\x00key", list(headers), 0)]
    assert client.producer_configs == [{"bootstrap.servers": "broker:9092", "message.timeout.ms": 250}]
    assert client.flush_timeouts == [0.25]
    asyncio.run(harness.cleanup())
    assert client.deleted == [source.name]
    assert set(client.native_threads).isdisjoint({threading.get_ident()})


@pytest.mark.asyncio
async def test_each_publication_requires_its_own_confirmation_and_receipt(client):
    harness = KafkaAdapterHarness("broker:9092")
    source = await harness.create_source(case_id="repeated-publication")
    first = await harness.publish(source, b"first")
    second = await harness.publish(source, b"second")
    assert first.accepted and second.accepted
    assert first.metadata["kafka.offset"] == 41
    assert second.metadata["kafka.offset"] == 42
    assert [entry[1] for entry in client.publications] == [b"first", b"second"]
    assert len(client.producer_configs) == 1
    assert len(client.flush_timeouts) == 2
    await harness.cleanup()


@pytest.mark.parametrize(
    "changes",
    [
        {"pending": 1},
        {"delivery_count": 0},
        {"delivery_count": 2},
        {"delivery_error": RuntimeError("SECRET_DELIVERY_FAILURE")},
    ],
)
@pytest.mark.asyncio
async def test_unconfirmed_or_ambiguous_delivery_never_returns_an_accepted_receipt(client, changes):
    harness = KafkaAdapterHarness("broker:9092")
    source = await harness.create_source(case_id="unconfirmed")
    for name, value in changes.items():
        setattr(client, name, value)
    try:
        with pytest.raises(TransportError, match="not confirmed") as caught:
            await harness.publish(source, b"payload")
        assert "SECRET" not in str(caught.value)
    finally:
        client.pending = 0
        await harness.cleanup()


@pytest.mark.parametrize("operation", ["producer_error", "produce_error", "flush_error"])
@pytest.mark.asyncio
async def test_native_publication_failures_have_safe_messages_and_preserve_causes(client, operation):
    harness = KafkaAdapterHarness("broker:9092")
    source = await harness.create_source(case_id="publish-failure")
    error = RuntimeError("SECRET_NATIVE_PUBLICATION")
    setattr(client, operation, error)
    try:
        with pytest.raises(TransportError, match="could not publish") as caught:
            await harness.publish(source, b"payload")
        assert caught.value.__cause__ is error
        assert "SECRET" not in str(caught.value)
    finally:
        setattr(client, operation, None)
        await harness.cleanup()


@pytest.mark.asyncio
async def test_ownership_prevents_publication_and_deletion_of_application_topics(client):
    harness = KafkaAdapterHarness("broker:9092")
    owned = await harness.create_source(case_id="owned")
    client.topics.add("application.events")
    with pytest.raises(ConfigurationError, match="own test sources"):
        await harness.publish(TransportSource("application.events"), b"payload")
    assert not client.producer_configs
    await harness.cleanup()
    assert client.deleted == [owned.name]
    assert client.topics == {"application.events"}
    requests_before = len(client.delete_requests)
    await harness.cleanup()
    assert len(client.delete_requests) == requests_before


@pytest.mark.parametrize("error", [RuntimeError("SECRET_CREATE_FAILURE"), TimeoutError("SECRET_TIMEOUT")])
@pytest.mark.asyncio
async def test_partially_created_topic_remains_owned_when_creation_confirmation_fails(client, error):
    harness = KafkaAdapterHarness("broker:9092", timeout=0.25)
    client.create_error = error
    client.topics.add("application.events")
    with pytest.raises(TransportError, match="could not create") as caught:
        await harness.create_source(case_id="partial")
    assert caught.value.__cause__ is error
    assert "SECRET" not in str(caught.value)
    created = client.created[0][0][0].topic
    assert created in client.topics
    assert len(client.future_timeouts) == 1 and 0 < client.future_timeouts[0] <= 0.25
    await harness.cleanup()
    assert client.deleted == [created]
    assert client.topics == {"application.events"}


@pytest.mark.asyncio
async def test_missing_optional_client_is_actionable_and_phantom_source_can_be_cleaned_later(client):
    harness = KafkaAdapterHarness("broker:9092")
    client.missing_client = True
    with pytest.raises(MissingOptionalDependencyError, match="'kafka' extra") as caught:
        await harness.create_source(case_id="missing-client")
    assert isinstance(caught.value.__cause__, ImportError)
    assert "SECRET" not in str(caught.value)
    assert not client.created
    client.missing_client = False
    # No broker topic was created. Unknown-topic confirmation is safe cleanup
    # of this harness's registered name, never a request to delete foreign data.
    await harness.cleanup()
    assert len(client.delete_requests[0][0]) == 1
    assert not client.topics


@pytest.mark.asyncio
async def test_cleanup_accepts_an_owned_topic_that_the_broker_already_removed(client):
    harness = KafkaAdapterHarness("broker:9092")
    source = await harness.create_source(case_id="already-gone")
    client.topics.remove(source.name)
    await harness.cleanup()
    requests_before = len(client.delete_requests)
    await harness.cleanup()
    assert len(client.delete_requests) == requests_before


@pytest.mark.asyncio
async def test_failed_topic_deletion_preserves_only_unresolved_ownership_for_cleanup_retry(client):
    harness = KafkaAdapterHarness("broker:9092", timeout=0.25)
    first = await harness.create_source(case_id="retry")
    second = await harness.create_source(case_id="removed")
    error = TimeoutError("SECRET_DELETE_TIMEOUT")
    client.delete_errors[first.name] = error
    with pytest.raises(TransportError, match="cleanup") as caught:
        await harness.cleanup()
    assert caught.value.__cause__ is error
    assert "SECRET" not in str(caught.value)
    assert client.topics == {first.name}
    assert client.deleted == [second.name]
    with pytest.raises(ConfigurationError, match="already cleaned up"):
        await harness.create_source(case_id="closed")
    with pytest.raises(ConfigurationError, match="already cleaned up"):
        await harness.publish(first, b"payload")
    client.delete_errors.clear()
    await harness.cleanup()
    assert client.delete_requests[-1][0] == [first.name]
    assert not client.topics


@pytest.mark.asyncio
async def test_failed_delete_request_is_safe_and_can_be_retried(client):
    harness = KafkaAdapterHarness("broker:9092")
    source = await harness.create_source(case_id="request-failure")
    error = RuntimeError("SECRET_DELETE_REQUEST")
    client.delete_error = error
    with pytest.raises(TransportError) as caught:
        await harness.cleanup()
    assert caught.value.__cause__ is error
    assert "SECRET" not in str(caught.value)
    assert source.name in client.topics
    client.delete_error = None
    await harness.cleanup()
    assert client.deleted == [source.name]


@pytest.mark.parametrize("flush_failure", ["pending", "exception"])
@pytest.mark.asyncio
async def test_cleanup_still_deletes_owned_topics_when_producer_drain_fails(client, flush_failure):
    harness = KafkaAdapterHarness("broker:9092")
    source = await harness.create_source(case_id="producer-cleanup")
    await harness.publish(source, b"payload")
    error = RuntimeError("SECRET_CLEANUP_FLUSH")
    if flush_failure == "pending":
        client.pending = 1
    else:
        client.flush_error = error
    with pytest.raises(TransportError, match="cleanup") as caught:
        await harness.cleanup()
    assert client.deleted == [source.name]
    assert not client.topics
    assert "SECRET" not in str(caught.value)
    if flush_failure == "exception":
        assert caught.value.__cause__ is error
    requests_before = len(client.delete_requests)
    await harness.cleanup()
    assert len(client.delete_requests) == requests_before
