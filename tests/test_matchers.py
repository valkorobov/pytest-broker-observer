from __future__ import annotations

from datetime import datetime, timezone
from itertools import product
from types import SimpleNamespace

import pytest

from event_observer.errors import (
    InvalidMatcherResultError,
    MatcherConfigurationError,
    MatcherEvaluationError,
    UnsupportedFieldPresenceError,
)
from event_observer.matchers import (
    MatchResult,
    MatchStatus,
    all_of,
    any_of,
    bind_matcher,
    event_predicate,
    field_equals,
    field_matches,
    has_field,
    header_equals,
    is_message_type,
    key_equals,
    not_,
    on_type,
    predicate,
    protobuf_partial,
    source_equals,
    trace_id_equals,
)
from event_observer.models import ObservedEvent, RawEvent


def observed(payload, *, headers=(), key=None, trace_context=None):
    return ObservedEvent(
        observer_id="test-observer",
        sequence=1,
        payload=payload,
        raw=RawEvent(b"wire", "events", headers=headers, key=key),
        recorded_at=datetime.now(timezone.utc),
        recorded_monotonic=1.0,
        trace_context=trace_context,
    )


class Constant:
    def __init__(self, status):
        self.status = status

    def match(self, value):
        return MatchResult(self.status)


STATES = tuple(MatchStatus)


@pytest.mark.parametrize("left,right", list(product(STATES, repeat=2)))
def test_all_of_truth_table(left, right):
    states = {left, right}
    expected = (
        MatchStatus.MISMATCH
        if MatchStatus.MISMATCH in states
        else MatchStatus.NOT_APPLICABLE
        if MatchStatus.NOT_APPLICABLE in states
        else MatchStatus.MATCH
    )
    result = bind_matcher(all_of(Constant(left), Constant(right))).match_event(observed(b""))
    assert result.status is expected


@pytest.mark.parametrize("left,right", list(product(STATES, repeat=2)))
def test_any_of_truth_table(left, right):
    states = {left, right}
    expected = (
        MatchStatus.MATCH
        if MatchStatus.MATCH in states
        else MatchStatus.NOT_APPLICABLE
        if MatchStatus.NOT_APPLICABLE in states
        else MatchStatus.MISMATCH
    )
    result = bind_matcher(any_of(Constant(left), Constant(right))).match_event(observed(b""))
    assert result.status is expected


@pytest.mark.parametrize(
    "status,expected",
    [
        (MatchStatus.MATCH, MatchStatus.MISMATCH),
        (MatchStatus.MISMATCH, MatchStatus.MATCH),
        (MatchStatus.NOT_APPLICABLE, MatchStatus.NOT_APPLICABLE),
    ],
)
def test_not_truth_table(status, expected):
    assert bind_matcher(not_(Constant(status))).match_event(observed(b"")).status is expected


@pytest.mark.parametrize("factory", [all_of, any_of])
def test_empty_composition_is_configuration_error(factory):
    with pytest.raises(MatcherConfigurationError):
        factory()


def test_any_short_circuits_after_first_match():
    def forbidden(value):
        raise AssertionError("must not be evaluated")

    matcher = bind_matcher(any_of(lambda value: True, forbidden))
    assert matcher.match_event(observed(b"")).status is MatchStatus.MATCH


def test_all_evaluates_every_branch_to_collect_diagnostics():
    calls = []
    result = bind_matcher(all_of(lambda value: False, lambda value: calls.append(value) is None)).match_event(
        observed(b"wire")
    )
    assert result.status is MatchStatus.MISMATCH
    assert calls == [b"wire"]


def test_composition_isolates_mutating_user_branches():
    payload = {"status": "original"}

    def mutate(value):
        value["status"] = "changed"
        return True

    matcher = bind_matcher(all_of(mutate, lambda value: value["status"] == "original"))
    assert matcher.match_event(observed(payload)).status is MatchStatus.MATCH
    assert payload == {"status": "original"}


def test_event_predicate_is_explicit_and_isolated():
    payload = {"original": True}

    def mutate(event):
        event.payload.clear()
        return event.raw.source == "events"

    assert bind_matcher(event_predicate(mutate)).match_event(observed(payload)).status is MatchStatus.MATCH
    assert payload == {"original": True}


@pytest.mark.parametrize("result", [1, None, "true", [], MatchResult(MatchStatus.MATCH)])
def test_predicate_requires_actual_bool(result):
    with pytest.raises(InvalidMatcherResultError):
        bind_matcher(lambda value: result).match_event(observed(b""))


def test_async_predicate_rejected_before_evaluation():
    async def callback(value):
        return True

    with pytest.raises(MatcherConfigurationError):
        predicate(callback)


def test_callback_returning_coroutine_is_closed():
    async def result():
        return True

    coroutine = result()
    with pytest.raises(InvalidMatcherResultError):
        bind_matcher(lambda value: coroutine).match_event(observed(b""))
    assert coroutine.cr_frame is None


def test_custom_matcher_result_is_validated():
    class Invalid:
        def match(self, value):
            return True

    with pytest.raises(InvalidMatcherResultError):
        bind_matcher(Invalid()).match_event(observed(b""))


def test_callback_exception_is_chained_with_safe_summary():
    def fail(value):
        raise ValueError("secret-token")

    with pytest.raises(MatcherEvaluationError) as caught:
        bind_matcher(fail).match_event(observed(b""))
    assert isinstance(caught.value.__cause__, ValueError)
    assert "secret-token" not in str(caught.value)


def test_base_exceptions_preserve_semantics():
    def interrupt(value):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        bind_matcher(interrupt).match_event(observed(b""))


def test_on_type_never_calls_wrong_type_callback():
    calls = []
    matcher = bind_matcher(on_type(dict, lambda value: calls.append(value) is None))
    assert matcher.match_event(observed(b"wire")).status is MatchStatus.NOT_APPLICABLE
    assert calls == []
    assert matcher.match_event(observed({})).status is MatchStatus.MATCH


def test_message_type_mismatch_is_not_not_applicable():
    assert bind_matcher(is_message_type(dict)).match_event(observed(b"wire")).status is MatchStatus.MISMATCH


def test_metadata_preserves_null_empty_and_duplicate_headers():
    event = observed(b"", headers=(("null", None), ("empty", b""), ("dup", b"a"), ("dup", b"b")), key=b"key")
    matcher = bind_matcher(
        all_of(
            header_equals("null", None),
            header_equals("empty", b""),
            key_equals(b"key"),
            source_equals("events"),
        )
    )
    assert matcher.match_event(event).status is MatchStatus.MATCH
    assert bind_matcher(header_equals("missing", None)).match_event(event).status is MatchStatus.MISMATCH
    with pytest.raises(MatcherEvaluationError):
        bind_matcher(header_equals("dup", b"a")).match_event(event)


def test_header_text_requires_explicit_strict_encoding():
    with pytest.raises(MatcherConfigurationError):
        header_equals("id", "hello")
    assert (
        bind_matcher(header_equals("id", "hello", encoding="utf-8"))
        .match_event(observed(b"", headers=(("id", b"hello"),)))
        .status
        is MatchStatus.MATCH
    )
    with pytest.raises(MatcherEvaluationError):
        bind_matcher(header_equals("id", "hello", encoding="utf-8")).match_event(
            observed(b"", headers=(("id", b"\xff"),))
        )


def test_trace_id_requires_valid_extracted_context():
    trace_id = "a" * 32
    matcher = bind_matcher(trace_id_equals(trace_id))
    assert (
        matcher.match_event(
            observed(b"", trace_context=SimpleNamespace(status="valid", trace_id=trace_id))
        ).status
        is MatchStatus.MATCH
    )
    assert (
        matcher.match_event(
            observed(b"", trace_context=SimpleNamespace(status="invalid", trace_id=trace_id))
        ).status
        is MatchStatus.MISMATCH
    )
    assert matcher.match_event(observed(b"")).status is MatchStatus.MISMATCH


@pytest.fixture(scope="module")
def message_types():
    pytest.importorskip("google.protobuf")
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    file = descriptor_pb2.FileDescriptorProto(
        name="matcher_fixture.proto", package="matcher_fixture", syntax="proto3"
    )
    child = file.message_type.add(name="Child")
    child.field.add(name="name", number=1, type=9, label=1)
    event = file.message_type.add(name="Event")
    event.field.add(name="operation_id", number=1, type=9, label=1)
    event.field.add(name="child", number=2, type=11, type_name=".matcher_fixture.Child", label=1)
    event.field.add(name="values", number=3, type=5, label=3)
    event.oneof_decl.add(name="choice")
    event.field.add(name="created", number=4, type=9, label=1, oneof_index=0)
    event.field.add(name="completed", number=5, type=9, label=1, oneof_index=0)
    entry = event.nested_type.add(name="LabelsEntry")
    entry.options.map_entry = True
    entry.field.add(name="key", number=1, type=9, label=1)
    entry.field.add(name="value", number=2, type=9, label=1)
    event.field.add(name="labels", number=6, type=11, type_name=".matcher_fixture.Event.LabelsEntry", label=3)
    other = file.message_type.add(name="Other")
    other.field.add(name="different", number=1, type=9, label=1)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return (
        message_factory.GetMessageClass(pool.FindMessageTypeByName("matcher_fixture.Event")),
        message_factory.GetMessageClass(pool.FindMessageTypeByName("matcher_fixture.Other")),
    )


def test_field_binding_rejects_typo_before_wait(message_types):
    Event, _ = message_types
    with pytest.raises(MatcherConfigurationError):
        bind_matcher(field_equals("operaton_id", "id"), (Event,))
    with pytest.raises(MatcherConfigurationError):
        bind_matcher(field_equals("operation_id", "id"))
    with pytest.raises(MatcherConfigurationError):
        bind_matcher(field_equals("operation_id", "id"), (bytes, type(None)))


def test_bound_mixed_types_mark_missing_field_not_applicable(message_types):
    Event, Other = message_types
    matcher = bind_matcher(field_equals("operation_id", "id"), (Event, Other))
    assert matcher.match_event(observed(Event(operation_id="id"))).status is MatchStatus.MATCH
    assert matcher.match_event(observed(Other())).status is MatchStatus.NOT_APPLICABLE


def test_field_matches_nested_descriptor(message_types):
    Event, _ = message_types
    matcher = bind_matcher(field_matches("child", field_equals("name", "nested")), (Event,))
    event = Event()
    event.child.name = "nested"
    assert matcher.match_event(observed(event)).status is MatchStatus.MATCH


def test_dynamic_structured_access_has_explicit_type_guard(message_types):
    Event, Other = message_types
    matcher = bind_matcher(on_type(Event, field_equals("operation_id", "id")))
    assert matcher.match_event(observed(Event(operation_id="id"))).status is MatchStatus.MATCH
    assert matcher.match_event(observed(Other())).status is MatchStatus.NOT_APPLICABLE


def test_explicit_presence_never_uses_default_value_guessing(message_types):
    Event, _ = message_types
    with pytest.raises(UnsupportedFieldPresenceError):
        bind_matcher(has_field("operation_id"), (Event,))
    matcher = bind_matcher(has_field("child"), (Event,))
    event = Event()
    assert matcher.match_event(observed(event)).status is MatchStatus.MISMATCH
    event.child.SetInParent()
    assert matcher.match_event(observed(event)).status is MatchStatus.MATCH


def test_nested_absent_message_does_not_match_default_child(message_types):
    Event, _ = message_types
    matcher = bind_matcher(field_equals("child.name", ""), (Event,))
    assert matcher.match_event(observed(Event())).status is MatchStatus.MISMATCH
    event = Event()
    event.child.SetInParent()
    assert matcher.match_event(observed(event)).status is MatchStatus.MATCH


def test_partial_nested_messages_repeated_maps_and_oneof(message_types):
    Event, _ = message_types
    event = Event(
        operation_id="id", values=[1, 2], completed="done", labels={"required": "yes", "extra": "allowed"}
    )
    event.child.name = "nested"
    matcher = bind_matcher(
        protobuf_partial(
            {
                "child": {"name": "nested"},
                "values": [1, 2],
                "completed": "done",
                "labels": {"required": "yes"},
            }
        ),
        (Event,),
    )
    assert matcher.match_event(observed(event)).status is MatchStatus.MATCH
    assert (
        bind_matcher(protobuf_partial({"values": [2, 1]}), (Event,)).match_event(observed(event)).status
        is MatchStatus.MISMATCH
    )
    assert (
        bind_matcher(protobuf_partial({"created": ""}), (Event,)).match_event(observed(event)).status
        is MatchStatus.MISMATCH
    )
    assert (
        bind_matcher(protobuf_partial({"labels": {"missing": "yes"}}), (Event,))
        .match_event(observed(event))
        .status
        is MatchStatus.MISMATCH
    )


@pytest.mark.parametrize(
    "mapping", [{"missing": 1}, {"child": {"missing": 1}}, {"values": None}, {"operation_id": None}]
)
def test_partial_rejects_invalid_configuration(message_types, mapping):
    Event, _ = message_types
    with pytest.raises(MatcherConfigurationError):
        bind_matcher(protobuf_partial(mapping), (Event,))


def test_protobuf_callback_mutation_does_not_reach_other_branches(message_types):
    Event, _ = message_types
    event = Event(operation_id="original")

    def mutate(value):
        value.operation_id = "mutated"
        return True

    matcher = bind_matcher(all_of(mutate, field_equals("operation_id", "original")), (Event,))
    assert matcher.match_event(observed(event)).status is MatchStatus.MATCH
    assert event.operation_id == "original"


def test_custom_payload_access_can_isolate_non_deepcopyable_payload():
    class CustomPayload:
        def __init__(self, value):
            self.value = value

        def __deepcopy__(self, memo):
            raise TypeError("custom copier required")

    def copier(event):
        return SimpleNamespace(
            payload=CustomPayload(event.payload.value), raw=event.raw, trace_context=event.trace_context
        )

    matcher = bind_matcher(
        all_of(lambda value: value.value == "a", lambda value: value.value == "a"), copy_event=copier
    )
    assert matcher.match_event(observed(CustomPayload("a"))).status is MatchStatus.MATCH


def test_trace_id_accepts_the_builtin_extractor():
    from event_observer.trace import extract_trace_context

    trace_id = "a" * 32
    context = extract_trace_context((("traceparent", f"00-{trace_id}-{'b' * 16}-01".encode()),))
    assert (
        bind_matcher(trace_id_equals(trace_id)).match_event(observed(b"", trace_context=context)).status
        is MatchStatus.MATCH
    )
