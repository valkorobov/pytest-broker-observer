"""Composable, three-valued matchers with isolated user callbacks.

Field paths use schema names. Bind structured matchers to a finite set of message
classes before evaluating a stream; ``on_type`` supplies an explicit binding for
a dynamic decoder.
"""

from __future__ import annotations

import copy
import inspect
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from itertools import islice
from types import SimpleNamespace
from typing import Any, Protocol, TypeVar

from .errors import (
    InvalidMatcherResultError,
    MatcherConfigurationError,
    MatcherEvaluationError,
    PayloadIsolationError,
    UnsupportedFieldPresenceError,
)
from .models import ObservedEvent

T = TypeVar("T")
T_contra = TypeVar("T_contra", contravariant=True)
_MAX_MISMATCHES = 32


class MatchStatus(Enum):
    MATCH = "match"
    MISMATCH = "mismatch"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class Mismatch:
    path: str
    reason_code: str
    expected: Any = None
    actual: Any = None


@dataclass(frozen=True, slots=True)
class MatchResult:
    status: MatchStatus
    mismatches: tuple[Mismatch, ...] = ()


class PayloadMatcher(Protocol[T_contra]):
    def match(self, value: T_contra) -> MatchResult: ...


class EventMatcher(Protocol[T]):
    def match_event(self, event: ObservedEvent[T]) -> MatchResult: ...


_MATCH = MatchResult(MatchStatus.MATCH)
_NA = MatchResult(MatchStatus.NOT_APPLICABLE)


def _value(value: Any, depth: int = 0) -> Any:
    """Do not invoke arbitrary repr or preserve a live object in diagnostics."""
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, (str, bytes)):
        return value[:256]
    if depth < 2 and isinstance(value, (tuple, list)):
        return tuple(_value(item, depth + 1) for item in value[:8])
    if depth < 2 and isinstance(value, dict):
        return {
            str(key)[:64]: _value(item, depth + 1)
            for key, item in islice(value.items(), 8)
            if isinstance(key, (str, int))
        }
    return "<value>"


def _mismatch(path: str, reason: str, expected: Any = None, actual: Any = None) -> MatchResult:
    return MatchResult(MatchStatus.MISMATCH, (Mismatch(path, reason, _value(expected), _value(actual)),))


def _copy_event(event: Any) -> Any:
    try:
        payload = copy.deepcopy(event.payload)
        if isinstance(event, ObservedEvent):
            return replace(event, payload=payload)
        return SimpleNamespace(payload=payload, raw=event.raw, trace_context=event.trace_context)
    except Exception as exc:
        raise PayloadIsolationError("Cannot isolate a matcher argument.") from exc


def _check_result(result: Any) -> MatchResult:
    if isinstance(result, MatchResult) and isinstance(result.status, MatchStatus):
        if not isinstance(result.mismatches, tuple) or not all(
            isinstance(item, Mismatch) for item in result.mismatches
        ):
            raise InvalidMatcherResultError("A matcher must return a valid MatchResult.")
        mismatches = []
        for item in result.mismatches[:_MAX_MISMATCHES]:
            if not isinstance(item.path, str) or not isinstance(item.reason_code, str):
                raise InvalidMatcherResultError("Mismatch paths and reason codes must be strings.")
            mismatches.append(
                Mismatch(item.path[:256], item.reason_code[:128], _value(item.expected), _value(item.actual))
            )
        return MatchResult(result.status, tuple(mismatches))
    _close_awaitable(result)
    raise InvalidMatcherResultError("A matcher must return MatchResult.")


def _close_awaitable(value: Any) -> None:
    if inspect.iscoroutine(value):
        value.close()


def _callback(fn: Callable[..., Any], argument: Any, *, boolean: bool) -> MatchResult:
    try:
        result = fn(argument)
    except (InvalidMatcherResultError, MatcherEvaluationError, PayloadIsolationError):
        raise
    except Exception as exc:
        raise MatcherEvaluationError("User matcher raised an exception.") from exc
    if not boolean:
        return _check_result(result)
    if type(result) is not bool:
        _close_awaitable(result)
        raise InvalidMatcherResultError("A predicate must return bool, synchronously.")
    return _MATCH if result else _mismatch("", "predicate_false")


def _validate_callback(fn: Any) -> None:
    if not callable(fn):
        raise MatcherConfigurationError("A predicate must be callable.")
    if inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(getattr(fn, "__call__", None)):
        raise MatcherConfigurationError("Async predicates are not supported.")


class _Node:
    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        raise NotImplementedError

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        pass

    def match_event(self, event: ObservedEvent[Any]) -> MatchResult:
        return self._eval(event, _copy_event)

    def match(self, value: Any) -> MatchResult:
        event = SimpleNamespace(payload=value, raw=None, trace_context=None)
        return self._eval(event, _copy_event)


@dataclass(frozen=True, slots=True)
class _Predicate(_Node):
    fn: Callable[..., Any]
    event: bool = False
    boolean: bool = True

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        detached = copier(event)
        return _callback(self.fn, detached if self.event else detached.payload, boolean=self.boolean)


def predicate(fn: Callable[[Any], bool]) -> PayloadMatcher[Any]:
    _validate_callback(fn)
    return _Predicate(fn)


def event_predicate(fn: Callable[[ObservedEvent[Any]], bool]) -> EventMatcher[Any]:
    _validate_callback(fn)
    return _Predicate(fn, event=True)


def _node(matcher: Any) -> _Node:
    if isinstance(matcher, _BoundMatcher):
        return matcher.node
    if isinstance(matcher, _Node):
        return matcher
    if callable(getattr(matcher, "match_event", None)):
        _validate_callback(matcher.match_event)
        return _Predicate(matcher.match_event, event=True, boolean=False)
    if callable(getattr(matcher, "match", None)):
        _validate_callback(matcher.match)
        return _Predicate(matcher.match, boolean=False)
    _validate_callback(matcher)
    return _Predicate(matcher)


@dataclass(frozen=True, slots=True)
class _BoundMatcher:
    node: _Node
    copier: Callable[[Any], Any]

    def match_event(self, event: ObservedEvent[Any]) -> MatchResult:
        return self.node._eval(event, self.copier)


def bind_matcher(
    matcher: Any,
    message_types: tuple[type, ...] | None = None,
    *,
    copy_event: Callable[[ObservedEvent[Any]], ObservedEvent[Any]] | None = None,
) -> EventMatcher[Any]:
    """Validate a matcher and adapt payload/event protocols to an event matcher.

    ``copy_event`` may implement a decoder's custom PayloadAccess. It is invoked
    independently for each user callback, including branches of a composition.
    """
    node = _node(matcher)
    node._bind(message_types)
    return _BoundMatcher(node, copy_event or _copy_event)


def _path(path: str) -> tuple[str, ...]:
    if not isinstance(path, str) or not path or any(not part.isidentifier() for part in path.split(".")):
        raise MatcherConfigurationError("A field path must contain schema field names separated by dots.")
    return tuple(path.split("."))


def _descriptor(cls: type) -> Any:
    return getattr(cls, "DESCRIPTOR", None)


def _field_descriptor(cls: type, parts: tuple[str, ...]) -> Any:
    descriptor = _descriptor(cls)
    if descriptor is None:
        return None
    field = None
    for index, part in enumerate(parts):
        field = descriptor.fields_by_name.get(part)
        if field is None:
            return None
        if index < len(parts) - 1:
            if field.message_type is None or _is_repeated(field):
                return None
            descriptor = field.message_type
    return field


def _is_repeated(field: Any) -> bool:
    value = getattr(field, "is_repeated", None)
    return value if value is not None else field.label == 3


def _presence(field: Any) -> bool:
    return bool(getattr(field, "has_presence", False))


def _read(value: Any, parts: tuple[str, ...], *, presence_only: bool = False) -> tuple[str, Any]:
    for index, part in enumerate(parts):
        descriptor = getattr(value, "DESCRIPTOR", None)
        if descriptor is not None:
            field = descriptor.fields_by_name.get(part)
            if field is None:
                return "not_applicable", None
            if presence_only and index == len(parts) - 1:
                if not _presence(field):
                    raise UnsupportedFieldPresenceError("The selected field has no explicit presence.")
                return "ok", bool(value.HasField(part))
            if _presence(field) and (field.message_type is not None or field.containing_oneof is not None):
                if not value.HasField(part):
                    return "absent", None
            value = getattr(value, part)
        elif isinstance(value, Mapping):
            if part not in value:
                return ("ok", False) if presence_only else ("not_applicable", None)
            if presence_only and index == len(parts) - 1:
                return "ok", True
            value = value[part]
        else:
            if not hasattr(value, part):
                return "not_applicable", None
            if presence_only and index == len(parts) - 1:
                raise UnsupportedFieldPresenceError("Explicit field presence requires a supported schema.")
            value = getattr(value, part)
    return "ok", value


@dataclass(frozen=True, slots=True)
class _Field(_Node):
    parts: tuple[str, ...]
    expected: Any = None
    nested: _Node | None = None
    presence: bool = False

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        if message_types is None:
            raise MatcherConfigurationError(
                "Structured fields require a finite type binding or on_type guard."
            )
        fields = [_field_descriptor(cls, self.parts) for cls in message_types]
        available = [field for field in fields if field is not None]
        if not available:
            raise MatcherConfigurationError("The field path is absent from every bound message schema.")
        if self.presence and any(not _presence(field) for field in available):
            raise UnsupportedFieldPresenceError("The selected field has no explicit presence.")
        if self.nested is not None:
            nested_types: list[type] = []
            for field in available:
                if field.message_type is not None and not _is_repeated(field):
                    from google.protobuf.message_factory import GetMessageClass

                    nested_types.append(GetMessageClass(field.message_type))
                else:
                    nested_types.append(object)
            self.nested._bind(tuple(nested_types))

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        status, value = _read(event.payload, self.parts, presence_only=self.presence)
        path = ".".join(self.parts)
        if status == "not_applicable":
            return _NA
        if status == "absent":
            return _mismatch(path, "field_absent", self.expected)
        if self.nested is not None:
            child = SimpleNamespace(payload=value, raw=event.raw, trace_context=event.trace_context)
            return self.nested._eval(child, _copy_event)
        expected = True if self.presence else self.expected
        try:
            equal = value == expected
            if type(equal) is not bool:
                raise InvalidMatcherResultError("Field equality did not produce bool.")
        except InvalidMatcherResultError:
            raise
        except Exception as exc:
            raise MatcherEvaluationError("Field comparison raised an exception.") from exc
        return _MATCH if equal else _mismatch(path, "field_not_equal", expected, value)


def field_equals(path: str, value: Any) -> PayloadMatcher[Any]:
    return _Field(_path(path), copy.deepcopy(value))


def field_matches(path: str, matcher: Any) -> PayloadMatcher[Any]:
    return _Field(_path(path), nested=_node(matcher))


def has_field(path: str) -> PayloadMatcher[Any]:
    return _Field(_path(path), presence=True)


@dataclass(frozen=True, slots=True)
class _Composition(_Node):
    operation: str
    children: tuple[_Node, ...]

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        for child in self.children:
            child._bind(message_types)

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        if self.operation == "not":
            result = self.children[0]._eval(event, copier)
            if result.status is MatchStatus.NOT_APPLICABLE:
                return result
            return _MATCH if result.status is MatchStatus.MISMATCH else _mismatch("", "negated_match")
        results: list[MatchResult] = []
        mismatches: list[Mismatch] = []
        for child in self.children:
            result = child._eval(event, copier)
            if self.operation == "any" and result.status is MatchStatus.MATCH:
                return _MATCH
            results.append(result)
            mismatches.extend(result.mismatches[: _MAX_MISMATCHES - len(mismatches)])
        states = {result.status for result in results}
        if self.operation == "all":
            status = (
                MatchStatus.MISMATCH
                if MatchStatus.MISMATCH in states
                else MatchStatus.NOT_APPLICABLE
                if MatchStatus.NOT_APPLICABLE in states
                else MatchStatus.MATCH
            )
        else:
            status = (
                MatchStatus.NOT_APPLICABLE if MatchStatus.NOT_APPLICABLE in states else MatchStatus.MISMATCH
            )
        return MatchResult(status, tuple(mismatches))


def all_of(*matchers: Any) -> EventMatcher[Any]:
    if not matchers:
        raise MatcherConfigurationError("all_of requires at least one matcher.")
    return _Composition("all", tuple(_node(matcher) for matcher in matchers))


def any_of(*matchers: Any) -> EventMatcher[Any]:
    if not matchers:
        raise MatcherConfigurationError("any_of requires at least one matcher.")
    return _Composition("any", tuple(_node(matcher) for matcher in matchers))


def not_(matcher: Any) -> EventMatcher[Any]:
    return _Composition("not", (_node(matcher),))


@dataclass(frozen=True, slots=True)
class _Type(_Node):
    message_type: type
    child: _Node | None = None

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        if self.child is not None:
            self.child._bind((self.message_type,))

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        if not isinstance(event.payload, self.message_type):
            return _NA if self.child is not None else _mismatch("", "message_type_mismatch")
        return _MATCH if self.child is None else self.child._eval(event, copier)


def is_message_type(message_type: type[T]) -> PayloadMatcher[T]:
    if not isinstance(message_type, type):
        raise MatcherConfigurationError("A message type must be a class.")
    return _Type(message_type)


def on_type(message_type: type[T], matcher: Any) -> EventMatcher[T]:
    if not isinstance(message_type, type):
        raise MatcherConfigurationError("A type guard requires a class.")
    return _Type(message_type, _node(matcher))


@dataclass(frozen=True, slots=True)
class _Metadata(_Node):
    kind: str
    expected: Any
    name: str | None = None
    encoding: str | None = None

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        if event.raw is None:
            raise MatcherConfigurationError("Metadata matchers require an ObservedEvent.")
        if self.kind == "header":
            values = [value for key, value in event.raw.headers if key == self.name]
            if not values:
                return _mismatch(self.name or "", "header_missing")
            if len(values) != 1:
                raise MatcherEvaluationError("The selected header is ambiguous.")
            actual = values[0]
            if self.encoding is not None and actual is not None:
                try:
                    actual = actual.decode(self.encoding, errors="strict")
                except (UnicodeError, LookupError) as exc:
                    raise MatcherEvaluationError("Header text decoding failed.") from exc
        elif self.kind == "trace_id":
            context = event.trace_context
            status = getattr(context, "status", None)
            status = getattr(status, "value", status)
            if context is None or not isinstance(status, str) or status.lower() != "valid":
                return _mismatch("trace_id", "trace_context_unavailable")
            actual = getattr(context, "trace_id", None)
        else:
            actual = getattr(event.raw, self.kind)
        return (
            _MATCH
            if actual == self.expected
            else _mismatch(self.name or self.kind, "metadata_not_equal", self.expected, actual)
        )


def header_equals(
    name: str, expected_bytes: bytes | str | None, *, encoding: str | None = None
) -> EventMatcher[Any]:
    if not isinstance(name, str) or not name:
        raise MatcherConfigurationError("A header name must be a nonempty string.")
    if encoding is None and expected_bytes is not None and not isinstance(expected_bytes, bytes):
        raise MatcherConfigurationError("Header comparison requires bytes, or an explicit encoding for text.")
    if encoding is not None and expected_bytes is not None and not isinstance(expected_bytes, str):
        raise MatcherConfigurationError("An encoded header comparison requires text.")
    if encoding is not None:
        try:
            "".encode(encoding)
        except LookupError as exc:
            raise MatcherConfigurationError("The header encoding is unknown.") from exc
    return _Metadata("header", expected_bytes, name, encoding)


def key_equals(expected: bytes | None) -> EventMatcher[Any]:
    if expected is not None and not isinstance(expected, bytes):
        raise MatcherConfigurationError("A message key must be bytes or None.")
    return _Metadata("key", expected)


def source_equals(expected: str) -> EventMatcher[Any]:
    if not isinstance(expected, str) or not expected:
        raise MatcherConfigurationError("A source must be a nonempty string.")
    return _Metadata("source", expected)


def trace_id_equals(expected: str) -> EventMatcher[Any]:
    if (
        not isinstance(expected, str)
        or len(expected) != 32
        or any(character not in "0123456789abcdef" for character in expected)
        or expected == "0" * 32
    ):
        raise MatcherConfigurationError("A trace ID must be a nonzero lowercase 32-digit hexadecimal value.")
    return _Metadata("trace_id", expected)


def _validate_partial(descriptor: Any, expected: Mapping[str, Any]) -> None:
    for name, value in expected.items():
        if not isinstance(name, str) or name not in descriptor.fields_by_name:
            raise MatcherConfigurationError("A partial comparison names an unknown schema field.")
        field = descriptor.fields_by_name[name]
        if field.message_type is not None:
            if _is_repeated(field):
                if field.message_type.GetOptions().map_entry:
                    if not isinstance(value, Mapping):
                        raise MatcherConfigurationError("A map partial comparison requires a mapping.")
                    value_field = field.message_type.fields_by_name["value"]
                    if value_field.message_type is not None:
                        for entry in value.values():
                            if not isinstance(entry, Mapping):
                                raise MatcherConfigurationError(
                                    "A nested message comparison requires a mapping."
                                )
                            _validate_partial(value_field.message_type, entry)
                else:
                    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                        raise MatcherConfigurationError("A repeated partial comparison requires a sequence.")
                    for entry in value:
                        if not isinstance(entry, Mapping):
                            raise MatcherConfigurationError("A nested message comparison requires a mapping.")
                        _validate_partial(field.message_type, entry)
            elif isinstance(value, Mapping):
                _validate_partial(field.message_type, value)
            else:
                raise MatcherConfigurationError("A nested message comparison requires a mapping.")
        elif _is_repeated(field) and (not isinstance(value, Sequence) or isinstance(value, (str, bytes))):
            raise MatcherConfigurationError("A repeated partial comparison requires a sequence.")
        if value is None:
            raise MatcherConfigurationError("None does not express Protobuf field absence.")


def _partial(value: Any, expected: Mapping[str, Any], prefix: str = "") -> MatchResult:
    mismatches: list[Mismatch] = []
    descriptor = value.DESCRIPTOR
    for name, wanted in expected.items():
        path = f"{prefix}.{name}" if prefix else name
        field = descriptor.fields_by_name[name]
        if _presence(field) and not value.HasField(name):
            result = _mismatch(path, "field_absent", wanted)
        else:
            actual = getattr(value, name)
            if (
                field.message_type is not None
                and _is_repeated(field)
                and field.message_type.GetOptions().map_entry
            ):
                value_field = field.message_type.fields_by_name["value"]
                children: list[Mismatch] = []
                for key, wanted_entry in wanted.items():
                    entry_path = f"{path}[{_value(key)}]"
                    if key not in actual:
                        children.extend(_mismatch(entry_path, "map_key_missing").mismatches)
                    elif value_field.message_type is not None:
                        children.extend(_partial(actual[key], wanted_entry, entry_path).mismatches)
                    elif actual[key] != wanted_entry:
                        children.extend(
                            _mismatch(entry_path, "field_not_equal", wanted_entry, actual[key]).mismatches
                        )
                    if len(children) >= _MAX_MISMATCHES:
                        break
                result = MatchResult(
                    MatchStatus.MISMATCH if children else MatchStatus.MATCH, tuple(children[:_MAX_MISMATCHES])
                )
            elif _is_repeated(field):
                if len(actual) != len(wanted):
                    result = _mismatch(path, "repeated_length", len(wanted), len(actual))
                elif field.message_type is not None:
                    children = []
                    for index, (actual_entry, wanted_entry) in enumerate(zip(actual, wanted)):
                        children.extend(_partial(actual_entry, wanted_entry, f"{path}[{index}]").mismatches)
                        if len(children) >= _MAX_MISMATCHES:
                            break
                    result = MatchResult(
                        MatchStatus.MISMATCH if children else MatchStatus.MATCH,
                        tuple(children[:_MAX_MISMATCHES]),
                    )
                else:
                    result = (
                        _MATCH
                        if all(
                            actual_entry == wanted_entry for actual_entry, wanted_entry in zip(actual, wanted)
                        )
                        else _mismatch(path, "field_not_equal", wanted, tuple(islice(actual, 8)))
                    )
            elif field.message_type is not None:
                result = _partial(actual, wanted, path)
            else:
                result = _MATCH if actual == wanted else _mismatch(path, "field_not_equal", wanted, actual)
        mismatches.extend(result.mismatches[: _MAX_MISMATCHES - len(mismatches)])
        if len(mismatches) >= _MAX_MISMATCHES:
            break
    return MatchResult(MatchStatus.MISMATCH if mismatches else MatchStatus.MATCH, tuple(mismatches))


@dataclass(frozen=True, slots=True)
class _Partial(_Node):
    expected: Mapping[str, Any]

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        if message_types is None:
            raise MatcherConfigurationError(
                "A Protobuf partial matcher requires a finite type binding or on_type guard."
            )
        descriptors = [_descriptor(cls) for cls in message_types if _descriptor(cls) is not None]
        if not descriptors:
            raise MatcherConfigurationError("A Protobuf partial matcher requires Protobuf message types.")
        valid = False
        for descriptor in descriptors:
            try:
                _validate_partial(descriptor, self.expected)
                valid = True
            except MatcherConfigurationError:
                pass
        if not valid:
            raise MatcherConfigurationError("The partial comparison does not match any bound schema.")

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        descriptor = getattr(event.payload, "DESCRIPTOR", None)
        if descriptor is None:
            return _NA
        try:
            _validate_partial(descriptor, self.expected)
        except MatcherConfigurationError:
            return _NA
        return _partial(event.payload, self.expected)


def protobuf_partial(mapping: Mapping[str, Any]) -> PayloadMatcher[Any]:
    if not isinstance(mapping, Mapping):
        raise MatcherConfigurationError("A Protobuf partial comparison requires a mapping.")
    return _Partial(copy.deepcopy(dict(mapping)))


__all__ = [
    "PayloadMatcher",
    "EventMatcher",
    "MatchStatus",
    "MatchResult",
    "Mismatch",
    "field_equals",
    "field_matches",
    "has_field",
    "protobuf_partial",
    "all_of",
    "any_of",
    "not_",
    "predicate",
    "event_predicate",
    "is_message_type",
    "on_type",
    "header_equals",
    "key_equals",
    "source_equals",
    "trace_id_equals",
    "bind_matcher",
]
