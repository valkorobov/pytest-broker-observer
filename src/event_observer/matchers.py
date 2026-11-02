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
    """Represent three-valued matcher applicability and success.

    :ivar MATCH: The event satisfies this matcher.
    :ivar MISMATCH: The matcher applies, but the event does not satisfy it.
    :ivar NOT_APPLICABLE: The event type or schema cannot be evaluated by this matcher.
    """

    MATCH = "match"
    MISMATCH = "mismatch"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class Mismatch:
    """Retain a bounded structural mismatch for later disclosure-aware diagnostics.

    :ivar path: Schema field or metadata path associated with the reason.
    :ivar reason_code: Stable explanation code without arbitrary exception text.
    :ivar expected: Bounded expected-value preview, still subject to the export disclosure policy.
    :ivar actual: Bounded actual-value preview, still subject to the export disclosure policy.
    """

    path: str
    reason_code: str
    expected: Any = None
    actual: Any = None


@dataclass(frozen=True, slots=True)
class MatchResult:
    """Return a three-valued decision together with bounded structural reasons.

    :ivar status: MatchStatus describing success, mismatch, or inapplicability.
    :ivar mismatches: Tuple of individual mismatch explanations; callback results are validated and
        bounded.
    """

    status: MatchStatus
    mismatches: tuple[Mismatch, ...] = ()


class PayloadMatcher(Protocol[T_contra]):
    """Define a synchronous matcher that receives only an isolated decoded payload."""

    def match(self, value: T_contra) -> MatchResult:
        """Evaluate one decoded payload without consuming its Journal record.

        :param value: Detached payload supplied to this matcher.
        :returns: A MatchResult with a supported status and structural mismatch tuple.
        """
        ...


class EventMatcher(Protocol[T]):
    """Define a synchronous matcher for an isolated observed envelope and its metadata."""

    def match_event(self, event: ObservedEvent[T]) -> MatchResult:
        """Evaluate a detached observed event without mutating canonical history.

        :param event: Observed envelope with an independently copied mutable payload.
        :returns: A MatchResult describing applicability and any structural mismatch.
        """
        ...


_MATCH = MatchResult(MatchStatus.MATCH)
_NA = MatchResult(MatchStatus.NOT_APPLICABLE)


def _value(value: Any, depth: int = 0) -> Any:
    """Bound diagnostic previews without invoking arbitrary repr or retaining a live object.

    :param value: Expected or actual comparison value to reduce.
    :param depth: Current preview-container nesting depth, bounded at two.
    :returns: A short scalar/container preview or the opaque value marker.
    """
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
    """Build one bounded mismatch result from comparison evidence.

    :param path: Schema or metadata path involved in the comparison.
    :param reason: Stable diagnostic reason code.
    :param expected: Expected value reduced through the bounded preview helper.
    :param actual: Actual value reduced through the bounded preview helper.
    :returns: A MISMATCH result containing one explanation.
    """
    return MatchResult(MatchStatus.MISMATCH, (Mismatch(path, reason, _value(expected), _value(actual)),))


def _copy_event(event: Any) -> Any:
    """Deep-copy a matcher event payload while preserving its immutable envelope metadata.

    :param event: ObservedEvent or lightweight internal event view.
    :returns: A replacement envelope with an independently copied payload.
    :raises PayloadIsolationError: The payload cannot be copied for safe callback evaluation.
    """
    try:
        payload = copy.deepcopy(event.payload)
        if isinstance(event, ObservedEvent):
            return replace(event, payload=payload)
        return SimpleNamespace(payload=payload, raw=event.raw, trace_context=event.trace_context)
    except Exception as exc:
        raise PayloadIsolationError("Cannot isolate a matcher argument.") from exc


def _check_result(result: Any) -> MatchResult:
    """Validate a custom matcher result and reduce its mismatch evidence to internal bounds.

    :param result: Object returned by a custom payload or event matcher.
    :returns: A normalized MatchResult with at most 32 bounded mismatches.
    :raises InvalidMatcherResultError: The result, status, or mismatch representation is invalid.
    """
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
    """Close an accidentally returned coroutine before reporting a synchronous-result violation.

    :param value: Callback result to inspect; non-coroutine values are left unchanged.
    """
    if inspect.iscoroutine(value):
        value.close()


def _callback(fn: Callable[..., Any], argument: Any, *, boolean: bool) -> MatchResult:
    """Invoke one user callback and normalize its synchronous boolean or structured result.

    :param fn: Validated payload or event callback.
    :param argument: Detached value supplied to that callback.
    :param boolean: Require an actual bool when True; otherwise validate MatchResult.
    :returns: MATCH, a predicate_false mismatch, or the validated structured result.
    :raises MatcherEvaluationError: The callback raises an ordinary exception.
    :raises InvalidMatcherResultError: The callback returns an awaitable or an invalid result.
    :raises PayloadIsolationError: The callback reports inability to maintain payload isolation.
    """
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
    """Reject non-callable or declared asynchronous matcher callbacks before evaluation.

    :param fn: Function, method, or callable object proposed as a synchronous callback.
    :raises MatcherConfigurationError: The candidate is not callable or is a coroutine function.
    """
    if not callable(fn):
        raise MatcherConfigurationError("A predicate must be callable.")
    if inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(getattr(fn, "__call__", None)):
        raise MatcherConfigurationError("Async predicates are not supported.")


class _Node:
    """Provide the internal matcher tree interface and payload-to-event adaptation."""

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        """Define the event-evaluation hook implemented by concrete matcher nodes.

        :param event: Internal observed-event view.
        :param copier: Helper for isolating a payload before each user callback.
        :raises NotImplementedError: The abstract base hook is called directly.
        """
        raise NotImplementedError

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        """Accept schema binding for a node that has no additional schema requirements.

        :param message_types: Optional finite allowed payload-class set; ignored by this base
            implementation.
        """
        pass

    def match_event(self, event: ObservedEvent[Any]) -> MatchResult:
        """Evaluate an observed envelope with the default callback-copy policy.

        :param event: Observed event to evaluate.
        :returns: The concrete node MatchResult.
        """
        return self._eval(event, _copy_event)

    def match(self, value: Any) -> MatchResult:
        """Lift a bare payload into an internal event view before evaluating the node.

        :param value: Decoded payload without transport metadata.
        :returns: The concrete node MatchResult.
        :raises MatcherConfigurationError: A metadata-dependent node requires a full observed event.
        """
        event = SimpleNamespace(payload=value, raw=None, trace_context=None)
        return self._eval(event, _copy_event)


@dataclass(frozen=True, slots=True)
class _Predicate(_Node):
    """Adapt one user callback into an isolated matcher-tree leaf.

    :ivar fn: Validated synchronous predicate or structured matcher method.
    :ivar event: Pass the complete detached event when True, otherwise only its payload.
    :ivar boolean: Require bool when True, otherwise validate MatchResult.
    """

    fn: Callable[..., Any]
    event: bool = False
    boolean: bool = True

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        """Copy the callback argument independently before invoking the selected user function.

        :param event: Input event from which to build the isolated callback value.
        :param copier: Configured event-copy policy, possibly supplied by a custom decoder.
        :returns: Normalized callback match result.
        :raises PayloadIsolationError: Argument copying fails.
        :raises MatcherEvaluationError: The callback raises an ordinary exception.
        :raises InvalidMatcherResultError: The callback result violates its declared protocol.
        """
        detached = copier(event)
        return _callback(self.fn, detached if self.event else detached.payload, boolean=self.boolean)


def predicate(fn: Callable[[Any], bool]) -> PayloadMatcher[Any]:
    """Wrap a synchronous payload predicate with strict boolean-result validation.

    :param fn: Function receiving an isolated payload and returning an actual bool.
    :returns: A reusable payload matcher.
    :raises MatcherConfigurationError: The callback is not callable or is asynchronous.
    """
    _validate_callback(fn)
    return _Predicate(fn)


def event_predicate(fn: Callable[[ObservedEvent[Any]], bool]) -> EventMatcher[Any]:
    """Explicitly wrap a predicate requiring the full observed event.

    :param fn: Function receiving an isolated envelope and returning an actual bool.
    :returns: A reusable event matcher.
    :raises MatcherConfigurationError: The callback is not callable or is asynchronous.
    """
    _validate_callback(fn)
    return _Predicate(fn, event=True)


def _node(matcher: Any) -> _Node:
    """Normalize supported matcher protocols and bare payload predicates into a tree node.

    :param matcher: Built-in node, bound matcher, custom match_event/match object, or callable payload
        predicate.
    :returns: An internal node preserving explicit payload/event protocol distinctions.
    :raises MatcherConfigurationError: The candidate exposes no valid synchronous matcher callback.
    """
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
    """Combine a validated matcher tree with the runtime payload-copy policy.

    :ivar node: Schema-bound internal matcher root.
    :ivar copier: Event isolation helper used independently for every user callback.
    """

    node: _Node
    copier: Callable[[Any], Any]

    def match_event(self, event: ObservedEvent[Any]) -> MatchResult:
        """Evaluate the bound tree using its captured isolation helper.

        :param event: Observed event to test without consuming it.
        :returns: A MatchResult from the bound tree.
        """
        return self.node._eval(event, self.copier)


def bind_matcher(
    matcher: Any,
    message_types: tuple[type, ...] | None = None,
    *,
    copy_event: Callable[[ObservedEvent[Any]], ObservedEvent[Any]] | None = None,
) -> EventMatcher[Any]:
    """Validate a matcher and adapt payload/event protocols to an event matcher.

    The copy helper may implement a decoder custom PayloadAccess. It is invoked
    independently for user callbacks, including separate composition branches.

    :param matcher: Built-in matcher, custom protocol implementation, or payload predicate.
    :param message_types: Optional finite allowed classes used for early schema/path validation.
    :param copy_event: Optional runtime isolation helper; defaults to deep-copy event adaptation.
    :returns: A schema-bound event matcher.
    :raises MatcherConfigurationError: The callback, field path, type binding, or composition is invalid.
    :raises UnsupportedFieldPresenceError: A selected field has no explicit presence semantics.
    """
    node = _node(matcher)
    node._bind(message_types)
    return _BoundMatcher(node, copy_event or _copy_event)


def _path(path: str) -> tuple[str, ...]:
    """Parse a dotted path containing schema identifiers only.

    :param path: Nonempty dot-separated schema field names.
    :returns: A tuple of validated field-name components.
    :raises MatcherConfigurationError: The path is empty or contains an invalid identifier.
    """
    if not isinstance(path, str) or not path or any(not part.isidentifier() for part in path.split(".")):
        raise MatcherConfigurationError("A field path must contain schema field names separated by dots.")
    return tuple(path.split("."))


def _descriptor(cls: type) -> Any:
    """Look up a generated message descriptor without importing Protobuf eagerly.

    :param cls: Candidate payload class.
    :returns: Its DESCRIPTOR attribute, or None when absent.
    """
    return getattr(cls, "DESCRIPTOR", None)


def _field_descriptor(cls: type, parts: tuple[str, ...]) -> Any:
    """Resolve a scalar-or-singular-message path through a generated descriptor.

    :param cls: Root generated message class.
    :param parts: Validated schema field-name components.
    :returns: The final field descriptor, or None if the path is absent or crosses a repeated/scalar
        parent.
    """
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
    """Read repeated-field semantics across supported Protobuf descriptor APIs.

    :param field: Generated field descriptor.
    :returns: True for a repeated field, including map fields.
    """
    value = getattr(field, "is_repeated", None)
    return value if value is not None else field.label == 3


def _presence(field: Any) -> bool:
    """Check whether a descriptor exposes explicit field presence.

    :param field: Generated field descriptor.
    :returns: True when HasField-style presence is available.
    """
    return bool(getattr(field, "has_presence", False))


def _read(value: Any, parts: tuple[str, ...], *, presence_only: bool = False) -> tuple[str, Any]:
    """Traverse a schema path while distinguishing absent fields from inapplicable types.

    :param value: Payload or nested value at the start of traversal.
    :param parts: Validated field-name components.
    :param presence_only: Return explicit presence at the final component instead of its value.
    :returns: A status/value pair with status ok, absent, or not_applicable.
    :raises UnsupportedFieldPresenceError: Presence is requested for an implicit-presence scalar or
        unsupported object.
    """
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
    """Compare a schema field, its explicit presence, or a nested matcher result.

    :ivar parts: Validated field-name components.
    :ivar expected: Copied equality target, unused for nested matching.
    :ivar nested: Optional matcher applied to the selected nested value.
    :ivar presence: Check explicit presence rather than equality when True.
    """

    parts: tuple[str, ...]
    expected: Any = None
    nested: _Node | None = None
    presence: bool = False

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        """Validate the field path and bind any nested matcher to the selected field types.

        :param message_types: Finite generated payload classes in which this path must exist.
        :raises MatcherConfigurationError: The type set is dynamic or the path exists in none of its
            schemas.
        :raises UnsupportedFieldPresenceError: The selected presence check is unsupported by a bound
            field.
        """
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
        """Read the selected field and evaluate presence, scalar equality, or its nested matcher.

        :param event: Observed event whose decoded payload provides the field.
        :param copier: Outer isolation policy; nested value evaluation uses the default nested-event
            copier.
        :returns: MATCH, a bounded mismatch, or NOT_APPLICABLE for an absent schema path.
        :raises MatcherEvaluationError: Field equality raises an ordinary exception.
        :raises InvalidMatcherResultError: Field equality does not return bool.
        :raises UnsupportedFieldPresenceError: The runtime field cannot supply explicit presence.
        """
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
    """Build schema-field equality with a copied expected value.

    :param path: Dot-separated schema field names, not JSON aliases.
    :param value: Expected field value copied when the matcher is created.
    :returns: A payload matcher for that field equality.
    :raises MatcherConfigurationError: The field path is malformed.
    """
    return _Field(_path(path), copy.deepcopy(value))


def field_matches(path: str, matcher: Any) -> PayloadMatcher[Any]:
    """Apply a nested matcher to the value selected by a schema field path.

    :param path: Dot-separated schema field names.
    :param matcher: Nested predicate or supported matcher protocol.
    :returns: A payload matcher bound to the selected nested field.
    :raises MatcherConfigurationError: The path or nested matcher callback is invalid.
    """
    return _Field(_path(path), nested=_node(matcher))


def has_field(path: str) -> PayloadMatcher[Any]:
    """Require explicit presence of the selected schema field.

    :param path: Dot-separated schema field names.
    :returns: A payload matcher that tests presence rather than comparing against a default.
    :raises MatcherConfigurationError: The field path is malformed.
    """
    return _Field(_path(path), presence=True)


@dataclass(frozen=True, slots=True)
class _Composition(_Node):
    """Combine child matcher nodes with the documented three-valued logic.

    :ivar operation: Internal all, any, or not composition discriminator.
    :ivar children: Immutable declaration-ordered matcher-node tuple.
    """

    operation: str
    children: tuple[_Node, ...]

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        """Propagate the same allowed schema set to every composition branch.

        :param message_types: Optional finite allowed payload classes.
        :raises MatcherConfigurationError: A child rejects its binding.
        :raises UnsupportedFieldPresenceError: A child requires unavailable explicit presence.
        """
        for child in self.children:
            child._bind(message_types)

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        """Evaluate three-valued composition with bounded evidence and deterministic alternatives.

        All branches may be evaluated for all-of diagnostics. Any-of stops at its
        first MATCH; not preserves NOT_APPLICABLE and otherwise reverses success.

        :param event: Input envelope shared only through independent callback copies.
        :param copier: Isolation helper passed to each evaluated branch.
        :returns: The combined MatchResult with at most 32 collected mismatch explanations.
        """
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
    """Require every applicable matcher branch under three-valued conjunction.

    :param matchers: One or more predicates or supported matcher definitions.
    :returns: A declaration-ordered conjunction; callers must use on_type for guaranteed type guarding.
    :raises MatcherConfigurationError: No matcher is supplied or a branch is not a valid synchronous
        matcher.
    """
    if not matchers:
        raise MatcherConfigurationError("all_of requires at least one matcher.")
    return _Composition("all", tuple(_node(matcher) for matcher in matchers))


def any_of(*matchers: Any) -> EventMatcher[Any]:
    """Choose the first successful branch under deterministic three-valued disjunction.

    :param matchers: One or more alternatives evaluated in declaration order.
    :returns: An event matcher that stops after the first MATCH.
    :raises MatcherConfigurationError: No alternative is supplied or a branch is invalid.
    """
    if not matchers:
        raise MatcherConfigurationError("any_of requires at least one matcher.")
    return _Composition("any", tuple(_node(matcher) for matcher in matchers))


def not_(matcher: Any) -> EventMatcher[Any]:
    """Negate MATCH/MISMATCH while preserving NOT_APPLICABLE.

    :param matcher: Predicate or supported matcher definition to negate.
    :returns: A reusable three-valued negation matcher.
    :raises MatcherConfigurationError: The wrapped callback is invalid.
    """
    return _Composition("not", (_node(matcher),))


@dataclass(frozen=True, slots=True)
class _Type(_Node):
    """Check a payload class and optionally guard a nested matcher against other types.

    :ivar message_type: Class required for payload applicability.
    :ivar child: Optional guarded matcher; another type then yields NOT_APPLICABLE.
    """

    message_type: type
    child: _Node | None = None

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        """Bind a guarded child only to the explicitly selected payload class.

        :param message_types: Outer allowed classes; the explicit guard supplies the child binding
            instead.
        """
        if self.child is not None:
            self.child._bind((self.message_type,))

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        """Test the payload class before evaluating a guarded child.

        :param event: Observed envelope whose payload type is inspected.
        :param copier: Isolation helper forwarded only when the child is applicable.
        :returns: A type mismatch for an unguarded check, NOT_APPLICABLE outside a guard, or the child
            result.
        """
        if not isinstance(event.payload, self.message_type):
            return _NA if self.child is not None else _mismatch("", "message_type_mismatch")
        return _MATCH if self.child is None else self.child._eval(event, copier)


def is_message_type(message_type: type[T]) -> PayloadMatcher[T]:
    """Build an ordinary class check that mismatches another payload type.

    :param message_type: Payload class accepted by isinstance.
    :returns: A payload type matcher.
    :raises MatcherConfigurationError: The supplied message type is not a class.
    """
    if not isinstance(message_type, type):
        raise MatcherConfigurationError("A message type must be a class.")
    return _Type(message_type)


def on_type(message_type: type[T], matcher: Any) -> EventMatcher[T]:
    """Guard a matcher so incompatible payload types never invoke its callback.

    :param message_type: Payload class that makes the nested matcher applicable.
    :param matcher: Predicate or matcher evaluated only after the type guard passes.
    :returns: An event matcher returning NOT_APPLICABLE for other types.
    :raises MatcherConfigurationError: The type or nested matcher is invalid.
    """
    if not isinstance(message_type, type):
        raise MatcherConfigurationError("A type guard requires a class.")
    return _Type(message_type, _node(matcher))


@dataclass(frozen=True, slots=True)
class _Metadata(_Node):
    """Compare one exact transport metadata value or a validated extracted trace ID.

    :ivar kind: Header, trace_id, key, or source comparison discriminator.
    :ivar expected: Validated comparison target.
    :ivar name: Exact header name when comparing a header.
    :ivar encoding: Optional explicit strict codec for a text header comparison.
    """

    kind: str
    expected: Any
    name: str | None = None
    encoding: str | None = None

    def _eval(self, event: Any, copier: Callable[[Any], Any]) -> MatchResult:
        """Read metadata without copying or parsing business payloads.

        :param event: Full observed envelope containing raw metadata and passive trace context.
        :param copier: Node-interface isolation helper, unused because this leaf reads immutable metadata.
        :returns: MATCH or a bounded metadata mismatch.
        :raises MatcherConfigurationError: A bare payload supplies no raw envelope.
        :raises MatcherEvaluationError: A header is duplicated or cannot be decoded with the explicit
            codec.
        """
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
    """Compare a unique exact header, optionally using an explicitly selected text codec.

    :param name: Nonempty exact transport header name.
    :param expected_bytes: Expected bytes or null without encoding; expected text or null with encoding.
    :param encoding: Optional known codec for strict text comparison.
    :returns: An event matcher distinguishing missing, null, empty, and ambiguous headers.
    :raises MatcherConfigurationError: The name, expected-value type, or codec is invalid.
    """
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
    """Compare the native key without treating empty bytes as a missing key.

    :param expected: Expected native key bytes or None.
    :returns: A key metadata matcher.
    :raises MatcherConfigurationError: The expected key is neither bytes nor None.
    """
    if expected is not None and not isinstance(expected, bytes):
        raise MatcherConfigurationError("A message key must be bytes or None.")
    return _Metadata("key", expected)


def source_equals(expected: str) -> EventMatcher[Any]:
    """Compare an exact nonempty transport source name.

    :param expected: Expected topic, exchange, stream, or source name.
    :returns: A source metadata matcher.
    :raises MatcherConfigurationError: The expected source is empty or not text.
    """
    if not isinstance(expected, str) or not expected:
        raise MatcherConfigurationError("A source must be a nonempty string.")
    return _Metadata("source", expected)


def trace_id_equals(expected: str) -> EventMatcher[Any]:
    """Compare only a valid passively extracted trace ID.

    :param expected: Nonzero lowercase 32-digit hexadecimal trace ID.
    :returns: An event matcher that mismatches absent, invalid, or ambiguous context.
    :raises MatcherConfigurationError: The expected ID has an invalid representation.
    """
    if (
        not isinstance(expected, str)
        or len(expected) != 32
        or any(character not in "0123456789abcdef" for character in expected)
        or expected == "0" * 32
    ):
        raise MatcherConfigurationError("A trace ID must be a nonzero lowercase 32-digit hexadecimal value.")
    return _Metadata("trace_id", expected)


def _validate_partial(descriptor: Any, expected: Mapping[str, Any]) -> None:
    """Validate a Protobuf partial mapping recursively against one concrete schema.

    :param descriptor: Generated message descriptor used for field lookup.
    :param expected: Specified field mapping, including nested mappings and ordered repeated values.
    :raises MatcherConfigurationError: A field, collection shape, nested message, or None value is
        invalid.
    """
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
    """Compare specified Protobuf fields while retaining bounded mismatch explanations.

    Nested messages recurse, repeated values compare the complete ordered
    sequence, and maps compare only the requested keys.

    :param value: Concrete generated Protobuf message being evaluated.
    :param expected: Previously validated partial mapping.
    :param prefix: Schema path prefix used in nested diagnostic explanations.
    :returns: A MATCH or MISMATCH result containing at most 32 structural reasons.
    """
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
    """Bind and evaluate an immutable definition of a Protobuf partial comparison.

    :ivar expected: Copied mapping of specified schema fields and desired values.
    """

    expected: Mapping[str, Any]

    def _bind(self, message_types: tuple[type, ...] | None) -> None:
        """Require at least one allowed Protobuf schema compatible with the partial mapping.

        :param message_types: Finite allowed generated classes, or an explicit type-guard binding.
        :raises MatcherConfigurationError: No finite schema set exists or no declared schema accepts the
            mapping.
        """
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
        """Evaluate a partial mapping only for a compatible runtime Protobuf descriptor.

        :param event: Envelope containing a candidate generated message.
        :param copier: Node-interface isolation helper, unused for read-only Protobuf field comparison.
        :returns: NOT_APPLICABLE for incompatible schemas, otherwise a bounded partial-comparison result.
        """
        descriptor = getattr(event.payload, "DESCRIPTOR", None)
        if descriptor is None:
            return _NA
        try:
            _validate_partial(descriptor, self.expected)
        except MatcherConfigurationError:
            return _NA
        return _partial(event.payload, self.expected)


def protobuf_partial(mapping: Mapping[str, Any]) -> PayloadMatcher[Any]:
    """Create a recursive Protobuf comparison from copied explicitly specified fields.

    :param mapping: Schema-field mapping; None never denotes field absence.
    :returns: A reusable payload matcher with ordered repeated and partial-map semantics.
    :raises MatcherConfigurationError: The input is not a mapping.
    """
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
