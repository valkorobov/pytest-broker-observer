"""Synchronous, network-free payload decoders and isolation helpers.

Importing this module does not import Protobuf. Its runtime is required only
when a Protobuf decoder or resolver is constructed.
"""

from __future__ import annotations

import copy
import inspect
import sys
from collections.abc import Callable, Iterator, Mapping
from itertools import chain
from typing import Any, Generic, Protocol, TypeVar

from ..errors import (
    AmbiguousMessageTypeError,
    ConfigurationError,
    EventDecodingError,
    InvalidPayloadError,
    MissingOptionalDependencyError,
    NullPayloadError,
    PayloadIsolationError,
    TypeResolverError,
    UnknownMessageTypeError,
    UnsupportedMessageTypeError,
)
from ..models import RawEvent

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class Decoder(Protocol[T_co]):
    """Define synchronous network-free conversion from a raw envelope to an owned payload."""

    def decode(self, event: RawEvent) -> T_co:
        """Decode one delivery without mutating its raw envelope or performing network I/O.

        :param event: Immutable transport envelope to decode.
        :returns: A payload whose ownership transfers to the observation runtime.
        :raises EventDecodingError: The selected decoder cannot interpret the delivery.
        """
        ...


class PayloadAccess(Protocol[T]):
    """Define payload isolation and deterministic accounting for a decoded value type."""

    def clone_for_user(self, value: T) -> T:
        """Detach a value before exposing it to user code or another independent callback.

        :param value: Runtime-owned canonical payload.
        :returns: An independent mutable copy, or the same safely immutable value.
        :raises PayloadIsolationError: The implementation cannot detach the payload safely.
        """
        ...

    def estimate_bytes(self, value: T) -> int:
        """Measure a payload using the implementation documented accounted-memory model.

        :param value: Decoded payload to account for.
        :returns: A nonnegative integer accounted size, distinct from total process RSS.
        :raises PayloadIsolationError: The implementation cannot obtain a bounded safe estimate.
        """
        ...


class RawDecoder:
    """Pass through bytes and null payloads without an optional runtime dependency."""

    def decode(self, event: RawEvent) -> bytes | None:
        """Return the original immutable raw payload without treating empty bytes as null.

        :param event: Transport delivery containing bytes or None.
        :returns: The raw bytes or None exactly as received.
        """
        return event.payload


def _message_class(message_type: Any) -> type:
    """Validate a concrete generated Protobuf class, importing Protobuf only on demand.

    :param message_type: Candidate class; a returned coroutine is closed before rejection.
    :returns: The validated generated message class.
    :raises MissingOptionalDependencyError: The Protobuf extra is unavailable.
    :raises UnsupportedMessageTypeError: The candidate is not a concrete generated Protobuf class.
    """
    if inspect.iscoroutine(message_type):
        message_type.close()
        raise UnsupportedMessageTypeError("Type resolver must return a class synchronously")
    try:
        from google.protobuf.message import Message
    except ImportError as error:
        raise MissingOptionalDependencyError("Protobuf support requires the 'protobuf' extra") from error
    if (
        not isinstance(message_type, type)
        or not issubclass(message_type, Message)
        or message_type is Message
        or not getattr(message_type, "DESCRIPTOR", None)
    ):
        raise UnsupportedMessageTypeError("Expected a generated Protobuf message class")
    return message_type


class ProtobufDecoder(Generic[T]):
    """Parse ordinary Protobuf bytes into a configured generated message type.

    :ivar message_type: Validated generated message class instantiated for each decode.
    :ivar validate_required_fields: Whether proto2 required-field initialization is enforced.
    :ivar known_message_types: Single-element tuple used for early matcher schema binding.
    """

    def __init__(self, message_type: type[T], *, validate_required_fields: bool = True) -> None:
        """Bind one generated class without guessing its type from serialized bytes.

        :param message_type: Concrete generated Protobuf message class.
        :param validate_required_fields: Reject parsed proto2 messages missing required fields when True.
        :raises ConfigurationError: The required-field option is not bool.
        :raises MissingOptionalDependencyError: The Protobuf extra is unavailable.
        :raises UnsupportedMessageTypeError: The supplied class is unsupported.
        """
        self.message_type = _message_class(message_type)
        if not isinstance(validate_required_fields, bool):
            raise ConfigurationError("validate_required_fields must be bool")
        self.validate_required_fields = validate_required_fields
        self.known_message_types = (self.message_type,)

    def decode(self, event: RawEvent) -> T:
        """Parse one new message instance and optionally verify required-field initialization.

        :param event: Raw delivery; None is rejected while empty bytes may form a valid default message.
        :returns: A newly constructed message of the configured class.
        :raises NullPayloadError: The delivery has a null payload.
        :raises InvalidPayloadError: Bytes cannot be parsed or required fields are missing.
        """
        if event.payload is None:
            raise NullPayloadError("Null payload is not a Protobuf message")
        try:
            message = self.message_type()
            message.ParseFromString(event.payload)
            if self.validate_required_fields and not message.IsInitialized():
                raise InvalidPayloadError("Protobuf required fields are missing")
            return message
        except EventDecodingError:
            raise
        except Exception as error:
            raise InvalidPayloadError("Protobuf payload could not be parsed") from error


class ProtobufTypeResolver(Protocol):
    """Select a Protobuf class using explicit transport metadata or caller logic."""

    def resolve(self, event: RawEvent) -> type | None:
        """Choose a class without treating successful wire parsing as type discovery.

        :param event: Raw envelope whose source, headers, or bytes inform explicit selection.
        :returns: A concrete generated class, or None when the type is explicitly unknown.
        :raises EventDecodingError: The discriminator is invalid, ambiguous, or cannot be evaluated.
        """
        ...


def _unknown(reason: str) -> UnknownMessageTypeError:
    # A stable reason is suitable for diagnostics; discriminator values are not.
    """Create an unknown-type error carrying a stable reason without discriminator contents.

    :param reason: Safe reason code such as missing_discriminator or unregistered_type.
    :returns: An UnknownMessageTypeError with the same reason attribute.
    """
    error = UnknownMessageTypeError(reason)
    error.reason = reason
    return error


class HeaderTypeResolver:
    """Resolve one unique transport header through a copied discriminator-to-class mapping.

    :ivar header: Exact case-sensitive discriminator header name.
    :ivar encoding: Optional explicit text codec; None keeps discriminator values as bytes.
    :ivar _types: Copied mapping of normalized discriminator values to validated generated classes.
    :ivar known_message_types: Distinct registered classes supplied for matcher schema binding.
    """

    def __init__(
        self,
        *,
        header: str,
        types: Mapping[bytes | str, type],
        encoding: str | None = None,
    ) -> None:
        """Normalize discriminator keys and validate every configured generated class.

        :param header: Nonempty exact transport header name.
        :param types: Mapping from bytes, or explicitly encoded text, to generated classes.
        :param encoding: Optional codec for strict text discriminator normalization.
        :raises TypeResolverError: The name, encoding, discriminator keys, or normalized uniqueness is
            invalid.
        :raises UnsupportedMessageTypeError: A configured message class is unsupported.
        :raises MissingOptionalDependencyError: The Protobuf extra is unavailable.
        """
        if not isinstance(header, str) or not header:
            raise TypeResolverError("header must be a nonempty string")
        if encoding is not None:
            try:
                "".encode(encoding)
            except (LookupError, TypeError) as error:
                raise TypeResolverError("Unknown discriminator encoding") from error
        mapped: dict[bytes | str, type] = {}
        for key, message_type in types.items():
            if encoding is None and not isinstance(key, bytes):
                raise TypeResolverError("Text discriminator keys require an explicit encoding")
            if encoding is not None:
                if isinstance(key, bytes):
                    try:
                        key = key.decode(encoding)
                    except (UnicodeError, LookupError) as error:
                        raise TypeResolverError("Invalid discriminator encoding") from error
                if not isinstance(key, str):
                    raise TypeResolverError("Discriminator keys must be bytes or text")
            if key in mapped:
                raise TypeResolverError("Duplicate normalized discriminator mapping")
            mapped[key] = _message_class(message_type)
        self.header = header
        self.encoding = encoding
        self._types = mapped
        self.known_message_types = tuple(dict.fromkeys(mapped.values()))

    def resolve(self, event: RawEvent) -> type:
        """Require exactly one non-null discriminator and resolve its normalized value.

        :param event: Raw delivery whose header pairs retain ambiguity.
        :returns: The registered generated message class.
        :raises UnknownMessageTypeError: The header is missing, null, or unregistered.
        :raises AmbiguousMessageTypeError: The header appears more than once.
        :raises TypeResolverError: Strict discriminator decoding fails.
        """
        values = [value for name, value in event.headers if name == self.header]
        if not values:
            raise _unknown("missing_discriminator")
        if len(values) != 1:
            raise AmbiguousMessageTypeError("Type discriminator header is not unique")
        value = values[0]
        if value is None:
            raise _unknown("missing_discriminator")
        if self.encoding is not None:
            try:
                value = value.decode(self.encoding)
            except (UnicodeError, LookupError) as error:
                raise TypeResolverError("Invalid discriminator encoding") from error
        try:
            return self._types[value]
        except KeyError:
            raise _unknown("unregistered_type") from None


class SourceTypeResolver:
    """Resolve exact transport source names through a copied generated-class mapping.

    :ivar _types: Copied source-name-to-class mapping.
    :ivar known_message_types: Distinct configured classes available for matcher schema binding.
    """

    def __init__(self, types: Mapping[str, type]) -> None:
        """Validate source names and bind their generated Protobuf classes.

        :param types: Mapping from nonempty exact source names to generated message classes.
        :raises TypeResolverError: A source name is not a nonempty string.
        :raises UnsupportedMessageTypeError: A configured class is unsupported.
        :raises MissingOptionalDependencyError: The Protobuf extra is unavailable.
        """
        if any(not isinstance(source, str) or not source for source in types):
            raise TypeResolverError("Sources must be nonempty strings")
        self._types = {key: _message_class(value) for key, value in types.items()}
        self.known_message_types = tuple(dict.fromkeys(self._types.values()))

    def resolve(self, event: RawEvent) -> type:
        """Select the generated class assigned to the delivery exact source.

        :param event: Raw delivery whose source is used as the discriminator.
        :returns: The registered generated message class.
        :raises UnknownMessageTypeError: No type is registered for that source.
        """
        try:
            return self._types[event.source]
        except KeyError:
            raise _unknown("unregistered_type") from None


class CallableTypeResolver:
    """Wrap an explicit synchronous resolver callback and optional finite type declaration.

    :ivar _callback: Caller-supplied function selecting a class from a raw envelope.
    :ivar known_message_types: Optional validated finite class set for early structured matcher binding.
    """

    def __init__(
        self,
        callback: Callable[[RawEvent], type | None],
        *,
        known_message_types: tuple[type, ...] | None = None,
    ) -> None:
        """Store a callable resolver and validate its optional declared result classes.

        :param callback: Synchronous function returning a generated class or None.
        :param known_message_types: Optional finite set of generated classes the callback may select.
        :raises TypeResolverError: The callback is not callable.
        :raises UnsupportedMessageTypeError: A declared class is unsupported.
        :raises MissingOptionalDependencyError: Validating declared classes requires the Protobuf extra.
        """
        if not callable(callback):
            raise TypeResolverError("resolver callback must be callable")
        self._callback = callback
        self.known_message_types = (
            tuple(_message_class(value) for value in known_message_types)
            if known_message_types is not None
            else None
        )

    def resolve(self, event: RawEvent) -> type | None:
        """Invoke the explicit resolver once and validate any non-null class result.

        :param event: Raw envelope passed to the callback.
        :returns: A validated generated class, or None for an explicitly unknown type.
        :raises TypeResolverError: The callback raises an ordinary exception.
        :raises UnsupportedMessageTypeError: The callback returns an unsupported class or an awaitable.
        """
        try:
            value = self._callback(event)
        except Exception as error:
            raise TypeResolverError("Type resolver callback failed") from error
        return None if value is None else _message_class(value)


class ResolvingProtobufDecoder:
    """Resolve a mixed stream explicitly before decoding each message.

    :ivar resolver: Object implementing resolve(event) without hidden type guessing.
    :ivar validate_required_fields: Whether decoded proto2 messages must be initialized.
    :ivar known_message_types: Optional validated finite class set advertised by the resolver.
    """

    def __init__(self, *, resolver: ProtobufTypeResolver, validate_required_fields: bool = True) -> None:
        """Validate the resolver interface and capture its optional finite schema set.

        :param resolver: Explicit type resolver exposing a callable resolve method.
        :param validate_required_fields: Enforce proto2 initialization when True.
        :raises ConfigurationError: The initialization-check option is not bool.
        :raises TypeResolverError: The resolver does not expose resolve(event).
        :raises UnsupportedMessageTypeError: An advertised class is unsupported.
        """
        if not isinstance(validate_required_fields, bool):
            raise ConfigurationError("validate_required_fields must be bool")
        if not callable(getattr(resolver, "resolve", None)):
            raise TypeResolverError("resolver must expose resolve(event)")
        self.resolver = resolver
        self.validate_required_fields = validate_required_fields
        self.known_message_types = getattr(resolver, "known_message_types", None)
        if self.known_message_types is not None:
            self.known_message_types = tuple(_message_class(value) for value in self.known_message_types)

    def decode(self, event: RawEvent) -> Any:
        """Resolve a concrete class once, then decode through the ordinary Protobuf path.

        :param event: Raw delivery to resolve and parse.
        :returns: A new instance of the selected generated class.
        :raises UnknownMessageTypeError: The resolver returns None or reports an unknown discriminator.
        :raises TypeResolverError: A non-decoding resolver exception requires normalization.
        :raises EventDecodingError: Resolution or parsing fails for another declared decoding reason.
        """
        try:
            message_type = self.resolver.resolve(event)
        except EventDecodingError:
            raise
        except Exception as error:
            raise TypeResolverError("Type resolver failed") from error
        if message_type is None:
            raise _unknown("unregistered_type")
        return ProtobufDecoder(message_type, validate_required_fields=self.validate_required_fields).decode(
            event
        )


class DefaultPayloadAccess(Generic[T]):
    """Detach payloads and estimate their accounted memory with bounded traversal.

    The estimate sums Python shallow sizes of unique objects, descending through
    built-in containers, instance dictionaries, and slots to at most 64 levels
    and 100,000 visited references. At most 65 iterators remain pending. Opaque
    native heaps require a custom PayloadAccess estimate; this is not an RSS cap.
    """

    def clone_for_user(self, value: T) -> T:
        """Share supported immutable values and independently copy mutable payloads.

        Generated Protobuf messages use a new instance and CopyFrom. Other values
        use deepcopy, with an explicit check against returning a mutable original.

        :param value: Canonical payload that must remain isolated from user mutation.
        :returns: A detached mutable payload or safely shared immutable value.
        :raises PayloadIsolationError: Copying fails or does not detach a mutable object.
        """
        if value is None or type(value) in (bytes, str, int, float, bool):
            return value
        try:
            # Generated messages expose CopyFrom and DESCRIPTOR. Avoid importing
            # google.protobuf merely to clone an ordinary Python payload.
            if hasattr(value, "DESCRIPTOR") and callable(getattr(value, "CopyFrom", None)):
                clone = type(value)()
                getattr(clone, "CopyFrom")(value)
                return clone
            clone = copy.deepcopy(value)
            if clone is value and not self._immutable(value):
                raise PayloadIsolationError("Mutable payload deepcopy did not detach the value")
            return clone
        except PayloadIsolationError:
            raise
        except Exception as error:
            raise PayloadIsolationError("Payload cannot be copied safely") from error

    @staticmethod
    def _immutable(value: Any, depth: int = 0) -> bool:
        """Recognize supported immutable scalar or recursively immutable tuple/set values.

        :param value: Candidate whose identity may be safely shared after deepcopy.
        :param depth: Current recursive nesting depth, bounded at 64.
        :returns: True only for the supported immutable forms within the traversal bound.
        """
        if depth > 64:
            return False
        if value is None or type(value) in (bytes, str, int, float, bool, complex):
            return True
        if type(value) in (tuple, frozenset):
            return all(DefaultPayloadAccess._immutable(item, depth + 1) for item in value)
        return False

    def estimate_bytes(self, value: T) -> int:
        """Estimate bytes without serializing payloads or retaining an unbounded traversal stack.

        None and bytes have fixed explicit formulas; Protobuf uses 128 plus twice
        ByteSize. Other objects contribute each unique Python shallow size once.

        :param value: Payload to measure under the default accounted-memory model.
        :returns: Nonnegative accounted bytes.
        :raises PayloadIsolationError: Attributes, ByteSize, shallow size, or bounded traversal cannot be
            evaluated safely.
        """
        if value is None:
            return 16
        if type(value) is bytes:
            return len(value) + 33
        try:
            byte_size = object.__getattribute__(value, "ByteSize")
            descriptor = object.__getattribute__(value, "DESCRIPTOR")
        except AttributeError:
            byte_size = descriptor = None
        except Exception as error:
            raise PayloadIsolationError("Payload attributes cannot be measured") from error
        if descriptor is not None and callable(byte_size):
            try:
                measured = byte_size()
                if isinstance(measured, bool) or not isinstance(measured, int) or measured < 0:
                    raise PayloadIsolationError("Payload ByteSize must be a nonnegative integer")
                return 128 + measured * 2
            except Exception as error:
                raise PayloadIsolationError("Payload size cannot be estimated") from error

        def children(item: Any):
            """Enumerate direct container, backing-buffer, instance-dictionary, and slot references.

            :param item: Object whose owned references are traversed using builtin access paths.
            :yields: Direct referenced values without first materializing an unbounded child list.
            """
            item_type = type(item)
            if issubclass(item_type, dict):
                yield from chain.from_iterable(dict.items(item))
            elif issubclass(item_type, list):
                yield from list.__iter__(item)
            elif issubclass(item_type, tuple):
                yield from tuple.__iter__(item)
            elif issubclass(item_type, set):
                yield from set.__iter__(item)
            elif issubclass(item_type, frozenset):
                yield from frozenset.__iter__(item)
            elif issubclass(item_type, memoryview):
                yield item.obj
            if type(item) in (bytes, str, int, float, bool, complex, bytearray, range, type(None)):
                return
            try:
                attributes = object.__getattribute__(item, "__dict__")
            except AttributeError:
                attributes = None
            if issubclass(type(attributes), dict):
                yield attributes
            for cls in type(item).__mro__:
                slots = vars(cls).get("__slots__", ())
                if isinstance(slots, str):
                    slots = (slots,)
                for name in slots:
                    if name in {"__dict__", "__weakref__"}:
                        continue
                    if name.startswith("__") and not name.endswith("__"):
                        name = f"_{cls.__name__.lstrip('_')}{name}"
                    try:
                        yield object.__getattribute__(item, name)
                    except AttributeError:
                        pass

        pending: list[tuple[Iterator[Any], int]] = [(iter((value,)), 0)]
        seen: set[int] = set()
        total = 0
        references = 0
        while pending:
            iterator, depth = pending[-1]
            try:
                item = next(iterator)
            except StopIteration:
                pending.pop()
                continue
            except Exception as error:
                raise PayloadIsolationError("Payload attributes cannot be measured") from error
            references += 1
            if references > 100_000:
                raise PayloadIsolationError("Payload estimate exceeds traversal budget")
            if id(item) in seen:
                continue
            seen.add(id(item))
            try:
                total += sys.getsizeof(item)
            except Exception as error:
                raise PayloadIsolationError("Payload shallow size cannot be measured") from error
            if depth >= 64:
                raise PayloadIsolationError("Payload estimate exceeds traversal budget")
            pending.append((iter(children(item)), depth + 1))
        return total


class ProtobufPayloadAccess(DefaultPayloadAccess[T]):
    """Expose the default CopyFrom cloning and Protobuf ByteSize accounting under a schema-specific name."""

    pass


__all__ = [
    "CallableTypeResolver",
    "Decoder",
    "DefaultPayloadAccess",
    "HeaderTypeResolver",
    "PayloadAccess",
    "ProtobufDecoder",
    "ProtobufPayloadAccess",
    "ProtobufTypeResolver",
    "RawDecoder",
    "ResolvingProtobufDecoder",
    "SourceTypeResolver",
]
