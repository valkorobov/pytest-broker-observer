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
    def decode(self, event: RawEvent) -> T_co: ...


class PayloadAccess(Protocol[T]):
    def clone_for_user(self, value: T) -> T: ...

    def estimate_bytes(self, value: T) -> int: ...


class RawDecoder:
    def decode(self, event: RawEvent) -> bytes | None:
        return event.payload


def _message_class(message_type: Any) -> type:
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
    def __init__(self, message_type: type[T], *, validate_required_fields: bool = True) -> None:
        self.message_type = _message_class(message_type)
        if not isinstance(validate_required_fields, bool):
            raise ConfigurationError("validate_required_fields must be bool")
        self.validate_required_fields = validate_required_fields
        self.known_message_types = (self.message_type,)

    def decode(self, event: RawEvent) -> T:
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
    def resolve(self, event: RawEvent) -> type | None: ...


def _unknown(reason: str) -> UnknownMessageTypeError:
    # A stable reason is suitable for diagnostics; discriminator values are not.
    error = UnknownMessageTypeError(reason)
    error.reason = reason
    return error


class HeaderTypeResolver:
    def __init__(
        self,
        *,
        header: str,
        types: Mapping[bytes | str, type],
        encoding: str | None = None,
    ) -> None:
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
    def __init__(self, types: Mapping[str, type]) -> None:
        if any(not isinstance(source, str) or not source for source in types):
            raise TypeResolverError("Sources must be nonempty strings")
        self._types = {key: _message_class(value) for key, value in types.items()}
        self.known_message_types = tuple(dict.fromkeys(self._types.values()))

    def resolve(self, event: RawEvent) -> type:
        try:
            return self._types[event.source]
        except KeyError:
            raise _unknown("unregistered_type") from None


class CallableTypeResolver:
    def __init__(
        self,
        callback: Callable[[RawEvent], type | None],
        *,
        known_message_types: tuple[type, ...] | None = None,
    ) -> None:
        if not callable(callback):
            raise TypeResolverError("resolver callback must be callable")
        self._callback = callback
        self.known_message_types = (
            tuple(_message_class(value) for value in known_message_types)
            if known_message_types is not None
            else None
        )

    def resolve(self, event: RawEvent) -> type | None:
        try:
            value = self._callback(event)
        except Exception as error:
            raise TypeResolverError("Type resolver callback failed") from error
        return None if value is None else _message_class(value)


class ResolvingProtobufDecoder:
    def __init__(self, *, resolver: ProtobufTypeResolver, validate_required_fields: bool = True) -> None:
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
    """Detached copies with a bounded, deterministic size estimate.

    The estimate sums Python shallow sizes of unique objects, descending through
    built-in containers, instance dictionaries and slots to at most 64 levels
    and 100,000 visited references. Traversal retains at most 65 iterators, rather
    than building a pending copy of an entire container. Opaque native heaps
    require a custom PayloadAccess estimate; this does not bound process RSS.
    """

    def clone_for_user(self, value: T) -> T:
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
        if depth > 64:
            return False
        if value is None or type(value) in (bytes, str, int, float, bool, complex):
            return True
        if type(value) in (tuple, frozenset):
            return all(DefaultPayloadAccess._immutable(item, depth + 1) for item in value)
        return False

    def estimate_bytes(self, value: T) -> int:
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
