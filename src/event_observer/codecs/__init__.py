"""Public codec extension API with lazy optional Protobuf support.

These names re-export the decoder implementations and payload-access protocols.
Importing the namespace does not import a native Protobuf runtime.
"""

from ..decoders import (
    CallableTypeResolver,
    Decoder,
    DefaultPayloadAccess,
    HeaderTypeResolver,
    PayloadAccess,
    ProtobufDecoder,
    ProtobufPayloadAccess,
    ProtobufTypeResolver,
    RawDecoder,
    ResolvingProtobufDecoder,
    SourceTypeResolver,
)

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
