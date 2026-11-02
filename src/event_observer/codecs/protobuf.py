"""Expose fixed-type and explicitly resolved Protobuf decoders and payload access.

The optional Protobuf runtime is loaded only when the corresponding decoder or
resolver is constructed, rather than by this re-export module.
"""

from ..decoders import ProtobufDecoder, ProtobufPayloadAccess, ResolvingProtobufDecoder

__all__ = ["ProtobufDecoder", "ProtobufPayloadAccess", "ResolvingProtobufDecoder"]
