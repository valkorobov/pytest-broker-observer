"""Re-export synchronous decoder and detached-payload extension contracts.

Custom decoders implement Decoder; payload isolation and accounted sizing are
supplied through PayloadAccess or its default implementation.
"""

from ..decoders import Decoder, DefaultPayloadAccess, PayloadAccess

__all__ = ["Decoder", "DefaultPayloadAccess", "PayloadAccess"]
