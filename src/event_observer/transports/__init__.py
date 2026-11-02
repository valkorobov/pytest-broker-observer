"""Explicit transport selection with no implicit clients or connections.

Import a concrete adapter module to configure Kafka, RabbitMQ, or Redis
Streams. Optional broker clients are loaded only when a session opens; this
package performs no registration, environment discovery, or network activity.
"""
