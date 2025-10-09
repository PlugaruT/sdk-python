#  Copyright 2018-Present The CloudEvents Authors
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Format-agnostic Kafka binding functions for CloudEvents.

This module provides functions for converting CloudEvents to/from Kafka messages
in both structured and binary modes. These functions work with any format
implementation (JSON, Avro, Protobuf, etc.).

Example:
    from cloudevents.core.v1.event import CloudEvent
    from cloudevents.core.formats.json import JSONFormat
    from cloudevents.core.bindings.kafka import to_structured, to_binary

    event = CloudEvent({"type": "example", "source": "test"}, {"key": "value"})

    # Structured mode
    message = to_structured(event, JSONFormat())

    # Binary mode with partitioning
    message = to_binary(event, JSONFormat(), key=b"my-key")
"""

from typing import Any, Callable, Optional

from cloudevents.core.base import BaseCloudEvent
from cloudevents.core.formats.base import Format
from cloudevents.core.v1.event import CloudEvent

from ._kafka_binding import KafkaBinding, KafkaKey, KafkaMessage

_kafka_binding = KafkaBinding()


def to_structured(
    event: BaseCloudEvent,
    event_format: Format,
    key: Optional[bytes] = None,
    key_mapper: Optional[Callable[[BaseCloudEvent], Optional[bytes]]] = None,
) -> KafkaMessage:
    """
    Convert CloudEvent to structured mode Kafka message.

    In structured mode, the entire event (attributes and data) is encoded
    in the message value using the specified format.

    Example:
        from cloudevents.core.v1.event import CloudEvent
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.kafka import to_structured

        event = CloudEvent({"type": "example", "source": "test"}, {"key": "value"})
        message = to_structured(event, JSONFormat())
        # message.headers = {"content-type": b"application/cloudevents+json"}
        # message.value = b'{"specversion":"1.0","type":"example",...}'

    :param event: The CloudEvent to convert.
    :param event_format: The format to use (JSONFormat, AvroFormat, etc.).
    :param key: Optional Kafka message key.
    :param key_mapper: Optional function to derive key from event.
    :return: KafkaMessage with headers, key, and value.
    """
    return _kafka_binding.to_structured(event, event_format, key, key_mapper)


def from_structured(
    message: KafkaMessage,
    event_format: Format,
    event_factory: Any = CloudEvent,
) -> BaseCloudEvent:
    """
    Parse structured mode Kafka message to CloudEvent.

    Example:
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.kafka import from_structured, KafkaMessage

        message = KafkaMessage(
            headers={"content-type": b"application/cloudevents+json"},
            key=None,
            value=b'{"type":"example","source":"test","data":{"key":"value"}}'
        )
        event = from_structured(message, JSONFormat())

    :param message: The Kafka message to parse.
    :param event_format: The format to use for deserialization.
    :param event_factory: Factory function to create CloudEvent instances.
    :return: The parsed CloudEvent.
    """
    return _kafka_binding.from_structured(message, event_format, event_factory)


def to_binary(
    event: BaseCloudEvent,
    event_format: Format,
    key: Optional[bytes] = None,
    key_mapper: Optional[Callable[[BaseCloudEvent], Optional[bytes]]] = None,
) -> KafkaMessage:
    """
    Convert CloudEvent to binary mode Kafka message.

    In binary mode, CloudEvents attributes are mapped to Kafka headers with
    'ce_' prefix, and the event data is placed in the message value.

    Example:
        from cloudevents.core.v1.event import CloudEvent
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.kafka import to_binary

        event = CloudEvent(
            {"type": "example", "source": "test", "partitionkey": "user123"},
            "Hello World"
        )
        message = to_binary(event, JSONFormat())
        # message.headers = {"ce_type": b"example", "ce_source": b"test", ...}
        # message.key = b"user123"  # from partitionkey
        # message.value = b"Hello World"

    :param event: The CloudEvent to convert.
    :param event_format: The format to use for data serialization if needed.
    :param key: Optional Kafka message key (overrides partitionkey).
    :param key_mapper: Optional function to derive key from event.
    :return: KafkaMessage with headers, key, and value.
    """
    return _kafka_binding.to_binary(event, event_format, key, key_mapper)


def from_binary(
    message: KafkaMessage,
    event_format: Format,
    event_factory: Any = CloudEvent,
) -> BaseCloudEvent:
    """
    Parse binary mode Kafka message to CloudEvent.

    Example:
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.kafka import from_binary, KafkaMessage

        message = KafkaMessage(
            headers={
                "ce_type": b"example",
                "ce_source": b"test",
                "ce_id": b"123",
                "ce_specversion": b"1.0"
            },
            key=b"user123",
            value=b"Hello World"
        )
        event = from_binary(message, JSONFormat())

    :param message: The Kafka message to parse.
    :param event_format: The format to use for data deserialization if needed.
    :param event_factory: Factory function to create CloudEvent instances.
    :return: The parsed CloudEvent.
    """
    return _kafka_binding.from_binary(message, event_format, event_factory)


def from_kafka_message(
    message: KafkaMessage,
    event_format: Format,
    event_factory: Any = CloudEvent,
) -> BaseCloudEvent:
    """
    Auto-detect and parse Kafka message to CloudEvent (structured or binary mode).

    This is a convenience function that automatically detects whether the message
    uses structured or binary content mode and calls the appropriate parser.

    Detection logic:
    - If any header starts with "ce_", uses binary mode
    - Otherwise uses structured mode

    Example:
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.kafka import from_kafka_message, KafkaMessage

        # Works with both structured and binary messages
        message = KafkaMessage(...)  # Any Kafka message format
        event = from_kafka_message(message, JSONFormat())

    :param message: The Kafka message to parse.
    :param event_format: The format to use for serialization/deserialization.
    :param event_factory: Factory function to create CloudEvent instances.
    :return: The parsed CloudEvent.
    """
    # Check for binary mode indicators (ce_ prefixed headers)
    for header_name in message.headers.keys():
        if header_name.startswith("ce_"):
            return from_binary(message, event_format, event_factory)
    
    # Default to structured mode if no ce_ headers found
    return from_structured(message, event_format, event_factory)


# Re-export types for convenience
__all__ = [
    "to_structured",
    "from_structured",
    "to_binary",
    "from_binary",
    "from_kafka_message",
    "KafkaMessage",
    "KafkaKey",
]
