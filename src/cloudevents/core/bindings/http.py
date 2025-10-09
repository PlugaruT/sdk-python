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

"""Format-agnostic HTTP binding functions for CloudEvents.

This module provides functions for converting CloudEvents to/from HTTP messages
in both structured and binary modes. These functions work with any format
implementation (JSON, Avro, Protobuf, etc.).
"""

from typing import Any, Tuple

from cloudevents.core.base import BaseCloudEvent
from cloudevents.core.formats.base import Format
from cloudevents.core.v1.event import CloudEvent

from ._http_binding import HTTPBinding, HTTPBody, HTTPHeaders, HTTPMessage

_http_binding = HTTPBinding()


def to_structured(
    event: BaseCloudEvent, event_format: Format
) -> Tuple[HTTPHeaders, HTTPBody]:
    """
    Convert CloudEvent to structured mode HTTP message.

    Example:
        from cloudevents.core.v1.event import CloudEvent
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.http import to_structured

        event = CloudEvent({"type": "example", "source": "test"}, {"key": "value"})
        headers, body = to_structured(event, JSONFormat())
    """
    return _http_binding.to_structured(event, event_format)


def from_structured(
    headers: HTTPHeaders,
    body: HTTPBody,
    event_format: Format,
    event_factory: Any = CloudEvent,
) -> BaseCloudEvent:
    """
    Parse structured mode HTTP message to CloudEvent.

    Example:
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.http import from_structured

        headers = {"content-type": "application/cloudevents+json"}
        body = b'{"type": "example", "source": "test", "data": {"key": "value"}}'
        event = from_structured(headers, body, JSONFormat())
    """
    return _http_binding.from_structured(headers, body, event_format, event_factory)


def to_binary(event: BaseCloudEvent, event_format: Format) -> Tuple[HTTPHeaders, HTTPBody]:
    """
    Convert CloudEvent to binary mode HTTP message.

    Example:
        from cloudevents.core.v1.event import CloudEvent
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.http import to_binary

        event = CloudEvent({"type": "example", "source": "test"}, "Hello World")
        headers, body = to_binary(event, JSONFormat())
    """
    return _http_binding.to_binary(event, event_format)


def from_binary(
    headers: HTTPHeaders,
    body: HTTPBody,
    event_format: Format,
    event_factory: Any = CloudEvent,
) -> BaseCloudEvent:
    """
    Parse binary mode HTTP message to CloudEvent.

    Example:
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.http import from_binary

        headers = {"ce-type": "example", "ce-source": "test", "content-type": "text/plain"}
        body = b"Hello World"
        event = from_binary(headers, body, JSONFormat())
    """
    return _http_binding.from_binary(headers, body, event_format, event_factory)


def from_http_message(
    headers: HTTPHeaders,
    body: HTTPBody,
    event_format: Format,
    event_factory: Any = CloudEvent,
) -> BaseCloudEvent:
    """
    Auto-detect and parse HTTP message to CloudEvent (structured or binary mode).

    This is a convenience function that automatically detects whether the message
    uses structured or binary content mode and calls the appropriate parser.

    Detection logic:
    - If any header starts with "ce-", uses binary mode
    - Otherwise uses structured mode

    Example:
        from cloudevents.core.formats.json import JSONFormat
        from cloudevents.core.bindings.http import from_http_message

        # Works with both structured and binary messages
        headers = {...}  # Any HTTP headers
        body = b"..."    # Any HTTP body
        event = from_http_message(headers, body, JSONFormat())

    :param headers: The HTTP headers dictionary.
    :param body: The HTTP body as bytes.
    :param event_format: The format to use for serialization/deserialization.
    :param event_factory: Factory function to create CloudEvent instances.
    :return: The parsed CloudEvent.
    """
    # Check for binary mode indicators (ce- prefixed headers)
    for header_name in headers.keys():
        if header_name.lower().startswith("ce-"):
            return from_binary(headers, body, event_format, event_factory)
    
    # Default to structured mode if no ce- headers found
    return from_structured(headers, body, event_format, event_factory)


# Export public API
__all__ = [
    "to_structured",
    "from_structured", 
    "to_binary",
    "from_binary",
    "from_http_message",
    "HTTPHeaders",
    "HTTPBody",
    "HTTPMessage",
]