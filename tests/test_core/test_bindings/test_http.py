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


import json
from datetime import datetime, timezone

from cloudevents.core.bindings.http import (
    from_binary,
    from_http_message,
    from_structured,
    to_binary,
    to_structured,
)
from cloudevents.core.formats.json import JSONFormat
from cloudevents.core.v1.event import CloudEvent


def test_to_structured_with_json_format() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "time": datetime(2023, 10, 25, 17, 9, 19, 736166, tzinfo=timezone.utc),
        "datacontenttype": "application/json",
    }
    event = CloudEvent(attributes=attributes, data={"key": "value"})
    event_format = JSONFormat()

    headers, body = to_structured(event, event_format)

    assert headers["content-type"] == "application/cloudevents+json"
    parsed_body = json.loads(body)
    assert parsed_body["id"] == "123"
    assert parsed_body["source"] == "test/source"
    assert parsed_body["type"] == "test.type"
    assert parsed_body["data"] == {"key": "value"}


def test_from_structured_with_json_format() -> None:
    headers = {"content-type": "application/cloudevents+json"}
    body = b'{"id": "123", "source": "test/source", "type": "test.type", "specversion": "1.0", "data": {"key": "value"}}'
    event_format = JSONFormat()

    event = from_structured(headers, body, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
    assert event.get_data() == {"key": "value"}


def test_to_binary_with_string_data() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "datacontenttype": "text/plain",
    }
    event = CloudEvent(attributes=attributes, data="Hello World")
    event_format = JSONFormat()

    headers, body = to_binary(event, event_format)

    assert headers["ce-id"] == "123"
    assert headers["ce-source"] == "test%2Fsource"  # URL encoded
    assert headers["ce-type"] == "test.type"
    assert headers["ce-specversion"] == "1.0"
    assert headers["content-type"] == "text/plain"
    assert body == b"Hello World"


def test_to_binary_with_json_data() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "datacontenttype": "application/json",
    }
    event = CloudEvent(attributes=attributes, data={"key": "value"})
    event_format = JSONFormat()

    headers, body = to_binary(event, event_format)

    assert headers["ce-id"] == "123"
    assert headers["content-type"] == "application/json"
    assert json.loads(body) == {"key": "value"}


def test_to_binary_with_bytes_data() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "datacontenttype": "application/octet-stream",
    }
    event = CloudEvent(attributes=attributes, data=b"\x00\x01\x02\x03")
    event_format = JSONFormat()

    headers, body = to_binary(event, event_format)

    assert headers["content-type"] == "application/octet-stream"
    assert body == b"\x00\x01\x02\x03"


def test_from_binary_with_string_data() -> None:
    headers = {
        "ce-id": "123",
        "ce-source": "test%2Fsource",  # URL encoded
        "ce-type": "test.type",
        "ce-specversion": "1.0",
        "content-type": "text/plain",
    }
    body = b"Hello World"
    event_format = JSONFormat()

    event = from_binary(headers, body, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"  # Should be decoded
    assert event.get_type() == "test.type"
    assert event.get_datacontenttype() == "text/plain"
    assert event.get_data() == "Hello World"


def test_from_binary_with_json_data() -> None:
    headers = {
        "ce-id": "123",
        "ce-source": "test/source",
        "ce-type": "test.type",
        "ce-specversion": "1.0",
        "content-type": "application/json",
    }
    body = b'{"key": "value"}'
    event_format = JSONFormat()

    event = from_binary(headers, body, event_format)

    assert event.get_data() == {"key": "value"}


def test_from_binary_with_bytes_data() -> None:
    headers = {
        "ce-id": "123",
        "ce-source": "test/source",
        "ce-type": "test.type",
        "ce-specversion": "1.0",
        "content-type": "application/octet-stream",
    }
    body = b"\x00\x01\x02\x03"
    event_format = JSONFormat()

    event = from_binary(headers, body, event_format)

    assert event.get_data() == b"\x00\x01\x02\x03"


def test_to_binary_with_time_attribute() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "time": datetime(2023, 10, 25, 17, 9, 19, 736166, tzinfo=timezone.utc),
    }
    event = CloudEvent(attributes=attributes, data=None)
    event_format = JSONFormat()

    headers, body = to_binary(event, event_format)

    assert headers["ce-time"] == "2023-10-25T17%3A09%3A19.736166Z"  # URL encoded


def test_from_binary_with_time_attribute() -> None:
    headers = {
        "ce-id": "123",
        "ce-source": "test/source",
        "ce-type": "test.type",
        "ce-specversion": "1.0",
        "ce-time": "2023-10-25T17%3A09%3A19.736166Z",  # URL encoded
    }
    body = b""
    event_format = JSONFormat()

    event = from_binary(headers, body, event_format)

    assert event.get_time() == datetime(2023, 10, 25, 17, 9, 19, 736166, tzinfo=timezone.utc)


def test_to_binary_with_extension_attributes() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "customext": "custom value",
        "anotherext": 42,
    }
    event = CloudEvent(attributes=attributes, data=None)
    event_format = JSONFormat()

    headers, body = to_binary(event, event_format)

    assert headers["ce-customext"] == "custom%20value"  # Space URL encoded
    assert headers["ce-anotherext"] == "42"


def test_from_binary_with_extension_attributes() -> None:
    headers = {
        "ce-id": "123",
        "ce-source": "test/source",
        "ce-type": "test.type",
        "ce-specversion": "1.0",
        "ce-customext": "custom%20value",  # Space URL encoded
        "ce-anotherext": "42",
    }
    body = b""
    event_format = JSONFormat()

    event = from_binary(headers, body, event_format)

    assert event.get_extension("customext") == "custom value"
    assert event.get_extension("anotherext") == "42"


def test_to_binary_with_no_data() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
    }
    event = CloudEvent(attributes=attributes, data=None)
    event_format = JSONFormat()

    headers, body = to_binary(event, event_format)

    assert "content-type" not in headers
    assert body == b""


def test_from_binary_with_no_data() -> None:
    headers = {
        "ce-id": "123",
        "ce-source": "test/source",
        "ce-type": "test.type",
        "ce-specversion": "1.0",
    }
    body = b""
    event_format = JSONFormat()

    event = from_binary(headers, body, event_format)

    assert event.get_data() is None


def test_structured_mode_preserves_all_attributes() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "time": datetime(2023, 10, 25, 17, 9, 19, 736166, tzinfo=timezone.utc),
        "datacontenttype": "application/json",
        "dataschema": "http://example.com/schema",
        "subject": "test/subject",
        "customext": "value",
    }
    event = CloudEvent(attributes=attributes, data={"key": "value"})
    event_format = JSONFormat()

    headers, body = to_structured(event, event_format)
    reconstructed = from_structured(headers, body, event_format)

    assert reconstructed.get_id() == event.get_id()
    assert reconstructed.get_source() == event.get_source()
    assert reconstructed.get_type() == event.get_type()
    assert reconstructed.get_time() == event.get_time()
    assert reconstructed.get_datacontenttype() == event.get_datacontenttype()
    assert reconstructed.get_dataschema() == event.get_dataschema()
    assert reconstructed.get_subject() == event.get_subject()
    assert reconstructed.get_extension("customext") == event.get_extension("customext")
    assert reconstructed.get_data() == event.get_data()


def test_to_binary_with_dict_data_non_json_content_type() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "datacontenttype": "application/xml",  # Non-JSON content type
    }
    event = CloudEvent(attributes=attributes, data={"key": "value"})
    event_format = JSONFormat()

    headers, body = to_binary(event, event_format)

    assert headers["content-type"] == "application/xml"
    # For non-JSON content type, dict data gets converted to its string representation
    assert body == b"{'key': 'value'}"


def test_binary_mode_roundtrip() -> None:
    attributes = {
        "id": "123",
        "source": "test/source with spaces",
        "type": "test.type",
        "specversion": "1.0",
        "time": datetime(2023, 10, 25, 17, 9, 19, 736166, tzinfo=timezone.utc),
        "datacontenttype": "text/plain",
        "customext": "value with special chars: @#$%",
    }
    event = CloudEvent(attributes=attributes, data="Test data")
    event_format = JSONFormat()

    headers, body = to_binary(event, event_format)
    reconstructed = from_binary(headers, body, event_format)

    assert reconstructed.get_id() == event.get_id()
    assert reconstructed.get_source() == event.get_source()
    assert reconstructed.get_type() == event.get_type()
    assert reconstructed.get_datacontenttype() == event.get_datacontenttype()
    assert reconstructed.get_extension("customext") == event.get_extension("customext")
    assert reconstructed.get_data() == event.get_data()


def test_from_http_message_detects_structured_mode() -> None:
    # Message without ce- headers should be detected as structured
    headers = {"content-type": "application/cloudevents+json"}
    body = b'{"id": "123", "source": "test/source", "type": "test.type", "specversion": "1.0", "data": {"key": "value"}}'
    event_format = JSONFormat()

    event = from_http_message(headers, body, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
    assert event.get_data() == {"key": "value"}


def test_from_http_message_detects_binary_mode() -> None:
    # Message with ce- headers should be detected as binary
    headers = {
        "ce-id": "123",
        "ce-source": "test/source",
        "ce-type": "test.type",
        "ce-specversion": "1.0",
        "content-type": "text/plain",
    }
    body = b"Hello World"
    event_format = JSONFormat()

    event = from_http_message(headers, body, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
    assert event.get_datacontenttype() == "text/plain"
    assert event.get_data() == "Hello World"


def test_from_http_message_defaults_to_structured() -> None:
    # Message without clear indicators should default to structured
    headers = {"some-other-header": "value"}
    body = b'{"id": "123", "source": "test/source", "type": "test.type", "specversion": "1.0"}'
    event_format = JSONFormat()

    event = from_http_message(headers, body, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"


def test_from_http_message_binary_mode_with_mixed_headers() -> None:
    # If ce- headers exist, should use binary mode regardless of other headers
    headers = {
        "content-type": "application/cloudevents+json",
        "ce-id": "123",
        "ce-source": "test/source",
        "ce-type": "test.type",
        "ce-specversion": "1.0",
        "ce-datacontenttype": "text/plain",
    }
    body = b"Hello World"
    event_format = JSONFormat()

    event = from_http_message(headers, body, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
    assert event.get_data() == "Hello World"


def test_from_http_message_case_insensitive_headers() -> None:
    # Header detection should be case insensitive (CE-, Ce-, ce-, etc.)
    headers = {
        "CE-ID": "123",
        "Ce-Source": "test/source",
        "ce-type": "test.type",
        "CE-SPECVERSION": "1.0",
        "content-type": "text/plain",
    }
    body = b"Hello World"
    event_format = JSONFormat()

    event = from_http_message(headers, body, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
    assert event.get_data() == "Hello World"


def test_from_http_message_with_custom_event_factory() -> None:
    # Should work with custom event factory in structured mode
    headers = {"content-type": "application/cloudevents+json"}
    body = b'{"id": "123", "source": "test/source", "type": "test.type", "specversion": "1.0"}'
    event_format = JSONFormat()

    event = from_http_message(headers, body, event_format, CloudEvent)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"