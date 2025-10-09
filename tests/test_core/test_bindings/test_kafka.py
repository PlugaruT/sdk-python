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

from cloudevents.core.base import BaseCloudEvent
from cloudevents.core.bindings.kafka import (
    KafkaMessage,
    from_binary,
    from_kafka_message,
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

    message = to_structured(event, event_format)

    assert message.headers == {"content-type": b"application/cloudevents+json"}
    assert message.key is None
    parsed_value = json.loads(message.value)
    assert parsed_value["id"] == "123"
    assert parsed_value["source"] == "test/source"
    assert parsed_value["type"] == "test.type"
    assert parsed_value["data"] == {"key": "value"}


def test_to_structured_with_key() -> None:
    event = CloudEvent(
        {"id": "123", "type": "test", "source": "test", "specversion": "1.0"}, None
    )
    event_format = JSONFormat()

    message = to_structured(event, event_format, key=b"my-key")

    assert message.key == b"my-key"


def test_to_structured_with_key_mapper() -> None:
    event = CloudEvent(
        {
            "id": "123",
            "type": "test",
            "source": "test",
            "specversion": "1.0",
            "subject": "user123",
        },
        None,
    )
    event_format = JSONFormat()

    def key_mapper(e: BaseCloudEvent) -> bytes:
        return str(e.get_subject()).encode("utf-8")

    message = to_structured(event, event_format, key_mapper=key_mapper)

    assert message.key == b"user123"


def test_from_structured_with_json_format() -> None:
    headers = {"content-type": b"application/cloudevents+json"}
    value = b'{"id": "123", "source": "test/source", "type": "test.type", "specversion": "1.0", "data": {"key": "value"}}'
    message = KafkaMessage(headers=headers, key=None, value=value)
    event_format = JSONFormat()

    event = from_structured(message, event_format)

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

    message = to_binary(event, event_format)

    # Check headers
    assert message.headers["ce_id"] == b"123"
    assert message.headers["ce_source"] == b"test/source"
    assert message.headers["ce_type"] == b"test.type"
    assert message.headers["ce_specversion"] == b"1.0"
    assert message.headers["ce_datacontenttype"] == b"text/plain"

    # Check data
    assert message.value == b"Hello World"
    assert message.key is None


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

    message = to_binary(event, event_format)

    assert message.headers["ce_datacontenttype"] == b"application/json"
    assert json.loads(message.value) == {"key": "value"}


def test_to_binary_with_partitionkey() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "partitionkey": "user123",
    }
    event = CloudEvent(attributes=attributes, data="test")
    event_format = JSONFormat()

    message = to_binary(event, event_format)

    # partitionkey should be used as message key
    assert message.key == b"user123"

    # partitionkey should also be in headers
    assert message.headers["ce_partitionkey"] == b"user123"


def test_to_binary_with_explicit_key_overrides_partitionkey() -> None:
    attributes = {
        "id": "123",
        "source": "test/source",
        "type": "test.type",
        "specversion": "1.0",
        "partitionkey": "user123",
    }
    event = CloudEvent(attributes=attributes, data="test")
    event_format = JSONFormat()

    message = to_binary(event, event_format, key=b"override-key")

    assert message.key == b"override-key"


def test_from_binary_with_string_data() -> None:
    headers = {
        "ce_id": b"123",
        "ce_source": b"test/source",
        "ce_type": b"test.type",
        "ce_specversion": b"1.0",
        "ce_datacontenttype": b"text/plain",
    }
    message = KafkaMessage(headers=headers, key=None, value=b"Hello World")
    event_format = JSONFormat()

    event = from_binary(message, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
    assert event.get_datacontenttype() == "text/plain"
    assert event.get_data() == "Hello World"


def test_from_binary_with_json_data() -> None:
    headers = {
        "ce_id": b"123",
        "ce_source": b"test/source",
        "ce_type": b"test.type",
        "ce_specversion": b"1.0",
        "ce_datacontenttype": b"application/json",
    }
    message = KafkaMessage(headers=headers, key=None, value=b'{"key": "value"}')
    event_format = JSONFormat()

    event = from_binary(message, event_format)

    assert event.get_data() == {"key": "value"}


def test_from_binary_with_bytes_data() -> None:
    headers = {
        "ce_id": b"123",
        "ce_source": b"test/source",
        "ce_type": b"test.type",
        "ce_specversion": b"1.0",
        "ce_datacontenttype": b"application/octet-stream",
    }
    message = KafkaMessage(headers=headers, key=None, value=b"\x00\x01\x02\x03")
    event_format = JSONFormat()

    event = from_binary(message, event_format)

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

    message = to_binary(event, event_format)

    assert message.headers["ce_time"] == b"2023-10-25T17:09:19.736166Z"


def test_from_binary_with_time_attribute() -> None:
    headers = {
        "ce_id": b"123",
        "ce_source": b"test/source",
        "ce_type": b"test.type",
        "ce_specversion": b"1.0",
        "ce_time": b"2023-10-25T17:09:19.736166Z",
    }
    message = KafkaMessage(headers=headers, key=None, value=b"")
    event_format = JSONFormat()

    event = from_binary(message, event_format)

    assert event.get_time() == datetime(
        2023, 10, 25, 17, 9, 19, 736166, tzinfo=timezone.utc
    )


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

    message = to_binary(event, event_format)

    assert message.headers["ce_customext"] == b"custom value"
    assert message.headers["ce_anotherext"] == b"42"


def test_from_binary_with_extension_attributes() -> None:
    headers = {
        "ce_id": b"123",
        "ce_source": b"test/source",
        "ce_type": b"test.type",
        "ce_specversion": b"1.0",
        "ce_customext": b"custom value",
        "ce_anotherext": b"42",
    }
    message = KafkaMessage(headers=headers, key=None, value=b"")
    event_format = JSONFormat()

    event = from_binary(message, event_format)

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

    message = to_binary(event, event_format)

    assert message.value == b""


def test_from_binary_with_no_data() -> None:
    headers = {
        "ce_id": b"123",
        "ce_source": b"test/source",
        "ce_type": b"test.type",
        "ce_specversion": b"1.0",
    }
    message = KafkaMessage(headers=headers, key=None, value=b"")
    event_format = JSONFormat()

    event = from_binary(message, event_format)

    assert event.get_data() is None


def test_structured_mode_roundtrip() -> None:
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

    message = to_structured(event, event_format, key=b"test-key")
    reconstructed = from_structured(message, event_format)

    assert reconstructed.get_id() == event.get_id()
    assert reconstructed.get_source() == event.get_source()
    assert reconstructed.get_type() == event.get_type()
    assert reconstructed.get_time() == event.get_time()
    assert reconstructed.get_datacontenttype() == event.get_datacontenttype()
    assert reconstructed.get_dataschema() == event.get_dataschema()
    assert reconstructed.get_subject() == event.get_subject()
    assert reconstructed.get_extension("customext") == event.get_extension("customext")
    assert reconstructed.get_data() == event.get_data()


def test_binary_mode_roundtrip() -> None:
    attributes = {
        "id": "123",
        "source": "test/source with spaces",
        "type": "test.type",
        "specversion": "1.0",
        "time": datetime(2023, 10, 25, 17, 9, 19, 736166, tzinfo=timezone.utc),
        "datacontenttype": "text/plain",
        "customext": "value with special chars: @#$%",
        "partitionkey": "partition123",
    }
    event = CloudEvent(attributes=attributes, data="Test data")
    event_format = JSONFormat()

    message = to_binary(event, event_format)
    reconstructed = from_binary(message, event_format)

    assert reconstructed.get_id() == event.get_id()
    assert reconstructed.get_source() == event.get_source()
    assert reconstructed.get_type() == event.get_type()
    assert reconstructed.get_datacontenttype() == event.get_datacontenttype()
    assert reconstructed.get_extension("customext") == event.get_extension("customext")
    assert reconstructed.get_extension("partitionkey") == event.get_extension(
        "partitionkey"
    )
    assert reconstructed.get_data() == event.get_data()
    # Key is derived from partitionkey
    assert message.key == b"partition123"


def test_from_kafka_message_detects_structured_mode() -> None:
    # Message without ce_ headers should be detected as structured
    headers = {"content-type": b"application/cloudevents+json"}
    value = b'{"id": "123", "source": "test/source", "type": "test.type", "specversion": "1.0", "data": {"key": "value"}}'
    message = KafkaMessage(headers=headers, key=None, value=value)
    event_format = JSONFormat()

    event = from_kafka_message(message, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
    assert event.get_data() == {"key": "value"}


def test_from_kafka_message_detects_binary_mode() -> None:
    # Message with ce_ headers should be detected as binary
    headers = {
        "ce_id": b"123",
        "ce_source": b"test/source",
        "ce_type": b"test.type",
        "ce_specversion": b"1.0",
        "ce_datacontenttype": b"text/plain",
    }
    message = KafkaMessage(headers=headers, key=None, value=b"Hello World")
    event_format = JSONFormat()

    event = from_kafka_message(message, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
    assert event.get_datacontenttype() == "text/plain"
    assert event.get_data() == "Hello World"


def test_from_kafka_message_defaults_to_structured() -> None:
    # Message without clear indicators should default to structured
    headers = {"some-other-header": b"value"}
    value = b'{"id": "123", "source": "test/source", "type": "test.type", "specversion": "1.0"}'
    message = KafkaMessage(headers=headers, key=None, value=value)
    event_format = JSONFormat()

    event = from_kafka_message(message, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"


def test_from_kafka_message_binary_mode_with_mixed_headers() -> None:
    # If ce_ headers exist, should use binary mode regardless of other headers
    headers = {
        "content-type": b"application/cloudevents+json",
        "ce_id": b"123",
        "ce_source": b"test/source",
        "ce_type": b"test.type",
        "ce_specversion": b"1.0",
        "ce_datacontenttype": b"text/plain",
    }
    value = b"Hello World"
    message = KafkaMessage(headers=headers, key=None, value=value)
    event_format = JSONFormat()

    event = from_kafka_message(message, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
    assert event.get_data() == "Hello World"


def test_from_kafka_message_structured_with_various_headers() -> None:
    # Structured mode should work with any non-ce_ headers
    headers = {"custom-header": b"value", "another-header": b"data"}
    value = b'{"id": "123", "source": "test/source", "type": "test.type", "specversion": "1.0"}'
    message = KafkaMessage(headers=headers, key=None, value=value)
    event_format = JSONFormat()

    event = from_kafka_message(message, event_format)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"


def test_from_kafka_message_with_custom_event_factory() -> None:
    # Should work with custom event factory in structured mode
    headers = {"content-type": b"application/cloudevents+json"}
    value = b'{"id": "123", "source": "test/source", "type": "test.type", "specversion": "1.0"}'
    message = KafkaMessage(headers=headers, key=None, value=value)
    event_format = JSONFormat()

    event = from_kafka_message(message, event_format, CloudEvent)

    assert event.get_id() == "123"
    assert event.get_source() == "test/source"
    assert event.get_type() == "test.type"
