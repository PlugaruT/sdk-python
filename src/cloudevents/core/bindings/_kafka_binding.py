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

"""Internal Kafka binding implementation."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from cloudevents.core.base import BaseCloudEvent
from cloudevents.core.formats.base import Format
from cloudevents.core.v1.event import CloudEvent

# Type aliases for Kafka message components
KafkaHeaders = Dict[str, bytes]
KafkaKey = Optional[bytes]
KafkaValue = bytes


@dataclass
class KafkaMessage:
    """Represents a Kafka message with headers, key, and value."""

    headers: KafkaHeaders
    key: KafkaKey
    value: KafkaValue


class KafkaBinding:
    """Kafka protocol binding for CloudEvents."""

    CE_PREFIX = "ce_"
    CONTENT_TYPE_HEADER = "content-type"

    def to_structured(
        self,
        event: BaseCloudEvent,
        event_format: Format,
        key: Optional[bytes] = None,
        key_mapper: Optional[Callable[[BaseCloudEvent], Optional[bytes]]] = None,
    ) -> KafkaMessage:
        """
        Convert CloudEvent to structured mode Kafka message.

        :param event: The CloudEvent to convert.
        :param event_format: The format to use for serialization.
        :param key: Optional Kafka message key.
        :param key_mapper: Optional function to derive key from event.
        :return: KafkaMessage with headers, key, and value.
        """
        # Determine content type
        content_type = event_format.get_content_type()

        # Create headers with content type
        headers: KafkaHeaders = {
            self.CONTENT_TYPE_HEADER: content_type.encode("utf-8")
        }

        # Serialize event to bytes
        value = event_format.write(event)

        # Determine message key
        if key_mapper:
            message_key = key_mapper(event)
        else:
            message_key = key

        return KafkaMessage(headers=headers, key=message_key, value=value)

    def from_structured(
        self,
        message: KafkaMessage,
        event_format: Format,
        event_factory: Any = CloudEvent,
    ) -> BaseCloudEvent:
        """
        Parse structured mode Kafka message to CloudEvent.

        :param message: The Kafka message.
        :param event_format: The format to use for deserialization.
        :param event_factory: Factory function to create CloudEvent instances.
        :return: The parsed CloudEvent.
        """
        return event_format.read(event_factory, message.value)

    def to_binary(
        self,
        event: BaseCloudEvent,
        event_format: Format,
        key: Optional[bytes] = None,
        key_mapper: Optional[Callable[[BaseCloudEvent], Optional[bytes]]] = None,
    ) -> KafkaMessage:
        """
        Convert CloudEvent to binary mode Kafka message.

        :param event: The CloudEvent to convert.
        :param event_format: The format to use for data serialization if needed.
        :param key: Optional Kafka message key.
        :param key_mapper: Optional function to derive key from event.
        :return: KafkaMessage with headers, key, and value.
        """
        headers: KafkaHeaders = {}

        # Map CloudEvents attributes to Kafka headers
        attributes = event.get_attributes()
        for attr_name, attr_value in attributes.items():
            header_name = f"{self.CE_PREFIX}{attr_name}"
            header_value = self._encode_header_value(attr_value)
            headers[header_name] = header_value.encode("utf-8")

        # Handle data using format's write_data method
        data = event.get_data()
        datacontenttype = event.get_datacontenttype()
        value = event_format.write_data(data, datacontenttype)

        # Determine message key
        if key_mapper:
            message_key = key_mapper(event)
        elif key is None:
            # Check for partitionkey extension
            partition_key = event.get_extension("partitionkey")
            if partition_key:
                message_key = str(partition_key).encode("utf-8")
            else:
                message_key = None
        else:
            message_key = key

        return KafkaMessage(headers=headers, key=message_key, value=value)

    def from_binary(
        self,
        message: KafkaMessage,
        event_format: Format,
        event_factory: Any = CloudEvent,
    ) -> BaseCloudEvent:
        """
        Parse binary mode Kafka message to CloudEvent.

        :param message: The Kafka message.
        :param event_format: The format to use for data deserialization if needed.
        :param event_factory: Factory function to create CloudEvent instances.
        :return: The parsed CloudEvent.
        """
        attributes: Dict[str, Any] = {}

        # Extract CloudEvents attributes from headers
        for header_name, header_value in message.headers.items():
            if header_name.startswith(self.CE_PREFIX):
                attr_name = header_name[len(self.CE_PREFIX) :]
                decoded_value = self._decode_header_value(header_value.decode("utf-8"))
                # Special handling for time attribute
                if attr_name == "time":
                    from dateutil.parser import isoparse

                    attributes[attr_name] = isoparse(decoded_value)
                else:
                    attributes[attr_name] = decoded_value

        # Parse data using format's read_data method
        data: Optional[Union[bytes, str, dict]] = None
        if message.value:
            data = event_format.read_data(
                message.value, attributes.get("datacontenttype")
            )

        return event_factory(attributes, data)

    def _encode_header_value(self, value: Any) -> str:
        """
        Encode a value for use in a Kafka header.

        :param value: The value to encode.
        :return: The encoded string.
        """
        if isinstance(value, datetime):
            str_value = value.isoformat()
            if str_value.endswith("+00:00"):
                str_value = str_value[:-6] + "Z"
        else:
            str_value = str(value)

        return str_value

    def _decode_header_value(self, value: str) -> str:
        """
        Decode a value from a Kafka header.

        :param value: The value to decode.
        :return: The decoded string.
        """
        return value
