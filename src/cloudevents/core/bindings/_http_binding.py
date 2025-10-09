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

"""Internal HTTP binding implementation."""

import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Union

from cloudevents.core.base import BaseCloudEvent
from cloudevents.core.formats.base import Format
from cloudevents.core.v1.event import CloudEvent

HTTPHeaders = Dict[str, str]
HTTPBody = bytes


@dataclass
class HTTPMessage:
    """Represents an HTTP message with headers and body."""

    headers: HTTPHeaders
    body: HTTPBody


class HTTPBinding:
    """HTTP protocol binding for CloudEvents."""

    CE_PREFIX = "ce-"
    CONTENT_TYPE_HEADER = "content-type"

    def to_structured(
        self, event: BaseCloudEvent, event_format: Format
    ) -> Tuple[HTTPHeaders, HTTPBody]:
        """
        Convert CloudEvent to structured mode HTTP message.

        :param event: The CloudEvent to convert.
        :param event_format: The format to use for serialization.
        :return: Tuple of (headers, body).
        """
        content_type = event_format.get_content_type()
        headers = {self.CONTENT_TYPE_HEADER: content_type}
        body = event_format.write(event)
        return headers, body

    def from_structured(
        self,
        headers: HTTPHeaders,
        body: HTTPBody,
        event_format: Format,
        event_factory: Any = CloudEvent,
    ) -> BaseCloudEvent:
        """
        Parse structured mode HTTP message to CloudEvent.

        :param headers: HTTP headers.
        :param body: HTTP body.
        :param event_format: The format to use for deserialization.
        :param event_factory: Factory function to create CloudEvent instances.
        :return: The parsed CloudEvent.
        """
        return event_format.read(event_factory, body)

    def to_binary(
        self, event: BaseCloudEvent, event_format: Format
    ) -> Tuple[HTTPHeaders, HTTPBody]:
        """
        Convert CloudEvent to binary mode HTTP message.

        :param event: The CloudEvent to convert.
        :param event_format: The format to use for data serialization if needed.
        :return: Tuple of (headers, body).
        """
        headers = {}

        # Map CloudEvents attributes to HTTP headers
        attributes = event.get_attributes()
        for attr_name, attr_value in attributes.items():
            if attr_name == "datacontenttype":
                headers[self.CONTENT_TYPE_HEADER] = attr_value
            else:
                header_name = f"{self.CE_PREFIX}{attr_name}"
                headers[header_name] = self._encode_header_value(attr_value)

        # Handle data using format's write_data method
        data = event.get_data()
        datacontenttype = event.get_datacontenttype()
        body = event_format.write_data(data, datacontenttype)

        return headers, body

    def from_binary(
        self,
        headers: HTTPHeaders,
        body: HTTPBody,
        event_format: Format,
        event_factory: Any = CloudEvent,
    ) -> BaseCloudEvent:
        """
        Parse binary mode HTTP message to CloudEvent.

        :param headers: HTTP headers.
        :param body: HTTP body.
        :param event_format: The format to use for data deserialization if needed.
        :param event_factory: Factory function to create CloudEvent instances.
        :return: The parsed CloudEvent.
        """
        attributes: Dict[str, Any] = {}

        # Extract CloudEvents attributes from headers
        for header_name, header_value in headers.items():
            header_lower = header_name.lower()
            if header_lower == self.CONTENT_TYPE_HEADER:
                attributes["datacontenttype"] = header_value
            elif header_lower.startswith(self.CE_PREFIX):
                attr_name = header_lower[len(self.CE_PREFIX) :]
                decoded_value = self._decode_header_value(header_value)
                # Special handling for time attribute
                if attr_name == "time":
                    from dateutil.parser import isoparse

                    attributes[attr_name] = isoparse(decoded_value)
                else:
                    attributes[attr_name] = decoded_value

        # Parse data using format's read_data method
        data: Optional[Union[bytes, str, dict]] = None
        if body:
            data = event_format.read_data(body, attributes.get("datacontenttype"))

        return event_factory(attributes, data)

    def _encode_header_value(self, value: Any) -> str:
        """
        Encode a value for use in an HTTP header.

        :param value: The value to encode.
        :return: The encoded string.
        """
        if isinstance(value, datetime):
            str_value = value.isoformat()
            if str_value.endswith("+00:00"):
                str_value = str_value[:-6] + "Z"
        else:
            str_value = str(value)

        # Percent-encode special characters
        return urllib.parse.quote(str_value, safe="")

    def _decode_header_value(self, value: str) -> str:
        """
        Decode a value from an HTTP header.

        :param value: The encoded value.
        :return: The decoded string.
        """
        return urllib.parse.unquote(value)