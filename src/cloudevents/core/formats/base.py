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


from typing import Callable, Optional, Protocol, Union

from cloudevents.core.base import BaseCloudEvent


class Format(Protocol):
    def read(
        self,
        event_factory: Callable[
            [dict, Optional[Union[dict, str, bytes]]], BaseCloudEvent
        ],
        data: Union[str, bytes],
    ) -> BaseCloudEvent: ...

    def write(self, event: BaseCloudEvent) -> bytes: ...

    def get_content_type(self) -> str:
        """
        Get the content-type string for structured mode serialization.

        :return: The content-type string (e.g., "application/cloudevents+json").
        """
        ...

    def write_data(
        self, data: Optional[Union[dict, str, bytes]], datacontenttype: Optional[str]
    ) -> bytes:
        """
        Serialize event data payload according to its content type.

        This method is used by bindings in binary mode to serialize just the
        data portion of a CloudEvent, respecting the datacontenttype attribute.

        :param data: The data payload to serialize.
        :param datacontenttype: The content type of the data.
        :return: The serialized data as bytes.
        """
        ...

    def read_data(
        self, data: bytes, datacontenttype: Optional[str]
    ) -> Optional[Union[dict, str, bytes]]:
        """
        Deserialize raw data bytes according to content type.

        This method is used by bindings in binary mode to deserialize the
        data portion of a CloudEvent based on the datacontenttype attribute.
        It is the inverse operation of write_data().

        :param data: The raw data bytes to deserialize.
        :param datacontenttype: The content type of the data.
        :return: The deserialized data (dict, str, or bytes).
        """
        ...
