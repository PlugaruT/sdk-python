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


from typing import Any, Callable, Generic, Protocol, TypeVar

from cloudevents.core.base import BaseCloudEvent
from cloudevents.core.formats.base import Format

T = TypeVar("T")  # Transport message type


class Binding(Protocol, Generic[T]):
    """Base protocol for all CloudEvents bindings."""

    def to_structured(self, event: BaseCloudEvent, event_format: Format) -> T:
        """Convert CloudEvent to structured mode transport message."""
        ...

    def from_structured(self, message: T, event_format: Format) -> BaseCloudEvent:
        """Parse structured mode transport message to CloudEvent."""
        ...

    def to_binary(self, event: BaseCloudEvent, event_format: Format) -> T:
        """Convert CloudEvent to binary mode transport message."""
        ...

    def from_binary(self, message: T, event_format: Format) -> BaseCloudEvent:
        """Parse binary mode transport message to CloudEvent."""
        ...