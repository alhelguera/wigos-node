"""Plugin interfaces.

A MessageGenerator turns an Event into Messages, a TransportLayer delivers
them, and a Publisher pairs one of each. Publishers run in their configured
order for every event, and each one gets the Deliveries (receipts) of the
publishers before it.

Byte-level consistency: a storage transport returns the Location of what it
stored. Before a generator that needs_content() runs, its Publisher
retrieves every earlier Location and hands over the file actually served, so
e.g. a WNM's integrity always describes the stored file.

The worker runs events in parallel threads and shares one instance of each
plugin between them, so plugins must be thread-safe (or keep per-thread
state themselves)."""

import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum


class Operation(StrEnum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


@dataclass(frozen=True)
class Event:
    """A committed station change, as queued in the outbox. Plugins only see
    this snapshot and never read or write stations themselves."""

    id: int
    operation: Operation
    station_id: int
    wigos_identifier: str
    wmdrv2: dict  # the record as stored when the change was made
    previous_wmdrv2: dict | None  # before an update, when known
    changed_by: int | None  # user id; None for superusers and the shell
    source: str  # "api", "admin" or "system"
    occurred_at: datetime


@dataclass(frozen=True)
class Message:
    body: bytes
    media_type: str
    name: str | None = None  # file name for storage transports
    topic: str | None = None  # overrides the transport's default topic
    headers: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Location:
    """Where a storage transport put a message."""

    url: str  # public link, e.g. a WNM href
    internal_url: str | None = None  # where this node retrieves it; default url

    @property
    def retrieve_url(self):
        return self.internal_url or self.url


@dataclass(frozen=True)
class Delivery:
    """Receipt of one sent message, handed to the publishers that run later."""

    publisher: str  # name of the publisher that sent it
    message: Message
    location: Location | None = None  # storage transports only
    content: bytes | None = None  # the file retrieved from location, see needs_content()


class PublishError(Exception):
    """A message could not be delivered. The worker retries the event later."""


class MessageGenerator(ABC):
    def __init__(self, **options):
        self.options = options

    def needs_content(self, event: Event) -> bool:
        """True to get the earlier deliveries with `content` filled in."""
        return False

    @abstractmethod
    def generate(self, event: Event, deliveries: list[Delivery]) -> list[Message]:
        """Messages for this event; [] if this generator ignores it (e.g. a
        record file generator on DELETE). `deliveries` are the receipts of
        the publishers that ran before this one."""


class TransportLayer(ABC):
    def __init__(self, **options):
        self.options = options

    def open(self):
        """Connect. Called once per worker before the first send."""

    def close(self):
        """Disconnect. Called once when the worker stops."""

    @abstractmethod
    def send(self, message: Message) -> Location | None:
        """Deliver one message and return where it is stored, if anywhere
        (storage transports); raise PublishError on failure."""

    def send_many(self, messages: list[Message]) -> list[Location | None]:
        """Deliver messages in order. Override to send in bulk."""
        return [self.send(message) for message in messages]


class Publisher(ABC):
    """The coordinating instance: pairs a generator with a transport and
    feeds the generator what it needs. Subclasses decide which events they
    publish, and may override publish() for special flows."""

    retrieve_timeout = 30  # seconds

    def __init__(self, name: str, generator: MessageGenerator, transport: TransportLayer):
        self.name = name
        self.generator = generator
        self.transport = transport

    @abstractmethod
    def should_publish(self, event: Event) -> bool:
        """Whether this publisher handles the event at all."""

    def open(self):
        self.transport.open()

    def close(self):
        self.transport.close()

    def retrieve(self, location: Location) -> bytes:
        """The file served at the location's internal URL."""
        try:
            with urllib.request.urlopen(location.retrieve_url, timeout=self.retrieve_timeout) as response:
                return response.read()
        except (OSError, ValueError) as e:  # URLError and HTTPError are OSErrors
            raise PublishError(f"Could not retrieve {location.retrieve_url}: {e}") from e

    def publish(self, event: Event, deliveries: list[Delivery]) -> list[Delivery]:
        """Run the generator and the transport; returns this publisher's
        deliveries for the publishers after it."""
        if not self.should_publish(event):
            return []
        if self.generator.needs_content(event):
            deliveries = [
                replace(d, content=self.retrieve(d.location)) if d.location else d
                for d in deliveries
            ]
        messages = self.generator.generate(event, deliveries)
        locations = self.transport.send_many(messages)
        return [Delivery(self.name, m, location) for m, location in zip(messages, locations)]


class DefaultPublisher(Publisher):
    """Publishes every event."""

    def should_publish(self, event: Event) -> bool:
        return True
