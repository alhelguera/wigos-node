import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from publishing.base import (
    DefaultPublisher,
    Delivery,
    Event,
    Location,
    Message,
    MessageGenerator,
    Operation,
    Publisher,
    PublishError,
    TransportLayer,
)


def make_event(operation=Operation.CREATE, **fields):
    values = {
        "id": 1,
        "operation": operation,
        "station_id": 7,
        "wigos_identifier": "0-20000-0-12345",
        "wmdrv2": {"id": "0-20000-0-12345"},
        "previous_wmdrv2": None,
        "changed_by": None,
        "source": "system",
        "occurred_at": datetime(2026, 10, 28, tzinfo=timezone.utc),
    }
    values.update(fields)
    return Event(**values)


class EchoGenerator(MessageGenerator):
    """One message per event, carrying the operation; none on DELETE."""

    def generate(self, event, deliveries):
        if event.operation == Operation.DELETE:
            return []
        return [Message(body=event.operation.encode(), media_type="text/plain", name=event.wigos_identifier)]


class ContentGenerator(MessageGenerator):
    """Echoes the retrieved content of the earlier deliveries."""

    def needs_content(self, event):
        return True

    def generate(self, event, deliveries):
        self.seen = deliveries
        return [Message(body=d.content, media_type="text/plain") for d in deliveries if d.content is not None]


class RecordingTransport(TransportLayer):
    """Remembers what it sent; reports named messages as stored at
    <url>/<name> when given a url option."""

    def __init__(self, **options):
        super().__init__(**options)
        self.sent = []
        self.opened = self.closed = False

    def open(self):
        self.opened = True

    def close(self):
        self.closed = True

    def send(self, message):
        self.sent.append(message)
        if self.options.get("url") and message.name:
            return Location(f"{self.options['url']}/{message.name}")
        return None


class CreatesOnlyPublisher(Publisher):
    def should_publish(self, event):
        return event.operation == Operation.CREATE


class FakeRetrievalPublisher(DefaultPublisher):
    """Serves retrievals from a dict instead of the network."""

    def __init__(self, *args, files, **kwargs):
        super().__init__(*args, **kwargs)
        self.files = files
        self.retrieved = []

    def retrieve(self, location):
        self.retrieved.append(location.retrieve_url)
        return self.files[location.retrieve_url]


class PublisherTestCase(unittest.TestCase):
    def setUp(self):
        self.transport = RecordingTransport()
        self.publisher = DefaultPublisher("echo", EchoGenerator(), self.transport)

    def test_publish_sends_generated_messages(self):
        self.publisher.publish(make_event(Operation.UPDATE), [])
        self.assertEqual([m.body for m in self.transport.sent], [b"update"])
        self.assertEqual(self.transport.sent[0].name, "0-20000-0-12345")

    def test_generator_returning_nothing_sends_nothing(self):
        self.assertEqual(self.publisher.publish(make_event(Operation.DELETE), []), [])
        self.assertEqual(self.transport.sent, [])

    def test_should_publish_filters_events(self):
        publisher = CreatesOnlyPublisher("creates", EchoGenerator(), self.transport)
        self.assertEqual(publisher.publish(make_event(Operation.UPDATE), []), [])
        publisher.publish(make_event(Operation.CREATE), [])
        self.assertEqual([m.body for m in self.transport.sent], [b"create"])

    def test_open_and_close_reach_the_transport(self):
        self.publisher.open()
        self.publisher.close()
        self.assertTrue(self.transport.opened)
        self.assertTrue(self.transport.closed)

    def test_send_many_keeps_order(self):
        messages = [Message(body=str(i).encode(), media_type="text/plain") for i in range(3)]
        self.assertEqual(self.transport.send_many(messages), [None, None, None])
        self.assertEqual(self.transport.sent, messages)

    def test_options_are_kept(self):
        self.assertEqual(RecordingTransport(topic="a/b").options, {"topic": "a/b"})

    def test_publisher_without_should_publish_is_abstract(self):
        class Incomplete(Publisher):
            pass

        with self.assertRaises(TypeError):
            Incomplete("x", EchoGenerator(), self.transport)


class DeliveryTestCase(unittest.TestCase):
    def test_publish_returns_receipts_with_locations(self):
        publisher = DefaultPublisher("files", EchoGenerator(), RecordingTransport(url="http://store"))
        [delivery] = publisher.publish(make_event(), [])
        self.assertEqual(delivery.publisher, "files")
        self.assertEqual(delivery.message.body, b"create")
        self.assertEqual(delivery.location, Location("http://store/0-20000-0-12345"))
        self.assertIsNone(delivery.content)

    def test_notification_receipts_have_no_location(self):
        [delivery] = DefaultPublisher("note", EchoGenerator(), RecordingTransport()).publish(make_event(), [])
        self.assertIsNone(delivery.location)

    def test_generator_gets_the_retrieved_file_not_the_sent_body(self):
        sent = Message(body=b"as sent", media_type="text/plain", name="f")
        earlier = [
            Delivery("files", sent, Location("https://public/f", "http://internal/f")),
            Delivery("note", Message(body=b"no file", media_type="text/plain")),
        ]
        generator = ContentGenerator()
        publisher = FakeRetrievalPublisher(
            "check", generator, RecordingTransport(), files={"http://internal/f": b"as stored"}
        )
        publisher.publish(make_event(), earlier)
        self.assertEqual(publisher.retrieved, ["http://internal/f"])
        self.assertEqual([d.content for d in generator.seen], [b"as stored", None])
        self.assertEqual([m.body for m in publisher.transport.sent], [b"as stored"])

    def test_nothing_is_retrieved_unless_needed(self):
        publisher = FakeRetrievalPublisher("echo", EchoGenerator(), RecordingTransport(), files={})
        publisher.publish(make_event(), [Delivery("files", Message(b"x", "text/plain"), Location("http://f"))])
        self.assertEqual(publisher.retrieved, [])

    def test_retrieve_reads_the_internal_url(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "record.json")
            path.write_bytes(b'{"stored":true}')
            location = Location("https://public.example/record.json", path.as_uri())
            self.assertEqual(Location("u").retrieve_url, "u")
            publisher = DefaultPublisher("check", ContentGenerator(), RecordingTransport())
            self.assertEqual(publisher.retrieve(location), b'{"stored":true}')

    def test_failed_retrieval_raises_publish_error(self):
        publisher = DefaultPublisher("check", ContentGenerator(), RecordingTransport())
        with self.assertRaises(PublishError):
            publisher.retrieve(Location("file:///nonexistent/record.json"))
        with self.assertRaises(PublishError):
            publisher.retrieve(Location("not a url"))
