import base64
import hashlib
import json
import threading
import time
from contextlib import contextmanager
from datetime import timedelta
from functools import partial
from unittest import mock

from django.core.management import call_command
from django.db import transaction
from django.test import SimpleTestCase, TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from rest_framework.test import APITestCase

from api.models import WIGOSNodeGroup, WIGOSNodeUser
from publishing.base import (
    DefaultPublisher,
    Delivery,
    Event,
    Location,
    Message,
    MessageGenerator,
    Operation,
    PublishError,
    Step,
    TransportLayer,
)
from publishing.generators.wmdr2 import RecordFileGenerator
from publishing.generators.wnm import WNMGenerator
from publishing.registry import ConfigurationError
from publishing.transports.mqtt import MQTTTransport
from publishing.transports.s3 import S3Transport

from . import actions, worker
from .checks import check_publishers
from .models import PublicationEvent
from .publishers import get_publishers

RECORD = {"id": "0-756-0-outbox", "properties": {"updated": "2026-10-29T09:00:00Z"}}


def make_row(**fields):
    values = {
        "operation": Operation.UPDATE,
        "station_id": 42,
        "wigos_identifier": "0-756-0-outbox",
        "wmdrv2": RECORD,
    }
    values.update(fields)
    return PublicationEvent.objects.create(**values)


class PublicationEventTestCase(TestCase):
    def test_defaults(self):
        row = make_row()
        self.assertEqual(row.status, PublicationEvent.Status.PENDING)
        self.assertEqual(row.source, PublicationEvent.Source.SYSTEM)
        self.assertEqual((row.attempts, row.completed, row.deliveries, row.issuers, row.ordering_keys), (0, [], [], [], []))
        self.assertIsNone(row.changed_by)
        self.assertIsNone(row.next_attempt_at)
        self.assertIsNotNone(row.created_at)

    def test_to_event(self):
        user = WIGOSNodeUser.objects.create(username="outbox", email="outbox@example.org")
        previous = {"id": "0-756-0-old"}
        row = make_row(previous_wmdrv2=previous, changed_by=user, source=PublicationEvent.Source.API)

        event = PublicationEvent.objects.get(pk=row.pk).to_event()

        self.assertEqual(event, Event(
            id=row.pk,
            operation=Operation.UPDATE,
            station_id=42,
            wigos_identifier="0-756-0-outbox",
            wmdrv2=RECORD,
            previous_wmdrv2=previous,
            changed_by=user.pk,
            source="api",
            occurred_at=row.created_at,
        ))
        self.assertIs(type(event.operation), Operation)

    def test_deleting_the_user_keeps_the_event(self):
        user = WIGOSNodeUser.objects.create(username="gone", email="gone@example.org")
        row = make_row(changed_by=user)
        user.delete()
        row.refresh_from_db()
        self.assertIsNone(row.changed_by)

    def test_events_by_issuer_and_ordering_key(self):
        swiss = make_row(issuers=["756"], ordering_keys=["station:1", "wsi:0-756-0-a"])
        moved = make_row(issuers=["756", "76"], ordering_keys=["station:2", "wsi:0-756-0-a", "wsi:0-76-0-a"])
        make_row(issuers=["76"], ordering_keys=["station:3"])

        self.assertQuerySetEqual(PublicationEvent.objects.filter(issuers__overlap=["756", "999"]), [swiss, moved])
        self.assertQuerySetEqual(PublicationEvent.objects.filter(ordering_keys__overlap=["wsi:0-756-0-a"]), [swiss, moved])


LOG = "publishing.transports.log.LogTransport"
BROKEN = [
    {"NAME": "files", "STEPS": [
        {"NAME": "store", "GENERATOR": {"CLASS": "publishing.generators.wmdr2.RecordFileGenerator"}, "TRANSPORT": {"CLASS": LOG}},
    ]},
    {"NAME": "files", "STEPS": [
        {"NAME": "store", "GENERATOR": {"CLASS": "nowhere.Generator"}, "TRANSPORT": {"CLASS": LOG}},
    ]},
]


class PublishersSettingTestCase(SimpleTestCase):
    def test_configured_publishers_build(self):
        self.assertEqual(check_publishers(None), [])
        [publisher] = get_publishers()
        storage, wnm = publisher.steps
        self.assertEqual((publisher.name, storage.name, wnm.name), ("wis2", "storage", "wnm"))
        self.assertIsInstance(storage.generator, RecordFileGenerator)
        self.assertIsInstance(storage.transport, S3Transport)
        self.assertIsInstance(wnm.generator, WNMGenerator)
        self.assertFalse(wnm.generator.skip_missing)
        self.assertIsInstance(wnm.transport, MQTTTransport)

    def test_publishers_are_built_once(self):
        self.assertIs(get_publishers()[0], get_publishers()[0])

    @override_settings(PUBLISHERS=BROKEN)
    def test_check_reports_every_problem(self):
        errors = check_publishers(None)
        self.assertEqual([e.id for e in errors], ["outbox.E001", "outbox.E001"])
        self.assertIn("centre_id", errors[0].msg)
        self.assertIn("NAME is used more than once", errors[1].msg)
        with self.assertRaises(ConfigurationError):
            get_publishers()

    def test_changed_setting_rebuilds(self):
        default = get_publishers()
        with override_settings(PUBLISHERS=[]):
            self.assertEqual(get_publishers(), [])
        self.assertEqual([p.name for p in get_publishers()], [p.name for p in default])

    @override_settings()
    def test_missing_setting_means_no_publishers(self):
        from django.conf import settings

        del settings.PUBLISHERS
        # deleting a setting sends no setting_changed, so reset by hand
        get_publishers.cache_clear()
        self.addCleanup(get_publishers.cache_clear)
        self.assertEqual(check_publishers(None), [])
        self.assertEqual(get_publishers(), [])


class MemoryStorage(TransportLayer):
    """Stores files in a dict at mem://store/<name>."""

    def __init__(self):
        super().__init__()
        self.files = {}
        self.puts = 0
        self.fail_remove = False

    def send(self, message):
        self.puts += 1
        url = f"mem://store/{message.name}"
        self.files[url] = message.body
        return Location(url)

    def remove(self, location):
        if self.fail_remove:
            raise PublishError("storage down")
        del self.files[location.url]


class MemoryBroker(TransportLayer):
    """Keeps the sent WNMs; refuses the next `failures` sends."""

    def __init__(self):
        super().__init__()
        self.sent = []
        self.failures = 0

    def send(self, message):
        if self.failures:
            self.failures -= 1
            raise PublishError("broker down")
        self.sent.append(json.loads(message.body))
        return None


class MemoryRetrievalPublisher(DefaultPublisher):
    def __init__(self, *args, storage, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage = storage

    def retrieve(self, location):
        return self.storage.files[location.url]


class BrokenGenerator(MessageGenerator):
    def generate(self, event, deliveries):
        raise RuntimeError("boom")


KEYS = ["station:42", "wsi:0-756-0-outbox"]


class WorkerTestCase(TestCase):
    def setUp(self):
        PublicationEvent.objects.all().delete()
        self.storage = MemoryStorage()
        self.broker = MemoryBroker()
        self.publishers = [
            MemoryRetrievalPublisher(
                "wis2",
                [
                    Step("storage", RecordFileGenerator(centre_id="io-test"), self.storage),
                    Step("wnm", WNMGenerator(centre_id="io-test"), self.broker),
                ],
                storage=self.storage,
            ),
        ]

    def process_due(self):
        PublicationEvent.objects.filter(status=PublicationEvent.Status.PENDING).update(next_attempt_at=None)
        return worker.process_next(self.publishers)

    def test_publishes_the_file_then_its_wnm(self):
        row = make_row(operation=Operation.CREATE, ordering_keys=KEYS)

        self.assertTrue(worker.process_next(self.publishers))

        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.PROCESSED)
        self.assertEqual(row.completed, ["wis2:storage", "wis2:wnm"])
        self.assertIsNotNone(row.finished_at)
        url = row.deliveries[0]["url"]
        self.assertEqual(row.deliveries, [
            {"publisher": "wis2", "step": "storage", "name": url.removeprefix("mem://store/"), "media_type": "application/geo+json",
             "topic": None, "url": url, "internal_url": None},
            {"publisher": "wis2", "step": "wnm", "name": None, "media_type": "application/geo+json",
             "topic": None, "url": None, "internal_url": None},
        ])
        [wnm] = self.broker.sent
        self.assertEqual((wnm["links"][0]["href"], wnm["links"][0]["rel"]), (url, "canonical"))
        digest = base64.b64encode(hashlib.sha512(self.storage.files[url]).digest()).decode()
        self.assertEqual(wnm["properties"]["integrity"]["value"], digest)
        self.assertFalse(worker.process_next(self.publishers))

    @mock.patch.object(worker, "MAX_ATTEMPTS", 5)  # with automatic retries turned on
    def test_retry_skips_completed_publishers_and_reuses_the_stored_file(self):
        row = make_row(ordering_keys=KEYS)
        self.broker.failures = 1

        with self.assertLogs("outbox", "WARNING"):
            worker.process_next(self.publishers)

        row.refresh_from_db()
        self.assertEqual((row.status, row.attempts, row.completed), (PublicationEvent.Status.PENDING, 1, ["wis2:storage"]))
        self.assertIn("wnm: broker down", row.last_error)
        self.assertGreater(row.next_attempt_at, timezone.now())
        self.assertFalse(worker.process_next(self.publishers))  # not due yet

        self.assertTrue(self.process_due())

        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.PROCESSED)
        self.assertEqual(self.storage.puts, 1)
        self.assertEqual(self.broker.sent[0]["links"][0]["href"], row.deliveries[0]["url"])

    def test_retry_delay_doubles_up_to_an_hour(self):
        self.assertEqual([worker.retry_delay(n) for n in (1, 2, 3)], [timedelta(seconds=s) for s in (30, 60, 120)])
        self.assertEqual(worker.retry_delay(20), timedelta(hours=1))

    def test_a_broker_outage_fails_the_event_at_once_and_removes_the_stored_file(self):
        row = make_row(ordering_keys=KEYS)
        self.broker.failures = 1

        with self.assertLogs("outbox", "ERROR"):
            worker.process_next(self.publishers)

        row.refresh_from_db()
        self.assertEqual((row.status, row.attempts), (PublicationEvent.Status.FAILED, 1))
        self.assertIn("wnm: broker down", row.last_error)
        self.assertEqual((row.completed, row.deliveries), ([], []))
        self.assertIsNotNone(row.finished_at)
        self.assertEqual(self.storage.files, {})
        self.assertFalse(self.process_due())  # retrying is up to a user

    def test_a_failing_undo_is_recorded_and_the_event_stays_failed(self):
        row = make_row(ordering_keys=KEYS)
        self.broker.failures = 99
        self.storage.fail_remove = True

        with self.assertLogs("outbox", "WARNING"):
            for _ in range(worker.MAX_ATTEMPTS):
                self.process_due()

        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.FAILED)
        self.assertIn("undo wis2: storage down", row.last_error)
        self.assertTrue(row.needs_cleanup)
        self.assertEqual(row.completed, ["wis2:storage"])  # the file may still be stored
        self.assertEqual([d["step"] for d in row.deliveries], ["storage"])
        self.assertEqual(len(self.storage.files), 1)

    def test_a_crashing_plugin_fails_the_event(self):
        row = make_row(ordering_keys=KEYS)
        publishers = [DefaultPublisher("broken", [Step("main", BrokenGenerator(), self.broker)])]

        with self.assertLogs("outbox", "WARNING"):
            worker.process_next(publishers)

        row.refresh_from_db()
        self.assertEqual((row.status, row.attempts), (PublicationEvent.Status.FAILED, 1))
        self.assertIn("broken:main: boom", row.last_error)

    def independent_publishers(self):
        """A working publisher next to the WIS2 one, each with its own storage."""
        self.other = MemoryBroker()
        return [DefaultPublisher("other", [Step("note", EventIdGenerator(), self.other)])] + self.publishers

    def test_a_failing_publisher_doesnt_affect_the_others(self):
        row = make_row(ordering_keys=KEYS)
        self.broker.failures = 1

        with self.assertLogs("outbox", "ERROR"):
            worker.process_next(self.independent_publishers())

        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.FAILED)
        self.assertIn("wis2:wnm: broker down", row.last_error)
        self.assertEqual(row.completed, ["other:note"])  # delivered, so not undone
        self.assertEqual([d["publisher"] for d in row.deliveries], ["other"])
        self.assertEqual(len(self.other.sent), 1)
        self.assertEqual(self.storage.files, {})  # wis2 undid its own steps
        self.assertFalse(row.needs_cleanup)

    def test_later_publishers_still_run_after_one_fails(self):
        row = make_row(ordering_keys=KEYS)
        self.broker.failures = 1

        with self.assertLogs("outbox", "ERROR"):
            worker.process_next(list(reversed(self.independent_publishers())))

        row.refresh_from_db()
        self.assertEqual((row.status, row.completed), (PublicationEvent.Status.FAILED, ["other:note"]))

    def test_a_failed_event_with_a_working_publisher_releases_its_station(self):
        make_row(ordering_keys=KEYS)
        later = make_row(ordering_keys=KEYS)
        self.broker.failures = 1

        with self.assertLogs("outbox", "ERROR"):
            worker.process_next(self.independent_publishers())

        self.assertEqual(worker.claim(), later)

    @mock.patch.object(worker, "MAX_ATTEMPTS", 5)  # with automatic retries turned on
    def test_a_retry_only_runs_the_publisher_that_failed(self):
        row = make_row(ordering_keys=KEYS)
        self.broker.failures = 1
        publishers = self.independent_publishers()

        with self.assertLogs("outbox", "WARNING"):
            worker.process_next(publishers)
        self.process_due_with(publishers)

        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.PROCESSED)
        self.assertEqual(len(self.other.sent), 1)  # not sent again
        self.assertEqual(len(self.broker.sent), 1)

    def process_due_with(self, publishers):
        PublicationEvent.objects.filter(status=PublicationEvent.Status.PENDING).update(next_attempt_at=None)
        return worker.process_next(publishers)

    def test_a_publisher_no_longer_configured_doesnt_hold_the_station(self):
        make_row(ordering_keys=KEYS, status=PublicationEvent.Status.FAILED, completed=["gone:step"])
        later = make_row(ordering_keys=KEYS)
        self.assertEqual(worker.claim(), later)

    def test_deletion_links_to_the_last_processed_file_and_keeps_it(self):
        create = make_row(operation=Operation.CREATE, ordering_keys=KEYS)
        worker.process_next(self.publishers)
        deletion = make_row(operation=Operation.DELETE, ordering_keys=KEYS)

        worker.process_next(self.publishers)

        create.refresh_from_db()
        deletion.refresh_from_db()
        self.assertEqual(deletion.status, PublicationEvent.Status.PROCESSED)
        link = self.broker.sent[1]["links"][0]
        self.assertEqual((link["href"], link["rel"]), (create.deliveries[0]["url"], "deletion"))
        self.assertEqual([d["step"] for d in deletion.deliveries], ["wnm"])  # the file is create's
        self.assertEqual(self.storage.puts, 1)
        self.assertIn(create.deliveries[0]["url"], self.storage.files)

    def later(self):
        return timezone.now() + timedelta(hours=1)

    def test_a_station_waits_for_its_older_events(self):
        make_row(ordering_keys=["station:1", "wsi:a"], next_attempt_at=self.later())  # retrying
        make_row(ordering_keys=["station:1", "wsi:a"])
        other = make_row(ordering_keys=["station:2", "wsi:b"])
        self.assertEqual(worker.claim(), other)

    def test_order_is_kept_across_a_wsi_change(self):
        make_row(ordering_keys=["station:1", "wsi:a", "wsi:b"], next_attempt_at=self.later())  # a renamed to b
        make_row(ordering_keys=["station:1", "wsi:b"])
        self.assertIsNone(worker.claim())

    def test_a_moved_wsi_waits_for_the_station_it_left(self):
        left = make_row(ordering_keys=["station:1", "wsi:a"], next_attempt_at=self.later())
        joined = make_row(ordering_keys=["station:2", "wsi:a", "wsi:c"])
        self.assertIsNone(worker.claim())

        left.status = PublicationEvent.Status.PROCESSED
        left.save()
        self.assertEqual(worker.claim(), joined)

    def test_a_cleaned_up_failed_event_releases_its_station(self):
        make_row(ordering_keys=["station:1"], status=PublicationEvent.Status.FAILED)
        later = make_row(ordering_keys=["station:1"])
        self.assertEqual(worker.claim(), later)

    def test_a_failed_event_with_leftovers_holds_back_its_station(self):
        make_row(ordering_keys=["station:1"], status=PublicationEvent.Status.FAILED, needs_cleanup=True)
        make_row(ordering_keys=["station:1"])
        self.assertIsNone(worker.claim())

    def test_the_next_change_goes_ahead_once_a_failed_event_is_undone(self):
        failed = make_row(ordering_keys=KEYS)
        later = make_row(ordering_keys=KEYS)
        self.broker.failures = worker.MAX_ATTEMPTS

        with self.assertLogs("outbox", "WARNING"):
            for _ in range(worker.MAX_ATTEMPTS):
                self.process_due()
        self.assertTrue(self.process_due())

        failed.refresh_from_db()
        later.refresh_from_db()
        self.assertEqual((failed.status, failed.completed), (PublicationEvent.Status.FAILED, []))
        self.assertEqual(later.status, PublicationEvent.Status.PROCESSED)

    def test_claiming_takes_a_lease(self):
        row = make_row(ordering_keys=KEYS)

        self.assertEqual(worker.claim(), row)

        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.PROCESSING)
        self.assertGreater(row.locked_until, timezone.now())
        self.assertIsNone(worker.claim())  # held by the lease

    def test_leases_follow_the_database_clock(self):
        row = make_row(ordering_keys=KEYS)
        worker.claim()
        # a worker whose clock runs a day ahead still sees the lease as held
        with mock.patch("django.utils.timezone.now", return_value=timezone.now() + timedelta(days=1)):
            self.assertIsNone(worker.claim())
        row.refresh_from_db()
        self.assertAlmostEqual(row.locked_until, worker.db_now() + worker.LEASE, delta=timedelta(seconds=5))

    def test_finishing_drops_the_lease(self):
        row = make_row(ordering_keys=KEYS)
        worker.process_next(self.publishers)
        row.refresh_from_db()
        self.assertEqual((row.status, row.locked_until), (PublicationEvent.Status.PROCESSED, None))

    def store_record_file(self, row):
        """As if the storage publisher ran before the worker crashed."""
        publisher = self.publishers[0]
        [delivery] = publisher.run_step(publisher.steps[0], row.to_event(), [])
        PublicationEvent.objects.filter(pk=row.pk).update(
            completed=["wis2:storage"], deliveries=[worker.dump_delivery(delivery)]
        )

    def crashed_row(self, attempts):
        row = make_row(
            ordering_keys=KEYS,
            status=PublicationEvent.Status.PROCESSING,
            locked_until=timezone.now() - timedelta(seconds=1),
            attempts=attempts,
        )
        self.store_record_file(row)
        return row

    def test_a_crashed_event_is_reclaimed_and_resumed_without_failing(self):
        row = self.crashed_row(attempts=0)

        with self.assertLogs("outbox", "WARNING"):
            self.assertTrue(worker.process_next(self.publishers))

        row.refresh_from_db()
        self.assertEqual((row.status, row.attempts), (PublicationEvent.Status.PROCESSED, 0))
        self.assertIn("lease expired", row.last_error)
        self.assertEqual(self.storage.puts, 1)  # not stored again
        self.assertEqual(self.broker.sent[0]["links"][0]["href"], row.deliveries[0]["url"])

    def test_a_worker_that_lost_its_lease_stops(self):
        row = make_row(ordering_keys=KEYS)
        claimed = worker.claim()
        # the lease expired and another worker claimed the event again
        PublicationEvent.objects.filter(pk=row.pk).update(locked_until=timezone.now() + timedelta(minutes=9))

        with self.assertRaises(worker.LeaseLost):
            worker.process(claimed, self.publishers)

        row.refresh_from_db()
        self.assertEqual(row.completed, [])  # nothing written by the old worker


class EventIdGenerator(MessageGenerator):
    def generate(self, event, deliveries):
        return [Message(body=str(event.id).encode(), media_type="text/plain")]


class TogetherTransport(TransportLayer):
    """Fails unless two sends happen at the same time."""

    def __init__(self):
        super().__init__()
        self.barrier = threading.Barrier(2, timeout=10)

    def send(self, message):
        try:
            self.barrier.wait()
        except threading.BrokenBarrierError:
            raise PublishError("sent alone")


class OverlapTransport(TransportLayer):
    """Slow sends, recording their order and the most that ran at once."""

    def __init__(self):
        super().__init__()
        self.lock = threading.Lock()
        self.active = self.most_active = self.opened = self.closed = 0
        self.order = []

    def open(self):
        self.opened += 1

    def close(self):
        self.closed += 1

    def send(self, message):
        with self.lock:
            self.active += 1
            self.most_active = max(self.most_active, self.active)
        time.sleep(0.1)
        with self.lock:
            self.active -= 1
            self.order.append(int(message.body))


class ParallelWorkerTestCase(TransactionTestCase):
    """Real threads with their own connections, so rows must be committed.

    These tests need none of the data seeded by migrations. The flush after
    each one removes it, which is fine because Django runs TransactionTestCases
    after every TestCase. (serialized_rollback can't restore it: the api Group
    post_save receiver doesn't skip raw saves.)"""

    def statuses(self):
        return set(PublicationEvent.objects.values_list("status", flat=True))

    def test_unrelated_stations_are_published_at_the_same_time(self):
        make_row(ordering_keys=["station:1", "wsi:a"])
        make_row(ordering_keys=["station:2", "wsi:b"])

        worker.run([DefaultPublisher("together", [Step("main", EventIdGenerator(), TogetherTransport())])], once=True, workers=2)

        self.assertEqual(self.statuses(), {PublicationEvent.Status.PROCESSED})

    def test_related_events_never_run_at_the_same_time(self):
        rows = [
            make_row(ordering_keys=["station:1", "wsi:a"]),
            make_row(ordering_keys=["station:1", "wsi:a", "wsi:b"]),  # a renamed to b
            make_row(ordering_keys=["station:2", "wsi:b"]),  # b moved to station 2
        ]
        transport = OverlapTransport()

        worker.run([DefaultPublisher("slow", [Step("main", EventIdGenerator(), transport)])], once=True, workers=3)

        self.assertEqual(self.statuses(), {PublicationEvent.Status.PROCESSED})
        self.assertEqual(transport.most_active, 1)
        self.assertEqual(transport.order, [row.pk for row in rows])
        self.assertEqual((transport.opened, transport.closed), (1, 1))  # shared by the threads

    @override_settings(PUBLISHERS=[
        {"NAME": "files", "STEPS": [
            {"NAME": "store", "GENERATOR": {"CLASS": "publishing.generators.wmdr2.RecordFileGenerator",
                                            "OPTIONS": {"centre_id": "io-test"}}, "TRANSPORT": {"CLASS": LOG}},
        ]},
    ])  # no storage in the tests
    def test_run_once_processes_the_queue_with_the_configured_publishers(self):
        make_row(operation=Operation.CREATE, ordering_keys=["station:1"])
        make_row(operation=Operation.CREATE, ordering_keys=["station:2"])

        with self.assertLogs("publishing", "INFO"):
            call_command("run_publishers", "--once")

        self.assertEqual(self.statuses(), {PublicationEvent.Status.PROCESSED})


def queue_and_notify(fail=False):
    """A committed (or rolled back) event, notified after commit."""
    with transaction.atomic():
        row = make_row(ordering_keys=["station:1"])
        transaction.on_commit(partial(worker.notify, row.pk))
        if fail:
            raise RuntimeError("rolled back")
    return row


@mock.patch.object(worker, "POLL_INTERVAL", 300)  # only notifications wake
class NotifyTestCase(TransactionTestCase):
    """LISTEN needs committed notifications, so no TestCase here either."""

    def start(self, target, *args, **kwargs):
        stop, ready = threading.Event(), threading.Event()
        thread = threading.Thread(target=target, args=args, kwargs={**kwargs, "stop": stop, "ready": ready})
        thread.start()

        def finish():
            stop.set()
            thread.join(10)
            self.assertFalse(thread.is_alive())

        self.addCleanup(finish)
        self.assertTrue(ready.wait(5))
        return finish

    def listening(self):
        wakeup = worker.Wakeup()
        self.start(lambda stop, ready: worker.listen(wakeup, stop, ready))
        return wakeup

    def test_a_committed_event_wakes_the_listener(self):
        wakeup = self.listening()
        seen = wakeup.current()
        queue_and_notify()
        self.assertTrue(wakeup.wait(seen, timeout=5))

    def test_a_rolled_back_event_wakes_nobody(self):
        wakeup = self.listening()
        seen = wakeup.current()
        with self.assertRaises(RuntimeError):
            queue_and_notify(fail=True)
        self.assertFalse(wakeup.wait(seen, timeout=2))

    def test_the_poll_wakes_the_listener_without_notifications(self):
        wakeup = self.listening()
        seen = wakeup.current()
        with mock.patch.object(worker, "POLL_INTERVAL", 1):
            self.assertTrue(wakeup.wait(seen, timeout=5))

    def test_the_running_worker_publishes_a_change_right_away(self):
        transport = OverlapTransport()
        finish = self.start(worker.run, [DefaultPublisher("slow", [Step("main", EventIdGenerator(), transport)])], workers=2)

        row = queue_and_notify()
        for _ in range(50):
            row.refresh_from_db()
            if row.status == PublicationEvent.Status.PROCESSED:
                break
            time.sleep(0.1)

        self.assertEqual(row.status, PublicationEvent.Status.PROCESSED)
        finish()  # stops: the threads end, the transports are closed
        self.assertEqual((transport.opened, transport.closed), (1, 1))


class AdminActionsTestCase(TestCase):
    """Real failed events, made by the worker."""

    def setUp(self):
        PublicationEvent.objects.all().delete()
        self.storage = MemoryStorage()
        self.broker = MemoryBroker()
        self.publishers = [
            MemoryRetrievalPublisher(
                "wis2",
                [
                    Step("storage", RecordFileGenerator(centre_id="io-test"), self.storage),
                    Step("wnm", WNMGenerator(centre_id="io-test"), self.broker),
                ],
                storage=self.storage,
            ),
        ]

    def failed_row(self, publishers=None, **fields):
        row = make_row(ordering_keys=KEYS, **fields)
        self.broker.failures = 1
        with self.assertLogs("outbox", "WARNING"):
            worker.process_next(publishers or self.publishers)
        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.FAILED)
        return row

    def needs_cleanup_row(self):
        self.storage.fail_remove = True
        row = self.failed_row()
        self.storage.fail_remove = False
        self.assertTrue(row.needs_cleanup)
        return row

    def test_retry_regenerates_the_file_and_publishes(self):
        row = self.failed_row()
        self.assertEqual(self.storage.files, {})

        actions.retry(row.pk, self.publishers)

        row.refresh_from_db()
        self.assertEqual((row.status, row.attempts, row.finished_at), (PublicationEvent.Status.PENDING, 0, None))
        worker.process_next(self.publishers)
        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.PROCESSED)
        self.assertEqual(self.storage.puts, 2)
        [wnm] = self.broker.sent
        stored = self.storage.files[wnm["links"][0]["href"]]
        digest = base64.b64encode(hashlib.sha512(stored).digest()).decode()
        self.assertEqual(wnm["properties"]["integrity"]["value"], digest)

    def test_retry_stores_the_file_again_even_when_an_undo_left_it(self):
        row = self.needs_cleanup_row()
        self.assertEqual(row.completed, ["wis2:storage"])

        actions.retry(row.pk, self.publishers)

        row.refresh_from_db()
        self.assertEqual((row.completed, row.deliveries, row.needs_cleanup), ([], [], False))
        worker.process_next(self.publishers)
        self.assertEqual(self.storage.puts, 2)

    def test_retry_leaves_a_working_publisher_alone(self):
        other = MemoryBroker()
        publishers = [DefaultPublisher("other", [Step("note", EventIdGenerator(), other)])] + self.publishers
        row = self.failed_row(publishers)

        actions.retry(row.pk, publishers)
        worker.process_next(publishers)

        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.PROCESSED)
        self.assertEqual(len(other.sent), 1)
        self.assertEqual(len(self.broker.sent), 1)

    def test_retry_wakes_the_worker(self):
        row = self.failed_row()
        with self.captureOnCommitCallbacks() as callbacks:
            actions.retry(row.pk, self.publishers)
        self.assertEqual(len(callbacks), 1)

    def test_retry_is_refused_once_a_later_event_shares_a_key(self):
        row = self.failed_row()
        make_row(ordering_keys=["station:99", KEYS[1]])
        with self.assertRaisesMessage(actions.ActionRefused, "save the station again"):
            actions.retry(row.pk, self.publishers)
        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.FAILED)

    def test_retry_is_not_held_back_by_unrelated_events(self):
        row = self.failed_row()
        make_row(ordering_keys=["station:99", "wsi:other"])
        actions.retry(row.pk, self.publishers)

    def test_only_failed_events_can_be_retried(self):
        row = make_row(ordering_keys=KEYS, status=PublicationEvent.Status.PROCESSED)
        with self.assertRaises(actions.ActionRefused):
            actions.retry(row.pk, self.publishers)

    def test_retry_cleanup_removes_the_file_and_releases_the_station(self):
        row = self.needs_cleanup_row()
        later = make_row(ordering_keys=KEYS)
        self.assertIsNone(worker.claim())

        actions.retry_cleanup(row.pk, self.publishers)

        row.refresh_from_db()
        self.assertEqual((row.status, row.needs_cleanup, row.completed), (PublicationEvent.Status.FAILED, False, []))
        self.assertEqual(self.storage.files, {})
        self.assertEqual(worker.claim(), later)

    def test_a_failing_retry_cleanup_keeps_holding_the_station(self):
        row = self.needs_cleanup_row()
        self.storage.fail_remove = True
        with self.assertLogs("outbox", "ERROR"), self.assertRaises(actions.ActionRefused):
            actions.retry_cleanup(row.pk, self.publishers)
        row.refresh_from_db()
        self.assertTrue(row.needs_cleanup)
        self.assertEqual(row.last_error.count("undo wis2: storage down"), 2)

    def test_retry_cleanup_of_a_publisher_no_longer_configured_needs_marking(self):
        row = self.needs_cleanup_row()
        with self.assertRaises(actions.ActionRefused):
            actions.retry_cleanup(row.pk, [])
        row.refresh_from_db()
        self.assertTrue(row.needs_cleanup)
        self.assertIn("no longer configured: wis2", row.last_error)

    def test_retry_cleanup_is_only_for_events_that_need_it(self):
        row = self.failed_row()
        with self.assertRaises(actions.ActionRefused):
            actions.retry_cleanup(row.pk, self.publishers)

    def test_mark_cleaned_up_forgets_the_leftovers_and_notes_who(self):
        row = self.needs_cleanup_row()
        self.assertEqual(len(actions.leftovers(row, self.publishers)), 1)

        actions.mark_cleaned_up(row.pk, self.publishers, "someone")

        row.refresh_from_db()
        self.assertEqual((row.needs_cleanup, row.completed, row.deliveries), (False, [], []))
        self.assertIn("marked as cleaned up by someone", row.last_error)
        self.assertEqual(len(self.storage.files), 1)  # nothing is deleted

    def test_mark_cleaned_up_keeps_what_a_working_publisher_delivered(self):
        publishers = [DefaultPublisher("other", [Step("note", EventIdGenerator(), MemoryBroker())])] + self.publishers
        self.storage.fail_remove = True
        row = self.failed_row(publishers)

        actions.mark_cleaned_up(row.pk, publishers, "someone")

        row.refresh_from_db()
        self.assertEqual(row.completed, ["other:note"])
        self.assertEqual([d["publisher"] for d in row.deliveries], ["other"])


class EventAdminTestCase(TestCase):
    """The actions through the admin pages."""

    failed_row = AdminActionsTestCase.failed_row
    needs_cleanup_row = AdminActionsTestCase.needs_cleanup_row

    def setUp(self):
        AdminActionsTestCase.setUp(self)
        self.admin = WIGOSNodeUser.objects.create_superuser(username="ev_admin", email="ev_admin@example.com", password="pw")
        self.client.force_login(self.admin)
        self.changelist = reverse("admin:outbox_publicationevent_changelist")

        @contextmanager
        def opened():
            yield self.publishers

        for target, replacement in (("get_publishers", lambda: self.publishers), ("opened_publishers", opened)):
            patcher = mock.patch(f"outbox.admin.{target}", replacement)
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_action(self, action, row, **extra):
        data = {"action": action, "_selected_action": [row.pk], **extra}
        return self.client.post(self.changelist, data, follow=True)

    def test_events_are_read_only(self):
        row = self.failed_row()
        detail = self.client.get(reverse("admin:outbox_publicationevent_change", args=[row.pk]))
        self.assertEqual(detail.status_code, 200)
        self.assertContains(detail, "wis2:wnm: broker down")
        self.assertContains(detail, "Retries are manual")
        self.assertNotContains(detail, 'name="_save"')
        self.assertEqual(self.client.get(reverse("admin:outbox_publicationevent_add")).status_code, 403)
        self.assertEqual(self.client.get(reverse("admin:outbox_publicationevent_delete", args=[row.pk])).status_code, 403)
        self.assertNotContains(self.client.get(self.changelist), "delete_selected")

    def test_filters_and_banner_show_what_needs_cleaning_up(self):
        self.assertNotContains(self.client.get(self.changelist), "need cleaning up")
        row = self.needs_cleanup_row()

        response = self.client.get(self.changelist)
        self.assertContains(response, "1 failed events need cleaning up")
        self.assertContains(response, "?needs_cleanup__exact=1")

        filtered = self.client.get(self.changelist, {"needs_cleanup__exact": 1})
        self.assertEqual([r.pk for r in filtered.context["cl"].result_list], [row.pk])

    def test_retry_action(self):
        row = self.failed_row()
        self.assertContains(self.run_action("retry", row), "1 events queued again")
        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.PENDING)

    def test_a_refused_retry_says_why(self):
        row = self.failed_row()
        make_row(ordering_keys=KEYS)
        self.assertContains(self.run_action("retry", row), "save the station again")
        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.FAILED)

    def test_retry_cleanup_action(self):
        row = self.needs_cleanup_row()
        self.assertContains(self.run_action("retry_cleanup", row), "1 events cleaned up")
        row.refresh_from_db()
        self.assertFalse(row.needs_cleanup)

    def test_mark_cleaned_up_asks_first_and_lists_the_files(self):
        row = self.needs_cleanup_row()
        url = row.deliveries[0]["url"]

        confirmation = self.run_action("mark_cleaned_up", row)
        self.assertContains(confirmation, url)
        self.assertContains(confirmation, 'name="confirmed"')
        row.refresh_from_db()
        self.assertTrue(row.needs_cleanup)

        self.assertContains(self.run_action("mark_cleaned_up", row, confirmed="yes"), "1 events marked as cleaned up")
        row.refresh_from_db()
        self.assertFalse(row.needs_cleanup)
        self.assertIn("marked as cleaned up by ev_admin", row.last_error)

    def test_mark_cleaned_up_of_events_that_dont_need_it(self):
        row = self.failed_row()
        self.assertContains(self.run_action("mark_cleaned_up", row), "None of these events need cleaning up")

    def test_superusers_see_the_full_log(self):
        make_row(issuers=["756"], ordering_keys=["station:1"])
        make_row(issuers=["76"], ordering_keys=["station:2"])
        make_row(issuers=[], ordering_keys=["station:3"])
        self.assertEqual(len(self.client.get(self.changelist).context["cl"].result_list), 3)

    def test_nobody_else_gets_in_not_even_staff_in_an_issuer_group(self):
        row = self.failed_row(issuers=["756"])
        user = WIGOSNodeUser.objects.create_user(username="ev_staff", email="ev_staff@example.com", password="pw", is_staff=True)
        user.groups.add(WIGOSNodeGroup.objects.get(issuer_identifier=756).group)
        self.client.force_login(user)

        self.assertEqual(self.client.get(self.changelist).status_code, 403)
        self.assertNotContains(self.client.get(reverse("admin:index")), "Publication events")
        self.assertEqual(self.run_action("retry", row).status_code, 403)
        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.FAILED)


class ReadBackReceiptsTestCase(TestCase):
    def test_a_read_back_receipt_has_no_body(self):
        delivery = Delivery("wis2", Message(b"secret", "text/plain", name="f"), Location("http://f"), step="storage")
        loaded = worker.load_delivery(worker.dump_delivery(delivery))
        self.assertIsNone(loaded.message.body)
        self.assertEqual(loaded.location, delivery.location)
        self.assertEqual((loaded.publisher, loaded.step, loaded.message.name), ("wis2", "storage", "f"))

    def test_a_generator_relying_on_the_body_fails_instead_of_sending_nothing(self):
        class Mirror(MessageGenerator):
            def generate(self, event, deliveries):
                return [Message(d.message.body, "text/plain") for d in deliveries]

        PublicationEvent.objects.all().delete()
        storage, mirrored = MemoryStorage(), MemoryBroker()
        publisher = DefaultPublisher("pipe", [
            Step("store", RecordFileGenerator(centre_id="io-test"), storage),
            Step("mirror", Mirror(), mirrored),
        ])
        row = make_row(ordering_keys=KEYS)
        [receipt] = publisher.run_step(publisher.steps[0], row.to_event(), [])
        # as if the worker crashed between the steps
        PublicationEvent.objects.filter(pk=row.pk).update(
            completed=["pipe:store"], deliveries=[worker.dump_delivery(receipt)]
        )

        with self.assertLogs("outbox", "ERROR"):
            worker.process_next([publisher])

        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.FAILED)
        self.assertIn("without a body", row.last_error)
        self.assertEqual(mirrored.sent, [])


class PublicationsApiTestCase(APITestCase):
    def setUp(self):
        PublicationEvent.objects.all().delete()
        self.member = self.user("api_member", 756)
        self.other = self.user("api_other", 76)
        self.outsider = self.user("api_outsider")
        self.admin = WIGOSNodeUser.objects.create_superuser(username="api_admin", email="api_admin@example.com", password="pw")

    def user(self, name, *issuers):
        user = WIGOSNodeUser.objects.create_user(username=name, email=f"{name}@example.com", password="pw")
        for issuer in issuers:
            user.groups.add(WIGOSNodeGroup.objects.get(issuer_identifier=issuer).group)
        return user

    def listed(self, user=None, **params):
        self.client.force_authenticate(user or self.member)
        response = self.client.get(reverse("publications"), params)
        self.assertEqual(response.status_code, 200, response.content)
        return response.json()["results"]

    def ids(self, user=None, **params):
        return [event["id"] for event in self.listed(user, **params)]

    def retry(self, event, user=None):
        self.client.force_authenticate(user or self.member)
        return self.client.post(reverse("publication-retry", args=[event.pk]))

    def test_login_is_required(self):
        row = make_row(issuers=["756"], status=PublicationEvent.Status.FAILED)
        self.client.force_authenticate(None)
        self.assertIn(self.client.get(reverse("publications")).status_code, (401, 403))
        self.assertIn(self.client.post(reverse("publication-retry", args=[row.pk])).status_code, (401, 403))

    def test_a_member_sees_the_events_of_their_issuers_newest_first(self):
        swiss = make_row(issuers=["756"], ordering_keys=["station:1"])
        moved = make_row(issuers=["756", "76"], ordering_keys=["station:2"])
        deleted = make_row(operation=Operation.DELETE, issuers=["756"], ordering_keys=["station:3"])
        brazilian = make_row(issuers=["76"], ordering_keys=["station:4"])

        self.assertEqual(self.ids(), [deleted.pk, moved.pk, swiss.pk])
        self.assertEqual(self.ids(self.other), [brazilian.pk, moved.pk])

    def test_a_user_without_issuer_groups_sees_nothing(self):
        make_row(issuers=["756"])
        self.assertEqual(self.ids(self.outsider), [])

    def test_superusers_see_the_full_log(self):
        make_row(issuers=["756"])
        make_row(issuers=["76"])
        make_row(issuers=[])
        self.assertEqual(len(self.ids(self.admin)), 3)

    def test_filters(self):
        failed = make_row(issuers=["756"], status=PublicationEvent.Status.FAILED, station_id=1, source="api", changed_by=self.member)
        processed = make_row(issuers=["756"], status=PublicationEvent.Status.PROCESSED, station_id=2, source="admin")
        self.assertEqual(self.ids(status="failed"), [failed.pk])
        self.assertEqual(self.ids(station_id=2), [processed.pk])
        self.assertEqual(self.ids(source="admin"), [processed.pk])
        self.assertEqual(self.ids(changed_by="me"), [failed.pk])
        self.assertEqual(self.ids(operation="delete"), [])

    def test_bad_filters_are_refused(self):
        self.client.force_authenticate(self.member)
        self.assertEqual(self.client.get(reverse("publications"), {"station_id": "x"}).status_code, 400)
        self.assertEqual(self.client.get(reverse("publications"), {"changed_by": "someone"}).status_code, 400)

    def test_filters_never_widen_what_a_user_sees(self):
        make_row(issuers=["76"], station_id=9)
        self.assertEqual(self.ids(station_id=9), [])

    def test_is_paginated(self):
        for _ in range(3):
            make_row(issuers=["756"])
        self.client.force_authenticate(self.member)
        body = self.client.get(reverse("publications"), {"limit": 2}).json()
        self.assertEqual((body["count"], len(body["results"])), (3, 2))
        self.assertIsNotNone(body["next"])

    def test_an_event_says_who_and_what_without_internals(self):
        make_row(
            issuers=["756"], source="api", changed_by=self.member, status=PublicationEvent.Status.FAILED,
            last_error="wis2:wnm: broker down",
            deliveries=[
                {"publisher": "wis2", "step": "storage", "name": "f", "media_type": "t", "topic": None,
                 "url": "https://public/f", "internal_url": "http://minio:9000/secret/f"},
                {"publisher": "wis2", "step": "wnm", "name": None, "media_type": "t", "topic": None,
                 "url": None, "internal_url": None},
            ],
        )
        [event] = self.listed()
        self.assertEqual(event["changed_by"], "api_member")
        self.assertEqual((event["source"], event["source_label"]), ("api", "API"))
        self.assertEqual(event["last_error"], "wis2:wnm: broker down")
        self.assertEqual(event["files"], ["https://public/f"])
        self.assertNotIn("minio", json.dumps(event))
        self.assertNotIn("wmdrv2", event)
        self.assertNotIn("deliveries", event)

    def test_an_administrators_change_shows_no_user_name(self):
        make_row(issuers=["756"], source="admin")
        [event] = self.listed()
        self.assertEqual((event["changed_by"], event["source_label"]), (None, "Administrator"))

    def test_can_retry_follows_the_retry_rules(self):
        retryable = make_row(issuers=["756"], status=PublicationEvent.Status.FAILED, ordering_keys=["station:1"])
        make_row(issuers=["756"], status=PublicationEvent.Status.PROCESSED, ordering_keys=["station:2"])
        outdated = make_row(issuers=["756"], status=PublicationEvent.Status.FAILED, ordering_keys=["station:3"])
        make_row(issuers=["756"], ordering_keys=["station:3"])

        flags = {e["id"]: e["can_retry"] for e in self.listed()}

        self.assertTrue(flags.pop(retryable.pk))
        self.assertFalse(flags.pop(outdated.pk))
        self.assertFalse(any(flags.values()))

    def test_a_member_can_retry_their_failed_event(self):
        row = make_row(issuers=["756"], status=PublicationEvent.Status.FAILED, ordering_keys=KEYS, attempts=1)

        with self.captureOnCommitCallbacks() as callbacks:
            response = self.retry(row)

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["status"], "pending")
        self.assertFalse(response.json()["can_retry"])
        self.assertEqual(len(callbacks), 1)
        row.refresh_from_db()
        self.assertEqual((row.status, row.attempts), (PublicationEvent.Status.PENDING, 0))

    def test_an_event_outside_the_users_groups_looks_like_it_doesnt_exist(self):
        row = make_row(issuers=["76"], status=PublicationEvent.Status.FAILED)
        for user in (self.member, self.outsider):
            self.assertEqual(self.retry(row, user).status_code, 404)
        row.refresh_from_db()
        self.assertEqual(row.status, PublicationEvent.Status.FAILED)
        self.client.force_authenticate(self.member)
        self.assertEqual(self.client.post(reverse("publication-retry", args=[row.pk + 1000])).status_code, 404)

    def test_either_issuer_of_a_moved_wsi_can_retry(self):
        row = make_row(issuers=["756", "76"], status=PublicationEvent.Status.FAILED)
        self.assertEqual(self.retry(row, self.other).status_code, 200)

    def test_superusers_can_retry_anything(self):
        row = make_row(issuers=["76"], status=PublicationEvent.Status.FAILED)
        self.assertEqual(self.retry(row, self.admin).status_code, 200)

    def test_a_refused_retry_says_why(self):
        outdated = make_row(issuers=["756"], status=PublicationEvent.Status.FAILED, ordering_keys=KEYS)
        make_row(issuers=["756"], ordering_keys=KEYS)
        response = self.retry(outdated)
        self.assertEqual(response.status_code, 409)
        self.assertIn("save the station again", response.json()["detail"])

        processed = make_row(issuers=["756"], status=PublicationEvent.Status.PROCESSED, ordering_keys=["station:7"])
        self.assertEqual(self.retry(processed).status_code, 409)

    def test_only_post_retries(self):
        row = make_row(issuers=["756"], status=PublicationEvent.Status.FAILED)
        self.client.force_authenticate(self.member)
        self.assertEqual(self.client.get(reverse("publication-retry", args=[row.pk])).status_code, 405)


class PublicationsAttentionAndLatestTestCase(PublicationsApiTestCase):
    """What the sidebar's counter and the stations' chips read."""

    def attention(self, user=None):
        self.client.force_authenticate(user or self.member)
        body = self.client.get(reverse("publications"), {"attention": 1, "limit": 1}).json()
        return body["count"]

    def latest(self, user=None):
        self.client.force_authenticate(user or self.member)
        response = self.client.get(reverse("publications-latest"))
        self.assertEqual(response.status_code, 200, response.content)
        return response.json()

    def failed(self, keys, **fields):
        return make_row(issuers=["756"], status=PublicationEvent.Status.FAILED, ordering_keys=keys, **fields)

    def test_a_failure_counts_until_a_later_change_replaces_it(self):
        self.failed(["station:1"])
        self.assertEqual(self.attention(), 1)
        make_row(issuers=["756"], ordering_keys=["station:1"], status=PublicationEvent.Status.PROCESSED)
        self.assertEqual(self.attention(), 0)

    def test_a_retry_takes_it_off_the_count(self):
        row = self.failed(["station:1"])
        self.assertEqual(self.retry(row).status_code, 200)
        self.assertEqual(self.attention(), 0)

    def test_a_failure_needing_cleanup_counts_even_after_a_later_change(self):
        self.failed(["station:1"], needs_cleanup=True)
        make_row(issuers=["756"], ordering_keys=["station:1"])
        self.assertEqual(self.attention(), 1)

    def test_only_failures_count(self):
        for status in ("pending", "processing", "processed", "published"):
            make_row(issuers=["756"], ordering_keys=[f"station:{status}"], status=status)
        self.assertEqual(self.attention(), 0)

    def test_the_count_only_covers_the_users_groups(self):
        self.failed(["station:1"])
        make_row(issuers=["76"], status=PublicationEvent.Status.FAILED, ordering_keys=["station:2"])
        self.assertEqual(self.attention(), 1)
        self.assertEqual(self.attention(self.other), 1)
        self.assertEqual(self.attention(self.outsider), 0)
        self.assertEqual(self.attention(self.admin), 2)

    def test_latest_gives_each_stations_newest_event(self):
        make_row(issuers=["756"], station_id=1, status=PublicationEvent.Status.FAILED)
        newest = make_row(issuers=["756"], station_id=1, status=PublicationEvent.Status.PROCESSED)
        other = make_row(issuers=["756"], station_id=2, status=PublicationEvent.Status.FAILED)

        latest = self.latest()

        self.assertEqual(set(latest), {"1", "2"})
        self.assertEqual((latest["1"]["id"], latest["1"]["status"]), (newest.pk, "processed"))
        self.assertEqual((latest["2"]["id"], latest["2"]["status"]), (other.pk, "failed"))

    def test_latest_only_covers_the_users_groups(self):
        make_row(issuers=["756"], station_id=1)
        make_row(issuers=["76"], station_id=2)
        self.assertEqual(set(self.latest()), {"1"})
        self.assertEqual(set(self.latest(self.other)), {"2"})
        self.assertEqual(self.latest(self.outsider), {})
        self.assertEqual(set(self.latest(self.admin)), {"1", "2"})

    def test_latest_needs_a_login(self):
        self.client.force_authenticate(None)
        self.assertIn(self.client.get(reverse("publications-latest")).status_code, (401, 403))
