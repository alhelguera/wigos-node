"""Tests of the publishing job, which need Django and the database. Not found by
the plain unittest discovery of this package; run them with:
python manage.py test publishing.django_tests"""

import base64
import copy
import hashlib
import json
from unittest import mock

from django.test import TestCase
from django.urls import reverse
from kombu.exceptions import OperationalError
from rest_framework.test import APITestCase

from api import stations
from api.models import Station, WIGOSNodeGroup, WIGOSNodeUser

from . import state, tasks
from .base import DefaultPublisher, Location, MessageGenerator, PublishError, Step, TransportLayer
from .generators.wmdr2 import RecordFileGenerator
from .generators.wnm import WNMGenerator

Status = Station.PublicationStatus

with open("/test_data/test_wmdrv2.json") as f:
    BASE_RECORD = json.load(f)


def record(wsi, updated="2026-11-05T10:00:00Z", title="Publishing station"):
    data = copy.deepcopy(BASE_RECORD)
    data["id"] = wsi
    data["properties"]["title"] = title
    data["properties"]["updated"] = updated
    return data


class FakeLock:
    def __init__(self, held, name):
        self.held = held
        self.name = name

    def acquire(self):
        if self.name in self.held:
            return False
        self.held.add(self.name)
        return True

    def release(self):
        self.held.discard(self.name)

    def locked(self):
        return self.name in self.held


class FakeValkey:
    def __init__(self):
        self.values = {}
        self.held = set()

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value

    def lock(self, name, **kwargs):
        return FakeLock(self.held, name)


class MemoryStorage(TransportLayer):
    def __init__(self):
        super().__init__()
        self.files = {}
        self.fail_remove = False

    def send(self, message):
        url = f"mem://store/{message.name}"
        self.files[url] = message.body
        return Location(url)

    def remove(self, location):
        if self.fail_remove:
            raise PublishError("storage down")
        del self.files[location.url]


class MemoryBroker(TransportLayer):
    def __init__(self):
        super().__init__()
        self.sent = []
        self.failures = 0
        self.opened = 0
        self.closed = 0
        self.before_send = None

    def open(self):
        self.opened += 1

    def close(self):
        self.closed += 1

    def send(self, message):
        if self.before_send:
            self.before_send()
        if self.failures:
            self.failures -= 1
            raise PublishError("broker down")
        self.sent.append(json.loads(message.body))


class MemoryPublisher(DefaultPublisher):
    def __init__(self, *args, storage, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage = storage

    def retrieve(self, location):
        return self.storage.files[location.url]


class BrokenGenerator(MessageGenerator):
    def generate(self, event, deliveries):
        raise RuntimeError("boom")


class JobTestCase(TestCase):
    def setUp(self):
        self.valkey = FakeValkey()
        patcher = mock.patch.object(state, "client", return_value=self.valkey)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.delay = mock.patch.object(tasks.publish_station, "delay").start()
        self.addCleanup(mock.patch.stopall)

        self.storage = MemoryStorage()
        self.broker = MemoryBroker()
        self.publisher = MemoryPublisher(
            "wis2",
            [
                Step("storage", RecordFileGenerator(centre_id="io-test"), self.storage),
                Step("wnm", WNMGenerator(centre_id="io-test"), self.broker),
            ],
            storage=self.storage,
        )
        self.use_publishers(self.publisher)

        self.user = WIGOSNodeUser.objects.create_superuser(username="job_admin", email="job_admin@example.com", password="pw")

    def use_publishers(self, *publishers):
        patcher = mock.patch.object(tasks, "opened_publishers", return_value=list(publishers))
        patcher.start()
        self.addCleanup(patcher.stop)

    def station(self, wsi="0-756-0-job1", **kwargs):
        return stations.create_station(self.user, record(wsi, **kwargs))

    def status_of(self, station):
        return Station.objects.values_list("publication_status", flat=True).get(pk=station.pk)

    def test_publishes_the_file_then_its_wnm(self):
        station = self.station()

        tasks.publish_station(station.pk)

        self.assertEqual(self.status_of(station), Status.PROCESSED)
        [url] = self.storage.files
        [wnm] = self.broker.sent
        self.assertEqual((wnm["links"][0]["href"], wnm["links"][0]["rel"]), (url, "canonical"))
        digest = base64.b64encode(hashlib.sha512(self.storage.files[url]).digest()).decode()
        self.assertEqual(wnm["properties"]["integrity"]["value"], digest)

        saved = state.load(station.pk)
        self.assertEqual([d["step"] for d in saved["deliveries"]], ["storage", "wnm"])
        self.assertEqual(saved["deliveries"][0]["url"], url)
        self.assertEqual((saved["error"], saved["leftovers"]), ("", []))
        self.assertIsNotNone(saved["finished_at"])
        self.assertFalse(state.is_locked(station.pk))
        self.delay.assert_not_called()

    def test_the_next_publication_is_an_update(self):
        station = self.station()
        tasks.publish_station(station.pk)

        stations.update_station(self.user, station, record("0-756-0-job1", updated="2026-11-05T11:00:00Z"))
        tasks.publish_station(station.pk)

        self.assertEqual([w["links"][0]["rel"] for w in self.broker.sent], ["canonical", "update"])

    def test_pubtime_is_the_records_updated_value_on_every_attempt(self):
        station = self.station(updated="2026-11-05T10:00:00Z")
        self.broker.failures = 1
        tasks.publish_station(station.pk)
        self.assertEqual(self.status_of(station), Status.FAILED)

        Station.objects.filter(pk=station.pk).update(publication_status=Status.PENDING)
        tasks.publish_station(station.pk)

        [wnm] = self.broker.sent
        self.assertEqual(wnm["properties"]["pubtime"], "2026-11-05T10:00:00Z")

    def test_a_failure_is_recorded_and_undoes_the_stored_file(self):
        station = self.station()
        self.broker.failures = 1

        tasks.publish_station(station.pk)

        self.assertEqual(self.status_of(station), Status.FAILED)
        self.assertEqual(self.storage.files, {})
        saved = state.load(station.pk)
        self.assertEqual(saved["error"], "wis2:wnm: broker down")
        self.assertEqual(saved["deliveries"], [])

    def test_a_create_that_failed_is_still_announced_as_canonical(self):
        station = self.station()
        self.broker.failures = 1
        tasks.publish_station(station.pk)

        stations.update_station(self.user, station, record("0-756-0-job1", updated="2026-11-05T11:00:00Z"))
        tasks.publish_station(station.pk)

        [wnm] = self.broker.sent
        self.assertEqual(wnm["links"][0]["rel"], "canonical")

    def test_a_failure_keeps_the_last_success_and_its_file(self):
        station = self.station()
        tasks.publish_station(station.pk)
        [url] = self.storage.files

        # the same `updated`, so the same file name
        Station.objects.filter(pk=station.pk).update(publication_status=Status.PENDING)
        self.broker.failures = 1
        tasks.publish_station(station.pk)

        self.assertEqual(self.status_of(station), Status.FAILED)
        self.assertIn(url, self.storage.files)
        self.assertEqual(state.load(station.pk)["deliveries"][0]["url"], url)

    def test_a_failed_undo_leaves_a_leftover_removed_on_the_next_attempt(self):
        station = self.station()
        self.broker.failures = 1
        self.storage.fail_remove = True
        tasks.publish_station(station.pk)

        saved = state.load(station.pk)
        self.assertEqual(saved["error"], "wis2:wnm: broker down\nundo wis2: storage down")
        [leftover] = saved["leftovers"]
        self.assertIn(leftover["url"], self.storage.files)

        stations.update_station(self.user, station, record("0-756-0-job1", updated="2026-11-05T11:00:00Z"))
        self.storage.fail_remove = False
        tasks.publish_station(station.pk)

        self.assertEqual(self.status_of(station), Status.PROCESSED)
        self.assertNotIn(leftover["url"], self.storage.files)
        self.assertEqual(state.load(station.pk)["leftovers"], [])

    def test_a_leftover_that_still_cant_be_removed_is_kept(self):
        station = self.station()
        self.broker.failures = 1
        self.storage.fail_remove = True
        tasks.publish_station(station.pk)
        [leftover] = state.load(station.pk)["leftovers"]

        stations.update_station(self.user, station, record("0-756-0-job1", updated="2026-11-05T11:00:00Z"))
        with self.assertLogs("publishing", "ERROR"):
            tasks.publish_station(station.pk)

        self.assertEqual(self.status_of(station), Status.PROCESSED)
        self.assertEqual(state.load(station.pk)["leftovers"], [leftover])

    def test_a_save_during_the_job_stays_pending_and_gets_a_new_job(self):
        station = self.station()

        def saved_meanwhile():
            self.broker.before_send = None
            stations.update_station(self.user, Station.objects.get(pk=station.pk), record("0-756-0-job1", updated="2026-11-05T11:00:00Z"))

        self.broker.before_send = saved_meanwhile
        tasks.publish_station(station.pk)

        self.assertEqual(self.status_of(station), Status.PENDING)
        self.delay.assert_called_once_with(station.pk)
        self.assertFalse(state.is_locked(station.pk))  # or the new job would stop at it

    def test_a_job_finding_the_station_locked_stops(self):
        station = self.station()
        self.assertTrue(state.station_lock(station.pk).acquire())

        tasks.publish_station(station.pk)

        self.assertEqual(self.status_of(station), Status.PENDING)
        self.assertEqual(self.broker.sent, [])
        self.assertTrue(state.is_locked(station.pk))  # still the first job's

    def test_a_second_job_finds_nothing_to_do(self):
        station = self.station()
        tasks.publish_station(station.pk)
        tasks.publish_station(station.pk)
        self.assertEqual(len(self.broker.sent), 1)
        self.delay.assert_not_called()

    def test_a_deleted_station_is_ignored(self):
        tasks.publish_station(999999)
        self.assertEqual(self.broker.sent, [])

    def test_a_crashed_job_releases_the_lock(self):
        station = self.station()
        with mock.patch.object(tasks, "_publish", side_effect=RuntimeError("valkey down")):
            with self.assertRaises(RuntimeError):
                tasks.publish_station(station.pk)
        self.assertFalse(state.is_locked(station.pk))
        self.assertEqual(self.status_of(station), Status.PROCESSING)  # a user's retry takes it from here

    def test_publishers_are_independent(self):
        other_storage = MemoryStorage()
        other = MemoryPublisher("other", [Step("storage", BrokenGenerator(), other_storage)], storage=other_storage)
        self.use_publishers(other, self.publisher)
        station = self.station()

        with self.assertLogs("publishing", "ERROR"):
            tasks.publish_station(station.pk)

        self.assertEqual(self.status_of(station), Status.FAILED)
        self.assertEqual(len(self.broker.sent), 1)  # wis2 still published
        saved = state.load(station.pk)
        self.assertEqual(saved["error"], "other:storage: boom")
        self.assertEqual({d["publisher"] for d in saved["deliveries"]}, {"wis2"})

    def test_a_record_that_cant_be_published_fails(self):
        station = self.station()
        wmdrv2 = record("0-756-0-job1")
        del wmdrv2["properties"]["updated"], wmdrv2["properties"]["created"]
        Station.objects.filter(pk=station.pk).update(wmdrv2=wmdrv2)

        with self.assertLogs("publishing", "ERROR"):
            tasks.publish_station(station.pk)

        self.assertEqual(self.status_of(station), Status.FAILED)
        self.assertIn("properties.updated", state.load(station.pk)["error"])


class OpenedPublishersTestCase(TestCase):
    def setUp(self):
        self.addCleanup(setattr, tasks, "_opened", None)
        tasks._opened = None

    def test_open_once_per_process_and_closed_on_shutdown(self):
        storage = MemoryStorage()
        broker = MemoryBroker()
        publisher = MemoryPublisher("wis2", [Step("wnm", WNMGenerator(centre_id="io-test"), broker)], storage=storage)

        with mock.patch("outbox.publishers.get_publishers", return_value=[publisher]):
            self.assertEqual(tasks.opened_publishers(), [publisher])
            self.assertEqual(tasks.opened_publishers(), [publisher])
        self.assertEqual(broker.opened, 1)

        tasks.close_publishers()
        self.assertEqual(broker.closed, 1)
        tasks.close_publishers()  # nothing open any more
        self.assertEqual(broker.closed, 1)


class EnqueueTestCase(APITestCase):
    """The explicit call in api.stations."""

    def setUp(self):
        self.delay = mock.patch.object(tasks.publish_station, "delay").start()
        self.addCleanup(mock.patch.stopall)
        self.member = WIGOSNodeUser.objects.create_user(username="enq_member", email="enq_member@example.com", password="pw")
        self.member.groups.add(WIGOSNodeGroup.objects.get(issuer_identifier=756).group)
        self.admin = WIGOSNodeUser.objects.create_superuser(username="enq_admin", email="enq_admin@example.com", password="pw")

    def test_a_create_is_pending_names_the_user_and_enqueues_after_commit(self):
        with self.captureOnCommitCallbacks() as callbacks:
            station = stations.create_station(self.member, record("0-756-0-enq1"))
        self.delay.assert_not_called()

        for callback in callbacks:
            callback()

        self.delay.assert_called_once_with(station.pk)
        station.refresh_from_db()
        self.assertEqual(station.publication_status, Status.PENDING)
        self.assertEqual(station.last_changed_by, self.member)
        self.assertEqual(station.owner, self.member)

    def test_an_update_by_a_colleague_changes_who_but_not_the_owner(self):
        colleague = WIGOSNodeUser.objects.create_user(username="enq_other", email="enq_other@example.com", password="pw")
        colleague.groups.add(WIGOSNodeGroup.objects.get(issuer_identifier=756).group)
        station = stations.create_station(self.member, record("0-756-0-enq2"))
        Station.objects.filter(pk=station.pk).update(publication_status=Status.PROCESSED)
        station.refresh_from_db()

        with self.captureOnCommitCallbacks(execute=True):
            stations.update_station(colleague, station, record("0-756-0-enq2", title="New"))

        station.refresh_from_db()
        self.assertEqual(station.publication_status, Status.PENDING)
        self.assertEqual(station.last_changed_by, colleague)
        self.assertEqual(station.owner, self.member)
        self.delay.assert_called_once_with(station.pk)

    def test_a_failure_to_enqueue_is_logged_and_keeps_the_change(self):
        self.delay.side_effect = OSError("valkey down")
        with self.assertLogs("django.test", "ERROR"):  # where the test helper logs it
            with self.captureOnCommitCallbacks(execute=True):
                station = stations.create_station(self.member, record("0-756-0-enq8"))
        station.refresh_from_db()
        self.assertEqual(station.publication_status, Status.PENDING)

    def test_after_the_broker_fails_only_the_first_save_tries_for_a_while(self):
        self.addCleanup(setattr, tasks, "_broker_down_until", 0.0)
        self.delay.side_effect = OperationalError("valkey down")

        with self.assertLogs("publishing", "ERROR"):
            tasks.request_publication(1)
        with self.assertLogs("publishing", "WARNING"):
            tasks.request_publication(2)
        self.delay.assert_called_once_with(1)

        # the window is over and the broker is back
        tasks._broker_down_until = 0.0
        self.delay.side_effect = None
        self.delay.reset_mock()
        tasks.request_publication(3)
        self.delay.assert_called_once_with(3)

    def test_superusers_are_not_named(self):
        station = stations.create_station(self.admin, record("0-756-0-enq3"))
        station.refresh_from_db()
        self.assertIsNone(station.last_changed_by)

    def test_a_rejected_save_enqueues_nothing(self):
        stations.create_station(self.member, record("0-756-0-enq4"))
        with self.captureOnCommitCallbacks(execute=True) as callbacks:
            with self.assertRaises(stations.StationError):
                stations.create_station(self.member, record("0-756-0-enq4"))  # WSI taken
        self.assertEqual(callbacks, [])

    def post_batch(self, updates, dry_run=False):
        self.client.force_authenticate(self.member)
        return self.client.post(reverse("wmdrv2-batch"), {"dry_run": dry_run, "updates": updates}, format="json")

    def test_a_batch_dry_run_enqueues_nothing(self):
        station = stations.create_station(self.member, record("0-756-0-enq5"))
        with self.captureOnCommitCallbacks(execute=True) as callbacks:
            response = self.post_batch([{"station_id": station.pk, "wmdrv2": record("0-756-0-enq5", title="New")}], dry_run=True)
        self.assertEqual(response.json()["summary"], {"ok": 1, "error": 0})
        self.assertEqual(callbacks, [])

    def test_a_batch_enqueues_only_the_accepted_updates(self):
        ok = stations.create_station(self.member, record("0-756-0-enq6"))
        other = stations.create_station(self.member, record("0-756-0-enq7"))
        with self.captureOnCommitCallbacks(execute=True):
            response = self.post_batch([
                {"station_id": ok.pk, "wmdrv2": record("0-756-0-enq6", title="New")},
                {"station_id": other.pk, "wmdrv2": record("0-756-0-enq6", title="Clash")},  # WSI of the first station
            ])
        self.assertEqual(response.json()["summary"], {"ok": 1, "error": 1})
        self.delay.assert_called_once_with(ok.pk)
