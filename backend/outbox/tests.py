from django.test import SimpleTestCase, TestCase, override_settings

from api.models import WIGOSNodeUser
from publishing.base import Event, Operation
from publishing.generators.wmdr2 import RecordFileGenerator
from publishing.generators.wnm import WNMGenerator
from publishing.registry import ConfigurationError

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
    {"NAME": "files", "GENERATOR": {"CLASS": "publishing.generators.wmdr2.RecordFileGenerator"}, "TRANSPORT": {"CLASS": LOG}},
    {"NAME": "files", "GENERATOR": {"CLASS": "nowhere.Generator"}, "TRANSPORT": {"CLASS": LOG}},
]


class PublishersSettingTestCase(SimpleTestCase):
    def test_configured_publishers_build(self):
        self.assertEqual(check_publishers(None), [])
        storage, wnm = get_publishers()
        self.assertEqual((storage.name, wnm.name), ("wis2-storage", "wis2-wnm"))
        self.assertIsInstance(storage.generator, RecordFileGenerator)
        self.assertIsInstance(wnm.generator, WNMGenerator)

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
