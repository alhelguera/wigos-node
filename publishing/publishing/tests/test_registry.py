import unittest

from publishing.base import DefaultPublisher
from publishing.registry import ConfigurationError, build_publishers

from .test_base import CreatesOnlyPublisher, EchoGenerator, RecordingTransport, make_event

HERE = "publishing.tests.test_base"


def config(name="echo", **fields):
    values = {
        "NAME": name,
        "GENERATOR": {"CLASS": f"{HERE}.EchoGenerator"},
        "TRANSPORT": {"CLASS": f"{HERE}.RecordingTransport", "OPTIONS": {"topic": "a/b"}},
    }
    values.update(fields)
    return values


class BuildPublishersTestCase(unittest.TestCase):
    def assertErrors(self, configs, *fragments):
        with self.assertRaises(ConfigurationError) as caught:
            build_publishers(configs)
        self.assertEqual(len(caught.exception.errors), len(fragments), caught.exception.errors)
        for error, fragment in zip(caught.exception.errors, fragments):
            self.assertIn(fragment, error)

    def test_builds_publishers_in_order(self):
        publishers = build_publishers([config("first"), config("second", CLASS=f"{HERE}.CreatesOnlyPublisher")])
        self.assertEqual([p.name for p in publishers], ["first", "second"])
        self.assertIsInstance(publishers[0], DefaultPublisher)
        self.assertIsInstance(publishers[1], CreatesOnlyPublisher)
        self.assertIsInstance(publishers[0].generator, EchoGenerator)
        self.assertIsInstance(publishers[0].transport, RecordingTransport)
        self.assertEqual(publishers[0].transport.options, {"topic": "a/b"})

    def test_built_publisher_publishes(self):
        [publisher] = build_publishers([config()])
        publisher.publish(make_event(), [])
        self.assertEqual([m.body for m in publisher.transport.sent], [b"create"])

    def test_empty_config_builds_nothing(self):
        self.assertEqual(build_publishers([]), [])

    def test_disabled_entries_are_skipped_without_importing(self):
        disabled = config("off", ENABLED=False, TRANSPORT={"CLASS": "not_installed.Transport"})
        self.assertEqual([p.name for p in build_publishers([disabled, config()])], ["echo"])

    def test_missing_name(self):
        self.assertErrors([config(NAME="")], "Publisher 0: NAME is required")

    def test_duplicate_names(self):
        # also among disabled entries: names identify outbox progress
        self.assertErrors([config(), config(ENABLED=False)], "'echo': NAME is used more than once")

    def test_bad_paths(self):
        self.assertErrors(
            [config(GENERATOR={"CLASS": "nowhere.Generator"}, TRANSPORT={"CLASS": f"{HERE}.Missing"})],
            "GENERATOR: cannot import nowhere.Generator",
            "TRANSPORT: cannot import publishing.tests.test_base.Missing",
        )

    def test_missing_class(self):
        self.assertErrors([config(TRANSPORT={})], "TRANSPORT: CLASS is required")

    def test_wrong_base_classes(self):
        self.assertErrors(
            [config(CLASS=f"{HERE}.EchoGenerator", GENERATOR={"CLASS": f"{HERE}.RecordingTransport"})],
            "EchoGenerator is not a Publisher",
            "RecordingTransport is not a MessageGenerator",
        )

    def test_abstract_publisher_cannot_be_created(self):
        self.assertErrors([config(CLASS="publishing.base.Publisher")], "Publisher could not be created")

    def test_bad_options(self):
        self.assertErrors(
            [config(GENERATOR={"CLASS": f"{HERE}.EchoGenerator", "OPTIONS": "oops"})],
            "GENERATOR: OPTIONS must be a dict",
        )
        self.assertErrors(
            [config(TRANSPORT={"CLASS": f"{HERE}.CreatesOnlyPublisher"})],
            "TRANSPORT: publishing.tests.test_base.CreatesOnlyPublisher is not a TransportLayer",
        )

    def test_every_problem_is_reported(self):
        self.assertErrors(
            [config(TRANSPORT={}), config("other", GENERATOR={"CLASS": "nowhere.Generator"})],
            "'echo' TRANSPORT: CLASS is required",
            "'other' GENERATOR: cannot import",
        )
