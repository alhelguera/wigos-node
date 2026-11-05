import unittest

from publishing.celery import app


class CeleryAppTestCase(unittest.TestCase):
    def test_uses_valkey_and_stores_no_results(self):
        self.assertTrue(app.conf.broker_url.startswith("redis://"))
        self.assertTrue(app.conf.task_ignore_result)
