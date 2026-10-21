from django.test import SimpleTestCase, TestCase, TransactionTestCase, override_settings
from .models import Station, WIGOSNodeUser
from . import wnm
from .renderers import render_wmdrv2

import base64
import hashlib
import json
import uuid
from datetime import datetime, timezone

from django.db.utils import IntegrityError

class StationCreationTestCase(TestCase):



    def test_double_creation_rejected(self):

        with open(r'/test_data/test_wmdrv2.json') as f:
            wmdrv2_data = json.load(f)

            some_owner = WIGOSNodeUser.objects.first()

            with self.assertRaises(ValueError):
                Station.objects.create(wmdrv2=wmdrv2_data, owner=some_owner)


    def test_primary_wsi_in_additional_identifier_rejected(self):
        with open(r'/test_data/test_wmdrv2.json') as f:
            wmdrv2_data = json.load(f)
            wmdrv2_data['id'] = '0-51-21-2121'         # WSI used in additional identifiers in test station

            some_owner = WIGOSNodeUser.objects.first()

            with self.assertRaises(ValueError):
                Station.objects.create(wmdrv2=wmdrv2_data, owner=some_owner)



    def test_additional_wsi_in_additional_identifier_rejected(self):
        with open(r'/test_data/test_wmdrv2.json') as f:
            wmdrv2_data = json.load(f)
            wmdrv2_data['id'] = '0-51-21-2121ok'         # WSI not yet used
            wmdrv2_data['properties']['additionalIds'] = ['0-51-21-2121'] # already used in additional identifiers in test station

            some_owner = WIGOSNodeUser.objects.first()

            with self.assertRaises(ValueError):
                Station.objects.create(wmdrv2=wmdrv2_data, owner=some_owner)


    def test_additional_wsi_in_primary_identifier_rejected(self):
        with open(r'/test_data/test_wmdrv2.json') as f:
            wmdrv2_data = json.load(f)
            wmdrv2_data['id'] = '0-51-21-2121ok'         # WSI not yet used
            wmdrv2_data['properties']['additionalIds'] = ['0-20000-0-123456'] # already used in primary identifier in test station

            some_owner = WIGOSNodeUser.objects.first()

            with self.assertRaises(ValueError):
                Station.objects.create(wmdrv2=wmdrv2_data, owner=some_owner)


@override_settings(WNM_CENTRE_ID='io-wis2dev-test-wigosnode', WNM_STORAGE_URL='https://example.org/webstorage/')
class WnmCreationTestCase(SimpleTestCase):

    def setUp(self):
        with open(r'/test_data/test_wmdrv2.json') as f:
            self.wmdrv2_data = json.load(f)

    def test_required_properties(self):
        message = wnm.create_wnm(self.wmdrv2_data)

        uuid.UUID(message['id'])
        self.assertEqual(message['type'], 'Feature')
        self.assertEqual(message['conformsTo'], [wnm.WNM_CONFORMANCE_CLASS])
        self.assertEqual(message['geometry'], {'type': 'Point', 'coordinates': [-23, 22]})
        self.assertEqual(message['properties']['data_id'],
                         'wmdr2/io-wis2dev-test-wigosnode/0-20000-0-123456/2026-08-07T11:13:00.000Z')
        self.assertEqual(message['properties']['datetime'], '2026-08-07T11:13:00Z')
        self.assertRegex(message['properties']['pubtime'], r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$')
        self.assertEqual(len(message['links']), 1)
        self.assertEqual(message['links'][0]['href'], 'https://example.org/webstorage/'
                         'wmdr2_io-wis2dev-test-wigosnode_0-20000-0-123456_2026-08-07T11_13_00_000.json')
        self.assertEqual(message['links'][0]['rel'], 'canonical')
        self.assertEqual(message['links'][0]['type'], 'application/geo+json')

    def test_version_falls_back_to_created(self):
        del self.wmdrv2_data['properties']['updated']
        self.wmdrv2_data['properties']['created'] = '2026-01-02T03:04:05Z'
        message = wnm.create_wnm(self.wmdrv2_data)

        self.assertTrue(message['properties']['data_id'].endswith('/2026-01-02T03:04:05Z'))
        self.assertTrue(message['links'][0]['href'].endswith('_2026-01-02T03_04_05.json'))

    def test_missing_version_rejected(self):
        del self.wmdrv2_data['properties']['updated']
        del self.wmdrv2_data['properties']['created']
        with self.assertRaises(ValueError):
            wnm.create_wnm(self.wmdrv2_data)

    def test_integrity_matches_record(self):
        message = wnm.create_wnm(self.wmdrv2_data)
        data = render_wmdrv2(self.wmdrv2_data)

        self.assertEqual(message['properties']['integrity'], {
            'method': 'sha512',
            'value': base64.b64encode(hashlib.sha512(data).digest()).decode('ascii'),
        })
        self.assertEqual(message['links'][0]['length'], len(data))

    def test_large_record_not_inlined(self):
        self.wmdrv2_data['properties']['description'] = 'x' * wnm.MAX_INLINE_CONTENT_SIZE
        message = wnm.create_wnm(self.wmdrv2_data)

        self.assertNotIn('content', message['properties'])
        self.assertIn('integrity', message['properties'])

    def test_update_and_deletion_links(self):
        update = wnm.create_wnm(self.wmdrv2_data,operation=wnm.UPDATE)
        deletion = wnm.create_wnm(self.wmdrv2_data,operation=wnm.DELETE)

        self.assertEqual(update['links'][0]['rel'], 'update')
        self.assertEqual(deletion['links'][0]['rel'], 'deletion')
        self.assertNotIn('integrity', deletion['properties'])
        self.assertNotIn('content', deletion['properties'])

    def test_metadata_id_is_wigos_identifier(self):
        for operation in wnm.OPERATIONS:
            message = wnm.create_wnm(self.wmdrv2_data,operation=operation)
            self.assertEqual(message['properties']['metadata_id'], '0-20000-0-123456')

    def test_pubtime(self):
        pubtime = datetime(2026, 10, 21, 12, 30, tzinfo=timezone.utc)
        message = wnm.create_wnm(self.wmdrv2_data,pubtime=pubtime)

        self.assertEqual(message['properties']['pubtime'], '2026-10-21T12:30:00Z')

    def test_unknown_operation_rejected(self):
        with self.assertRaises(ValueError):
            wnm.create_wnm(self.wmdrv2_data,operation='replace')

    def test_missing_wigos_identifier_rejected(self):
        del self.wmdrv2_data['id']
        with self.assertRaises(ValueError):
            wnm.create_wnm(self.wmdrv2_data)


                