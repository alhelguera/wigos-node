from django.test import TestCase, TransactionTestCase
from .models import Station, WIGOSNodeUser

import json

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
