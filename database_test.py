from database import DB
from sqlalchemy.orm import sessionmaker
import unittest
import json

from database_definitions import Forecast, Status, Extraction, Office


class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.db = DB()
        Session = sessionmaker(bind=self.db.engine)
        self.session = Session()
        office_info = 'data/NWS_OFFICE_LOCATION.json'
        with open(office_info, 'r') as j:
            self.office_locations = json.load(j)

    def test_validate_office_information(self):
        for office_id, location in self.office_locations.items():
            office = self.session.query(Office).filter_by(name=office_id).one()
            self.assertEqual(office.name, office_id)
            self.assertEqual(office.city, location['City'])
            self.assertEqual(office.address, location['Address'])
            self.assertEqual(office.state, location['State'])



if __name__ == "__main__":
    unittest.main()
