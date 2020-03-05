from sqlalchemy.orm import sessionmaker
from pathlib import Path
import json

from database import DB
from database_definitions import Office


def populate_office_information():

    office_info = Path('data/NWS_OFFICE_LOCATION.json')
    with open(office_info, 'r') as j:
        office_locations = json.load(j)

    db = DB()
    Session = sessionmaker(bind=db.engine)
    session = Session()

    for office_id, location in office_locations.items():
        o = Office(
            name=office_id,
            city=location['City'],
            state=location['State'],
            address=location['Address']
        )
        session.merge(o)

    session.flush()
    session.commit()


if __name__ == "__main__":
    populate_office_information()
