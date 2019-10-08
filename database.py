from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from database_definitions import Forecast, Status, Extraction, Office


class DB:

    def __init__(self,
                 url='mysql+mysqldb://root:b14z3r5@10.0.0.100:3306/testdb',
                 engine=None,
                 echo=False):
        if engine:
            self.session = Session(bind=engine)
        else:
            engine = create_engine(url, echo=False)
            self.session = Session(bind=engine)

    def _reset_databse():
        pass

