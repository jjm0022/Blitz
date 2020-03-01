import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from database_definitions import Forecast, Status, Extraction, Office


class DB:
    def __init__(
        self,
        url=None,
        engine=None,
        echo=False,
    ):
        if not url:
            config = configparser.ConfigParser()
            config.read('config.ini')
            url = f"mysql+mysqldb://{config['db']['user']}:{config['db']['passwd']}@{config['db']['host']}:{config['db']['port']}/{config['db']['schema']}"
        if engine:
            self.session = Session(bind=engine)
        else:
            engine = create_engine(url, echo=False)
            self.session = Session(bind=engine)

    def _reset_databse():
        pass
