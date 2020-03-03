import configparser
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.schema import MetaData
from pathlib import Path

from database_definitions import Forecast, Status, Extraction, Office


class DB:
    def __init__(self, url=None, engine=None, echo=False, config_path="config.ini"):
        if not url:
            config = configparser.ConfigParser()
            if Path(config_path).exists():
                config.read()
                url = f"mysql+mysqldb://{config['db']['user']}:{config['db']['passwd']}@{config['db']['host']}:{config['db']['port']}/{config['db']['schema']}"
            else:
                # Hopefully this means we're building in circleCI
                url = "sqlite:///ci_build.db"

        if engine:
            self.engine = engine
            self.session = Session(bind=self.engine)
        else:
            self.engine = create_engine(url, echo=False)
            self.session = Session(bind=engine)

        self.create_tables([Forecast, Office, Status, Extraction])

    def create_tables(self, tables):
        """
        Description:
            Creates a table in connected DB if it does not already exist

        Arguments:
            tables {list} -- list of pre-defined SQLAlchemy tables
        """
        for t in tables:
            assert isinstance(
                t, sqlalchemy.ext.declarative.api.DeclarativeMeta
                ), f"'{t}' must be an instance of sqlalchemy.ext.declarative.api.DeclarativeMeta"

            if t.__tablename__ in self.engine.table_names():
                print(f"table '{t.__tablename__}' already exists")
                continue
            else:
                print(f"Creating table '{t.__tablename__}'")
                t.__table__.create(bind=self.engine, checkfirst=True)
            
