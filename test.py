# coding: utf-8
from sqlalchemy import create_engine, func, ForeignKey
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref, Session

from database import DB

from datetime import datetime
from dotmap import DotMap
import json

Base = declarative_base()
DBURL = 'mysql+mysqldb://jmiller:b14z3r5!@localhost/test'


class Forecast(Base):
    __tablename__ = 'forecast'

    id = Column(Integer, primary_key=True)

    office_id = Column(Integer, ForeignKey('office.id'))
    office = relationship("Office", back_populates='forecasts')
    status_id = Column(Integer, ForeignKey('status.id'))
    status = relationship("Status", back_populates='forecast')

    time_stamp = Column(DateTime)
    date_accessed = Column(DateTime, default=func.now())
    raw_text = Column(Text)
    processed_text = Column(Text)


class Office(Base):
    __tablename__ = 'office'

    id = Column(Integer, primary_key=True)

    forecasts = relationship("Forecast", back_populates='office')

    name = Column(String(4))
    city = Column(String(255))
    state = Column(String(255))
    address = Column(String(255))


class Status(Base):
    __tablename__ = 'status'

    id = Column(Integer, primary_key=True)

    forecast = relationship("Forecast", uselist=False, back_populates='status')

    unique = Column(Boolean, default=False)
    clean = Column(Boolean, default=False)
    extracted = Column(Boolean, default=False)
    in_dataset = Column(Boolean, default=False)


def setup_database(echo=True):
    global engine
    engine = create_engine(DBURL, echo=echo)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def test_flush_no_pk():
    """Individual INSERT statements via the ORM, calling upon last row id"""
    with open('data/nws_office_info.json', 'r') as j:
        office_info = json.load(j)
    session = Session(bind=engine)
    session.add_all(
        [
            Office(
                name=office,
                city=office_info[office]['City'],
                state=office_info[office]['State'],
                address=office_info[office]['Address']
            )
            for office in office_info
        ]
    )
    session.flush()
    session.commit()


def getOffice(name):
    session = Session(bind=engine)
    return session.query(Office).filter(Office.name == name).one()

if __name__ == '__main__':
    setup_database()
    test_flush_no_pk()

    o = getOffice('BOX')

    db = DB(db_path='/home/jmiller/git/AFDTools/forecasts.db')
    
    with db.connection as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM Forecast WHERE Office == 'BOX';")
        rows = cur.fetchall()

    session = Session(bind=engine)
    forecasts = [
        Forecast(
            time_stamp=row['TimeStamp'],
            date_accessed=datetime.now(),
            raw_text=row['Forecast']
            )
        for row in rows
    ]
    statuses = [
        Status(
            forecast=forecast
        )
        for forecast in forecasts
    ]
    o.forecasts.extend(forecasts)
    session.add_all(forecasts)
    session.flush()
    session.commit()
