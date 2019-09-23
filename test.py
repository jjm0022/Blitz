# coding: utf-8
from sqlalchemy import create_engine, func, ForeignKey
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref, Session, deferred

from database import DB

from datetime import datetime
from dateutil.parser import parse
from dotmap import DotMap
import json
from loguru import logger
import sys

Base = declarative_base()
#DBURL = 'mysql+mysqldb://jmiller:b14z3r5@10.0.0.100:3306/plexserverdb'
DBURL = 'mysql+mysqldb://root:b14z3r5@10.0.0.100:3306/testdb'

OFFICES = ["BOX","GSP","EPZ","FWD","BOU","BOI","PAH","FSD","LIX","OAX",
           "PIH","FGZ","RIW","RAH","TAE","OHX","MQT","LBF","FGF","IWX",
           "MRX","JAX","GLD","APX","BGM","JAN","CHS","PHI","PQR","MAF",
           "GYX","FFC","LKN","CRP","GJT","GUM","TSA","DTX","DMX","AMA",
           "HUN","MFL","MFR","BMX","BUF","LSX","AFG","GRR","SHV","DLH",
           "AFC","EWX","GRB","MHX","DDC","PBZ","RNK","BTV","ABQ","OKX",
           "TBW","LZK","PSR","MOB","MPX","DVN","CTP","ILN","AKQ","ILM",
           "REV","EKA","LUB","HFO","HNX","ILX","SEW","GGW","OUN","JKL",
           "ICT","UNR","CLE","PDT","SJT","SJU","VEF","MEG","PUB","BRO",
           "LMK","RLX","KEY","ALY","TWC","LWX","CAR","TOP","MSO","LCH",
           "LOT","BYZ","LOX","CAE","MKX","CYS","SGX","EAX","BIS","SGF",
           "MLB","HGX","MTR","STO","IND","ABR","AJK","SLC","ARX","OTX",
           "GID","TFX"]

class Forecast(Base):
    __tablename__ = 'forecast'

    id = Column(String(25), 
                primary_key=True)

    office_id = Column(Integer, ForeignKey('office.id'))
    office = relationship("Office", back_populates='forecasts')
    status_id = Column(Integer, ForeignKey('status.id'))
    status = relationship("Status",
                          back_populates='forecast',
                          single_parent=True,
                          cascade='all, delete, delete-orphan')
    #extraction_id = Column(Integer, ForeignKey('extraction.id'))
    extractions = relationship("Extraction",
                               back_populates='forecast',
                               cascade='all, delete, delete-orphan')

    time_stamp = Column(DateTime)
    date_accessed = Column(DateTime, default=func.now())
    raw_text = deferred(Column(Text(16000000)))
    processed_text = deferred(Column(Text(16000000)))


class Office(Base):
    __tablename__ = 'office'

    id = Column(Integer, primary_key=True)

    forecasts = relationship("Forecast",
                              back_populates='office',
                              cascade='all, delete, delete-orphan')

    name = Column(String(4))
    city = Column(String(255))
    state = Column(String(255))
    address = Column(String(255))


class Status(Base):
    __tablename__ = 'status'

    id = Column(Integer, primary_key=True)

    forecast = relationship("Forecast",
                            uselist=False,
                            back_populates='status')

    unique = Column(Boolean, default=False)
    clean = Column(Boolean, default=False)
    extracted = Column(Boolean, default=False)
    in_dataset = Column(Boolean, default=False)


class Extraction(Base):
    __tablename__ = 'extraction'

    id = Column(Integer, primary_key=True)

    forecast_id = Column(String(25), ForeignKey('forecast.id'))
    forecast = relationship("Forecast", 
                            uselist=False,
                            back_populates='extractions')

    phrase = Column(String(50))
    start_index = Column(Integer)
    end_index = Column(Integer)


def setup_database(echo=True):
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


def populateForecastByOffice(office):
    """Individual INSERT statements via the ORM, calling upon last row id"""
    db = DB(db_path='/home/jmiller/git/AFDTools/forecasts.db')

    logger.info(f"Inserting forecasts for {office}")
    
    with db.connection as con:
        cur = con.cursor()
        cur.execute(f"SELECT * FROM Forecast WHERE Office == '{office}';")
        rows = cur.fetchall()

    session = Session(bind=engine)
    Of = session.query(Office).filter(Office.name == office).one()
    ids = list()
    total = 0
    skipped = 0
    for row in rows:
        id = str(Of.name) + '-' + parse(row['TimeStamp']).strftime('%Y-%m-%d %H:%M:%S')
        if id in ids:
            logger.debug(f"Duplicate time_stamp: {id}")
            skipped += 1
            continue
        f = Forecast(
                id=id, 
                office=Of,
                status=Status(unique=0, clean=0, extracted=0, in_dataset=0),
                time_stamp=parse(row['TimeStamp']).strftime('%Y-%m-%d %H:%M:%S'),
                date_accessed=datetime.now(),
                raw_text=row['Forecast']
            )
        session.merge(f)
        ids.append(id)
        total += 1

    logger.info(f"{total} forecasts migrated to new DB")
    logger.info(f"{skipped} duplicate forecasts were skipped")
    session.flush()
    session.commit()


def checkID(row):
    with open('existing_ids.json', 'r') as j:
        d = json.load(j)
    time_stamps = d.get(row['Office'], None)
    if isinstance(time_stamps, list):
        if row['TimeStamp'] not in time_stamps:
            return True
        else:
            logger.info('huh')
            return False
    else:
        logger.info('huh')
        return False

def addID(row):
    with open('existing_ids.json', 'r') as j:
        d = json.load(j)
    d[row['Office']].append(row['TimeStamp'])
    with open('existing_ids.json', 'w') as j:
        json.dump(d, j, indent=4, sort_keys=True)


def populatePhraseByForecast(forecast, session):
    """Individual INSERT statements via the ORM, calling upon last row id"""
    db = DB(db_path='/home/jmiller/git/AFDTools/forecasts.db')
    time_stamp_ = forecast.time_stamp.strftime('%Y%m%dT%H:%M:%S')
    with db.connection as con:
        cur = con.cursor()
        cur.execute(f'''SELECT * FROM Phrase 
                        WHERE Office == '{forecast.office.name}'
                        AND TimeStamp == '{time_stamp_}';''')
        rows = cur.fetchall()
    try:

        if not checkID(rows[0]):
            logger.debug(f"Skipping {rows[0]['TimeStamp']} from {rows[0]['Office']}")
            return
        else:
            logger.debug(f"Adding {rows[0]['TimeStamp']} from {rows[0]['Office']}")
            addID(rows[0])
    except IndexError:
        logger.warning(f'Time Stamp <{time_stamp_}> not found in SQLite3 DB...')
        return
    #session = Session(bind=engine)
    extractions = list()
    total = 0
    for row in rows:
        extractions.append(
            Extraction(
                forecast=forecast,
                phrase=str(row['Phrase']),
                start_index=int(row['StartIndex']),
                end_index=int(row['EndIndex'])
            )
        )

    session.add_all(extractions)
    session.flush()
    session.commit()


def getOffice(name):
    session = Session(bind=engine)
    return session.query(Office).filter(Office.name == name).one()

if __name__ == '__main__':
    config = {
        "handlers": [
            {
                "sink": sys.stdout,
                "format": "<g>{time:YYYY-MM-DD HH:mm:ss.SSS}</> | <lvl>{level: <8}</> | <c>{name}</>:<c>{function}</>:<c>{line}</> | <lvl>{message}</>",
                "colorize": "True",
                "backtrace": "True",
                "catch": "True",
                "level": "INFO"
            },
            {
                #"sink": f"{__file__.split('.')[0]}.log",
                "sink": f"test.log",
                "format": "<g>{time:YYYY-MM-DD HH:mm:ss.SSS}</> | <lvl>{level: <8}</> | <cyan>{name: <10}</><cyan>{function: <10}</><cyan>{line: <5}</> | <lvl>{message}</>",
                "rotation": "100 MB",
                "retention": 3
            },
        ],
        "extra": {"user": "jmiller"}
    }
    logger.configure(**config)

    global engine
    engine = create_engine(DBURL, echo=False)
    session = Session(bind=engine)
    #setup_database(echo=False)
    #test_flush_no_pk()
    #for office in OFFICES:
    #    populateForecastByOffice(office)
    #for office in OFFICES:
        #logger.info(f'Inserting phrases for: {office}')
        #office_ = session.query(Office).filter(Office.name == office).one()
        #for ind, forecast in enumerate(office_.forecasts): 
        #    populatePhraseByForecast(forecast, session)
        #if ind == 2:
        #    break

    #stats = session.query(Status).filter(Status.extracted == 0).all()
    #for status, extraction in session.query(Status, Extraction).\
    #                                  filter(Status.forecast == Extraction.forecast).\
    #                                  filter(Status.):
    #    extractions = session.query(Extraction).filter(Extraction.forecast_id == status.forecast.id).all()
    #    if len(extractions) > 0:
    #        status.extracted = 1
    #        session.commit()
    #    else:
    #        logger.warning(f"Nothing extracted from {status.forecast.id}")

    #session = Session(bind=engine)
    #for row in session.query(Forecast).filter(Forecast.id == 1).all():
    #    row.status.unique=1
    #session.flush()
    #session.commit()


    #for row in rows:
    #    f = Forecast(
    #        time_stamp=row['TimeStamp'],
    #        date_accessed=datetime.now(),
    #        raw_text=row['Forecast']
    #        )

    #    s = Status(
    #        forecast=f
    #    )

    #    session = Session(bind=engine)
    #    o = session.query(Office).filter(Office.name == 'BOX').one()
    #    o.forecasts.append(f)
    #    session.add(f)
    #    session.add(s)
    #    session.flush()
    #    session.commit()
    #    session.close()
