from sqlalchemy import func, ForeignKey
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session, deferred

Base = declarative_base()

class Forecast(Base):
    __tablename__ = "forecast"

    id = Column(String(25), primary_key=True)

    office_id = Column(Integer, ForeignKey("office.id"))
    office = relationship("Office", back_populates="forecasts")
    status_id = Column(Integer, ForeignKey("status.id"))
    status = relationship(
        "Status",
        back_populates="forecast",
        single_parent=True,
        cascade="all, delete, delete-orphan",
    )
    # extraction_id = Column(Integer, ForeignKey('extraction.id'))
    extractions = relationship(
        "Extraction", back_populates="forecast", cascade="all, delete, delete-orphan"
    )

    time_stamp = Column(DateTime)
    date_accessed = Column(DateTime, default=func.now())
    raw_text = deferred(Column(Text(16000000)))
    processed_text = deferred(Column(Text(16000000)))


class Office(Base):
    __tablename__ = "office"

    id = Column(Integer, primary_key=True)

    forecasts = relationship(
        "Forecast", back_populates="office", cascade="all, delete, delete-orphan"
    )

    name = Column(String(4))
    city = Column(String(255))
    state = Column(String(255))
    address = Column(String(255))


class Status(Base):
    __tablename__ = "status"

    id = Column(Integer, primary_key=True)

    forecast = relationship("Forecast", uselist=False, back_populates="status")

    unique = Column(Boolean, default=False)
    clean = Column(Boolean, default=False)
    extracted = Column(Boolean, default=False)
    in_dataset = Column(Boolean, default=False)


class Extraction(Base):
    __tablename__ = "extraction"

    id = Column(Integer, primary_key=True)

    forecast_id = Column(String(25), ForeignKey("forecast.id"))
    forecast = relationship("Forecast", uselist=False, back_populates="extractions")

    phrase = Column(String(50))
    start_index = Column(Integer)
    end_index = Column(Integer)
