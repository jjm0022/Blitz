import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as dateparse
from datetime import datetime

from database import DB
from database_definitions import Office, Forecast, Status


class Connection:
    def __init__(self):
        self.db = DB()
        self.session = self.db.session

    def getMostRecent(self):
        """
        Returns the row-dict for the most recent forcast
        """
        forecasts = (
            self.session.query(Forecast)
            .filter(Office.id == Forecast.office_id)
            .filter(Office.name == self.office.name)
            .order_by(Forecast.time_stamp)
            .all()
        )
        return forecasts[-1]

    def insert(self, forecast):
        self.session.add(forecast)
        self.session.commit()


class Downloader(Connection):
    def __init__(self, office):
        """
        """
        Connection.__init__(self)
        self.office = self.session.query(Office).filter_by(name=office).one()
        self.url = f"https://forecast.weather.gov/product.php?site={self.office.name}&issuedby={self.office.name}&product=AFD&format=txt&version=1&glossary=0"
        self.text = None
        self.current_time_stamp = None
        self.previous = self.getMostRecent()

    def download(self):
        """
        This function will check NWS link for new/updated forecast
        """
        response = requests.get(self.url)
        if not response:
            print(f"{response.status_code} error; Reason: {response.reason}")
            print(f"{response.status_code} error; URL: {self.url}")
            return None
        else:
            if response.status_code != 200:
                print(f"{response.status_code} error; Reason: {response.reason}")
                return None
            elif self.parse(response.text):
                print(
                    f"Downloaded forecast from {self.office.name} valid for {self.current_time_stamp.strftime('%c')}"
                )
                return Forecast(
                    id=self.office.name
                    + "-"
                    + self.current_time_stamp.strftime("%Y-%m-%d %H:%M:%S"),
                    office=self.office,
                    status=Status(unique=0, clean=0, extracted=0, in_dataset=0),
                    time_stamp=self.current_time_stamp,
                    date_accessed=datetime.now(),
                    raw_text=self.text,
                )
            else:
                return None

    def isNew(self):
        """
        Checks to see if the current forecast is newer than the most recent
        """
        recent = self.getMostRecent()
        return self.current_time_stamp > dateparse(self.previous.time_stamp)

    def to_forecast(self, office_id, text, forecast_time=None):
        """
        Returns a database_definitions.Forecast object
        """
        office = self.to_office(office_id, session=self.session)
        if not forecast_time:
            forecast_time = self.get_forecast_time(text)

        return Forecast(
                id=office_id + "-" + forecast_time.strftime("%Y-%m-%d %H:%M:%S"),
                office=office,
                status=Status(unique=0, clean=0, extracted=0, in_dataset=0),
                time_stamp=forecast_time,
                date_accessed=datetime.now(),
                raw_text=text,
                )

    def to_office(self, office_id, session=None):
        """
        Returns a database_definitions.Office object
        """
        if not session:
            db = DB()
            session = self.session
            office = session.query(Office).filter_by(name=office_id).one()
            session.close()
        else:
            print(session)
            office = session.query(Office).filter_by(name=office_id).one()
        return office

    def get_forecast_time(self, forecast_text):
        """
        Searches the forecast text for the forecast issue time
        """
        sections = forecast_text.split("\n\n")
        lines = list()
        for sec in sections:
            for line in sec.split("\n"):
                lines.append(line)

        # Clean up a little
        for ind, line in enumerate(lines):
            if line == "":
                lines.pop(ind)

        # Search for the line with the timestamp
        for line in lines:
            try:
                # Remove the timezone since python doesn't like CST/CDT and others
                # TODO: Find a way to store TZ information
                line = " ".join(line.split(" ")[:2] + line.split(" ")[3:])
                timestamp = datetime.strptime(line, "%I%M %p %a %b %d %Y")
                return timestamp
            except ValueError:
                continue
        return None
