from forecast import Downloader
from datetime import datetime
import unittest


class TestForecast(unittest.TestCase):

    def setUp(self):
        forecast_path = 'test_data/kbox.txt'
        self.office_name = 'BOX'
        with open(forecast_path, 'r') as t:
            self.text = t.read()
        self.d = Downloader(self.office_name)

    def test_downloader(self):
        self.assertIsInstance(self.d, Downloader)

    def test_to_forecast(self):
        from database_definitions import Forecast
        forecast = self.d.to_forecast(self.office_name, self.text)
        self.assertIsInstance(forecast, Forecast)
    
    def test_to_office(self):
        from database_definitions import Office
        office = self.d.to_office(self.office_name)
        self.assertIsInstance(office, Office)

    def test_get_forecast_time(self):
        forecast_time = self.d.get_forecast_time(self.text)
        print(forecast_time)
        self.assertEqual(forecast_time, datetime(2019, 12, 5, 16, 43))


if __name__ == "__main__":
    unittest.main()
