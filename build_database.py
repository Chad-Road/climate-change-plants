from noaa_sdk import noaa
import sqlite3
import datetime

class WeatherRequest:
    """
    A class to create an sqlite weather database from NOAA data within the given times
    """
    def __init__(self, zip_code, country, start_date, end_date, table_name):
        """
        Includes the basic information needed for NOAA api and table name

        Arguments
        ---------
        zip_code : str
            five digit zip code

        country : str
            two digit country code

        start_date : str (date-time format: yyyy-mm-dd)
            the starting time to fetch weather data

        end_date : str (date-time format: yyyy-mm-dd)
            the ending time for fetching weather data

        table_name : str
            the table name that will be created in sqlite


        Note
        ----
        Time codes have been added to the start date and end date to 
        fetch the beginning and end of day respectively

        """
        self.zip_code = zip_code
        self.country = country
        self.start_date = start_date + "T00:00:00Z"
        self.end_date = end_date + "T23:59:59Z"
        self.table_name = table_name

    def setup_database(self):
        """
        
        """
        database_name = "WEATHER.db"
        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()

        # Drop table from database if it already exists
        drop_table = f"DROP TABLE IF EXISTS {self.table_name}"
        cursor.execute(drop_table)

        create_table = 







if __name__ == "__main__":
    pass