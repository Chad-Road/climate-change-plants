from noaa_sdk import noaa
import sqlite3
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

class CurrentWeather:
    """ A class to create an sqlite weather database from NOAA data for the most recent month
        and then display the stored data."""

    def __init__(self, zip_code, country, start_date, table_name, output_file_name):
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
            should choose location or name of city to differentiate in database

        output_file_name : str
            the name to write the output file to in the write_csv function


        Note
        ----
        Time codes have been added to the start date and end date to 
        fetch the beginning and end of day respectively
        """

        self.zip_code = zip_code
        self.country = country
        self.start_date = start_date
        self.end_date = datetime.datetime.now().strftime("%Y-%m-%d")
        self.table_name = table_name
        self.output_file_name = output_file_name


    def setup_database(self):
        """ Creates a new sqlite database """

        database_name = "WEATHER.db"
        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()

        # Drop table if exists already and create new table
        drop_table = f"DROP TABLE IF EXISTS {self.table_name}"
        create_table = f""" CREATE TABLE IF NOT EXISTS {self.table_name} (
                                time TEXT NOT NULL PRIMARY KEY,
                                temperature REAL,
                                min24temp REAL,
                                max24temp REAL,
                                relativeHumidity REAL,
                                last6HoursPrecipitation REAL
                            );
                            """
        
        # Use sql cursor to execute table drop and creation
        cursor.execute(drop_table)
        cursor.execute(create_table)
        print(f"New table named {self.table_name} created in weather database")


    def insert_weather_data(self):
        """ Connect to NOAA and insert data into previously created table """

        database_name = "WEATHER.db"
        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()

        count = 0
        n = noaa.NOAA()
        observations = n.get_observations(self.zip_code, self.country, start=self.start_date, end=self.end_date)

        insert_command = f"""
            INSERT INTO {self.table_name} (
                time, temperature, min24temp, max24temp, relativeHumidity, last6HoursPrecipitation
            )
            VALUES (
                ?, ?, ?, ?, ?, ?
            )
        """

        for obs in observations:
            insert_values = (
                obs["timestamp"],
                obs["temperature"]["value"],
                obs["minTemperatureLast24Hours"]["value"],
                obs["maxTemperatureLast24Hours"]["value"],
                obs["relativeHumidity"]["value"],
                obs["precipitationLast6Hours"]["value"],
            )
            cursor.execute(insert_command, insert_values)
            count += 1
        if count > 0:
            cursor.execute("COMMIT;")

    def write_csv(self):
        """ Writes the specified table to a CSV file """

        database_name = "WEATHER.db"
        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()


        select_data = f"""SELECT time, temperature, min24temp, max24temp, relativeHumidity, last6HoursPrecipitation
                            FROM {self.table_name}
                            ORDER BY time;"""

        cursor.execute(select_data)
        all_rows = cursor.fetchall()
        row_count = len(all_rows) // 2
        rows = all_rows[:row_count]

        with open(self.output_file_name, "w+") as out_file:
            out_file.write("Time,Temperature,Min24Temp,Max24Temp,RelativeHumidity,Last6HoursPrecipitation")
            out_file.write("\n")
            for row in rows:
                time = row[0]
                temp = row[1]
                mintemp = row[2]
                maxtemp = row[3]
                relhum = row[4]
                last6 = row[5]
                out_file.write(f"{time},{temp},{mintemp},{maxtemp},{relhum},{last6}")
                out_file.write("\n")
            print("finished writing to file")

    def display_variance(self):
        df = pd.read_csv(self.output_file_name)

        df["Time"] = pd.to_datetime(df["Time"], infer_datetime_format=True)
        #print(df["Time"])
        x_tick_labels = df["Time"].apply(lambda x: x.strftime("%Y-%m-%d:%H:%M"))

        df["Temperature"].replace(to_replace=["None"], value=np.nan, inplace=True)
        df["Temperature"] = df["Temperature"].astype(float)
        df["Temperature"] = df["Temperature"].interpolate()
        
        df["RelativeHumidity"].replace(to_replace=["None"], value=np.nan, inplace=True)
        df["RelativeHumidity"] = df["RelativeHumidity"].astype(float)
        df["RelativeHumidity"] = df["RelativeHumidity"].interpolate()

        

        # #print(df.columns)

        ax = sns.lineplot(
            data=df,
            x="Time",
            y="Temperature",
            color="r"
        )
        
        ax2 = ax.twinx()
        sns.lineplot(
            data=df,
            x="Time",
            y="RelativeHumidity",
            color="b",
            ax=ax2
        )

        ax.set_xticklabels(df["Time"], rotation=90, ha="right", rotation_mode="anchor")
        ax.figure.legend()
        plt.tight_layout()
        plt.show()



if __name__ == "__main__":
    # An example of the CurrentWeather class with New York as the location
    new_york_weather = CurrentWeather("10001", "US", "2022-05-26", "new_york", "new_york_weather.csv")
    new_york_weather.setup_database()
    new_york_weather.insert_weather_data()
    new_york_weather.write_csv()
    new_york_weather.display_variance()