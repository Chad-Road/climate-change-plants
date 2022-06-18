from noaa_sdk import noaa
import sqlite3
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

class CurrentWeather:
    """ A class to create an sqlite weather database from NOAA data for the most recent two weeks
        and then display that areas temperature and relative humidity on the same graph"""

    def __init__(self, zip_code, country, table_name, output_file_name):
        """
        Includes the basic information needed for NOAA api and table name

        Arguments
        ---------
        zip_code : str
            five digit zip code

        country : str
            two digit country code

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
        self.table_name = table_name
        self.output_file_name = output_file_name
     
        # Sets the date range to be retrieved from NOAA database
        # Uses the most recent two weeks and sets data format for NOAA standards
        self.end_date = datetime.datetime.now()
        self.start_date = self.end_date - datetime.timedelta(days=7)
        self.end_date = self.end_date.strftime("%Y-%m-%d")
        self.start_date = self.start_date.strftime("%Y-%m-%d")

    def setup_database(self):
        """ Creates a new sqlite database """

        # Creates a sqlite databe called WEATHER and connects to that database
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

        # Connects to WEATHER database and initializes cursor
        database_name = "WEATHER.db"
        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()

        # Connects to NOAA and gets observations for set parameters
        n = noaa.NOAA()
        observations = n.get_observations(self.zip_code, self.country, start=self.start_date, end=self.end_date)

        # Creates format for later inserted values
        insert_command = f"""
            INSERT INTO {self.table_name} (
                time, temperature, min24temp, max24temp, relativeHumidity, last6HoursPrecipitation
            )
            VALUES (
                ?, ?, ?, ?, ?, ?
            )
        """

        # Loops through values in observations and commits values to table
        count = 0
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

        # Connects to WEATHER database
        database_name = "WEATHER.db"
        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()

        # Queries data from database
        select_data = f"""SELECT time, temperature, min24temp, max24temp, relativeHumidity, last6HoursPrecipitation
                            FROM {self.table_name}
                            ORDER BY time;"""

        # fetchall can be used instead to access all items at once
        rows = cursor.execute(select_data)
        ### rows = cursor.fetchall()

        # Opens file and writes to CSV with context manager
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
        """ Used to display CSV file to a plot """

        # Read retrieved database file to dataframe
        df = pd.read_csv(self.output_file_name)

        # Get timestamps and transform them into a more readable format
        x_time = pd.to_datetime(df["Time"], infer_datetime_format=True)
        x_tick_labels = x_time.apply(lambda x: x.strftime("%b %d,  %I%p"))
        
        # Removes two out of three labels to make x ticks easier to read
        x_tick_labels.iloc[1::3] = ""
        x_tick_labels.iloc[2::3] = ""

        # Replace "None" values with nan, change values to float, interpolate missing values
        df["Temperature"].replace(to_replace=["None"], value=np.nan, inplace=True)
        df["Temperature"] = df["Temperature"].astype(float)
        df["Temperature"] = df["Temperature"].interpolate()
        df["RelativeHumidity"].replace(to_replace=["None"], value=np.nan, inplace=True)
        df["RelativeHumidity"] = df["RelativeHumidity"].astype(float)
        df["RelativeHumidity"] = df["RelativeHumidity"].interpolate()

        # Displays lineplot with modified dark styling and zoom level for notebook context
        sns.set_context("notebook")
        sns.set(rc={'figure.figsize': (10,5)})
        sns.set_style("dark")
        ax = sns.lineplot(
            data=df,
            x=x_tick_labels,
            y="Temperature",
            color="r", 
            label="Temperature",
            legend=None
        )
        
        # Copies the axis to allow for second plot on same graph
        ax2 = ax.twinx()
        sns.lineplot(
            data=df,
            x=x_tick_labels,
            y="RelativeHumidity",
            color="b",
            label="Relative Humidity",
            legend=None,
            ax=ax2
        )

        # Sets labels and titles and displays combined plot
        ax.set_xticklabels(x_tick_labels, rotation=90)
        ax.figure.legend()
        ax.set_title("Temperature & Relative Humidity", fontdict= {'fontsize': 20, 'fontweight': 'bold'})
        ax.set_ylabel("Temperature", fontdict={'fontweight': 'bold'})
        ax2.set_ylabel("Relative Humidity", fontdict={'fontweight': 'bold'})
        ax.set_xlabel("Date & Time", fontdict={'fontweight': 'bold'})
        plt.tight_layout()
        plt.show()

if __name__ == "__main__":
    # An example of the CurrentWeather class with New York as the location
    new_york_weather = CurrentWeather("10001", "US", "new_york", "new_york_weather.csv")
    new_york_weather.setup_database()
    new_york_weather.insert_weather_data()
    new_york_weather.write_csv()
    new_york_weather.display_variance()