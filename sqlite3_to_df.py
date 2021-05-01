#todo test this

import os
import sqlite3
import pandas as pd

from Database import get_daily_filelist

# generator usable for producing one day at a time
def daily_buses_df_generator():
    datadir = os.getcwd()+'/data/'
    dailies = get_daily_filelist(datadir)

    for daily_filename in dailies:
        sqlite_filename = datadir + daily_filename[-3]
        # Create your connection.
        cnx = sqlite3.connect(sqlite_filename)
        yield pd.read_sql_query("SELECT * FROM buses", cnx)


if __name__ == "__main__":
    for day in daily_buses_df_generator():
        pass
