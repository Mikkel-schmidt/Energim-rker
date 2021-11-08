import sqlalchemy
#import psycopg2 as db
from sqlalchemy import create_engine
import pandas as pd


SERVER = "redshift.bi.obviux.dk"
PORT = '5439'  # Redshift default
USER = "mrs"
PASSWORD = "j89Foijf8fIJFAD8dsIFJA8DFMasf_D7fa9df"
DATABASE = "redshift"

    # cnxn = pyodbc.connect(
    #     "DRIVER={Amazon Redshift (x64)};SERVER="
    #     + SERVER
    #     + ";DATABASE="
    #     + DATABASE
    #     + ";UID="
    #     + USER
    #     + ";PWD="
    #     + PASSWORD
    # )

# engine = create_engine('redshift:///?User='
#                        + USER
#                        + '&;Password='
#                        + PASSWORD
#                        + '&Database='
#                        + DATABASE
#                        + '&Server='
#                        + SERVER
#                        + '&Port='
#                        + PORT)

engine = db.connect(host=SERVER, database=DATABASE, user=USER, password=PASSWORD, port=PORT)

sql = "SELECT COUNT(shorttext) FROM redshift.energylabels.proposals;"
n = 100
hej = pd.read_sql(sql, con=engine)
hej.head()
# cursor = cnxn.cursor()
# print(cursor.execute(sql).fetchall())
print("Connected to Redshift")
