import pyodbc
import numpy as np
import pandas as pd
from tqdm import tqdm
import psycopg2 as db
import os
print(os.getcwd())

pd.set_option("max_rows", None)
pd.set_option("max_columns", None)

# %%

start_time = time.time()
SERVER = "redshift.bi.obviux.dk"
PORT = '5439'  # Redshift default
USER = "mrs"
PASSWORD = "j89Foijf8fIJFAD8dsIFJA8DFMasf_D7fa9df"
DATABASE = "redshift"

cnxn = db.connect(host=SERVER, database=DATABASE, user=USER, password=PASSWORD, port=PORT)

# %%

# Hent data baseret på hvilket teknikområde der er.
df = pd.read_sql(
    """select top 100000 * from redshift.energylabels.proposals where seebclassification = '2-1-5-0'""",
    cnxn,
)
print(df.head(1000))

# %%
#Hent data for det specifikke energimærke med information om det her.
df = pd.read_sql(
    """select * from energylabels.building_data where energylabel_id IN (311473251) """,
    cnxn,
)

# %%
df.sort_values(['energylabel_id']).head(1000)

df_build.sort_values('energylabel_id').head(1000)
