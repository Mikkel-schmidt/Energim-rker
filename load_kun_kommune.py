import pyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import streamlit as st
from tqdm import tqdm
import time

#BBR_list = list(np.loadtxt('energimærker', delimiter=""))
#BBR = np.array(BBR_list)
#BBR_tuple = BBR[:,0]
#print(BBR_tuple)

SERVER = "redshift.bi.obviux.dk"
PORT = 5439  # Redshift default
USER = "mrs"
PASSWORD = "j89Foijf8fIJFAD8dsIFJA8DFMasf_D7fa9df"
DATABASE = "redshift"

cnxn = pyodbc.connect(
    "DRIVER={Amazon Redshift (x64)};SERVER="
    + SERVER
    + ";DATABASE="
    + DATABASE
    + ";UID="
    + USER
    + ";PWD="
    + PASSWORD
)
sql = "SELECT COUNT(shorttext) FROM redshift.energylabels.proposals"
cursor = cnxn.cursor()
print(cursor.execute(sql).fetchall())

# %% ###################################################################################################

start_time = time.time()

df_build = pd.read_sql(
    """select * from energylabels.building_data where ownership != 'Private' """,
    cnxn,
)
print("--- Build %s seconds ---" % (time.time() - start_time))
print(df_build)
df_creation = pd.read_sql("""select top 10 * from energylabels.creation_data""", cnxn)
print("--- Creation %s seconds ---" % (time.time() - start_time))

eID_list = df_build['energylabel_id'].unique()

print(eID_list)

# %% ####################################################################################################


# df_build.columns = df_build.columns.str.replace(" ", "_")
# list = df_build["energylabel_id"].tolist()
# list = ",".join(map(str, list))
# print(list)
query = """
SELECT  *
FROM energylabels.proposals
WHERE energylabel_id
IN {};
""".format(tuple(eID_list))

#'energylabel id', 'proposal id', 'shorttext', 'lifetime', 'investment', 'data_input_type', 'data_value'
n = 10000
# df_prop = pd.read_sql(query, cnxn,)
dfs = []
for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
    dfs.append(chunk)
    # print("--- Proposals %s seconds ---" % (time.time() - start_time))
df_prop = pd.concat(dfs)
print("--- Proposals %s seconds ---" % (time.time() - start_time))
data_time = time.time() - start_time
print("Connected to Redshift")
print(df_prop.columns)
print(df_prop.head())
print(df_prop.shape)
