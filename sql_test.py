import pyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import streamlit as st
from tqdm import tqdm
import time

pd.set_option("max_rows", None)
pd.set_option("max_columns", None)

#BR_list = list(np.loadtxt('energimærker', delimiter=""))
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

df_prop = pd.read_sql(
    """select  * from energylabels.proposals where energylabel_id IN (311147258) """,
    cnxn,
)

df_prop_group = pd.read_sql(
    """select  * from energylabels.proposal_groups where energylabel_id IN (311147258) """,
    cnxn,
)
df_prop_group = pd.read_sql(
    """select  * from energylabels.proposal_groups where energylabel_id IN (311147258) """,
    cnxn,
)
# %%
query = """select  * from energylabels.proposal_group_references
    where energylabel_id IN (311147258) """

n = 10000
dfs = []
for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
    dfs.append(chunk)
df = pd.concat(dfs)
# %%
df_prop.sort_values('proposal_id')
df_prop_group.sort_values('proposal_group_id')

df.sort_values('proposal_group_id')
df.head(2000)
