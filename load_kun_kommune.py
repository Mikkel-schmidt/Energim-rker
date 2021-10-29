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
    """select  * from energylabels.building_data where energylabel_id in (310001535)""",
    cnxn,
)
print("--- Build %s seconds ---" % (time.time() - start_time))
print(df_build)
df_creation = pd.read_sql("""select top 10 * from energylabels.creation_data""", cnxn)
print("--- Creation %s seconds ---" % (time.time() - start_time))

eID_list = df_build['energylabel_id'].unique()
print(df_build.energylabel_id.nunique())


# %% ####################################################################################################
start_time = time.time()

# df_build.columns = df_build.columns.str.replace(" ", "_")
# list = df_build["energylabel_id"].tolist()
# list = ",".join(map(str, list))     results_profitability
# print(list)
query = """
SELECT results_fuelsavings.energylabel_id,
results_profitability.profitability,
proposal_groups.proposal_group_id,
building_data.energylabel_id, building_data.propertynumber, building_data.ownership, building_data.reviewdate, building_data.municipalitynumber, building_data.streetname, building_data.housenumber,
building_data.postalcode, building_data.postalcity, building_data.usecode, building_data.dwellingarea, building_data.commercialarea
FROM (((energylabels.building_data
INNER JOIN energylabels.results_fuelsavings ON building_data.energylabel_id = results_fuelsavings.energylabel_id AND ownership='Municipality')
INNER JOIN energylabels.proposal_groups ON building_data.energylabel_id = proposal_groups.energylabel_id AND ownership='Municipality')
INNER JOIN energylabels.results_profitability ON building_data.energylabel_id = results_profitability.energylabel_id AND ownership='Municipality')
"""
#result_data_energylabels.energylabel_id, result_data_energylabels.energylabelclassification, result_data_energylabels.status_energylabelclassification, result_data_energylabels.resultforallprofitable_energylabelclassification, result_data_energylabels.resultforallproposals_energylabelclassification
# proposal_groups.proposal_group_id , proposal_groups.shorttext, proposal_groups.investment, proposal_groups.seebclassification,
#INNER JOIN energylabels.result_data_energylabels ON building_data.energylabel_id = result_data_energylabels.energylabel_id )

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
print(df_prop.energylabel_id.nunique())

# %%
df_build = pd.read_sql(
    """select top 100 * from energylabels.building_data  """,
    cnxn,
)
#print(df_build.head())

df_prop = pd.read_sql(
    """select  * from energylabels.result_data_energylabels where energylabel_id IN (311001601) """,
    cnxn,
)
print(df_build.columns)


# %%

df_prop = pd.read_sql(
    """select  * from energylabels.proposals where energylabel_id IN (311418890) """,
    cnxn,
)

proposal_group_references = pd.read_sql(
    """select top 100 * from energylabels.proposal_group_references""",
    cnxn,
)

proposal_groups = pd.read_sql(
    """select top 100 * from energylabels.proposal_groups ORDER BY energylabel_id""",
    cnxn,
)

results_fuelsavings = pd.read_sql(
    """select  * from energylabels.results_fuelsavings where energylabel_id IN (310001535)  """,
    cnxn,
)

results_profitability = pd.read_sql(
    """select * from energylabels.results_profitability where energylabel_id IN (311418890)""",
    cnxn,
)

# %%

proposal_groups.nunique()
df_prop
proposal_group_references
proposal_groups
results_fuelsavings
results_profitability
# %%

start_time = time.time()
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

query = """
        WITH build AS
        (
        SELECT energylabel_id, build.propertynumber, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
        build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
        FROM energylabels.building_data AS build
        WHERE municipalitynumber = 101 AND ownership != 'Private'
        )
        SELECT build.energylabel_id,
        fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
        fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
        prof.proposal_group_id as proposal_group_id_prof, prof.profitability, prof.investment, prof.investmentlifetime,
        build.propertynumber, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
        build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea,
        result.status_energylabelclassification, result.resultforallprofitable_energylabelclassification,
        result.resultforallproposals_energylabelclassification, result.energylabelclassification

        FROM energylabels.results_fuelsavings AS fuel
        RIGHT JOIN build ON build.energylabel_id = fuel.energylabel_id
        LEFT JOIN energylabels.results_profitability AS prof ON build.energylabel_id = prof.energylabel_id
        LEFT JOIN energylabels.result_data_energylabels AS result ON build.energylabel_id = result.energylabel_id
        ORDER BY build.energylabel_id, fuel.proposal_group_id
        """
        #prop.seebclassification
        #INNER JOIN energylabels.proposal_group_references AS ref ON build.energylabel_id = ref.energylabel_id AND fuel.proposal_group_id = ref.proposal_group_id
        #INNER JOIN energylabels.proposals AS prop ON build.energylabel_id = prop.energylabel_id AND prop.proposal_id = ref.proposal_id

n = 10000
dfs = []
for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
    dfs.append(chunk)
df = pd.concat(dfs)
print("--- df %s seconds ---" % (time.time() - start_time))

# %%

df.shape
df.nunique()
print(df.iloc[:5,10:20])
df.head()
missing_values_count = df.isnull().sum()
print(missing_values_count)

total_cells = np.product(df.shape)
total_missing = missing_values_count.sum()

# percent of data that is missing
percent_missing = (total_missing/total_cells) * 100
print(percent_missing)

print(df.investmentlifetime.isnull().sum()/df.investmentlifetime.shape)
# %%

energy = pd.read_sql(
    """select DISTINCT energylabel_id from energylabels.building_data WHERE municipalitynumber = 101 AND ownership = 'Municipality'""",
    cnxn,
)

energy.shape
