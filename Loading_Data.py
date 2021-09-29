import pyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import streamlit as st
import time

pd.set_option("max_rows", 10)
pd.set_option("max_columns", None)

# %%

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
sql = "SELECT COUNT(shorttext) FROM redshift.energylabels.input_data;"
cursor = cnxn.cursor()
print(cursor.execute(sql).fetchall())

print("Connected to Redshift")

# %%

show_tables = cursor.tables()
for row in show_tables:
    print(row)

# %%
start_time = time.time()
df_creation = pd.read_sql(
    """select top 10000 * from redshift.energylabels.creation_data""", cnxn
)
print("--- Creation %s seconds ---" % (time.time() - start_time))

start_time = time.time()
df_input = pd.read_sql(
    """select top 100000 * from redshift.energylabels.input_data""", cnxn
)
print("--- Input %s seconds ---" % (time.time() - start_time))

start_time = time.time()
df_build = pd.read_sql("""select * from redshift.energylabels.building_data""", cnxn)
print("--- Build %s seconds ---" % (time.time() - start_time))

start_time = time.time()
df_prop = pd.read_sql(
    """select top 100000 * from redshift.energylabels.proposals""", cnxn
)
print("--- Proposals %s seconds ---" % (time.time() - start_time))

start_time = time.time()
df_result = pd.read_sql(
    """select top 100000 * from redshift.energylabels.result_data_energylabels""", cnxn
)
print("--- Results %s seconds ---" % (time.time() - start_time))


# %%

print(df_build.dtypes)


# %%

df_creation.columns = df_creation.columns.str.replace(" ", "_")
df_input.columns = df_input.columns.str.replace(" ", "_")
df_build.columns = df_build.columns.str.replace(" ", "_")
df_prop.columns = df_prop.columns.str.replace(" ", "_")
df_result.columns = df_result.columns.str.replace(" ", "_")

df_build["municipality"] = df_build["municipalitynumber"]
df_build["municipality"].replace(
    {
        "101": "København",
        "147": "Frederiksberg",
        "151": "Ballerup",
        "153": "Brøndby",
        "155": "Dragør",
        "157": "Gentofte",
        "159": "Gladsaxe",
        "161": "Glostrup",
        "163": "Herlev",
        "165": "Albertslund",
        "167": "Hvidovre",
        "169": "Høje Taastrup",
        "173": "Lyngby-Taarbæk",
        "175": "Rødovre",
        "183": "Ishøj",
        "185": "Tårnby",
        "187": "Vallensbæk",
        "190": "Furesø",
        "201": "Allerød",
        "210": "Fredensborg",
        "217": "Helsingør",
        "219": "Hillerød",
        "223": "Hørsholm",
        "230": "Rudersdal",
        "240": "Egedal",
        "250": "Frederikssund",
        "253": "Greve",
        "259": "Køge",
        "260": "Halsnæs",
        "265": "Roskilde",
        "269": "Solrød",
        "270": "Gribskov",
        "306": "Odsherred",
        "316": "Holbæk",
        "320": "Faxe",
        "326": "Kalundborg",
        "329": "Ringsted",
        "330": "Slagelse",
        "336": "Stevns",
        "340": "Sorø",
        "350": "Lejre",
        "360": "Lolland",
        "370": "Næstved",
        "376": "Guldborgsund",
        "390": "Vordingborg",
        "400": "Bornholm",
        "410": "Middelfart",
        "420": "Assens",
        "430": "Faaborg-Midtfyn",
        "440": "Kerteminde",
        "450": "Nyborg",
        "461": "Odense",
        "479": "Svendborg",
        "480": "Nordfyns",
        "482": "Langeland",
        "492": "Ærø",
        "510": "Haderslev",
        "530": "Billund",
        "540": "Sønderborg",
        "550": "Tønder",
        "561": "Esbjerg",
        "563": "Fanø",
        "573": "Varde",
        "575": "Vejen",
        "580": "Aabenraa",
        "607": "Fredericia",
        "615": "Horsens",
        "621": "Kolding",
        "630": "Vejle",
        "657": "Herning",
        "661": "Holstebro",
        "665": "Lemvig",
        "671": "Struer",
        "706": "Syddjurs",
        "707": "Norddjurs",
        "710": "Favrskov",
        "727": "Odder",
        "730": "Randers",
        "740": "Silkeborg",
        "741": "Samsø",
        "746": "Skanderborg",
        "751": "Aarhus",
        "756": "Ikast-Brande",
        "760": "Ringkøbing",
        "766": "Hedensted",
        "773": "Morsø",
        "779": "Skive",
        "787": "Thisted",
        "791": "Viborg",
        "810": "Brønderslev",
        "813": "Frederikshavn",
        "820": "Vesthimmerlands",
        "825": "Læsø",
    },
    inplace=True,
)

df_creation1 = df_creation[
    [
        "energylabel_id",
        "validfrom",
        "validto",
        "usage",
        "isnewbuild",
        "includesvat",
        "ismixedusage",
        "company_name",
    ]
]
df_input1 = df_input[
    [
        "energylabel_id",
        "building_id",
        "zone_id",
        "status_id",
        "shorttext",
        "seebclassification",
        "data_input_type",
        "data_category",
        "data_value",
    ]
]
df_build1 = df_build[
    [
        "energylabel_id",
        "reviewdate",
        "municipalitynumber",
        "municipality",
        "building_id",
        "buildingnumber",
        "propertynumber",
        "postalcode",
        "postalcity",
        "ownership",
        "usecode",
        "yearofconstruction",
        "yearofrenovation",
    ]
]
df_prop1 = df_prop
df_result1 = df_result[
    [
        "energylabel_id",
        "resultforallprofitable_energylabelclassification",
        "energylabelclassification",
    ]
]

# %%
df_creation.columns
df_input.columns
df_build.columns
df_prop.columns
df_result.columns

# %%

result_prop = pd.merge(df_prop, df_build, on="energylabel_id")

buildings = result_prop[pd.to_numeric(result_prop["municipalitynumber"]) == 101]
buildings.head()

# %%
buildings1 = buildings[buildings["ownership"] == "Municipality"]
buildings1.head(1000)

# %%
st.title("Completely new app")
municipalities = st.multiselect("Multiselect", df_build["municipality"])
