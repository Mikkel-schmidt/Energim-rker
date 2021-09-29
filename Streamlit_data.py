import pyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import streamlit as st
from tqdm import tqdm
import time

st.set_page_config(layout="wide", page_title="SHARK APP", page_icon=":shark:")
sns.set_theme(
    context="notebook",
    style="darkgrid",
    palette="deep",
    font="sans-serif",
    font_scale=1,
    color_codes=True,
    rc=None,
)
# pd.options.display.float_format = '{:,.2f}'.format
start_time = time.time()


# %% Data loading ###############################################################################################################################
@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def connect():
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
    sql = "SELECT COUNT(shorttext) FROM redshift.energylabels.proposals;"
    cursor = cnxn.cursor()
    print(cursor.execute(sql).fetchall())

    start_time = time.time()
    df_creation = pd.read_sql(
        """select  * from redshift.energylabels.creation_data""", cnxn
    )
    print("--- Creation %s seconds ---" % (time.time() - start_time))
    df_build = pd.read_sql(
        """select * from redshift.energylabels.building_data""", cnxn
    )
    print("--- Build %s seconds ---" % (time.time() - start_time))
    query = """
    SELECT TOP 1000000 *
    FROM redshift.energylabels.proposals
    """  #'energylabel id', 'proposal id', 'shorttext', 'lifetime', 'investment', 'data_input_time', 'data_value'
    n = 100000
    # df_prop = pd.read_sql(query, cnxn,)
    dfs = []
    for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        dfs.append(chunk)
        # print("--- Proposals %s seconds ---" % (time.time() - start_time))
    df_prop = pd.concat(dfs)
    print("--- Proposals %s seconds ---" % (time.time() - start_time))
    data_time = time.time() - start_time
    print("Connected to Redshift")

    return df_creation, df_build, df_prop, data_time


df_creation, df_build, df_prop, data_time = connect()

# %% Data cleaning ################################################################################################################################


@st.cache
def data_cleaning():
    df_creation.columns = df_creation.columns.str.replace(" ", "_")
    # df_input.columns    = df_input.columns.str.replace(' ', '_')
    df_build.columns = df_build.columns.str.replace(" ", "_")
    df_prop.columns = df_prop.columns.str.replace(" ", "_")
    # df_result.columns   = df_result.columns.str.replace(' ', '_')
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
    df_build["address"] = (
        df_build[["postalcode", "postalcity", "streetname", "housenumber"]]
        .astype(str)
        .agg(" ".join, axis=1)
    )
    return df_build


df_build = data_cleaning()

# %% Data Filtering ################################################################################################################################

municipalities = st.sidebar.multiselect(
    "Vælg dine yndlingskommuner",
    options=list(np.unique(df_build["municipality"])),
    default=["København", "Ballerup"],
)

bygningstyper = st.sidebar.multiselect(
    "Hvilken type bygninger skal medtages",
    options=list(np.unique(df_build["ownership"])),
    default=["Private"],
)


st.title("The brand new shark (:whale:) application")

col1_1, col2_1 = st.columns(2)
col1_1.header("Plotting")
col1_1.subheader(
    "Det tog %.6s sekunder at hente data " % data_time,
)


fig, ax = plt.subplots(figsize=(9, 4))
hist_values = df_build[df_build["municipality"].isin(municipalities)]
ax = sns.countplot(x="ownership", hue="municipality", data=hist_values)
ax.tick_params(labelrotation=90)
ax.set_title("Fordeling af energimærker på bygningstype")
col1_1.pyplot(fig)


@st.cache
def bygningstype(bygningstyper):
    energy = df_prop[df_prop["energylabel_id"].isin(hist_values["energylabel_id"])]
    energy = energy.merge(hist_values, on="energylabel_id")
    energy["investment"] = pd.to_numeric(energy["investment"])
    energy = energy[energy["ownership"].isin(bygningstyper)]
    return energy


energy = bygningstype(bygningstyper)


@st.cache
def proposal_individual(energy):
    proposals = energy
    for i in tqdm(proposals["energylabel_id"].unique().astype("int64")):
        temp = proposals[proposals["energylabel_id"] == i]
        temp = temp.drop_duplicates(subset="proposal_id")
        proposals = proposals[proposals["energylabel_id"] != i]
        proposals = proposals.append(temp)
    return proposals


proposals = proposal_individual(energy)


col2_1.header("Raw data")
col2_1.write("Proposals")
col2_1.write(proposals[0:10000])

col1_2, col2_2 = st.columns(2)

fig_hist, ax_hist = plt.subplots(figsize=(9, 4))
ax_hist = sns.histplot(
    x="investment",
    hue="municipality",
    data=proposals,
    bins=30,
    multiple="stack",
    binrange=(0, 300000),
)
ax_hist.tick_params(labelrotation=0)
ax_hist.set_title("Investering")
ax_hist.set_xlim(0, 300000)
col1_2.pyplot(fig_hist)

fig_his, ax_his = plt.subplots(figsize=(9, 4))
ax_his = sns.histplot(
    x="investment",
    hue="municipality",
    data=proposals,
    bins=25,
    multiple="stack",
    binrange=(0, 50000),
)
ax_his.tick_params(labelrotation=0)
ax_his.set_title("Investering < 50000 DKK")
ax_his.set_xlim(0, 50000)
col1_2.pyplot(fig_his)


fig_hist, ax_hist = plt.subplots(figsize=(9, 4))
ax_hist = sns.countplot(x="lifetime", hue="municipality", data=proposals)
ax_hist.tick_params(labelrotation=0)
ax_hist.set_title("Levetid")
col2_2.pyplot(fig_hist)


fig, ax = plt.subplots(figsize=(9, 4))
# hist_values = df_build[df_build["municipality"].isin(municipalities)]
ax = sns.countplot(x="data_input_type", hue="municipality", data=proposals)
ax.tick_params(labelrotation=90)
col1_2.pyplot(fig)


option = col2_2.selectbox(
    "Choose an energylabel", options=np.unique(proposals["address"])
)
col2_2.table(
    proposals[proposals["address"] == option][
        ["shorttext", "investment", "lifetime"]
    ].sort_values(by="investment")
)

# col1_2.write(np.unique(proposals["shorttext"]))

print("All done")
print("--- Script finished in %s seconds ---" % (time.time() - start_time))
