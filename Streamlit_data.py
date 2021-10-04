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
# st.markdown("<style>body{background-color: Blue;}</style>", unsafe_allow_html=True)
st.markdown(
    f"""
<style>
    .reportview-container .main .block-container{{
        max-width: 90%;
        padding-top: 5rem;
        padding-right: 5rem;
        padding-left: 5rem;
        padding-bottom: 5rem;
    }}
    img{{
    max-width:40%;
    margin-bottom:40px;
    }}
</style>
""",
    unsafe_allow_html=True,
)


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
        """select top 10 * from energylabels.creation_data""", cnxn
    )
    print("--- Creation %s seconds ---" % (time.time() - start_time))
    df_build = pd.read_sql(
        """select * from energylabels.building_data WHERE ownership='Municipality' """,
        cnxn,
    )
    print("--- Build %s seconds ---" % (time.time() - start_time))
    # df_build.columns = df_build.columns.str.replace(" ", "_")
    # list = df_build["energylabel_id"].tolist()
    # list = ",".join(map(str, list))
    # print(list)
    query = """
    SELECT top 1000000 *
    FROM energylabels.proposals
    """
    # INNER JOIN energylabels.building_data
    # ON 'energylabels.building_data.energylabel id' ='energylabels.proposals.energylabel id'

    # WHERE EXISTS (SELECT 'energylabel id' FROM energylabels.building_data
    # WHERE 'energylabels.building_data.energylabel id' = 'energylabels.proposals.energylabel id')
    # """  # AND 'energylabels.building_data.energylabel id' = 'energylabels.proposals.energylabel id'
    # )"""
    # RIGHT JOIN redshift.energylabels.building_data
    # ON 'redshift.energylabels.proposals.energylabel id' = 'redshift.energylabels.building_data.energylabel id'

    # WHERE EXISTS (SELECT * FROM redshift.energylabels.building_data
    # WHERE ownership='Municipality'
    # AND 'redshift.energylabels.building_data.energylabel id' = 'redshift.energylabels.proposals.energylabel id'

    #'energylabel id', 'proposal id', 'shorttext', 'lifetime', 'investment', 'data_input_type', 'data_value'
    n = 100
    # df_prop = pd.read_sql(query, cnxn,)
    dfs = []
    for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        dfs.append(chunk)
        # print("--- Proposals %s seconds ---" % (time.time() - start_time))
    df_prop = pd.concat(dfs)
    print("--- Proposals %s seconds ---" % (time.time() - start_time))
    data_time = time.time() - start_time
    print("Connected to Redshift")

    return df_creation, df_build, df_prop, data_time  #


df_creation, df_build, df_prop, data_time = connect()


# %% Data cleaning ################################################################################################################################
col0_1, col0_2 = st.columns(2)
col0_1.header("Raw data")
col0_1.write("df_build")
col0_1.write(df_build[0:10000])
col0_2.write("df_prop")
col0_2.write(df_prop[0:10000])


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
    return df_creation, df_build, df_prop


df_creation, df_build, df_prop = data_cleaning()

# %% Sidebar ################################################################################################################################
st.sidebar.image("andel_logo_white_rgb.png")
st.sidebar.write("Version 1.0")

municipalities = st.sidebar.multiselect(
    "Vælg dine yndlingskommuner",
    options=list(np.unique(df_build["municipality"])),
    default=["København", "Ballerup"],
)

bygningstyper = st.sidebar.multiselect(
    "Hvilken type bygninger skal medtages",
    options=list(np.unique(df_build["ownership"])),
    default=["Municipality"],
)


# %%

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


@st.cache(allow_output_mutation=True)
def bygningstype(bygningstyper):
    energy = df_prop[df_prop["energylabel_id"].isin(hist_values["energylabel_id"])]
    energy = energy.merge(hist_values, on="energylabel_id")
    energy["investment"] = pd.to_numeric(energy["investment"])
    energy = energy[energy["ownership"].isin(bygningstyper)]
    return energy


energy = bygningstype(bygningstyper)


@st.cache(allow_output_mutation=True)
def individuelle_forslag(energy):
    proposals = energy
    for i in tqdm(proposals["energylabel_id"].unique().astype("int64")):
        temp = proposals[proposals["energylabel_id"] == i]
        temp = temp.drop_duplicates(subset="proposal_id")
        proposals = proposals[proposals["energylabel_id"] != i]
        proposals = proposals.append(temp)
    return proposals


proposals = individuelle_forslag(energy)


col2_1.header("Raw data")
col2_1.write("df_build")
col2_1.write(df_build[0:10000])
col2_1.write("df_prop")
col2_1.write(energy[0:10000])
col2_1.write("proposals")
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
ax = sns.countplot(x="data_input_type", hue="municipality", data=proposals)
ax.tick_params(labelrotation=90)
col1_2.pyplot(fig)


option = col2_2.selectbox(
    "Find forslag til forbedringer for en adresse",
    options=np.unique(proposals["address"]),
)
col2_2.table(
    proposals[proposals["address"] == option][
        ["shorttext", "investment", "lifetime"]
    ].sort_values(by="investment")
)


col3_1, col3_2 = st.columns(2)


proposals["filtered_shorttext"] = proposals["shorttext"].astype(str)
proposals["not_filtered_shorttext"] = proposals["shorttext"].astype(str)


def filter_shorttext_window(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("energi", case=False),
        "filtered_shorttext",
    ] = "Energirude udskiftning"
    # Termorude
    data.loc[
        data["filtered_shorttext"].str.contains("termorude", case=False),
        "filtered_shorttext",
    ] = "Termorude udskiftning"
    # Forsatsrude
    data.loc[
        data["filtered_shorttext"].str.contains("forsatsrude", case=False),
        "filtered_shorttext",
    ] = "Forsatsrude"
    # Yderdør
    data.loc[
        data["filtered_shorttext"].str.contains("yderdør", case=False),
        "filtered_shorttext",
    ] = "Yderdør"
    # terasse
    data.loc[
        data["filtered_shorttext"].str.contains("terrasse", case=False),
        "filtered_shorttext",
    ] = "Terrasse"
    # Kælder
    data.loc[
        data["filtered_shorttext"].str.contains("kælder", case=False),
        "filtered_shorttext",
    ] = "Kælder"
    # Vinduer andet
    data.loc[
        data["filtered_shorttext"].str.contains("vindue", case=False),
        "filtered_shorttext",
    ] = "andet"
    # Dør andet
    data.loc[
        data["filtered_shorttext"].str.contains("dør", case=False),
        "filtered_shorttext",
    ] = "Vin og dør andet"
    Search_for_These_values = [
        "energi",
        "termorude",
        "forsatsrude",
        "yderdør",
        "terrasse",
        "kælder",
        "vindue",
        "dør",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "Vindue Andet generelt"
    return data


def filter_shorttext_buildingpart(data):
    # Hanebånd
    data.loc[
        data["filtered_shorttext"].str.contains("hanebånd", case=False),
        "filtered_shorttext",
    ] = "Hanebånd efteris-olering"
    # Skråvæg
    data.loc[
        data["filtered_shorttext"].str.contains("skrå", case=False),
        "filtered_shorttext",
    ] = "skråvæg efteris-olering"
    # Ydervæg
    data.loc[
        data["filtered_shorttext"].str.contains("ydervæg", case=False),
        "filtered_shorttext",
    ] = "ydervæg efteris-olering"
    # skunk
    data.loc[
        data["filtered_shorttext"].str.contains("skunk", case=False),
        "filtered_shorttext",
    ] = "skunk efteris-olering"
    # kvist
    data.loc[
        data["filtered_shorttext"].str.contains("kvist", case=False),
        "filtered_shorttext",
    ] = "kvist efteris-olering"
    # Gulv
    data.loc[
        data["filtered_shorttext"].str.contains("gulv", case=False),
        "filtered_shorttext",
    ] = "gulv"
    # Loft
    data.loc[
        data["filtered_shorttext"].str.contains("loft", case=False),
        "filtered_shorttext",
    ] = "loft/tag efteris-olering"
    # Tag
    data.loc[
        data["filtered_shorttext"].str.contains("tag", case=False),
        "filtered_shorttext",
    ] = "loft/tag efteris-olering"
    # Hulmur
    data.loc[
        data["filtered_shorttext"].str.contains("Hulmur", case=False),
        "filtered_shorttext",
    ] = "Hulmur is-olering"
    # Hanebånd
    data.loc[
        data["filtered_shorttext"].str.contains("etageadskil", case=False),
        "filtered_shorttext",
    ] = "etageadskillelse is-olering"
    # Hanebånd
    data.loc[
        data["filtered_shorttext"].str.contains("kælder", case=False),
        "filtered_shorttext",
    ] = "kælder is-olering"
    # Tag
    data.loc[
        data["filtered_shorttext"].str.contains("ræn", case=False),
        "filtered_shorttext",
    ] = "terrændæk efteris-olering"
    # Tag
    data.loc[
        data["filtered_shorttext"].str.contains("dør", case=False),
        "filtered_shorttext",
    ] = "døre"
    # Tag
    data.loc[
        data["filtered_shorttext"].str.contains("vindue", case=False),
        "filtered_shorttext",
    ] = "vindue"
    # Tag
    data.loc[
        data["filtered_shorttext"].str.contains("rude", case=False),
        "filtered_shorttext",
    ] = "vindue"
    # Andet iso
    data.loc[
        data["filtered_shorttext"].str.contains("iso", case=False),
        "filtered_shorttext",
    ] = "Andet isolering"
    Search_for_These_values = [
        "hanebånd",
        "skrå",
        "ydervæg",
        "skunk",
        "kvist",
        "gulv",
        "loft",
        "tag",
        "hulmur",
        "etageadskil",
        "kælder",
        "ræn",
        "dør",
        "vindue",
        "rude",
        "iso",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "Build andet"
    return data


def filter_shorttext_ColdWater(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("toilet", case=False),
        "filtered_shorttext",
    ] = "Toilet"
    # Termorude
    data.loc[
        data["filtered_shorttext"].str.contains("WC", case=False),
        "filtered_shorttext",
    ] = "Toilet"
    # Forsatsrude
    data.loc[
        data["filtered_shorttext"].str.contains("brus", case=False),
        "filtered_shorttext",
    ] = "bruser"
    # Yderdør
    data.loc[
        data["filtered_shorttext"].str.contains("vask", case=False),
        "filtered_shorttext",
    ] = "Håndvask"
    # terasse
    data.loc[
        data["filtered_shorttext"].str.contains("blanding", case=False),
        "filtered_shorttext",
    ] = "Blandingsbatteri"
    Search_for_These_values = [
        "toilet",
        "WC",
        "brus",
        "vask",
        "blanding",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "CW Andet"
    return data


def filter_shorttext_HeatDistributionPump(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("pumpe", case=False),
        "filtered_shorttext",
    ] = "Pumpe"
    # Termorude
    data.loc[
        data["filtered_shorttext"].str.contains("Varme", case=False),
        "filtered_shorttext",
    ] = "Varme"
    # Forsatsrude
    data.loc[
        data["filtered_shorttext"].str.contains("gas", case=False),
        "filtered_shorttext",
    ] = "Gas"
    # Yderdør
    data.loc[
        data["filtered_shorttext"].str.contains("vand", case=False),
        "filtered_shorttext",
    ] = "vand"
    Search_for_These_values = [
        "pumpe",
        "Varme",
        "gas",
        "vand",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "HDP Andet"
    return data


def filter_shorttext_HeatDistributionPipe(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("rør", case=False),
        "filtered_shorttext",
    ] = "Rør isolering"
    Search_for_These_values = [
        "rør",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "HDpipe Andet"
    return data


def filter_shorttext_HotWaterPipe(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("rør", case=False),
        "filtered_shorttext",
    ] = "Rør"
    # Termorude
    data.loc[
        data["filtered_shorttext"].str.contains("solvarme", case=False),
        "filtered_shorttext",
    ] = "Solvarme"
    Search_for_These_values = [
        "rør",
        "solvarme",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "HWP Andet"
    return data


def filter_shorttext_AutomaticControl(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("ventil", case=False),
        "filtered_shorttext",
    ] = "Ventiler"
    # Termorude
    data.loc[
        data["filtered_shorttext"].str.contains("styring", case=False),
        "filtered_shorttext",
    ] = "Styring"
    # Termorude
    data.loc[
        data["filtered_shorttext"].str.contains("ude", case=False),
        "filtered_shorttext",
    ] = "Udekompensering/føling"
    Search_for_These_values = [
        "ventil",
        "styring",
        "ude",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "AuC Andet"
    return data


def filter_shorttext_LinearThermalTransmittance(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("kælder", case=False),
        "filtered_shorttext",
    ] = "Kælder"
    Search_for_These_values = [
        "kælder",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "LTT Andet"
    return data


def filter_shorttext_SolarHeatingPlant(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("sol", case=False),
        "filtered_shorttext",
    ] = "Solvarme"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("varme", case=False),
        "filtered_shorttext",
    ] = "Solvarme"
    Search_for_These_values = [
        "sol",
        "varme",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "SHP Andet"
    return data


def filter_shorttext_SolarCell(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("sol", case=False),
        "filtered_shorttext",
    ] = "Solceller"
    Search_for_These_values = [
        "sol",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "SC Andet"
    return data


def filter_shorttext_HeatPump(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("luft/luft", case=False),
        "filtered_shorttext",
    ] = "Luft/luft varmepumpe"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("luft til luft", case=False),
        "filtered_shorttext",
    ] = "Luft/luft varmepumpe"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("luft-luft", case=False),
        "filtered_shorttext",
    ] = "Luft/luft varmepumpe"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("luft/vand", case=False),
        "filtered_shorttext",
    ] = "Luft/vand varmepumpe"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("væske/vand", case=False),
        "filtered_shorttext",
    ] = "Væske/vand varmepumpe"
    Search_for_These_values = [
        "luft/luft",
        "luft til luft",
        "luft-luft",
        "luft/vand",
        "væske/vand",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "HP Andet"
    return data


def filter_shorttext_HotWaterTank(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("sol", case=False),
        "filtered_shorttext",
    ] = "Solvarme"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("varmtvand", case=False),
        "filtered_shorttext",
    ] = "Varmtvandsbeholder"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("vvb", case=False),
        "filtered_shorttext",
    ] = "Varmtvandsbeholder"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("rør", case=False),
        "filtered_shorttext",
    ] = "Rør isolering"
    Search_for_These_values = [
        "sol",
        "varmtvand",
        "vvb",
        "rør",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "HWT Andet"
    return data


def filter_shorttext_Boiler(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("kedel", case=False),
        "filtered_shorttext",
    ] = "Kedel"
    Search_for_These_values = [
        "kedel",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "Kedel Andet"
    return data


def filter_shorttext_HotWaterConsumption(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("sol", case=False),
        "filtered_shorttext",
    ] = "Solvarme"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("rør", case=False),
        "filtered_shorttext",
    ] = "Rør isolering"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("pumpe", case=False),
        "filtered_shorttext",
    ] = "Pumpe"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("ventil", case=False),
        "filtered_shorttext",
    ] = "Ventil"
    Search_for_These_values = [
        "sol",
        "rør",
        "pumpe",
        "ventil",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "HWC Andet"
    return data


def filter_shorttext_HeatDistributionSystem(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("fordeling", case=False),
        "filtered_shorttext",
    ] = "Fordelingssystem"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("streng", case=False),
        "filtered_shorttext",
    ] = "Fler-strengssystem"
    Search_for_These_values = [
        "fordeling",
        "streng",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "HDS Andet"
    return data


def filter_shorttext_HotWaterCirculationPump(data):
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("pumpe", case=False),
        "filtered_shorttext",
    ] = "Pumpe til brugsvandanlæg"
    # Energirude
    data.loc[
        data["filtered_shorttext"].str.contains("styring", case=False),
        "filtered_shorttext",
    ] = "styring"
    Search_for_These_values = [
        "pumpe",
        "styring",
    ]
    pattern = "|".join(Search_for_These_values)
    data.loc[
        ~data["filtered_shorttext"].str.contains(pattern, case=False),
        "filtered_shorttext",
    ] = "HWCP Andet"
    return data


def filter(proposals):
    proposals[proposals["data_input_type"] == "Window"] = filter_shorttext_window(
        proposals[proposals["data_input_type"] == "Window"]
    )

    proposals[
        proposals["data_input_type"] == "BuildingPart"
    ] = filter_shorttext_buildingpart(
        proposals[proposals["data_input_type"] == "BuildingPart"]
    )

    proposals[proposals["data_input_type"] == "ColdWater"] = filter_shorttext_ColdWater(
        proposals[proposals["data_input_type"] == "ColdWater"]
    )

    proposals[
        proposals["data_input_type"] == "HeatDistributionPump"
    ] = filter_shorttext_HeatDistributionPump(
        proposals[proposals["data_input_type"] == "HeatDistributionPump"]
    )

    proposals[
        proposals["data_input_type"] == "HeatDistributionPipe"
    ] = filter_shorttext_HeatDistributionPipe(
        proposals[proposals["data_input_type"] == "HeatDistributionPipe"]
    )
    proposals[
        proposals["data_input_type"] == "HotWaterPipe"
    ] = filter_shorttext_HotWaterPipe(
        proposals[proposals["data_input_type"] == "HotWaterPipe"]
    )
    proposals[
        proposals["data_input_type"] == "AutomaticControl"
    ] = filter_shorttext_AutomaticControl(
        proposals[proposals["data_input_type"] == "AutomaticControl"]
    )
    proposals[
        proposals["data_input_type"] == "LinearThermalTransmittance"
    ] = filter_shorttext_LinearThermalTransmittance(
        proposals[proposals["data_input_type"] == "LinearThermalTransmittance"]
    )
    proposals[
        proposals["data_input_type"] == "SolarHeatingPlant"
    ] = filter_shorttext_SolarHeatingPlant(
        proposals[proposals["data_input_type"] == "SolarHeatingPlant"]
    )
    proposals[proposals["data_input_type"] == "SolarCell"] = filter_shorttext_SolarCell(
        proposals[proposals["data_input_type"] == "SolarCell"]
    )
    proposals[proposals["data_input_type"] == "HeatPump"] = filter_shorttext_HeatPump(
        proposals[proposals["data_input_type"] == "HeatPump"]
    )
    proposals[
        proposals["data_input_type"] == "HotWaterTank"
    ] = filter_shorttext_HotWaterTank(
        proposals[proposals["data_input_type"] == "HotWaterTank"]
    )
    proposals[proposals["data_input_type"] == "Boiler"] = filter_shorttext_Boiler(
        proposals[proposals["data_input_type"] == "Boiler"]
    )
    proposals[
        proposals["data_input_type"] == "HotWaterConsumption"
    ] = filter_shorttext_HotWaterConsumption(
        proposals[proposals["data_input_type"] == "HotWaterConsumption"]
    )
    proposals[
        proposals["data_input_type"] == "HeatDistributionSystem"
    ] = filter_shorttext_HeatDistributionSystem(
        proposals[proposals["data_input_type"] == "HeatDistributionSystem"]
    )
    proposals[
        proposals["data_input_type"] == "HotWaterCirculationPump"
    ] = filter_shorttext_HotWaterCirculationPump(
        proposals[proposals["data_input_type"] == "HotWaterCirculationPump"]
    )

    return proposals


proposals = filter(proposals)

kategori = proposals  # [proposals["data_input_type"] == "HotWaterCirculationPump"]

col3_2.table(kategori["filtered_shorttext"].unique())
col3_1.write(kategori[0:1000])


col4_1, col4_2 = st.columns(2)

fig, ax = plt.subplots(figsize=(9, 4))
ax = sns.countplot(x="filtered_shorttext", hue="municipality", data=kategori)
ax.tick_params(labelrotation=90)
col4_1.pyplot(fig)

# fig, ax = plt.subplots(figsize=(9, 4))
# ax = sns.countplot(x="shorttext", hue="municipality", data=kategori)
# ax.tick_params(labelrotation=90)
# col4_2.pyplot(fig)

fig, ax = plt.subplots(figsize=(20, 4))
ax = sns.countplot(x="filtered_shorttext", hue="municipality", data=kategori)
ax.tick_params(labelrotation=90)
st.pyplot(fig)

print("All done")
print("--- Script finished in %s seconds ---" % (time.time() - start_time))
st.sidebar.write("--- Script finished in %.4s seconds ---" % (time.time() - start_time))
