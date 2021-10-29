import pyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import streamlit as st
from tqdm import tqdm
import time
import plotly.express as px
import plotly.io as pio
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb
import math
from pyecharts import options as opts
from pyecharts.charts import Bar, Pie, Grid, Line, Scatter, Sankey
from streamlit_echarts import st_pyecharts

pio.templates.default = "simple_white"


st.set_page_config(layout="wide", page_title="Energimærke forslag", page_icon="andel_a.png")

st.title("Filtrering af energimærkeforslag")

start_time = time.time()
st.sidebar.image("andel_logo_white_rgb.png")
st.sidebar.write("Version 0.2")
with st.sidebar.expander("Upload BBR data"):
    st.write("Data skal stå på følgende måde:")
    st.code(
        """BBR Kommunenr, BBR Ejendomsnummer
BBR Kommunenr, BBR Ejendomsnummer
..."""
    )
    uploaded_BBR = st.file_uploader("BBR data", type=["txt", "csv"])
    if uploaded_BBR is not None:
        BBR_list = list(np.loadtxt(uploaded_BBR, delimiter=",", dtype="int"))
        st.code(BBR_list)
    else:
        BBR_list=[]

with st.sidebar.expander("Upload Energimærke ID"):
    st.write("Data skal stå i en kolonne af:")
    st.code(
        """EnergimærkeID
EnergimærkeID
..."""
    )
    uploaded_enerID = st.file_uploader("EnergimærkeID", type=["txt", "csv"])
    if uploaded_enerID is not None:
        ener_list = list(np.loadtxt(uploaded_enerID, dtype="int"))
        st.code(ener_list)
    else:
        ener_list=[]


# %% Data loading ###############################################################################################################################

@st.experimental_memo
def hent_data(BBR_list, ener_list):
    start_time = time.time()
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
    print("Connected to Redshift")

    if BBR_list:
        print('BBR Data')
        query = """
                WITH build AS
                (
                SELECT energylabel_id, build.propertynumber, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE energylabel_id IN {}
                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.seebclassification,
                build.propertynumber, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea,
                result.status_energylabelclassification, result.resultforallprofitable_energylabelclassification,
                result.resultforallproposals_energylabelclassification, result.energylabelclassification

                FROM energylabels.results_fuelsavings AS fuel
                RIGHT JOIN build ON build.energylabel_id = fuel.energylabel_id
                LEFT JOIN energylabels.results_profitability AS prof ON build.energylabel_id = prof.energylabel_id AND fuel.proposal_group_id = prof.proposal_group_id
                LEFT JOIN energylabels.result_data_energylabels AS result ON build.energylabel_id = result.energylabel_id
                LEFT JOIN energylabels.proposal_groups AS prop_group ON build.energylabel_id = prop_group.energylabel_id AND fuel.proposal_group_id = prop_group.proposal_group_id

                ORDER BY build.energylabel_id, fuel.proposal_group_id, prof.proposal_group_id
                """.format(tuple(BBR[:,0]), tuple(BBR[:,1]))
        n = 10000
        dfs = []
        for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
            dfs.append(chunk)
        df = pd.concat(dfs)
        print("--- df %s seconds ---" % (time.time() - start_time))

    elif ener_list:
        print('Energimærke Data')
        query = """
                WITH build AS
                (
                SELECT energylabel_id, build.propertynumber, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE energylabel_id IN {}
                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.seebclassification,
                build.propertynumber, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea,
                result.status_energylabelclassification, result.resultforallprofitable_energylabelclassification,
                result.resultforallproposals_energylabelclassification, result.energylabelclassification

                FROM energylabels.results_fuelsavings AS fuel
                RIGHT JOIN build ON build.energylabel_id = fuel.energylabel_id
                LEFT JOIN energylabels.results_profitability AS prof ON build.energylabel_id = prof.energylabel_id AND fuel.proposal_group_id = prof.proposal_group_id
                LEFT JOIN energylabels.result_data_energylabels AS result ON build.energylabel_id = result.energylabel_id
                LEFT JOIN energylabels.proposal_groups AS prop_group ON build.energylabel_id = prop_group.energylabel_id AND fuel.proposal_group_id = prop_group.proposal_group_id

                ORDER BY build.energylabel_id, fuel.proposal_group_id, prof.proposal_group_id
                """.format(tuple(ener_list))
        n = 10000
        dfs = []
        for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
            dfs.append(chunk)
        df = pd.concat(dfs)
        print("--- df %s seconds ---" % (time.time() - start_time))

    else:
        print('Normal datahentning')
        query = """
                WITH build AS
                (
                SELECT energylabel_id, build.propertynumber, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE ownership = 'Municipality'
                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.seebclassification,
                build.propertynumber, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea,
                result.status_energylabelclassification, result.resultforallprofitable_energylabelclassification,
                result.resultforallproposals_energylabelclassification, result.energylabelclassification

                FROM energylabels.results_fuelsavings AS fuel
                RIGHT JOIN build ON build.energylabel_id = fuel.energylabel_id
                LEFT JOIN energylabels.results_profitability AS prof ON build.energylabel_id = prof.energylabel_id AND fuel.proposal_group_id = prof.proposal_group_id
                LEFT JOIN energylabels.result_data_energylabels AS result ON build.energylabel_id = result.energylabel_id
                LEFT JOIN energylabels.proposal_groups AS prop_group ON build.energylabel_id = prop_group.energylabel_id AND fuel.proposal_group_id = prop_group.proposal_group_id

                ORDER BY build.energylabel_id, fuel.proposal_group_id, prof.proposal_group_id
                """
        n = 10000
        dfs = []
        for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
            dfs.append(chunk)
        df = pd.concat(dfs)
        print("--- df %s seconds ---" % (time.time() - start_time))
    data_time = time.time() - start_time

    return df, data_time  #

df, data_time = hent_data(BBR_list, ener_list)
df_base = df['energylabel_id']
# %% Data cleaning ################################################################################################################################

@st.experimental_memo
def data_cleaning(df):
    df["municipality"] = df["municipalitynumber"]
    df["municipality"].replace(
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
    df["use"] = df["usecode"]
    df["use"].replace(
        {
            "110": "110 - Stuehus til landbrugsejendom",
            "120": "120 - Fritliggende enfamiliehus",
            "121": "121 - Sammenbygget enfamiliehus",
            "122": "122 - Fritliggende enfamiliehus i tæt-lav bebyggelse",
            "130": "130 - (UDFASES) Række-, kæde-, eller dobbelthus",
            "131": "131 - Række-, kæde- og klyngehus",
            "132": "132 - Dobbelthus",
            "140": "140 - Etagebolig-bygning, flerfamiliehus eller to-familiehus",
            "150": "150 - Kollegium",
            "160": "160 - Boligbygning til døgninstitution",
            "185": "185 - Anneks i tilknytning til helårsbolig",
            "190": "190 - Anden helårsbeboelse",
            "210": "210 - (UDFASES) Erhvervsmæssig produktion vedrørende landbrug, gartneri, råstofudvinding o. lign",
            "211": "211 - Stald til svin",
            "212": "212 - Stald til kvæg, får mv.",
            "213": "213 - Stald til fjerkræ",
            "214": "214 - Minkhal",
            "215": "215 - Væksthus",
            "216": "216 - Lade til foder, afgrøder mv.",
            "217": "217 - Maskinhus, garage mv.",
            "218": "218 - Lade til halm, hø mv.",
            "219": "219 - Anden bygning til landbrug mv.",
            "220": "220 - (UDFASES) Erhvervsmæssig produktion vedrørende industri, håndværk m.v.",
            "221": "221 - Industri med integreret produktionsapparat",
            "222": "222 - Industri uden integreret produktionsapparat ",
            "223": "223 - Værksted",
            "229": "229 - Anden til produktion",
            "230": "230 - (UDFASES) El-, gas-, vand- eller varmeværk, forbrændingsanstalt m.v.",
            "231": "231 - Energiproduktion",
            "232": "232 - Forsyning- og energidistribution",
            "233": "233 - Vandforsyning",
            "234": "234 - Til håndtering af affald og spildevand",
            "239": "239 - Anden til energiproduktion og -distribution",
            "290": "290 - (UDFASES) Anden til landbrug, industri etc.",
            "310": "310 - (UDFASES) Transport- og garageanlæg",
            "311": "311 - Jernbane- og busdrift",
            "312": "312 - Luftfart",
            "313": "313 - Parkering- og transportanlæg",
            "314": "314 - Parkering af flere end to køretøjer i tilknytning til boliger",
            "315": "315 - Havneanlæg",
            "319": "319 - Andet transportanlæg",
            "320": "320 - (UDFASES) Til kontor, handel, lager, herunder offentlig administration",
            "321": "321 - Kontor",
            "322": "322 - Detailhandel",
            "323": "323 - Lager",
            "324": "324 - Butikscenter",
            "325": "325 - Tankstation",
            "329": "329 - Anden til kontor, handel og lager",
            "330": "330 - (UDFASES) Til hotel, restaurant, vaskeri, frisør og anden servicevirksomhed",
            "331": "331 - Hotel, kro eller konferencecenter med overnatning",
            "332": "332 - Bed & breakfast mv.",
            "333": "333 - Restaurant, café og konferencecenter uden overnatning",
            "334": "334 - Privat servicevirksomhed som frisør, vaskeri, netcafé mv.",
            "339": "339 - Anden til serviceerhverv",
            "390": "390 - (UDFASES) Anden til transport, handel etc",
            "410": "410 - (UDFASES) Biograf, teater, erhvervsmæssig udstilling, bibliotek, museum, kirke o. lign.",
            "411": "411 - Biograf, teater, koncertsted mv.",
            "412": "412 - Museum",
            "413": "413 - Bibliotek",
            "414": "414 - Kirke eller anden til trosudøvelse for statsanerkendte trossamfund",
            "415": "415 - Forsamlingshus",
            "416": "416 - Forlystelsespark",
            "419": "419 - Anden til kulturelle formål",
            "420": "420 - (UDFASES) Undervisning og forskning (skole, gymnasium, forskningslabratorium o.lign.).",
            "421": "421 - Grundskole",
            "422": "422 - Universitet",
            "429": "429 - Anden til undervisning og forskning",
            "430": "430 - (UDFASES) Hospital, sygehjem, fødeklinik o. lign.",
            "431": "431 - Hospital og sygehus",
            "432": "432 - Hospice, behandlingshjem mv.",
            "433": "433 - Sundhedscenter, lægehus, fødeklinik mv.",
            "439": "439 - Anden til sundhedsformål",
            "440": "440 - (UDFASES) Daginstitution",
            "441": "441 - Daginstitution",
            "442": "442 - Servicefunktion på døgninstitution",
            "443": "443 - Kaserne",
            "444": "444 - Fængsel, arresthus mv.",
            "449": "449 - Anden til institutionsformål",
            "490": "490 - (UDFASES) Anden institution, herunder kaserne, fængsel o. lign.",
            "510": "510 - Sommerhus",
            "520": "520 - (UDFASES) Feriekoloni, vandrehjem o.lign. bortset fra sommerhus",
            "521": "521 - Feriecenter, center til campingplads mv.",
            "522": "522 - Ferielejligheder til erhvervsmæssig udlejning",
            "523": "523 - Ferielejligheder til eget brug",
            "529": "529 - Anden til ferieformål",
            "530": "530 - (UDFASES) I forbindelse med idrætsudøvelse (klubhus, idrætshal, svømmehal o. lign.)",
            "531": "531 - Klubhus i forbindelse med fritid og idræt",
            "532": "532 - Svømmehal",
            "533": "533 - Idrætshal",
            "534": "534 - Tribune i forbindelse med stadion",
            "535": "535 - Til træning og opstaldning af heste",
            "539": "539 - Anden til idrætformål",
            "540": "540 - Kolonihavehus",
            "585": "585 - Anneks i tilknytning til fritids- og sommerhus",
            "590": "590 - Anden til fritidsformål",
            "910": "910 - Garage (med plads til et eller to køretøjer)",
            "920": "920 - Carport",
            "930": "930 - Udhus",
            "940": "940 - Drivhus",
            "950": "950 - Fritliggende overdækning",
            "960": "960 - Fritliggende udestue",
            "970": "970 - Tiloversbleven landbrugsbygning",
            "990": "990 - Faldefærdig bygning",
            "999": "999 - Ukendt bygning",
        },
        inplace=True,
    )
    df["address_long"] = df.postalcode + ' ' + df.postalcity + ' ' + df.streetname + ' ' + df.housenumber
    df["Adresse"] = df.streetname + ' ' + df.housenumber
    df["dwellingarea"] = pd.to_numeric(df["dwellingarea"])
    df["commercialarea"] = pd.to_numeric(df["commercialarea"])
    df['areal'] = df['dwellingarea'] + df["commercialarea"]
    df["Seeb_beskrivelse"] = df["seebclassification"]
    df["Seeb_beskrivelse"].replace(
        {
            "1-0-0-0": "Bygningen",
            "1-1-0-0": "Tag og loft",
            "1-1-1-0": "Loft",
            "1-1-2-0": "Fladt tag",
            "1-2-0-0": "Ydervægge",
            "1-2-1-0": "Hule ydervægge",
            "1-2-2-0": "Massive ydervægge",
            "1-2-3-0": "Lette ydervægge",
            "1-2-1-1": "Hule vægge mod uopvarmet rum",
            "1-2-2-1": "Massive vægge mod uopvarmet rum",
            "1-2-3-1": "Lette vægge mod uopvarmet rum",
            "1-2-4-0": "Kælder ydervægge",
            "1-3-0-0": "Vinduer, ovenlys og døre",
            "1-3-1-0": "Vinduer",
            "1-3-2-0": "Ovenlys",
            "1-3-3-0": "Yderdøre",
            "1-4-0-0": "Gulve",
            "1-4-1-0": "Terrændæk",
            "1-4-2-0": "Etageadskillelse",
            "1-4-3-0": "Krybekælder",
            "1-4-4-0": "Kældergulv",
            "1-4-1-1": "Terrændæk med gulvvarme",
            "1-4-2-1": "Etageadskillelse med gulvvarme",
            "1-4-3-1": "Krybekælder med gulvvarme",
            "1-4-4-1": "Kældergulv med gulvvarme",
            "1-4-5-0": "Linjetab",
            "1-5-0-0": "Ventilation",
            "1-5-1-0": "Ventilation",
            "1-5-2-0": "Ventilationskanaler",
            "1-5-3-0": "Køling",
            "1-6-0-0": "Internt varmetilskud",
            "1-6-1-0": "Internt varmetilskud",
            "2-0-0-0": "Varmeanlæg",
            "2-1-0-0": "Varmeanlæg",
            "2-1-1-0": "Varmeanlæg",
            "2-1-2-0": "Kedler",
            "2-1-3-0": "Fjernvarme",
            "2-1-4-0": "Ovne",
            "2-1-5-0": "Varmepumper",
            "2-1-6-0": "Solvarme",
            "2-2-0-0": "Varmefordeling",
            "2-2-1-0": "Varmefordeling",
            "2-2-2-0": "Varmerør",
            "2-2-3-0": "Varmefordelingspumper",
            "2-2-4-0": "Automatik",
            "3-0-0-0": "Varmt og koldt vand",
            "3-1-0-0": "Varmt brugsvand",
            "3-1-1-0": "Varmt brugsvand",
            "3-1-2-0": "Armaturer",
            "3-1-3-0": "Varmtvandsrør",
            "3-1-4-0": "Varmtvandspumper",
            "3-1-5-0": "Varmtvandsbeholder",
            "3-2-0-0": "Koldt vand",
            "3-2-1-0": "Koldt vand",
            "4-0-0-0": "El",
            "4-1-0-0": "El",
            "4-1-1-0": "Belysning",
            "4-1-2-0": "Apparater",
            "4-1-3-0": "Solceller",
            "4-1-4-0": "Vindmøller",
        },
        inplace=True,
    )
    df["investment"] = pd.to_numeric(df["investment"])
    df["investmentlifetime"] = pd.to_numeric(df["investmentlifetime"])
    df["energylabel_id"] = df["energylabel_id"].astype("int64")
    #df["proposal_group_id"] = df["proposal_group_id"].astype("int64")
    # df["proposal_id"] = pd.to_numeric(df["proposal_id"])
    return df

df = data_cleaning(df)
print(df.columns)
print(df.head(10))
print(df.dtypes)
# %% Sidebar ################################################################################################################################

with st.sidebar.expander('Vælg kommune og ejerskab'):
    municipalities = st.multiselect(
        "Vælg dine yndlingskommuner",
        options=list(np.unique(df["municipality"])),
        default=["København"],
    )
    bygningstyper = st.multiselect(
        "Hvilken type bygninger skal medtages",
        options=list(np.unique(df["ownership"])),
        default=["Municipality"],
    )

# %%


@st.experimental_memo
def bygningstype(bygningstyper, municipalities, df):
    """ Filters if the data is in the municipalities chosen and in the types of buildings chosen.
    Also merges the proposals and building_data tables. """

    df = df[df["municipality"].isin(municipalities)]
    df = df[df["ownership"].isin(bygningstyper)]
    return df

df = bygningstype(bygningstyper, municipalities, df)



with st.sidebar.expander('Vælg anvendelse, specifikke adresser eller teknikområde'):
    brugskode = st.multiselect(
        "Hvilke anvendelser skal medtages?",
        options=list(np.unique(df["use"])),
    )
    adresse = st.multiselect(
        "Hvilke adresser skal medtages?",
        options=list(np.unique(df["Adresse"])),
    )
    teknik = st.multiselect(
        "Hvilke teknikområder?",
        options=list(df["Seeb_beskrivelse"].unique()),
        default=df["Seeb_beskrivelse"].unique()
    )


elpris = st.sidebar.number_input('Vælg den nuværende elpris', min_value=0.0, max_value=7.0, value=1.01, step=0.01)
CO2perkwh = st.sidebar.number_input('Hvor meget CO2 sparer man per kWh?', min_value=0.0, max_value=7.0, value=1.01, step=0.01)

def filtrer_sidebar(df):
    temp = df
    if brugskode:
        temp = df[df['use'].isin(brugskode)]
    if adresse:
        temp = df[df['Adresse'].isin(adresse)]
    if teknik:
        temp = df[df['Seeb_beskrivelse'].isin(teknik)]
    if temp is not None:
            df = temp
    return df

df = filtrer_sidebar(df)
df_orig = df

@st.experimental_memo
def fuel_calculations(df):
    df['original_CO2'] = df['fuelsaved'] * df['co2perunit']
    df['original_units'] = df['fuelsaved'] * df['energyperunit']
    df['investmentlifetime_new'] = df['investmentlifetime'].fillna((df['profitability']*df['investment'])/df['original_cost']).round()
    df['TBT'] = df['investment'] / df['original_cost']
    df['TBT'] = df['TBT'].round()
    df.loc[df['profitability'] < 0,'profitability'] =  np.nan

    df = df.groupby(['energylabel_id', 'proposal_group_id']).agg({
                                                                                               'fuelsaved': 'first',
                                                                                               'material': ', '.join,
                                                                                               'unit': 'first',
                                                                                               'energyperunit': 'first',
                                                                                               'co2perunit': 'first',
                                                                                               'costperunit': 'first',
                                                                                               'fixedcostperyear': 'first',
                                                                                               'original_cost': 'first',
                                                                                               'original_CO2': 'sum',
                                                                                               'original_units': 'sum',
                                                                                               'profitability': 'first',
                                                                                               'investment': 'first',
                                                                                               'investmentlifetime': 'first',
                                                                                               'investmentlifetime_new': 'first',
                                                                                               'TBT': 'first',
                                                                                               'shorttext': 'first',
                                                                                               'seebclassification': 'first',
                                                                                               'Seeb_beskrivelse': 'first',
                                                                                               'propertynumber': 'first',
                                                                                               'ownership': 'first',
                                                                                               'reviewdate': 'first',
                                                                                               'municipalitynumber': 'first',
                                                                                               'municipality': 'first',
                                                                                               'streetname': 'first',
                                                                                               'housenumber': 'first',
                                                                                               'postalcode': 'first',
                                                                                               'postalcity': 'first',
                                                                                               'usecode': 'first',
                                                                                               'use': 'first',
                                                                                               'dwellingarea': 'first',
                                                                                               'commercialarea': 'first',
                                                                                               'status_energylabelclassification': 'first',
                                                                                               'resultforallprofitable_energylabelclassification': 'first',
                                                                                               'resultforallproposals_energylabelclassification': 'first',
                                                                                               'energylabelclassification': 'first',
                                                                                               'address_long': 'first',
                                                                                               'Adresse': 'first',
                                                                                               'areal': 'first'
                                                                                               })
    df = df.reset_index()
    df = df.drop_duplicates(subset=(['energylabel_id', 'proposal_group_id']))
    return df

df = fuel_calculations(df)
missing_values_count = df.isnull().sum()
print('NaNs')
print(missing_values_count)

col0_1, col0_2, col0_3, col0_4 = st.columns(4)
col0_1.metric('Energimærker i al data', df_base.nunique())
col0_1.metric('Energimærker i valgt data', df_orig['energylabel_id'].nunique(), df_orig['energylabel_id'].nunique()-df["energylabel_id"].nunique())
col0_1.metric("Energimærker i valgt data med forslag", df["energylabel_id"].nunique(), -(df_orig['energylabel_id'].nunique()-df["energylabel_id"].nunique()))
col0_2.metric('Antal forslag i valgt data', df.shape[0])
col0_2.metric('Elsparepris', '{:.2f} Kr./kWh'.format(df["investment"].sum()/df['original_units'].sum()))
col0_2.metric('CO2 sparepris', '{:.2f} Kr./kg'.format(df["investment"].sum()/(df['original_CO2'].sum()/1000)))
col0_3.metric('Samlet årlig økonomisk besparelse', '{:.2f} mio. Kr.'.format(df['original_cost'].sum()/1000000))
col0_3.metric('Samlet årlig klimamæssig besparelse', '{:.2f} Ton.'.format(df['original_CO2'].sum()/1000000))
col0_3.metric('Samlet årlig energibesparelse', '{:.2f} mio. kWh.'.format(df['original_units'].sum()/1000000))
col0_4.metric("Tid for at hente data", '{:.2f} min'.format(data_time/60))
col0_4.metric("Samlet investering i DKK", '{:.2f} mio. Kr.'.format(np.sum(df["investment"])/1000000))
col0_4.metric("Middelrentabilitet", '{:.2f}'.format(df['profitability'].mean()))

@st.experimental_singleton
def grid_bar_pie(column, titel, bins, sortering):
    hej = column.value_counts(dropna=True, bins =bins).rename_axis('Name').reset_index(name='Antal')
    #hej['Value'] = hej.Name.astype(str)
    hej = hej.sort_values(by=sortering)
    hej2 = [list(z) for z in zip(hej.Name, hej.Antal)]
    #data_pair.sort(key=lambda x: x[1])
    p = (
        Pie()
        .add(
            series_name=titel,
            data_pair=hej2,
            #rosetype="area",
            radius=["30%", "60%"],
            center=["85%", "50%"],
            label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
        )
        .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="right", type_='scroll', is_show=False),
            title_opts=opts.TitleOpts(
                title=titel, subtitle="Fordeling af forslag på {}".format(titel), pos_left="center"
            ),
            toolbox_opts=opts.ToolboxOpts(),
        )
    )
    print(hej.shape)
    print(hej.Name.shape)
    print(hej.Antal.shape)
    b = (
        Bar()
        .add_xaxis(list(hej.Name))
        .add_yaxis(titel, list(hej.Antal),label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
        .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="left", type_='scroll', is_show=True),
            #label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
            # title_opts=opts.TitleOpts(
            #     title="Top cloud providers 2018", subtitle="2017-2018 Revenue", pos_left="center"
            # ),
            toolbox_opts=opts.ToolboxOpts(orient='vertical', is_show=False),
        )
        .set_series_opts(
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="max", name="Maximum"),
                opts.MarkPointItem(type_="min", name="Minimum"),
            ]
        ),
        markline_opts=opts.MarkLineOpts(
            data=[
                opts.MarkLineItem(type_="max", name="Maximum"),
                opts.MarkLineItem(type_="average", name="Gennemsnit"),
            ]
        ),
    )
    )

    grid = (
        Grid()
        .add(b, grid_opts=opts.GridOpts(pos_right="35%"))
        .add(p, grid_opts=opts.GridOpts(pos_left="55%"))
        )
    return grid

@st.experimental_singleton
def grid_bar_bar(column, titel1, bin1, range1, titel2, bin2, range2, titel):
    test_y, test_x = np.histogram(column, bin1, range=(0,range1))
    test = pd.DataFrame(test_y, test_x[:-1])
    test = test.reset_index()
    test.columns = ['x','y']
    b1 = (
        Bar()
        .add_xaxis(list(test.x))
        .add_yaxis(titel1, list(test.y), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
        .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="left", type_='scroll', is_show=True),
            #label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
            title_opts=opts.TitleOpts(
                title=titel, subtitle="Fordeling af antal forslag på {}".format(titel), pos_left="center"
            ),
            toolbox_opts=opts.ToolboxOpts(orient='vertical', is_show=False),
        )
        .set_series_opts(
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="max", name="Maximum"),
                opts.MarkPointItem(type_="min", name="Minimum"),
            ]
        ),
        markline_opts=opts.MarkLineOpts(
            data=[
                opts.MarkLineItem(type_="max", name="Maximum"),
                opts.MarkLineItem(type_="average", name="Gennemsnit"),
            ]
        ),
    )
    )
    test_y, test_x = np.histogram(column, bin2, range=(0,range2))
    test = pd.DataFrame(test_y, test_x[:-1])
    test = test.reset_index()
    test.columns = ['x','y']
    b2 = (
        Bar()
        .add_xaxis(list(test.x))
        .add_yaxis(titel2, list(test.y), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"))
        .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="right", type_='scroll', is_show=True),
            #datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
            #label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
            # title_opts=opts.TitleOpts(
            #     title="Top cloud providers 2018", subtitle="2017-2018 Revenue", pos_left="center"
            # ),
            toolbox_opts=opts.ToolboxOpts(orient='vertical', is_show=True),
        )
        .set_series_opts(
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="max", name="Maximum"),
                opts.MarkPointItem(type_="min", name="Minimum"),
            ]
        ),
        markline_opts=opts.MarkLineOpts(
            data=[
                opts.MarkLineItem(type_="max", name="Maximum"),
                opts.MarkLineItem(type_="average", name="Gennemsnit"),
            ]
        ),
    )

    )

    grid = (
        Grid()
        .add(b1, grid_opts=opts.GridOpts(pos_right="55%"))
        .add(b2, grid_opts=opts.GridOpts(pos_left="55%"))
        )
    return grid


with st.expander("Data oversigt"):
    container = st.container()
    col_1, col_2 = container.columns(2)

    container.write(df.head(1000))
    container.write(df_orig.head(1000))



################## OVERBLIK ###################################

with st.expander("Overblik"):
    container = st.container()

    container.header(', '.join(map(str, municipalities)))

    grid = grid_bar_pie(df['use'], 'Anvendelse', None, 'Antal')
    st_pyecharts(grid, height='400px')
    grid = grid_bar_pie(df['Seeb_beskrivelse'], 'Teknikområde', None, 'Antal')
    st_pyecharts(grid, height='400px')



############# Investering, besparelse, TBT, levetid overblik ################

with st.expander("Investering, besparelse, TBT, levetid"):
    container = st.container()
    col_top1, col_top2 = container.columns(2)

    liste = ['Investering', 'Levetid', 'Rentabilitet', 'Besparelse kroner', 'Besparelse CO2', 'Tilbagebetalingstid (TBT)']
    plots = col_top1.multiselect(
        "Hvilke ting skal vises?",
        options=liste,
    )

    if 'Investering' in plots:
        grid = grid_bar_bar(df['investment'], 'Under 300000', 30, 300000, 'Under 50000', 25, 50000, 'Investering')
        st_pyecharts(grid, height='400px')

    if 'Levetid' in plots:
        df.loc[df['investmentlifetime_new'] == np.inf,'investmentlifetime_new'] = np.nan
        df.loc[df['investmentlifetime_new'] <= 0,'investmentlifetime_new'] = np.nan
        df.loc[df['investmentlifetime_new'] >= 75,'investmentlifetime_new'] = np.nan
        grid = grid_bar_pie(df["investmentlifetime_new"].dropna(), 'Levetid', None, 'Name')
        st_pyecharts(grid, height='400px')

    if 'Rentabilitet' in plots:
        grid = grid_bar_bar(df['profitability'], 'Op til 15', 30, 15, 'Op til 3', 30, 3, 'Rentabilitet')
        st_pyecharts(grid, height='400px')

        # fig_hist, ax_hist = plt.subplots(figsize=(9, 4))
        # bin_width= 0.2
        # nbins = math.ceil((df["profitability"].max() - df["profitability"].min()) / bin_width)
        # fig_hist = px.histogram(df, x='profitability', color='use', nbins=nbins, range_x=(0,10), labels={'x':'Rentabilitet', 'y':'Antal forslag'}, title='Rentabilitet for alle forslag')
        # fig_hist.add_vline(x=1, line_dash = 'solid', line_color = 'red')
        # fig_hist.update_traces(opacity=1)
        # st.plotly_chart(fig_hist,use_container_width=True)

    if 'Besparelse kroner' in plots:
        grid = grid_bar_bar(df['original_cost'], 'Op til 100000', 100, 100000, 'Op til 5000', 50, 5000, 'Økonomisk besparelse')
        st_pyecharts(grid, height='400px')

        # fig_hist, ax_hist = plt.subplots(figsize=(9, 4))
        # bin_width= 2000
        # nbins = math.ceil((df["original_cost"].max() - df["original_cost"].min()) / bin_width)
        # fig_hist = px.histogram(df, x='original_cost', color='use', nbins=nbins, range_x=(0,100000), labels={'x':'Årlig besparelse [kr.]', 'y':'Antal forslag'}, title='Årlig besparelse kroner')
        # fig_hist.update_traces(opacity=1)
        # st.plotly_chart(fig_hist, use_container_width=True)

    if 'Besparelse CO2' in plots:
        CO2_1 = st.slider('Figur 1 op til hvor mange kg CO2? (stepsize 100)',  0., 100000., 10000., 100.)
        CO2_2 = st.slider('Figur 2 op til hvor mange kg CO2? (stepsize 1)',  0., 100., 10., 1.)
        grid = grid_bar_bar(df['original_CO2']/1000, 'Op til {} kg CO2'.format(CO2_1), 100, CO2_1, 'Op til {} kg CO2'.format(CO2_2), 100, CO2_2, 'Klimamæssig/CO2 besparelse')
        st_pyecharts(grid, height='400px')

        # fig_hist, ax_hist = plt.subplots(figsize=(9, 4))
        # bin_width= 100000
        # nbins = math.ceil((df["original_CO2"].max() - df["original_CO2"].min()) / bin_width)
        # fig_hist = px.histogram(df, x='original_CO2', color='use', nbins=nbins, range_x=(0,10000000), labels={'x':'Årlig besparelse [kr.]', 'y':'Antal forslag'}, title='Årlig besparelse CO2')
        # fig_hist.update_traces(opacity=1)
        # st.plotly_chart(fig_hist, use_container_width=True)

    if 'Tilbagebetalingstid (TBT)' in plots:
        df.loc[df['TBT'] == np.inf,'TBT'] = np.nan
        df.loc[df['TBT'] <= 0,'TBT'] = np.nan
        df.loc[df['TBT'] >= 200,'TBT'] = np.nan
        grid = grid_bar_bar(df['TBT'], 'Op til 10.000 kg CO2', 100, 100, 'Op til 10 kg CO2', 60, 60, 'Tilbagebetalingstid (TBT)')
        st_pyecharts(grid, height='400px')


# sankey_nodes = pd.DataFrame(df['Seeb_beskrivelse'].unique())
# sankey_nodes = pd.concat([sankey_nodes,pd.DataFrame(df['use'].unique())])
# sankey_nodes.columns = ['name']
# #st.write(sankey_nodes)
# sankey_nodes = sankey_nodes.to_dict('records')
#
# magi = pd.crosstab(df['Seeb_beskrivelse'], df['use'])
# #magi.reset_index()
# #st.write(magi)
# #magi.apply(lambda x: {'Source': x.index, 'target': x.name, 'value': magi.loc[[x.index, x.name]]})
# #magi = magi.apply(lambda x: {'Source': x.index, 'target': x.name, 'value': magi.loc[[x.index, x.name]]}, axis=1)
# #magi =magi.set_index(['source','magi.Index'])
# magi = magi.to_dict('index')
# #st.write(magi)
#
# #print(magi)
#
# nodes = [
#     {"name": "category1"},
#     {"name": "category2"},
#     {"name": "category3"},
#     {"name": "category4"},
#     {"name": "category5"},
#     {"name": "category6"},
# ]
#
# links = [
#     {"source": "category1", "target": "category3", "value": 50},
#     {"source": "category1", "target": "category2", "value": 10},
#     {"source": "category2", "target": "category3", "value": 15},
#     {"source": "category3", "target": "category4", "value": 20},
#     {"source": "category5", "target": "category3", "value": 5},
#     {"source": "category5", "target": "category6", "value": 25},
# ]
# #nodes = sankey_nodes
# #links = magi
#
# c = (
#     Sankey()
#     .add(
#         "sankey",
#         nodes,
#         links,
#         linestyle_opt=opts.LineStyleOpts(opacity=0.2, curve=0.5, color="source"),
#         label_opts=opts.LabelOpts(position="right"),
#     )
#     .set_global_opts(title_opts=opts.TitleOpts(title="Sankey"))
# )
# st_pyecharts(c, height='600px')

#################### FORSLAG ########################################

with st.expander("Forslag"):
    container = st.container()
    colfor_1, colfor_2 = container.columns((4,2))
    colfor_3, colfor_4 = container.columns((3,2))

hej = df['Seeb_beskrivelse'].value_counts().rename_axis('Værdi').reset_index(name='Antal')
fig, ax = plt.subplots(figsize=(9, 6))
fig = px.histogram(df, x="Seeb_beskrivelse", color="municipality", barmode='group', title='Fordeling af forslag på teknikområde')
fig.update_xaxes(tickangle=90)
fig.update_layout(height=600)
colfor_1.plotly_chart(fig,  use_container_width=True)

colfor_2.header('Antal forslag indenfor hvert teknikområde')
colfor_2.write(hej)

colfor_4.header('Vælg data til rapport')
renta = colfor_4.slider('Hvilken mindste rentabilitet ønsker de?',  0.7, 2.5, 1., 0.1)
invest = colfor_4.slider('Hvad må højeste enkelt investering være?',  0, 10000000, 4000000, 10000)
slider = colfor_4.slider('Hvilken mindste rentabilitet ønsker de dem?',  0.7, 2.55, 1.5, 0.1)
bespar = colfor_4.slider('Hvilken mindste enkelt besparelse i kr. ønsker de?',  0, 100000, 0, 1000)


def slider_filter(df):
    df = df.loc[df['investment'] <= invest]
    return df

df = slider_filter(df)

colfor_3.write('Alle forslag ud fra valgte kriterier')
container.table(df[['energylabel_id', 'investment', 'investmentlifetime', 'Adresse', 'use']].sort_values(['energylabel_id']))



################### Rapport med energiforslag #################################

with st.expander("Rapport med energiforslag"):
    container = st.container()
    coldown_1, coldown_2 = container.columns((3,2))
    coldown_3, coldown_4 = container.columns((3,2))

kolonne_valg = coldown_2.multiselect(
    "Hvilke kolonner?",
    options=df.columns,
    default=('energylabel_id', 'investment', 'investmentlifetime', 'Adresse', 'use')
)
Forslag = df[df.columns.intersection(kolonne_valg)]
Forslag.sort_values(['energylabel_id'])

@st.cache
def convert_df_csv(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

@st.cache
def convert_df_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    format1 = workbook.add_format({'num_format': '0.00'})
    worksheet.set_column('A:A', None, format1)
    writer.save()
    processed_data = output.getvalue()
    return processed_data

csv   = convert_df_csv(df)
excel = convert_df_excel(df)

coldown_1.download_button(
                          label='📥 Download forslag som excel fil',
                          data=excel,
                          file_name='Energimærkeforslag.xlsx',
                          mime='xlsx'
)
coldown_1.download_button(
                          label='📥 Download forslag som CSV fil',
                          data=csv,
                          file_name='Energimærkeforslag.csv',
                          mime='text/csv'
)


#st.balloons()
