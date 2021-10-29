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

pio.templates.default = "plotly_white"

# Custom imports
#from eksterne_funktioner import


st.set_page_config(layout="wide", page_title="Energimærke forslag", page_icon="andel_a.png")
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
# st.markdown("<style>body{background-color: Green;}</style>", unsafe_allow_html=True)

st.title("Filtrering af energimærkeforslag")

start_time = time.time()
st.sidebar.image("andel_logo_white_rgb.png")
st.sidebar.write("Version 1.0")
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
@st.experimental_singleton
def database_forbindelse():
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
    return cnxn

@st.experimental_memo
def hent_data(_cnxn, BBR_list, ener_list):
    if BBR_list:
        print('BBR Data')
        BBR = np.array(BBR_list)

        df_creation = pd.read_sql("""select top 10 *
                                  from energylabels.creation_data
                                  where municipalitynumber IN {}
                                  AND propertynumber IN {};""".format(
                                  tuple(BBR[:,0]), tuple(BBR[:,1])
                                  ), cnxn)
        print("--- Creation %s seconds ---" % (time.time() - start_time))

        df_build = pd.read_sql("""select *
                                from energylabels.building_data
                                where municipalitynumber IN {}
                                AND propertynumber IN {};""".format(
                                tuple(BBR[:,0]), tuple(BBR[:,1])
                                ), cnxn )
        print("--- Build %s seconds ---" % (time.time() - start_time))

        query = """
        SELECT top 1000000 *
        FROM energylabels.proposals
        WHERE municipalitynumber IN {}
        AND propertynumber IN {};""".format(
            tuple(BBR[:,0]), tuple(BBR[:,1])
        )

        pd.read_sql(query, con=cnxn, chunksize=n)
        print("--- Proposals %s seconds ---" % (time.time() - start_time))

    elif ener_list:
        print('Energimærke Data')
        df_creation = pd.read_sql("""select top 10 * from energylabels.creation_data where energylabel_id IN {}""", cnxn)
        print("--- Creation %s seconds ---" % (time.time() - start_time))

        df_build = pd.read_sql("""select * from energylabels.building_data where energylabel_id IN {}""", cnxn )
        print("--- Build %s seconds ---" % (time.time() - start_time))

        query = """
        SELECT top 1000000 *
        FROM energylabels.proposals
        """
        pd.read_sql(query, con=cnxn, chunksize=n)
        print("--- Proposals %s seconds ---" % (time.time() - start_time))

    else:
        print('Normal datahentning')
        df_creation = pd.read_sql(
            """select top 10 * from energylabels.creation_data""", cnxn)
        print("--- Creation %s seconds ---" % (time.time() - start_time))

        df_build = pd.read_sql(
            """select energylabel_id, municipalitynumber, propertynumber, streetname, housenumber, postalcode, postalcity, ownership, usecode, dwellingarea, commercialarea
            from energylabels.building_data
            where ownership = 'Municipality'""", cnxn)
        print("--- Build %s seconds ---" % (time.time() - start_time))
        eID_list = df_build['energylabel_id'].unique()

        #df_input = pd.read_sql(
        #    """select * from energylabels.input_data """, cnxn)
        #print("--- Result %s seconds ---" % (time.time() - start_time))

        #df_result = pd.read_sql(
        #    """select energylabel_id, resultforallprofitable_energylabelclassification, resultforallproposals_energylabelclassification, energylabelclassification from energylabels.result_data_energylabels """, cnxn)
        #print("--- Result %s seconds ---" % (time.time() - start_time))

        query = """
        SELECT *
        FROM energylabels.proposals
        WHERE energylabel_id
        IN {};
        """.format(tuple(eID_list))

        #'energylabel id', 'proposal id', 'shorttext', 'lifetime', 'investment', 'data_input_type', 'data_value'
        n = 100000
        dfs = []
        for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
            dfs.append(chunk)
        df_prop = pd.concat(dfs)
        print("--- Proposals %s seconds ---" % (time.time() - start_time))
    data_time = time.time() - start_time


    return df_creation, df_build, df_prop, data_time  #

cnxn = database_forbindelse()
df_creation, df_build, df_prop, data_time = hent_data(cnxn, BBR_list, ener_list)

# %% Data cleaning ################################################################################################################################


@st.experimental_memo
def data_cleaning_df_build(df):
    df.columns = df.columns.str.replace(" ", "_")
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
    df["address_long"] = (
        df[["postalcode", "postalcity", "streetname", "housenumber"]]
        .astype(str)
        .agg(" ".join, axis=1)
    )
    df["Adresse"] = (
        df[["streetname", "housenumber"]]
        .astype(str)
        .agg(" ".join, axis=1)
    )
    df["dwellingarea"] = pd.to_numeric(df["dwellingarea"])
    df["commercialarea"] = pd.to_numeric(df["commercialarea"])
    return df


@st.experimental_memo
def data_cleaning_df_prop(df):
    df.columns = df.columns.str.replace(" ", "_")
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
    return df


df_build = data_cleaning_df_build(df_build)
df_prop = data_cleaning_df_prop(df_prop)

# %% Sidebar ################################################################################################################################



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


@st.experimental_memo
def bygningstype(bygningstyper, municipalities):
    """ Filters if the data is in the municipalities chosen and in the types of buildings chosen.
    Also merges the proposals and building_data tables. """

    hist_values = df_build[df_build["municipality"].isin(municipalities)]
    energy = df_prop[df_prop["energylabel_id"].isin(hist_values["energylabel_id"])]
    energy = energy.merge(hist_values, on="energylabel_id")

    energy = energy[energy["ownership"].isin(bygningstyper)]
    return energy


energy = bygningstype(bygningstyper, municipalities)
my_bar = st.progress(0)

@st.experimental_memo
def individuelle_forslag(energy):
    proposals = energy
    j=0
    for i in tqdm(proposals["energylabel_id"].unique().astype("int64")):
        temp = proposals[proposals["energylabel_id"] == i]
        temp = temp.drop_duplicates(subset="proposal_id")
        proposals = proposals[proposals["energylabel_id"] != i]
        proposals = proposals.append(temp)
        j += 0.99/len(proposals["energylabel_id"].unique().astype("int64"))
        my_bar.progress(j)
    return proposals

proposals = individuelle_forslag(energy)

brugskode = st.sidebar.multiselect(
    "Hvilke anvendelser skal medtages?",
    options=list(np.unique(proposals["use"])),
)
adresse = st.sidebar.multiselect(
    "Hvilke adresser skal medtages?",
    options=list(np.unique(proposals["Adresse"])),
)

teknik = st.sidebar.multiselect(
    "Hvilke teknikområder?",
    options=list(proposals["Seeb_beskrivelse"].unique()),
    default=proposals["Seeb_beskrivelse"].unique()
)

def filtrer_sidebar(df):
    temp = []
    if brugskode:
        temp = df[df['use'].isin(brugskode)]
    if adresse:
        temp = temp
        temp = df[df['Adresse'].isin(adresse)]
    if teknik:
        temp = temp
        temp = df[df['Seeb_beskrivelse'].isin(teknik)]
    if temp is not None:
        temp = temp
        df = temp
    return df

proposals = filtrer_sidebar(proposals)



col0_1, col0_2, col0_3, col0_4 = st.columns(4)
col0_1.subheader("Antal energimærker i data")
col0_1.code(df_build["energylabel_id"].nunique())
col0_1.subheader("Antal energimærker i forslag")
col0_1.code(df_prop["energylabel_id"].nunique())
col0_2.subheader('Antal forslag')
col0_2.code(proposals.shape[0])
col0_2.subheader('Antal kategorier')
col0_2.code(proposals.shape[1])
col0_3.subheader("Tid for at hente data")
col0_3.code(data_time)
col0_4.subheader("Samlet investering i DKK")
col0_4.code(np.sum(proposals["investment"]))


################## OVERBLIK ###################################

with st.expander("Overblik"):
    container = st.container()
    colkom_1, colkom_2 = container.columns((3,2))
    colkom_3, colkom_4 = container.columns((3,2))

colkom_1.header(', '.join(map(str, municipalities)))

fig, ax = plt.subplots(figsize=(9, 6))
fig = px.histogram(proposals, x="use", color="municipality", barmode='group', title='Fordeling af forslag på anvendelse')
fig.update_xaxes(tickangle=45)
fig.update_layout(height=800)
colkom_1.plotly_chart(fig,  use_container_width=True)

hej = proposals['use'].value_counts().rename_axis('Værdi').reset_index(name='Antal')
fig = px.pie(hej, values='Antal', names='Værdi', hole=.3, title='Fordeling af forslag på anvendelse (alle valgte kunder)')
fig.update_traces(textposition='inside')
fig.update_layout(showlegend=False)
colkom_2.plotly_chart(fig)

fig, ax = plt.subplots(figsize=(9, 6))
fig = px.histogram(proposals, x="Seeb_beskrivelse", color="municipality", barmode='group', title='Fordeling af forslag på teknikområde')
fig.update_xaxes(tickangle=45)
fig.update_layout(height=600)
colkom_3.plotly_chart(fig,  use_container_width=True)

hej = proposals['Seeb_beskrivelse'].value_counts().rename_axis('Værdi').reset_index(name='Antal')
fig1 = px.pie(hej, values='Antal', names='Værdi', hole=.3, title='Fordeling af forslag på teknikområde (alle valgte kunder)')
fig1.update_traces(textposition='inside')
fig1.update_layout(showlegend=False)
colkom_4.plotly_chart(fig1)

############# Investering, besparelse, TBT, levetid ###########################

with st.expander("Investering, besparelse, TBT, levetid"):
    container = st.container()
    colinv_1, colinv_2 = container.columns(2)
    colinv_3, colinv_4 = container.columns((3,2))

fig_hist, ax_hist = plt.subplots(figsize=(9, 4))
fig_hist = px.histogram(proposals, x='investment', color='municipality', nbins=int(max(proposals['investment'])/5000), range_x=(0,300000), labels={'x':'Investering [kr.]', 'y':'Antal forslag'}, title='Investering under 300.000 kr.')
fig_hist.update_traces(opacity=0.75)
colinv_1.plotly_chart(fig_hist)

fig_hist, ax_hist = plt.subplots(figsize=(9, 4))
fig_hist = px.histogram(proposals, x='investment', color='municipality', nbins=int(max(proposals['investment'])/2500), range_x=(0,50000), labels={'x':'Investering [kr.]', 'y':'Antal forslag'}, title='Investering under 300.000 kr.')
fig_hist.update_traces(opacity=0.75)
colinv_2.plotly_chart(fig_hist)

fig_hist, ax_hist = plt.subplots(figsize=(9, 4))
fig_hist = px.histogram(proposals, x='lifetime', color='municipality', barmode='group', range_x=(0,50), labels={'x':'Investering [kr.]', 'y':'Antal forslag'}, title='Levetid')
fig_hist.update_traces(opacity=0.75)
colinv_3.plotly_chart(fig_hist)

hej = proposals['lifetime'].value_counts().rename_axis('Værdi').reset_index(name='Antal')
fig1 = px.pie(hej, values='Antal', names='Værdi', hole=.3, title='Fordeling af forslag på levetid (alle valgte kunder)')
fig1.update_layout()
colinv_4.plotly_chart(fig1)


#################### FORSLAG ########################################

with st.expander("Forslag"):
    container = st.container()
    colfor_1, colfor_2 = container.columns((3,2))
    colfor_3, colfor_4 = container.columns((3,2))

hej = proposals['Seeb_beskrivelse'].value_counts().rename_axis('Værdi').reset_index(name='Antal')
fig, ax = plt.subplots(figsize=(9, 6))
fig = px.histogram(proposals, x="Seeb_beskrivelse", color="municipality", barmode='group', title='Fordeling af forslag på teknikområde')
fig.update_xaxes(tickangle=45)
fig.update_layout(height=600)
colfor_1.plotly_chart(fig,  use_container_width=True)

colfor_2.header('Antal forslag indenfor hvert teknikområde')
colfor_2.write(hej)


#################### FORSLAG ########################################

with st.expander("Rapport med energiforslag"):
    container = st.container()
    coldown_1, coldown_2 = container.columns((3,2))
    coldown_3, coldown_4 = container.columns((3,2))

coldown_2.header('Vælg data til rapport')
slider = coldown_2.slider('Hvilken mindste rentabilitet ønsker de?',  0.7, 2.5, 1., 0.1)
kolonne_valg = coldown_2.multiselect(
    "Hvilke kolonner?",
    options=proposals.columns,
    default=('energylabel_id', 'proposal_id', 'Seeb_beskrivelse', 'investment', 'lifetime', 'Adresse', 'use')
)
coldown_2.write(list(kolonne_valg))
coldown_2.write(proposals.columns)

Forslag = proposals[proposals.columns.intersection(kolonne_valg)]
Forslag.sort_values(['energylabel_id','proposal_id'])


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

csv   = convert_df_csv(proposals)
excel = convert_df_excel(proposals)

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
coldown_1.write('Forslag eksempel')
coldown_1.write(Forslag.sort_values(['energylabel_id','proposal_id']))

################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
with st.expander(" "):
    container = st.container()


with st.expander("Overblik over indhentet data"):
    container = st.container()
    col1_1, col1_2 = container.columns(2)


fig, ax = plt.subplots(figsize=(9, 6))
hist_values = df_build[df_build["municipality"].isin(municipalities)]
ax = sns.countplot(x="ownership", hue="municipality", data=hist_values)
ax.tick_params(labelrotation=90)
ax.set_ylabel("Antal Energimærker")
ax.set_title("Fordeling af energimærker på bygningstype")
col1_1.pyplot(fig)

fig1, ax1 = plt.subplots(figsize=(9, 6))
area = df_build['dwellingarea'] + df_build['commercialarea']
hist_values['area'] = area
ax1 = sns.violinplot(x='use', y='area', hue="municipality", data=hist_values, kind="box")
ax1.tick_params(axis='both', labelrotation=90)
ax1.set_title("Fordeling af kvadratmeter i energimærkede bygninger")
col1_2.pyplot(fig1)

col1_1.write("df_build")
col1_1.write(df_build[0:10000])
col1_2.write("df_prop")
col1_2.write(df_prop[0:10000])


with st.expander("Investering og levetid visualiseret"):
    container2 = st.container()
    col2_1, col2_2 = container2.columns(2)

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
#ax_hist.set_xlim(0, 300000)
col2_1.pyplot(fig_hist)

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
col2_1.pyplot(fig_his)


fig_hist, ax_hist = plt.subplots(figsize=(9, 4))
ax_hist = sns.countplot(x="lifetime", hue="municipality", data=proposals)
ax_hist.tick_params(labelrotation=0)
ax_hist.set_title("Levetid")
col2_2.pyplot(fig_hist)


fig, ax = plt.subplots(figsize=(9, 4))
ax = sns.countplot(x="Seeb_beskrivelse", hue="municipality", data=proposals)
ax.tick_params(labelrotation=90)
col2_1.pyplot(fig)


option = col2_2.selectbox(
    "Find forslag til forbedringer for en adresse",
    options=np.unique(proposals["address_long"]),
)
col2_2.table(
    proposals[proposals["address_long"] == option][
        ["shorttext", "investment", "lifetime"]
    ].sort_values(by="investment")
)

with st.expander("Teknikområder og filtering visualiseret"):
    container3 = st.container()
    col3_1, col3_2 = container3.columns(2)


proposals["filtered_shorttext"] = proposals["shorttext"].astype(str)
proposals["not_filtered_shorttext"] = proposals["shorttext"].astype(str)


def filter(proposals):
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

    def filter_shorttext_SecondaryElectricHeat(data):
        Search_for_These_values = [
            "kedel",
        ]
        pattern = "|".join(Search_for_These_values)
        data.loc[
            ~data["filtered_shorttext"].str.contains(pattern, case=False),
            "filtered_shorttext",
        ] = "SEH Andet"
        return data

    def filter_shorttext_DistrictHeatWithExchanger(data):
        Search_for_These_values = [
            "kedel",
        ]
        pattern = "|".join(Search_for_These_values)
        data.loc[
            ~data["filtered_shorttext"].str.contains(pattern, case=False),
            "filtered_shorttext",
        ] = "DHWE Andet"
        return data

    def filter_shorttext_Ventilation(data):
        # Energirude
        data.loc[
            data["filtered_shorttext"].str.contains("dør", case=False),
            "filtered_shorttext",
        ] = "Dør"
        Search_for_These_values = [
            "kedel",
        ]
        pattern = "|".join(Search_for_These_values)
        data.loc[
            ~data["filtered_shorttext"].str.contains(pattern, case=False),
            "filtered_shorttext",
        ] = "Ventilation Andet"
        return data

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
    proposals[
        proposals["data_input_type"] == "SecondaryElectricHeat"
    ] = filter_shorttext_SecondaryElectricHeat(
        proposals[proposals["data_input_type"] == "SecondaryElectricHeat"]
    )
    proposals[
        proposals["data_input_type"] == "DistrictHeatWithExchanger"
    ] = filter_shorttext_DistrictHeatWithExchanger(
        proposals[proposals["data_input_type"] == "DistrictHeatWithExchanger"]
    )
    proposals[
        proposals["data_input_type"] == "Ventilation"
    ] = filter_shorttext_Ventilation(
        proposals[proposals["data_input_type"] == "Ventilation"]
    )

    proposals["filtered_shorttext"].str.replace("-", "", regex=True)

    return proposals


proposals = filter(proposals)

kategori = proposals  # [proposals["data_input_type"] == "SecondaryElectricHeat"]

col3_2.table(kategori["filtered_shorttext"].unique())
col3_1.write(kategori[0:1000])
# col3_1.write(kategori[kategori["filtered_shorttext"] == "Ventilation"])


fig, ax = plt.subplots(figsize=(9, 4))
ax = sns.countplot(x="Seeb_beskrivelse", hue="municipality", data=kategori)
ax.tick_params(labelrotation=90)
col3_1.pyplot(fig)



# fig, ax = plt.subplots(figsize=(9, 4))
# ax = sns.countplot(x="shorttext", hue="municipality", data=kategori)
# ax.tick_params(labelrotation=90)
# col4_2.pyplot(fig)

# fig, ax = plt.subplots(figsize=(20, 4))
# ax = sns.countplot(x="filtered_shorttext", hue="municipality", data=kategori)
# ax.tick_params(labelrotation=90)
# st.pyplot(fig)
print("All done")
print("--- Script finished in %s seconds ---" % (time.time() - start_time))
st.sidebar.write("--- Script finished in %.4s seconds ---" % (time.time() - start_time))
