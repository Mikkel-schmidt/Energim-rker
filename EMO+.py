#import pyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
import streamlit as st
from tqdm import tqdm
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb
import math
from pyecharts import options as opts
from pyecharts.charts import Bar, Pie, Grid, Line, Scatter, Sankey, WordCloud
from pyecharts.commons.utils import JsCode
from streamlit_echarts import st_pyecharts
import psycopg2 as db
import pulp
from pulp import *
import time
import urllib.request as request
import urllib
import json
import contextlib
import re

pd.options.display.float_format = '{:,}'.format

st.set_page_config(layout="wide", page_title="Energy+improvements", page_icon="andel_e.png")

st.image('Energy+.png', width=800)

start_time = time.time()
st.sidebar.image("andel_logo_white_rgb.png")
st.sidebar.write("Version 0.2")


# """
# The following section checks if any files are uploaded containing BBR information or energylabel IDs
# If they are then prepare them for the SQL queries below.
# """

with st.sidebar.expander("Upload BBR data"):
    st.write("Data skal stå på følgende måde:")
    st.code(
        """BBR Kommunenr, BBR Ejendomsnummer
BBR Kommunenr, BBR Ejendomsnummer
..."""
    )
    uploaded_BBR = st.file_uploader("BBR data", type=["txt", "csv"])
    BBR0 =[]
    BBR1 =[]
    BBR_list = []
    if uploaded_BBR is not None:
        BBR_list = np.loadtxt(uploaded_BBR, delimiter='\t', dtype="int")
        st.code(BBR_list)
        st.code(BBR_list[:,0])
        st.code(tuple(BBR_list[:,1]))
        BBR0 = tuple(BBR_list[:,0])
        BBR1 = tuple(BBR_list[:,1])
        print(BBR_list)
        print(BBR1)
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

with st.sidebar.expander('Søg efter firma eller kommune'):

    @st.cache
    def cvrapi(cvr, country='dk'):
      request_a = request.Request(
        url='http://cvrapi.dk/api?search=%s&country=%s' % (urllib.parse.quote(cvr), country),
        headers={
          'User-Agent': 'Andel'})
      with contextlib.closing(request.urlopen(request_a)) as response:
        return json.loads(response.read())

    cvrsøg = st.text_input('Hvilket firma søger du efter?')
    if cvrsøg:
        cvr = cvrapi(cvrsøg)

        if 'name' in cvr:
            st.write(cvr['name'])
        st.code(cvr)
        outer_dictionary = cvr
        list_of_dictionaries = outer_dictionary['productionunits']

        adress = []
        post = []
        for dictionary in list_of_dictionaries:
            if "address" in dictionary:
                adress.append(dictionary["address"])
                post.append(dictionary["zipcode"])

        st.code(adress)
        st.code(post)
        adress = pd.DataFrame(adress, columns = ['Adresse'])
        st.write(adress)
        adress[['streetname', 'housenumber', '', 'floor']] = adress['Adresse'].str.extract(r'([A-Za-z ]+)([\d]+)(,?(.*))')
        adress['streetname'] = adress['streetname'].str.strip()
    else:
        adress = []
        cvr = []
        #adress = re.split(r'(^[^\d]+)', string)[1:]
    st.code(adress)



# %% Data loading ###############################################################################################################################
@st.experimental_memo
def kommune_og_ejerskab():
    """
    Finder unikke værdier på kommune og ejerskab i den forbundne database.

    Parameters
    ----------
    None

    Returns
    -------
    kommune     en liste med de unikke kommuner
    ejerskab    en liste med de unikke ejerskabsformer
    """
    SERVER = "redshift.bi.obviux.dk"
    PORT = '5439'  # Redshift default
    USER = "mrs"
    PASSWORD = "j89Foijf8fIJFAD8dsIFJA8DFMasf_D7fa9df"
    DATABASE = "redshift"

    cnxn = db.connect(host=SERVER, database=DATABASE, user=USER, password=PASSWORD, port=PORT)

    query = """SELECT DISTINCT ownership
    FROM energylabels.building_data
    """
    n = 10000
    dfs = []
    for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        dfs.append(chunk)
    ejerskab = pd.concat(dfs)

    query = """SELECT DISTINCT municipalitynumber
    FROM energylabels.building_data
    """
    n = 10000
    dfs = []
    for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        dfs.append(chunk)
    kommune = pd.concat(dfs)

    kommune["Kommune"] = kommune["municipalitynumber"].astype(str)
    kommune["Kommune"].replace(
        {   "101": "København",
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
    ejerskab["ejerskab"] = ejerskab["ownership"].astype(str)
    ejerskab["ejerskab"].replace(
        {   'OtherMunicipality': 'Anden kommune',
            'State': 'Staten',
            'Municipality': 'Iboliggende kommune',
            'Other': 'Andet',
            'County': 'Region',
            'Company': 'Firmaer og selskaber',
            'Private': 'Privat',
            'NonProfitHousingAssociation': 'Almennyttig boligselskab',
            'IndependentInstitution': 'Forening, legat eller selvejende institution',
            'HousingAssociation': ' Privat andelsboligforening',
        },
        inplace=True,
    )
    return kommune, ejerskab

kommune, ejerskab = kommune_og_ejerskab()

# I denne sektion vælger man kommune og ejerskab.
# Koden stopper og giver advarsel hvis der ikke er valgt værdi på begge.

with st.sidebar.expander('Vælg kommune og ejerskab'):
    container = st.container()
    all = st.checkbox("Vælg alle kommuner")

    if all:
        municipalities = container.multiselect("Vælg dine yndlingskommuner",
            options=list(np.unique(kommune["Kommune"])),
            default=list(np.unique(kommune["Kommune"])))
    else:
        municipalities =  container.multiselect("Vælg dine yndlingskommuner",
            options=list(np.unique(kommune["Kommune"])))

    container = st.container()
    all2 = st.checkbox("Vælg alle ejerskabsformer")

    if all2:
        bygningstyper = container.multiselect("Hvilken ejerskabsform skal medtages",
            options=list(np.unique(ejerskab["ejerskab"])),
            default=list(np.unique(ejerskab["ejerskab"])))
    else:
        bygningstyper =  container.multiselect("Hvilken ejerskabsform skal medtages",
            options=list(np.unique(ejerskab["ejerskab"])))
    st.info('Vælg kun det data der skal bruges')

if not BBR0:
    if not ener_list:
        if not cvr:
            if not municipalities:
                st.info('Du skal vælge en kommune')
                st.stop()
            if not bygningstyper:
                st.info('Du skal vælge en ejerskabsform')
                st.stop()


kommune = kommune[kommune['Kommune'].isin(municipalities)]
ejerskab = ejerskab[ejerskab['ejerskab'].isin(bygningstyper)]


# %%
@st.experimental_memo
def hent_data(BBR0, BBR1, ener_list, kommune, ejerskab, cvr):
    """
    Forbinder til Redshift serveren med EMO data.
    Afhængig af inputs i BBR eller energimærkeID henter den de samme kolonner, men kun for energimærker fundet i BBR eller energimærkeID listen.
    Hvis ingen filer i BBR eller energimærkeID finder den alle energimærker der er i valgte kommuner og ejerskab.

        Parameters
        ----------
        BBR0            Liste af kommune BBR numre
        BBR1            Liste af ejendoms BBR numre
        ener_list       Liste af energimærkeID uploadet
        kommune         Liste af valgte kommuner
        ejerskab        Liste af ejerskabsformer

        Returns
        -------
        df              Pandas DataFrame med alle kolonnerne der står i querien kun fra energimærker i valgte begrænsninger (se ovenstående)
        data_time       Antal sekunder for at hente data
    """
    start_time = time.time()
    SERVER = "redshift.bi.obviux.dk"
    PORT = '5439'  # Redshift default
    USER = "mrs"
    PASSWORD = "j89Foijf8fIJFAD8dsIFJA8DFMasf_D7fa9df"
    DATABASE = "redshift"

    cnxn = db.connect(host=SERVER, database=DATABASE, user=USER, password=PASSWORD, port=PORT)

    print("Connected to Redshift")

    if BBR0:
        print('BBR Data')
        query = """
                WITH build AS
                (
                SELECT energylabel_id, CAST(build.propertynumber AS INT), build.building_id,, build.addressname build.ownership, build.reviewdate, CAST(build.municipalitynumber AS INT), build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE (municipalitynumber IN {}) AND (propertynumber IN {})
                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.longttext, prop_group.seebclassification,
                build.propertynumber, build.building_id, build.addressname, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea,
                result.status_energylabelclassification, result.resultforallprofitable_energylabelclassification,
                result.resultforallproposals_energylabelclassification, result.energylabelclassification

                FROM energylabels.results_fuelsavings AS fuel
                RIGHT JOIN build ON build.energylabel_id = fuel.energylabel_id
                LEFT JOIN energylabels.results_profitability AS prof ON build.energylabel_id = prof.energylabel_id AND fuel.proposal_group_id = prof.proposal_group_id
                LEFT JOIN energylabels.result_data_energylabels AS result ON build.energylabel_id = result.energylabel_id
                LEFT JOIN energylabels.proposal_groups AS prop_group ON build.energylabel_id = prop_group.energylabel_id AND fuel.proposal_group_id = prop_group.proposal_group_id

                ORDER BY build.energylabel_id, fuel.proposal_group_id, prof.proposal_group_id
                """.format(BBR0, BBR1)

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
                SELECT energylabel_id, build.propertynumber, build.building_id, build.addressname, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE energylabel_id IN {}
                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.longttext, prop_group.seebclassification,
                build.propertynumber, build.building_id, build.addressname, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
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

    elif cvr:
        print('Firma energimærker')
        query = """
                WITH build AS
                (
                SELECT energylabel_id, CAST(build.propertynumber AS INT), build.building_id, build.addressname, build.ownership, build.reviewdate, CAST(build.municipalitynumber AS INT), build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE (streetname IN {}) AND (housenumber IN {}) AND (postalcode in {})
                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.longttext, prop_group.seebclassification,
                build.propertynumber, build.building_id, build.addressname, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea,
                result.status_energylabelclassification, result.resultforallprofitable_energylabelclassification,
                result.resultforallproposals_energylabelclassification, result.energylabelclassification

                FROM energylabels.results_fuelsavings AS fuel
                RIGHT JOIN build ON build.energylabel_id = fuel.energylabel_id
                LEFT JOIN energylabels.results_profitability AS prof ON build.energylabel_id = prof.energylabel_id AND fuel.proposal_group_id = prof.proposal_group_id
                LEFT JOIN energylabels.result_data_energylabels AS result ON build.energylabel_id = result.energylabel_id
                LEFT JOIN energylabels.proposal_groups AS prop_group ON build.energylabel_id = prop_group.energylabel_id AND fuel.proposal_group_id = prop_group.proposal_group_id

                ORDER BY build.energylabel_id, fuel.proposal_group_id, prof.proposal_group_id
                """.format(tuple(adress['streetname'].replace("'","")), tuple(adress['housenumber']), tuple(post)) #.format(", ".join(adress['streetname']), ", ".join(adress['housenumber']) ) #
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
                SELECT energylabel_id, CAST(build.propertynumber AS INT), build.building_id, build.addressname, build.ownership, build.reviewdate,  CAST(build.municipalitynumber AS INT), build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE (municipalitynumber IN {}) AND (ownership IN {})

                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.longttext, prop_group.seebclassification,
                build.propertynumber, build.building_id, build.addressname, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea,
                result.status_energylabelclassification, result.resultforallprofitable_energylabelclassification,
                result.resultforallproposals_energylabelclassification, result.energylabelclassification

                FROM energylabels.results_fuelsavings AS fuel
                RIGHT JOIN build ON build.energylabel_id = fuel.energylabel_id
                LEFT JOIN energylabels.results_profitability AS prof ON build.energylabel_id = prof.energylabel_id AND fuel.proposal_group_id = prof.proposal_group_id
                LEFT JOIN energylabels.result_data_energylabels AS result ON build.energylabel_id = result.energylabel_id
                LEFT JOIN energylabels.proposal_groups AS prop_group ON build.energylabel_id = prop_group.energylabel_id AND fuel.proposal_group_id = prop_group.proposal_group_id

                ORDER BY build.energylabel_id, fuel.proposal_group_id, prof.proposal_group_id
                """.format(str(tuple(kommune['municipalitynumber'])).replace(',','') if len(kommune['municipalitynumber']) == 1 else tuple(kommune['municipalitynumber']),
                           str(tuple(ejerskab['ownership'])).replace(',','') if len(ejerskab['ownership']) == 1 else tuple(ejerskab['ownership']))
        n = 10000
        dfs = []
        for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
            dfs.append(chunk)
        df = pd.concat(dfs)
        print("--- df %s seconds ---" % (time.time() - start_time))
    data_time = time.time() - start_time

    return df, data_time  #

df_start, data_time = hent_data(BBR0, BBR1, ener_list, kommune, ejerskab, cvr)
dF_start = df_start.drop_duplicates(['energylabel_id', 'proposal_group_id', 'fuelsaved', 'material'], keep='first', inplace=True)
emo_antal = df_start['energylabel_id'].nunique()
# %% Data cleaning ################################################################################################################################
@st.experimental_memo
def oversat_kolonnenavne(df):
    """
    Oversætter alle kolonnenavnene

        Parameters
        ----------
        df              Rå Pandas DataFrame fra hent data

        Returns
        -------
        df              Pandas DataFrame med oversatte kolonnenavne
    """
    df.rename(columns={
                       'energylabel_id': 'EnergimærkeID',
                       'proposal_group_id': 'Forslagets gruppe ID',
                       'fuelsaved': 'Brændstof',
                       'material': 'Materiale',
                       'unit': 'Enhed',
                       'energyperunit': 'kWh per enhed',
                       'co2perunit': 'CO2 per enhed [g]',
                       'costperunit': 'Besparelse kr. per enhed',
                       'fixedcostperyear': 'Faste årlige omkostninger',
                       'original_cost': 'Besparelse [kr.]',
                       'profitability': 'Rentabilitet',
                       'investment': 'Investering',
                       'investmentlifetime': 'Levetid',
                       'shorttext': 'Tekst',
                       'longttext': 'Uddybende tekst',
                       'seebclassification': 'Tekniknr',
                       'propertynumber': 'ejendomsnr',
                       'building_id': 'bygningsnr',
                       'ownership': 'ejerskab',
                       'reviewdate': 'besigtigelsesdato',
                       'municipalitynumber': 'kommunenr',
                       'addressname': 'Adressenavn',
                       'streetname': 'vejnavn',
                       'housenumber': 'husnr',
                       'postalcode': 'postnr',
                       'postalcity': 'by',
                       'usecode': 'brugskode',
                       'dwellingarea': 'beboelsesareal',
                       'commercialarea': 'Erhvervs areal',
                       'status_energylabelclassification': 'klassificering_status',
                       'resultforallprofitable_energylabelclassification': 'klassificering rentable',
                       'resultforallproposals_energylabelclassification': 'klassificering alle',
                       'energylabelclassification': 'klassificering',
                       }, inplace=True)
    return df



@st.experimental_memo
def data_cleaning(df):
    """
    Oversætter talværdier til kommunenavne, anvendelsesnavne, teknikområde, håndværkstype
    Oversætter materiale og ejerskab til dansk
    Laver nye kolonner om adresse, areal og år

        Parameters
        ----------
        df              Pandas Dataframe givet ved oversat_kolonnenavne funktionen.

        Returns
        -------
        df              Pandas DataFrame der har de ovenstående rettelser ift. input.
    """
    df = oversat_kolonnenavne(df)

    df["Kommune"] = df["kommunenr"].astype(str)
    df["Kommune"].replace(
        {   "101": "København",
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
    df["Anvendelse"] = df["brugskode"]
    df["Anvendelse"].replace(
        {
            "110": "Stuehus til landbrugsejendom",
            "120": "Fritliggende enfamiliehus",
            "121": "Sammenbygget enfamiliehus",
            "122": "Fritliggende enfamiliehus i tæt-lav bebyggelse",
            "130": "Række-, kæde-, eller dobbelthus",
            "131": "Række-, kæde- og klyngehus",
            "132": "Dobbelthus",
            "140": "Etagebolig-bygning, flerfamiliehus eller to-familiehus",
            "150": "Kollegium",
            "160": "Boligbygning til døgninstitution",
            "185": "Anneks i tilknytning til helårsbolig",
            "190": "Anden helårsbeboelse",
            "210": "Erhvervsmæssig produktion vedrørende landbrug, gartneri, råstofudvinding o. lign",
            "211": "Stald til svin",
            "212": "Stald til kvæg, får mv.",
            "213": "Stald til fjerkræ",
            "214": "Minkhal",
            "215": "Væksthus",
            "216": "Lade til foder, afgrøder mv.",
            "217": "Maskinhus, garage mv.",
            "218": "Lade til halm, hø mv.",
            "219": "Anden bygning til landbrug mv.",
            "220": "Erhvervsmæssig produktion vedrørende industri, håndværk m.v.",
            "221": "Industri med integreret produktionsapparat",
            "222": "Industri uden integreret produktionsapparat ",
            "223": "Værksted",
            "229": "Anden til produktion",
            "230": "El-, gas-, vand- eller varmeværk, forbrændingsanstalt m.v.",
            "231": "Energiproduktion",
            "232": "Forsyning- og energidistribution",
            "233": "Vandforsyning",
            "234": "Til håndtering af affald og spildevand",
            "239": "Anden til energiproduktion og -distribution",
            "290": "Anden til landbrug, industri etc.",
            "310": "Transport- og garageanlæg",
            "311": "Jernbane- og busdrift",
            "312": "Luftfart",
            "313": "Parkering- og transportanlæg",
            "314": "Parkering af flere end to køretøjer i tilknytning til boliger",
            "315": "Havneanlæg",
            "319": "Andet transportanlæg",
            "320": "Til kontor, handel, lager, herunder offentlig administration",
            "321": "Kontor",
            "322": "Detailhandel",
            "323": "Lager",
            "324": "Butikscenter",
            "325": "Tankstation",
            "329": "Anden til kontor, handel og lager",
            "330": "Til hotel, restaurant, vaskeri, frisør og anden servicevirksomhed",
            "331": "Hotel, kro eller konferencecenter med overnatning",
            "332": "Bed & breakfast mv.",
            "333": "Restaurant, café og konferencecenter uden overnatning",
            "334": "Privat servicevirksomhed som frisør, vaskeri, netcafé mv.",
            "339": "Anden til serviceerhverv",
            "390": "Anden til transport, handel etc",
            "410": "Biograf, teater, erhvervsmæssig udstilling, bibliotek, museum, kirke o. lign.",
            "411": "Biograf, teater, koncertsted mv.",
            "412": "Museum",
            "413": "Bibliotek",
            "414": "Kirke eller anden til trosudøvelse for statsanerkendte trossamfund",
            "415": "Forsamlingshus",
            "416": "Forlystelsespark",
            "419": "Anden til kulturelle formål",
            "420": "Undervisning og forskning (skole, gymnasium, forskningslabratorium o.lign.).",
            "421": "Grundskole",
            "422": "Universitet",
            "429": "Anden til undervisning og forskning",
            "430": "Hospital, sygehjem, fødeklinik o. lign.",
            "431": "Hospital og sygehus",
            "432": "Hospice, behandlingshjem mv.",
            "433": "Sundhedscenter, lægehus, fødeklinik mv.",
            "439": "Anden til sundhedsformål",
            "440": "Daginstitution",
            "441": "Daginstitution",
            "442": "Servicefunktion på døgninstitution",
            "443": "Kaserne",
            "444": "Fængsel, arresthus mv.",
            "449": "Anden til institutionsformål",
            "490": "Anden institution, herunder kaserne, fængsel o. lign.",
            "510": "Sommerhus",
            "520": "Feriekoloni, vandrehjem o.lign. bortset fra sommerhus",
            "521": "Feriecenter, center til campingplads mv.",
            "522": "Ferielejligheder til erhvervsmæssig udlejning",
            "523": "Ferielejligheder til eget brug",
            "529": "Anden til ferieformål",
            "530": "I forbindelse med idrætsudøvelse (klubhus, idrætshal, svømmehal o. lign.)",
            "531": "Klubhus i forbindelse med fritid og idræt",
            "532": "Svømmehal",
            "533": "Idrætshal",
            "534": "Tribune i forbindelse med stadion",
            "535": "Til træning og opstaldning af heste",
            "539": "Anden til idrætformål",
            "540": "Kolonihavehus",
            "585": "Anneks i tilknytning til fritids- og sommerhus",
            "590": "Anden til fritidsformål",
            "910": "Garage (med plads til et eller to køretøjer)",
            "920": "Carport",
            "930": "Udhus",
            "940": "Drivhus",
            "950": "Fritliggende overdækning",
            "960": "Fritliggende udestue",
            "970": "Tiloversbleven landbrugsbygning",
            "990": "Faldefærdig bygning",
            "999": "Ukendt bygning",
        },
        inplace=True,
    )
    df["adresse lang"] = df.postnr + ' ' + df.by + ' ' + df.vejnavn + ' ' + df.husnr
    df["Adresse"] = df.vejnavn + ' ' + df.husnr
    df["beboelsesareal"] = pd.to_numeric(df["beboelsesareal"])
    df["Erhvervs areal"] = pd.to_numeric(df["Erhvervs areal"])
    df['areal'] = df['beboelsesareal'] + df["Erhvervs areal"]
    df['Teknikområde'] = df['Tekniknr']
    df['Teknikområde'].replace(
        {
            "1-0-0-0": "Bygningen",
            "1-1-0-0": "Tag og loft",
            "1-1-1-0": "Loft",
            "1-1-2-0": "Fladt tag",
            "1-2-0-0": "Ydervægge",
            "1-2-1-0": "Hule ydervægge",
            "1-2-2-0": "Massive ydervægge",
            "1-2-3-0": "Lette ydervægge",
            "1-2-1-1": "Hule vægge uopvarmet rum",
            "1-2-2-1": "Vægge mod uopvarmet rum",
            "1-2-3-1": "Let væg mod uopvarmet rum",
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
    df["håndværkstype"] = df['Tekniknr']
    df["håndværkstype"].replace(
        {
            '1-0-0-0':		'Murer/tømrer',
            '1-1-0-0':		'Murer/tømrer',
            '1-1-1-0':		'Murer/tømrer',
            '1-1-2-0':		'Murer/tømrer',
            '1-2-0-0':		'Murer/tømrer',
            '1-2-1-0':		'Murer/tømrer',
            '1-2-2-0':		'Murer/tømrer' ,
            '1-2-3-0':		'Murer/tømrer' ,
            '1-2-1-1':		'Murer/tømrer' ,
            '1-2-2-1':		'Murer/tømrer' ,
            '1-2-3-1':		'Murer/tømrer' ,
            '1-2-4-0':		'Murer/tømrer' ,
            '1-3-0-0':		'Murer/tømrer' ,
            '1-3-1-0':		'Murer/tømrer' ,
            '1-3-2-0':		'Murer/tømrer' ,
            '1-3-3-0':		'Murer/tømrer' ,
            '1-4-0-0':		'Murer/tømrer' ,
            '1-4-1-0':		'Murer/tømrer' ,
            '1-4-2-0':		'Murer/tømrer' ,
            '1-4-3-0':		'Murer/tømrer' ,
            '1-4-4-0':		'Murer/tømrer' ,
            '1-4-1-1':		'Murer/tømrer' ,
            '1-4-2-1':		'Murer/tømrer' ,
            '1-4-3-1':		'Murer/tømrer' ,
            '1-4-4-1':		'Murer/tømrer' ,
            '1-4-5-0':		'Murer/tømrer' ,
            '1-5-0-0':	 	'Ventilation',
            '1-5-1-0':	 	'Ventilation',
            '1-5-2-0':	 	'Ventilation' ,
            '1-5-3-0':	 	'Ventilation' ,
            '1-6-0-0':	 	'VVS' ,
            '1-6-1-0':	 	'VVS',
            '2-0-0-0':	 	'VVS' ,
            '2-1-0-0':	 	'VVS' ,
            '2-1-1-0':	 	'VVS' ,
            '2-1-2-0':	 	'VVS',
            '2-1-3-0':	 	'VVS',
            '2-1-4-0':	 	'VVS' ,
            '2-1-6-0':		'Elektriker',
            '2-1-5-0':		'Elektriker',
            '2-2-0-0':	 	'VVS' ,
            '2-2-1-0':	 	'VVS' ,
            '2-2-2-0':	 	'VVS',
            '2-2-3-0':	 	'VVS',
            '2-2-4-0':	 	'VVS',
            '3-0-0-0':	 	'VVS' ,
            '3-1-0-0':	 	'VVS' ,
            '3-1-1-0':	 	'VVS' ,
            '3-1-3-0':	 	'VVS',
            '3-1-2-0':	 	'VVS',
            '3-1-4-0':	 	'VVS',
            '3-1-5-0':	 	'VVS',
            '3-2-0-0':	 	'VVS' ,
            '3-2-1-0':	 	'VVS' ,
            '4-0-0-0':	 	'Elektriker' ,
            '4-1-0-0':	 	'Elektriker' ,
            '4-1-1-0':		'Elektriker',
            '4-1-2-0':	 	'Elektriker' ,
            '4-1-3-0':		'Elektriker',
            '4-1-4-0':		'Elektriker',
        },
        inplace=True,
    )
    df["Enhed"].replace({"CubicMeter": "m^3",}, inplace=True,)
    df["Materiale"].replace(
        {   'DistrictHeat': 'Fjernvarme',
            'Electricity': 'El',
            'NaturalGas': 'Naturgas',
            'FuelOil': 'Olie',
            'Wood': 'Træ',
            'CityGas': 'Bygas',
            'WoodPellets': 'Træpiller',
            'FuelGasOil': 'Fyringsgasolie',
            'Briquettes': 'Briketter?',
            'RapeOil': 'Rapsolie',
            'Straw': 'Halm',
        },
        inplace=True,
    )
    df["ejerskab"].replace(
        {   'OtherMunicipality': 'Anden kommune',
            'State': 'Staten',
            'Municipality': 'Iboliggende kommune',
            'Other': 'Andet',
            'County': 'Region',
            'Company': 'Firmaer og selskaber',
            'Private': 'Privat',
            'NonProfitHousingAssociation': 'Almennyttig boligselskab',
            'IndependentInstitution': 'Forening, legat eller selvejende institution',
            'HousingAssociation': ' Privat andelsboligforening',
        },
        inplace=True,
    )
    df["Investering"] = pd.to_numeric(df["Investering"])
    df['Levetid'] = pd.to_numeric(df['Levetid'])
    df["energimærkeId"] = df["EnergimærkeID"].astype("int64")
    df["Forslagets gruppe ID"] = pd.to_numeric(df["Forslagets gruppe ID"])
    df['år'] = pd.to_datetime(df['besigtigelsesdato'], errors = 'coerce').dt.year
    df['Besparelse kr. per enhed'] = pd.to_numeric(df['Besparelse kr. per enhed'])

    return df

df = data_cleaning(df_start)


# Mulighed for ekstra filtrering på baggrund af anvendelse, adresse eller teknikområde.

with st.sidebar.expander('Vælg anvendelse, specifikke adresser eller teknikområde'):
    brugskode = st.multiselect(
        "Hvilke anvendelser skal medtages?",
        options=list(np.unique(df["Anvendelse"])),
    )
    adresse = st.multiselect(
        "Hvilke adresser skal medtages?",
        options=list(np.unique(df["Adresse"])),
    )
    teknik = st.multiselect(
        "Hvilke teknikområder?",
        options=list(df['Teknikområde'].unique()),
        default=df['Teknikområde'].unique()
    )

@st.experimental_memo
def filtrer_sidebar(df, brugskode, adresse, teknik):
    """
    Hvis der er valgte begrænsninger i anvendelse, adresse eller teknikområder skal den filtreres på disse.
    """
    df = df
    if brugskode:
        df = df[df['Anvendelse'].isin(brugskode)]
    if adresse:
        df = df[df['Adresse'].isin(adresse)]
    if teknik:
        df = df[df['Teknikområde'].isin(teknik)]
    return df

df = filtrer_sidebar(df, brugskode, adresse, teknik)
# df_orig = df
emo_antal_orig = df["EnergimærkeID"].nunique()


# Denne sektion giver mulighed for at opdatere priser og udledning på brændelstyper.

with st.sidebar.expander('Justér pris og CO2 på enheder'):
    st.subheader('El')
    elpris   = st.number_input('Nuværende elpris [kr/kWh]', min_value=0.0, max_value=5., value=2.08, step=0.01)
    elCO2    = st.number_input('CO2 per kWh El [g/kWh]', min_value=0., max_value=500., value=128., step=1.)
    st.subheader('Fjernvarme')
    DHpris   = st.number_input('Nuværende fjernvarmepris [kr/kWh]', min_value=0.0, max_value=1000., value=650., step=1.)
    DHCO2    = st.number_input('CO2 per kWh fjernvarme [g/kWh]', min_value=0., max_value=300., value=141., step=1.)
    st.subheader('Naturgas')
    NGpris   = st.number_input('Nuværende naturgaspris [kr/kWh]', min_value=0.0, max_value=15., value=9.0, step=0.01)
    NGCO2    = st.number_input('CO2 per kWh naturgas [g/kWh]', min_value=0., max_value=300., value=204., step=1.)
    st.subheader('Bygas')
    CGpris   = st.number_input('Nuværende bygaspris [kr/kWh]', min_value=0.0, max_value=15., value=9.0, step=0.01)
    CGCO2    = st.number_input('CO2 per kWh bygas [g/kWh]', min_value=0., max_value=300., value=204., step=1.)
    st.subheader('Fyringsgasolie')
    FGOpris  = st.number_input('Nuværende fyringsgasoliepris [kr/kWh]', min_value=0.0, max_value=15., value=9.0, step=0.01)
    FGOCO2   = st.number_input('CO2 per kWh fyringsgasolie [g/kWh]', min_value=0., max_value=300., value=266., step=1.)
    st.subheader('Olie')
    Oliepris = st.number_input('Nuværende oliepris [kr/kWh]', min_value=0.0, max_value=15., value=9.0, step=0.01)
    OlieCO2  = st.number_input('CO2 per kWh olie [g/kWh]', min_value=0., max_value=300., value=281., step=1.)
    st.subheader('Træ')
    woodpris = st.number_input('Nuværende pris for træ [kr/kWh]', min_value=0.0, max_value=15., value=9.0, step=0.01)
    woodCO2  = st.number_input('CO2 per kWh træ [g/kWh]', min_value=0., max_value=300., value=0., step=1.)
    st.subheader('Træpiller')
    WPpris   = st.number_input('Nuværende træpillepris [kr/kWh]', min_value=0.0, max_value=15., value=9.0, step=0.01)
    WPCO2    = st.number_input('CO2 per kWh træpiller [g/kWh]', min_value=0., max_value=300., value=0., step=1.)
    st.subheader('Rapsolie')
    Rapspris   = st.number_input('Nuværende rapsoliepris [kr/kWh]', min_value=0.0, max_value=15., value=9.0, step=0.01)
    RapsCO2    = st.number_input('CO2 per kWh rapsolie [g/kWh]', min_value=0., max_value=300., value=0., step=1.)
    st.subheader('Halm')
    Halmpris   = st.number_input('Nuværende Halmpris [kr/kWh]', min_value=0.0, max_value=15., value=9.0, step=0.01)
    HalmCO2    = st.number_input('CO2 per kWh Halm [g/kWh]', min_value=0., max_value=300., value=0., step=1.)

@st.experimental_memo
def udregn_co2(df):
    """
    Justerer CO2 udledningerne på brændselstyper

        Parameters
        ----------
        df              Rå Pandas DataFrame

        Returns
        -------
        df              Pandas DataFrame
    """
    df['CO2 per enhed [g] rådata'] = df['CO2 per enhed [g]']
    df['besparelse kr. per enhed_orig'] = df['Besparelse kr. per enhed']

    #df['Besparelse kr. per enhed'][(df['Materiale'] == 'El')] = elpris
    df['CO2 per enhed [g]'][(df['Materiale'] == 'El')] = elCO2
    df['CO2 per enhed [g]'][(df['Materiale'] == 'Fjernvarme')] = DHCO2
    df['CO2 per enhed [g]'][(df['Materiale'] == 'Naturgas')] = NGCO2
    df['CO2 per enhed [g]'][(df['Materiale'] == 'Bygas')] = CGCO2
    df['CO2 per enhed [g]'][(df['Materiale'] == 'Fyringsgasolie')] = FGOCO2
    df['CO2 per enhed [g]'][(df['Materiale'] == 'Olie')] = OlieCO2
    df['CO2 per enhed [g]'][(df['Materiale'] == 'Træ')] = woodCO2
    df['CO2 per enhed [g]'][(df['Materiale'] == 'Træpiller')] = WPCO2
    df['CO2 per enhed [g]'][(df['Materiale'] == 'Rapsolie')] = RapsCO2
    df['CO2 per enhed [g]'][(df['Materiale'] == 'Halm')] = HalmCO2
    return df

df = udregn_co2(df)
df_orig = udregn_co2(df)

@st.experimental_memo
def fuel_calculations(df):
    """
    Udregner forslagenes samlede besparelse ift. kun besparelse på hver enkel brændstoftype - hvert forslag kan have flere brændstoftyper
    Filtrerer desuden på meget store eller meget lave værdier hvor der er fejl i datasættet.

        Parameters
        ----------
        df              Pandas DataFrame

        Returns
        -------
        df              Pandas DataFrame med en række per forslag og samlet besparelse
        df_s            Pandas DataFrame med alle rettelser inden forslag samles til en række per forslag.

    """
    df = df[df['Brændstof'].notna()]
    df['Brændstof rådata'] = df['Brændstof']
    df['Besparelse Kr. mid'] = df['Brændstof'] * df['Besparelse kr. per enhed']
    df['Brændstof'][(df['Materiale'] == 'Træpiller') & (df['Brændstof'] > 1000)] = df['Brændstof'][(df['Materiale'] == 'Træpiller') & (df['Brændstof'] > 1000)]/1000
    df['Brændstof'][(df['Materiale'] == 'Træpiller') & (df['Brændstof'] < -1000)] = df['Brændstof'][(df['Materiale'] == 'Træpiller') & (df['Brændstof'] < -1000)]/1000
    df['Brændstof'][(df['Materiale'] == 'Fjernvarme') & (df['Besparelse Kr. mid']/df['Investering'] > 1)]  = df['Brændstof'][(df['Materiale'] == 'Fjernvarme') & (df['Besparelse Kr. mid']/df['Investering'] > 2)]/1000
    df['Brændstof'][(df['Materiale'] == 'Fjernvarme') & (df['Besparelse Kr. mid']/df['Investering'] < -1)] = df['Brændstof'][(df['Materiale'] == 'Fjernvarme') & (df['Besparelse Kr. mid']/df['Investering'] < -2)]/1000


    df['Besparelse kr. per enhed'][(df['Enhed'] == 'MWh') & (df['Besparelse kr. per enhed'] > 1000)] = df['Besparelse kr. per enhed'][(df['Enhed'] == 'MWh') & (df['Besparelse kr. per enhed'] > 1000)]/1000
    # df['CO2 per enhed [g]'][(df['Enhed'] == 'MWh') & (df['Besparelse kr. per enhed'] > 1000)] = df['CO2 per enhed [g]'][(df['Enhed'] == 'MWh') & (df['Besparelse kr. per enhed'] > 1000)]/1000
    df['Besparelse Kr. mid'] = df['Brændstof'] * df['Besparelse kr. per enhed']
    df['Besparelse CO2 [g]'] = df['Brændstof'] * df['CO2 per enhed [g]'] * df['kWh per enhed']
    df['Besparelse [kWh]'] = df['Brændstof'] * df['kWh per enhed']

    df['Rentabilitet'][(df['Rentabilitet'] >= 30)] = ((df['Besparelse [kr.]'][(df['Rentabilitet'] >= 30)] * df['Levetid'][(df['Rentabilitet'] >= 30)]) / df['Investering'][(df['Rentabilitet'] >= 30)])
    df.loc[df['Rentabilitet'] < 0,'Rentabilitet'] =  np.nan
    df_s = df

    df = df.groupby(['EnergimærkeID', 'Forslagets gruppe ID', 'Adresse']).agg({
                                                                                               'Brændstof': 'max',
                                                                                               'Brændstof rådata': 'max',
                                                                                               'Materiale': ', '.join,
                                                                                               'Enhed': ', '.join,
                                                                                               'kWh per enhed': 'first',
                                                                                               'CO2 per enhed [g]': 'first',
                                                                                               'CO2 per enhed [g] rådata': 'first',
                                                                                               'Besparelse kr. per enhed': 'first',
                                                                                               'Faste årlige omkostninger': 'sum',
                                                                                               'Besparelse [kr.]': 'sum',
                                                                                               'Besparelse Kr. mid': 'sum',
                                                                                               'Besparelse CO2 [g]': 'sum',
                                                                                               'Besparelse [kWh]': 'sum',
                                                                                               'Rentabilitet': 'first',
                                                                                               'Investering': 'first',
                                                                                               'Levetid': 'first',
                                                                                               'Tekst': 'first',
                                                                                               'Uddybende tekst': 'first',
                                                                                               'Tekniknr': 'first',
                                                                                               'Teknikområde': 'first',
                                                                                               'håndværkstype': 'first',
                                                                                               'ejendomsnr': 'first',
                                                                                               'bygningsnr': ', '.join,
                                                                                               'ejerskab': 'first',
                                                                                               'besigtigelsesdato': 'first',
                                                                                               'år': 'first',
                                                                                               'kommunenr': 'first',
                                                                                               'Kommune': 'first',
                                                                                               'Adressenavn': 'first',
                                                                                               'vejnavn': 'first',
                                                                                               'husnr': 'first',
                                                                                               'postnr': 'first',
                                                                                               'by': 'first',
                                                                                               'brugskode': 'first',
                                                                                               'Anvendelse': 'first',
                                                                                               'beboelsesareal': 'first',
                                                                                               'Erhvervs areal': 'first',
                                                                                               'klassificering_status': 'first',
                                                                                               'klassificering rentable': 'first',
                                                                                               'klassificering alle': 'first',
                                                                                               'klassificering': 'first',
                                                                                               'adresse lang': 'first',
                                                                                               'areal': 'first'
                                                                                               })
    df = df.reset_index()
    df = df.drop_duplicates(subset=(['EnergimærkeID', 'Forslagets gruppe ID', 'Adresse']))
    df['levetid_ny'] = df['Levetid'].fillna((df['Rentabilitet']*df['Investering'])/df['Besparelse [kr.]']).round()

    df['Besparelse [kr.]_orig'] = df['Besparelse [kr.]']
    df['besp_diff'] = (df['Besparelse [kr.]']-df['Besparelse Kr. mid']).round()
    df['Besparelse [kr.]'] = np.where(df['besp_diff'] > 0, np.where(df['Besparelse Kr. mid'] > 0, df['Besparelse Kr. mid'], df['Besparelse [kr.]_orig']), np.where(df['Besparelse [kr.]_orig'] < 0, df['Besparelse Kr. mid'], df['Besparelse [kr.]_orig']))
    #Hvis Besparelse Kr. mid mindre end nul brug Besparelse [kr.]. Hvis differencen er større end nul (mindre besparelse) så brug denne. Dette fjerner store misberegninger i original data.

    df['rentabilitet_ny'] = ((df['Besparelse [kr.]'] * df['levetid_ny']) / df['Investering'])
    df['TBT'] = df['Investering'] / df['Besparelse [kr.]']
    df['TBT'] = df['TBT']

    return df, df_s

df, df_s = fuel_calculations(df)



# Visualisér basis værdier på valgte energimærker.

col0_1, col0_2, col0_3, col0_4 = st.columns(4)
col0_1.metric('Antal forslag i valgt data', df.shape[0])
col0_1.metric('Energimærker i valgt data', emo_antal_orig, emo_antal_orig-df["EnergimærkeID"].nunique())
col0_2.metric('Samlet årlig energibesparelse', '{:.2f} mio. kWh'.format(df['Besparelse [kWh]'].sum()/1000000))
col0_2.metric('CO2 sparepris', '{:.2f} Kr./kg'.format(df["Investering"].sum()/(df['Besparelse CO2 [g]'].sum()/1000)))
col0_3.metric('Samlet årlig økonomisk besparelse', '{:.2f} mio. Kr/år.'.format(df['Besparelse [kr.]'].sum()/1000000))
col0_3.metric('Samlet årlig klimamæssig besparelse', '{:.2f} Ton/år.'.format(df['Besparelse CO2 [g]'].sum()/1000000))
col0_4.metric("Samlet investering i DKK", '{:.2f} mio. Kr.'.format(np.sum(df["Investering"])/1000000))
col0_4.metric("Median rentabilitet", '{:.2f}'.format(df['Rentabilitet'].median()))

with st.expander("Data oversigt"):
    container = st.container()
    col_1, col_2 = container.columns(2)

    # # container.write(df.head(100000).style.background_gradient(cmap='Blues').set_precision(2))
    #container.write(df_s[['Forslagets gruppe ID', 'Adresse', 'Adressenavn', 'Teknikområde', 'Tekst', 'Investering', 'Materiale', 'Brændstof', 'Brændstof rådata', 'år', 'Besparelse [kr.]', 'Besparelse Kr. mid', 'Besparelse CO2 [g]', 'CO2 per enhed [g]', 'CO2 per enhed [g] rådata', 'Besparelse kr. per enhed', 'Rentabilitet']])
    #container.write(df.head(10000))
    container.write(df[['Forslagets gruppe ID', 'Adresse', 'postnr', 'by', 'Adressenavn', 'Teknikområde', 'Tekst', 'Investering', 'Besparelse [kr.]', 'TBT', 'Materiale', 'Brændstof', 'Brændstof rådata', 'år', 'Besparelse Kr. mid', 'Besparelse [kr.]_orig', 'besp_diff', 'Besparelse CO2 [g]', 'CO2 per enhed [g]', 'CO2 per enhed [g] rådata', 'Besparelse kr. per enhed', 'Rentabilitet', 'rentabilitet_ny', 'levetid_ny']]\
                    .style.background_gradient(cmap='Blues').set_precision(2))
    # # container.write(df_orig.head(1000))


# color_function = """
#         function (params) {
#             if (params.value > 0 && params.value < 25) {
#                 return 'red';
#             } else if (params.value >= 25 && params.value < 50) {
#                 return 'orange';
#             } else if (params.value >= 50 && params.value < 100) {
#                 return 'yellow';
#             } else if (params.value >= 100 && params.value < 150) {
#                 return 'green';
#             } else if (params.value >= 150 && params.value < 200) {
#                 return 'blue';
#             } else if (params.value >= 200 && params.value < 250) {
#                 return 'purple';
#             }
#             return 'grey';
#         }
#         """

@st.experimental_singleton
def grid_bar_pie(column, valg):
    hej = df.groupby(column).sum().reset_index()
    #hej['Value'] = hej.Name.astype(str)
    hej = hej.sort_values(by=valg)
    print(hej.shape)
    hej2 = [list(z) for z in zip(hej[column], hej[valg])]
    #data_pair.sort(key=lambda x: x[1])
    p = (
        Pie()
        .add(
            series_name=valg,
            data_pair=hej2,
            #rosetype="area",
            radius=["30%", "60%"],
            center=["85%", "50%"],
            label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
            #itemstyle_opts=opts.ItemStyleOpts(color=JsCode(color_function)    )
        )
        .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="right", type_='scroll', is_show=False),
            title_opts=opts.TitleOpts(
                title=column, subtitle="Fordeling af forslag på {}".format(valg), pos_left="center"
            ),
            toolbox_opts=opts.ToolboxOpts(),
        )
    )
    print(hej.shape)
    b = (
        Bar()
        .add_xaxis(list(hej[column]))
        .add_yaxis(column, list(hej[valg]),label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
                  # itemstyle_opts=opts.ItemStyleOpts(color=JsCode(color_function))
                  )
        .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="left", type_='scroll', is_show=True),
            #label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
            # title_opts=opts.TitleOpts(
            #     title="Top cloud providers 2018", subtitle="2017-2018 Revenue", pos_left="center"
            # ),
            toolbox_opts=opts.ToolboxOpts(orient='vertical', is_show=False),
        )
        .set_series_opts(
        # markpoint_opts=opts.MarkPointOpts(
        #     data=[
        #         opts.MarkPointItem(type_="max", name="Maximum"),
        #         opts.MarkPointItem(type_="min", name="Minimum"),
        #     ]
        # ),
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

@st.experimental_singleton
def figur(df, grader):
    b1 = (
        Bar()
        .add_xaxis(list(df.index))
        .add_yaxis('Besparelse i kr./år', list(df['Besparelse [kr.]']), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
        .add_yaxis('Besparelse i kg CO2/år', list(df['Besparelse CO2 [g]']/1000), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
        #.add_yaxis('Antal projekter per år', list(pd.to_numeric(df['values'])), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
         .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="left", is_show=True),
            #label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=grader)),
            title_opts=opts.TitleOpts(

                title='Besparelse', subtitle="Fordeling af antal forslag på {}".format('besparelse'), pos_left="center"
            ),
            toolbox_opts=opts.ToolboxOpts(orient='vertical', is_show=False),
        )
        .set_series_opts(
        # markpoint_opts=opts.MarkPointOpts(
        #     data=[
        #         opts.MarkPointItem(type_="max", name="Maximum"),
        #         opts.MarkPointItem(type_="min", name="Minimum"),
        #     ]
        # ),
        markline_opts=opts.MarkLineOpts(
            data=[
                opts.MarkLineItem(type_="max", name="Maximum"),
                opts.MarkLineItem(type_="average", name="Gennemsnit"),
            ]
        ),
    )
    )
    return b1






################## OVERBLIK ###################################

with st.expander("Overblik"):
    container = st.container()

    container.header(', '.join(map(str, municipalities)))
    col_top1, col_top2 = container.columns(2)
    val = col_top1.selectbox('Graftype', options=['Investering',
                                                            'Besparelse [kr.]',
                                                            'Besparelse CO2 [g]'])

    grid = grid_bar_pie('Anvendelse', val)
    st_pyecharts(grid, height='400px')
    grid = grid_bar_pie('Teknikområde', val)
    st_pyecharts(grid, height='400px')



############# Investering, besparelse, TBT, levetid overblik ################

with st.expander("Investering, besparelse, TBT, levetid"):
    container = st.container()
    col_top1, col_top2 = container.columns(2)

    liste = ['Investering', 'Levetid', 'Rentabilitet', 'Besparelse kroner', 'Besparelse CO2', 'Tilbagebetalingstid (TBT)']
    container = col_top1.container()
    col_top2.write(' ')
    col_top2.write(' ')
    col_top2.write(' ')
    all3 = col_top2.checkbox("Vælg alle plots")

    if all3:
         plots = container.multiselect("Vælg de plots der skal vises",
            options=list(np.unique(liste)),
            default=list(np.unique(liste)))
    else:
        plots =  container.multiselect("Vælg de plots der skal vises",
            options=list(np.unique(liste)))

    if 'Investering' in plots:
        grid = grid_bar_bar(df['Investering'], 'Under 300000', 10, 500000, 'Under 50000', 25, 50000, 'Investering')
        st_pyecharts(grid, height='400px')

    if 'Levetid' in plots:
        df.loc[df['levetid_ny'] == np.inf,'levetid_ny'] = np.nan
        df.loc[df['levetid_ny'] <= 0,'levetid_ny'] = np.nan
        df.loc[df['levetid_ny'] >= 100,'levetid_ny'] = np.nan
        grid = grid_bar_pie("levetid_ny", 'Besparelse [kr.]')
        st_pyecharts(grid, height='400px')

    if 'Rentabilitet' in plots:
        grid = grid_bar_bar(df['Rentabilitet'], 'Op til 15', 30, 15, 'Op til 3', 30, 3, 'Rentabilitet')
        st_pyecharts(grid, height='400px')

    if 'Besparelse kroner' in plots:
        grid = grid_bar_bar(df['Besparelse [kr.]'], 'Op til 100000', 100, 100000, 'Op til 5000', 50, 5000, 'Økonomisk besparelse')
        st_pyecharts(grid, height='400px')


    if 'Besparelse CO2' in plots:
        CO2_1 = st.slider('Figur 1 op til hvor mange kg CO2? (stepsize 100)',  0., 100000., 10000., 100.)
        CO2_2 = st.slider('Figur 2 op til hvor mange kg CO2? (stepsize 1)',  0., 100., 10., 1.)
        grid = grid_bar_bar(df['Besparelse CO2 [g]']/1000, 'Op til {} kg CO2'.format(CO2_1), 100, CO2_1, 'Op til {} kg CO2'.format(CO2_2), 100, CO2_2, 'Klimamæssig/CO2 besparelse')
        st_pyecharts(grid, height='400px')

    if 'Tilbagebetalingstid (TBT)' in plots:
        df.loc[df['TBT'] == np.inf,'TBT'] = np.nan
        df.loc[df['TBT'] <= 0,'TBT'] = np.nan
        df.loc[df['TBT'] >= 200,'TBT'] = np.nan
        grid = grid_bar_bar(df['TBT'], 'Op til 10.000 kg CO2', 100, 100, 'Op til 10 kg CO2', 60, 60, 'Tilbagebetalingstid (TBT)')
        st_pyecharts(grid, height='400px')

############################### Enkelte teknik ########################################

with st.expander("Potentiale ved de enkelte teknikområder"):
    container = st.container()
    colfor_1, colfor_2 = container.columns(2)
    colfor_3, colfor_4 = container.columns(2)



    anv = colfor_1.multiselect('Anvendelser', options=df['Anvendelse'], default=df['Anvendelse'].sort_values().unique())
    teo = colfor_2.multiselect('Teknikområde', df['Teknikområde'], default=df['Teknikområde'].sort_values().unique())
    hvt = colfor_2.multiselect('Håndværkstype', df['håndværkstype'], default=df['håndværkstype'].sort_values().unique())

    valg = colfor_3.selectbox('Hvad skal vises?', options=['Investering',
                                                            'Besparelse [kr.]',
                                                            'Besparelse CO2 [g]',
                                                            'besp per inv'])

    def crosst(df, valg):
        df_t = df[df['Anvendelse'].isin(anv) & df['Teknikområde'].isin(teo) & df['håndværkstype'].isin(hvt)]
        if valg == 'besp per inv':
            cross = pd.crosstab(df_t['Teknikområde'], df_t['Anvendelse'], df_t['Besparelse [kr.]']/df_t['Investering'], aggfunc='mean')
        else:
            cross = pd.crosstab(df_t['Teknikområde'], df_t['Anvendelse'], df_t[valg], aggfunc='sum')
        return cross

    cross = crosst(df, valg)
    # st.write(cross)

    # fig = px.density_heatmap(df, x='Anvendelse', y='Teknikområde', z=valg, histfunc="sum", marginal_x="histogram", marginal_y="histogram")
    # fig.update_layout(height=800)
    # fig.update_xaxes(tickangle=30, tickfont=dict(family='Rockwell', size=12))
    # fig.update_yaxes(tickfont=dict(family='Rockwell', size=12))
    # st.plotly_chart(fig, use_container_width=True, height=900)



    fig = make_subplots(rows=2, cols=2,
                    row_heights=[0.05, 0.8],
                    column_widths=[0.8, 0.03],
                    vertical_spacing = 0.02,
                    horizontal_spacing = 0.02,
                    shared_yaxes=True,
                    shared_xaxes=True)
    cbarlocs = [.85, .5]

    fig.add_trace(go.Heatmap(
                   z=cross,
                   x=cross.columns,
                   y=cross.index,
                   hoverongaps = False,
                   coloraxis='coloraxis')
                   , row = 2, col = 1,
                   )
    cross.loc['Sum Anvendelse']= cross.sum(numeric_only=True, axis=0)
    cross.loc[:,'Sum Teknikområder'] = cross.sum(numeric_only=True, axis=1)
    cross.reset_index()
    # st.write(cross.iloc[-1:])
    # st.write(cross.iloc[:,-1:])
    # st.write(cross.index[:-1])
    # st.write(cross.columns[-1:])

    fig.add_trace(go.Heatmap(
                   z=cross.iloc[-1:],
                   x=cross.columns[:-1],
                   y=cross.index[-1:],
                   hoverongaps = False,
                   showscale=False,
                   coloraxis='coloraxis2'),
                   row = 1, col = 1,
                   )

    fig.add_trace(go.Heatmap(
                   z=cross.iloc[:,-1:],
                   x=cross.columns[-1:],
                   y=cross.index[:-1],
                   hoverongaps = False,
                   showscale=False,
                   coloraxis='coloraxis2'),
                   row = 2, col = 2,
                   )

    # colorbar=dict(len=0.25, y=cbarlocs[0]),
    #         zmin=0, zmax=zmax[0])

    fig.update_xaxes(tickangle=30, tickfont=dict(family='Rockwell', size=12))
    fig.update_yaxes(tickfont=dict(family='Rockwell', size=12))
    fig.update_layout(height=800,
                      coloraxis=dict(colorscale='viridis', colorbar_x=1.0075, colorbar_thickness=23),
                  coloraxis2=dict(colorscale='matter_r', colorbar_x=1.08, colorbar_thickness=23))
    st.plotly_chart(fig, use_container_width=True, height=800)

    # fig = px.imshow(cross)
    # fig = go.Figure(data=go.Heatmap(
    #                z=cross,
    #                x=cross.columns,
    #                y=cross.index,
    #                hoverongaps = False,
    #                marginal_x = 'histogram'))
    # fig.update_layout(height=800)
    # st.plotly_chart(fig, use_container_width=True, height=900)

    # column_labels = [col.replace(' ', '\n') for col in cross.columns[:-1]]
    # fig, (ax1, ax2) = plt.subplots(ncols=2, figsize=(12, 10),
    #                            gridspec_kw={'width_ratios': [10, 1], 'wspace': 0.02, 'bottom': 0.14})
    # cmap = sns.diverging_palette(20, 145)
    # sns.heatmap(cross[cross.columns[:-1]],  vmin=0, vmax=100, annot=True, fmt='.0f', annot_kws={'fontsize': 10},
    #         lw=0.6, xticklabels=column_labels, cbar=False, ax=ax1)
    # sns.heatmap(cross[cross.columns[-1:]],  vmin=0, vmax=1100, annot=True, fmt='.0f', annot_kws={'fontsize': 10},
    #         lw=0.6, yticklabels=[], cbar=False, ax=ax2)
    # st.pyplot(fig)



########################## Potentiale priser ##################################
with st.expander("Potentiale ved priser"):
    container = st.container()
    colfor_1, colfor_2 = container.columns(2)
    colfor_3, colfor_4 = container.columns(2)

    mini  = colfor_1.number_input('Mindste pris ved enkelt projekt?', 0, 1000000, 0)
    maxi  = colfor_1.number_input('Højeste pris ved enkelt projekt?', 0, 100000000, 100000)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric('Investering [kr.]', '{:,.2f} kr.'.format(df['Investering'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()))
    col1.metric('Investering af fuld potentiale i %', '{:,.1f} %'.format(df['Investering'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()/df['Investering'].sum()*100))
    col1.metric('Gennemsnitlig investering [kr.]', '{:,.2f} kr.'.format(df['Investering'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()/df[(df['Investering'] > mini) & (df['Investering'] < maxi)].shape[0]))

    col2.metric('Besparelse [kr./år]', '{:,.2f} kr./år'.format(df['Besparelse [kr.]'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()))
    col2.metric('Besparelse af fuld potentiale i %', '{:,.1f} %'.format(df['Besparelse [kr.]'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()/df['Besparelse [kr.]'].sum()*100))
    col2.metric('Gennemsnitlig besparelse [kr./år]', '{:,.2f} kr./år'.format(df['Besparelse [kr.]'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()/df[(df['Investering'] > mini) & (df['Investering'] < maxi)].shape[0]))

    col3.metric('Besparelse CO2 [ton/år]', '{:,.2f} ton/år'.format(df['Besparelse CO2 [g]'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()/1000000))
    col3.metric('Besparelse CO2 af fuld potentiale i %', '{:,.1f} %'.format(df['Besparelse CO2 [g]'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()/df['Besparelse CO2 [g]'].sum()*100))
    col3.metric('Gennemsnitlig besparelse CO2 [ton/år]', '{:,.2f} ton/år'.format(df['Besparelse CO2 [g]'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()/1000000/df[(df['Investering'] > mini) & (df['Investering'] < maxi)].shape[0]))

    col4.metric('Antal forslag', df[(df['Investering'] > mini) & (df['Investering'] < maxi)].shape[0])
    col4.metric('Gennemsnitlig tilbagebetalingstid [år]', '{:,.2f} år'.format(df['TBT'][(df['Investering'] > mini) & (df['Investering'] < maxi)].sum()/df[(df['Investering'] > mini) & (df['Investering'] < maxi)].shape[0]))
    col4.metric('Median rentabilitet', '{:,.2f}'.format(df['Rentabilitet'][(df['Investering'] > mini) & (df['Investering'] < maxi)].median()))


########################## Potentiale priser ##################################

with st.expander("Energimærkeklassificering"):
    container = st.container()
    colfor_1, colfor_2 = container.columns(2)
    colfor_3, colfor_4 = container.columns(2)

    emo = df.groupby('EnergimærkeID').agg({
                                           'klassificering_status': 'first',
                                           'klassificering rentable': 'first',
                                           'klassificering alle': 'first',
                                           'klassificering': 'first',
                                           })
    st.write(emo)
    fig = go.Figure()
    fig.add_trace(go.Histogram(histfunc="count",  x=df['klassificering_status'], name='Klassificering'))
    fig.add_trace(go.Histogram(histfunc="count",  x=df['klassificering rentable'], name='Alle rentable forslag'))
    fig.add_trace(go.Histogram(histfunc="count",  x=df['klassificering alle'], name='Alle forslag'))
    fig.update_xaxes(categoryorder='category descending')
    st.plotly_chart(fig)















#################### Potentiale ###############################################

@st.experimental_memo
def optimization_yearly(df, aar, invv, optt):
    df_res = df
    df_frem = df
    df_frem['values'] = 0
    df_res = pd.DataFrame()

    for j in range(int(aar)):
        df_frem = df_frem[df_frem['values'] != 1]
        prob = LpProblem("The Optimization Problem yearly", LpMaximize)
        df_frem['var'] = [LpVariable('x' + str(i), cat='Binary') for i in range(df_frem.shape[0])]

        if 'Økonomisk besparelse' in optt:
            prob += lpSum(df_frem['Besparelse [kr.]']*df_frem['var']), 'optimering DKK'
        elif 'Klimamæssig besparelse' in optt:
            prob += lpSum(df_frem['Besparelse CO2 [g]']*df_frem['var']), 'optimering CO2'
        else:
            st.warning('Der er ikke valgt optimering')

        # The constraints are entered
        prob += lpSum(df_frem['Investering']*df_frem['var']) <= invv, "Investering"


        prob.writeLP("OptimizationModel_yearly.lp")
        prob.solve()

        # The status of the solution is printed to the screen
        status = LpStatus[prob.status]

        df_frem['values'] = [i.varValue for i in df_frem['var']]
        df_frem['var'] = df_frem['var'].astype(str)
        df_frem['år'] = df_frem['values']
        df_frem["år"].replace(
            {   0:    0,
                1:    j+1,},
            inplace=True,)
        df_res = pd.concat([df_res, df_frem[df_frem['values']==1]])

    years = df_res.groupby('år').sum()[['Besparelse [kr.]', 'Besparelse CO2 [g]', 'Besparelse [kWh]', 'values' ]]
    years = pd.concat([years, df_res.groupby('år').mean()[['Rentabilitet', 'Investering', 'levetid_ny', 'TBT']]], axis=1)

    years_cum = df_res.groupby('år').sum()[['Besparelse [kr.]', 'Besparelse CO2 [g]', 'Besparelse [kWh]', 'values' ]].cumsum().reset_index()

    return df_res, years, years_cum

@st.experimental_memo
def pot_handvark(df):
    count = df.groupby('håndværkstype').count()
    df_tek = df.groupby('håndværkstype').sum()[['Besparelse [kr.]', 'Besparelse CO2 [g]', 'Besparelse [kWh]', 'Investering']]
    df_tek = pd.concat([df_tek, df.groupby('håndværkstype').mean()[['Rentabilitet', 'Levetid', 'TBT','Investering']]], axis=1)
    return df_tek

with st.expander("Potentiale indenfor håndværkerområder og årlig fremskrivning"):
    col1, col2 = st.columns(2)
    pot = col1.checkbox('Potentiale indenfor håndværkerområder')
    frem = col2.checkbox('Årlig fremskrivning')

    if pot:
        st.header('Potentiale indenfor håndværkerområder')
        df_tek = pot_handvark(df)
        text0 = st.checkbox('Vis data')
        if text0:
            st.write(df_tek)

        b1 = figur(df_tek, 90)
        st_pyecharts(b1, height='500px',  key = 'count1')
        st.markdown('---')

    if frem:
        st.header('Årlig fremskrivning')
        aar  = st.number_input('Hvor mange år frem?', 0, 30, 10)
        invv = st.number_input('Hvor mange kr. investeres per år?', 0, 10000000000, 2000000, format='%i')
        optt = st.selectbox('Hvordan skal det optimeres i årlig fremskrivning?', options=['Økonomisk besparelse',
                                                                'Klimamæssig besparelse'])

        df_res, years, years_cum = optimization_yearly(df, aar, invv, optt)

        text =  st.checkbox('Vis data', key = 'hej')
        if text:
            st.subheader('Alle de foreslåede forslag efter hvilket år de er estimeret til udførsel')
            st.write(df_res)
            st.subheader('Samlet data for hvert enkelt år.')
            st.text('Samlet sum: besparelse i kr., besparelse af CO2 i gram, besparelse i enheder af kWh og values (antal projekter)')
            st.text('Gennemsnitsværdier: rentabilitet, levetid, tilbagebetalingstiden og investeringen')
            st.write(years)

        b1 = figur(years, 0)
        st_pyecharts(b1, height='500px')

        col1, col2, col3, col4 = st.columns(4)
        col1.metric('Investering [kr.]', '{:,.2f} kr.'.format(years['Investering'].sum()))
        col1.metric('Gennemsnitlig investering [kr.]', '{:,.2f} kr.'.format(years['Investering'].mean()))
        col1.metric('Investering af fuld potentiale i %', '{:,.1f} %'.format(years['Investering'].sum()/df['Investering'].sum()*100))

        col2.metric('Besparelse [kr./år]', '{:,.2f} kr./år'.format(years['Besparelse [kr.]'].sum()))
        col2.metric('Gennemsnitlig besparelse [kr./år]', '{:,.2f} kr./år'.format(years['Besparelse [kr.]'].mean()))
        col2.metric('Besparelse af fuld potentiale i %', '{:,.1f} %'.format(years['Besparelse [kr.]'].sum()/df['Besparelse [kr.]'].sum()*100))
        col2.metric('Energisparepris', '{:,.2f} kr./kWh'.format(years['Investering'].sum()/years['Besparelse [kr.]'].sum()))
        col3.metric('Besparelse CO2 [ton/år]', '{:,.2f} ton/år'.format(years['Besparelse CO2 [g]'].sum()/1000000))
        col3.metric('Gennemsnitlig besparelse CO2 [ton/år]', '{:,.2f} ton/år'.format(years['Besparelse CO2 [g]'].mean()/1000000))
        col3.metric('Besparelse CO2 af fuld potentiale i %', '{:,.1f} %'.format(years['Besparelse CO2 [g]'].sum()/df['Besparelse CO2 [g]'].sum()*100))
        col3.metric('CO2 sparepris', '{:,.2f} kr./kg CO2'.format(years['Investering'].sum()/(years['Besparelse CO2 [g]'].sum()/1000)))
        col4.metric('Antal forslag', years['values'].sum())
        col4.metric('Gennemsnitlig tilbagebetalingstid [år]', '{:,.2f} år'.format(years['TBT'].mean()))
        col4.metric('Mean rentabilitet', '{:,.2f}'.format(years['Rentabilitet'].mean()))


        df_min = df[df['Investering'] >= 300000]
        df_res_min, years_min, years_cum_min = optimization_yearly(df_min, aar, invv, optt)

        # total_forbrug = df['Besparelse [kWh]'].sum()
        # udviklingCO2 =  (68.18 - np.array([73.44,	68.26,	48.22,	40.35,	35.12,	30.46,	24.21,	19.88,	12.04,	9.91])) #* total_forbrug
        # cum_udviklingCO2 = np.zeros(10)
        # for i in range(10):
        #     cum_udviklingCO2[i] = np.sum(udviklingCO2[:i+1])

        years_cum['cum_Besparelse [kr.]'] = years_cum['Besparelse [kr.]']
        years_cum['cum_Besparelse CO2 [g]'] = years_cum['Besparelse CO2 [g]']
        years_cum_min['cum_Besparelse [kr.]'] = years_cum_min['Besparelse [kr.]']
        years_cum_min['cum_Besparelse CO2 [g]'] = years_cum_min['Besparelse CO2 [g]']
        #years_cum['enheder_mwh'] = years_cum['Besparelse [kWh]']/1000
        #years_cum['CO2_enheder_mwh'] = years_cum['enheder_mwh'] * udviklingCO2
        #years_cum['cum_CO2_enheder_mwh'] = years_cum['CO2_enheder_mwh']
        for i in range(int(aar)):
            years_cum['cum_Besparelse [kr.]'][i] = years_cum['Besparelse [kr.]'][:i+1].sum()
            years_cum['cum_Besparelse CO2 [g]'][i] = years_cum['Besparelse CO2 [g]'][:i+1].sum()
            #years_cum['cum_CO2_enheder_mwh'][i] = years_cum['CO2_enheder_mwh'][:i+1].sum()
            years_cum_min['cum_Besparelse [kr.]'][i] = years_cum_min['Besparelse [kr.]'][:i+1].sum()
            years_cum_min['cum_Besparelse CO2 [g]'][i] = years_cum_min['Besparelse CO2 [g]'][:i+1].sum()

        st.subheader('Akkumuleret besparelse')
        st.info("""De optimerede projekter følger samme model som ovenover. Altså er det optimeret pga økonomisk eller klimamæssig besparelse baseret på alle projekter.
                 Projekter over 300.000 kr. bruger samme algoritme som ovenover, men bruger kun de projekter over 300.000 kr.
                 For begge gælder det at der er den AKKUMULEREDE SUM. Altså er besparelsen fra tidligere år lagt oveni besparelse for det pågældende år.""")
        text2 = st.checkbox('Vis data for akkumulerede summer')
        if text2:
            st.subheader('Akkumulerede summer for hvert år ved de optimerede projektforslag')
            st.write(years_cum)
            st.subheader('Akkumulerede summer for hvert år ved store projekter over 300.000 kr. i investering')
            st.write(years_cum_min)


        @st.experimental_singleton
        def figur_cum(df, df1, text1, text2):
            b1 = (
                Line()
                .add_xaxis(list(df.index))
                .add_yaxis(text1, list(df), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),areastyle_opts=opts.AreaStyleOpts(opacity=0.3),)
                .add_yaxis(text2, list(df1), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),areastyle_opts=opts.AreaStyleOpts(opacity=0.3),)
                .set_global_opts(
                    legend_opts=opts.LegendOpts(orient='vertical', pos_left="center", is_show=True),
                    title_opts=opts.TitleOpts(
                    ),
                    toolbox_opts=opts.ToolboxOpts(orient='vertical', is_show=False),
                )
                .set_series_opts(
                )
            )
            return b1

        colfor_1, colfor_2 = container.columns(2)

        b1 = figur_cum(years_cum['cum_Besparelse [kr.]'], years_cum_min['cum_Besparelse [kr.]'], 'Besparelse i kr./år optimerede projekter', 'Besparelse i kr./år v/ projekter over 300.000')
        b2 = figur_cum(years_cum['cum_Besparelse CO2 [g]']/1000, years_cum_min['cum_Besparelse CO2 [g]']/1000, 'Besparelse i kg CO2/år optimerede projekter','Besparelse i kg CO2/år v/ projekter over 300.000')
        st_pyecharts(b1, height='500px',  key = 'kr')
        st_pyecharts(b2, height='500px',  key = 'CO2')

#################### FORSLAG ########################################

with st.expander("Forslag"):
    st.info('Husk at de valg der er taget i filtreringen i siden også gælder med')
    container = st.container()
    colfor_1, colfor_2 = container.columns((4,2))
    colfor_3, colfor_4 = container.columns((3,2))

    @st.experimental_memo
    def teknik_value(string):
        hej = df[string].value_counts().rename_axis().reset_index(name='Antal')
        return hej
    hej = teknik_value('Teknikområde')

    colfor_4.header('Antal forslag i teknikområderne')
    colfor_4.write(hej)



    colfor_3.header('Forslag efter økonomisk eller CO2 besparelse')
    optimer = colfor_3.selectbox('Hvordan skal det optimeres?', options=['Økonomisk besparelse',
                                                            'Klimamæssig besparelse'])



    constraint = colfor_3.multiselect('Hvordan skal det optimeres?', options=['Investering',
                                                                'Tilbagebetalingstid (gennemsnitlig)',
                                                                'Håndværkstype',
                                                                'Rentabilitet (mindste)',
                                                                'Energimærkets alder (højest)',
                                                                'Udeluk forslagstyper',
                                                                'Sikkerhedsfaktor ganges på investering',
                                                                'Minimum CO2',
                                                                'Maximum CO2'],
                         default='Investering')

    if 'Investering' in constraint:
        invest = colfor_3.number_input('Total investeringssum', 0, 10000000000, 10000000)
    if 'Tilbagebetalingstid (gennemsnitlig)' in constraint:
        tilbage = colfor_3.number_input('Højeste gennemsnitlige tilbagebetalingstid', 0, 1000, 50)
    if 'Håndværkstype' in constraint:
        hand = colfor_3.multiselect(
            "Hvilke håndværkstyper?",
            options=list(df["håndværkstype"].unique()),
            default=df["håndværkstype"].unique())
    if 'Rentabilitet (mindste)' in constraint:
        rent = colfor_3.number_input('Mindste rentabilitet accepteret', max_value=3.0, value=1.0)
    if 'Energimærkets alder (højest)' in constraint:
        alder = colfor_3.number_input('Højeste alder på energimærket accepteret', max_value=15, value=10)
    if 'Udeluk forslagstyper' in constraint:
        fors = colfor_3.multiselect(
            "Hvilke forslagstyper skal udelukkes?",
            options=list(df['Teknikområde'].unique()))
    if 'Sikkerhedsfaktor ganges på investering' in constraint:
        faktor = colfor_3.number_input('Sikkerhedsfaktor der ganges på investering', max_value=100, value=10)
    if 'Minimum CO2' in constraint:
        minCO2 = colfor_3.number_input('Minimum samlet CO2 reduktion i kg', 0, 10000000, 0)*1000
    if 'Maximum CO2' in constraint:
        maxCO2 = colfor_3.number_input('Maksimum samlet CO2 reduktion i kg. (Pris for at reducere x mængde CO2)', 0, 10000000, 1000)*1000


    def OptimizationModel(df):

        if 'Rentabilitet (mindste)' in constraint:
            df = df[df["Rentabilitet"] >= rent]
        if 'Håndværkstype' in constraint:
            df = df[df['håndværkstype'].isin(hand)]
        if 'Energimærkets alder (højest)' in constraint:
            df = df[df['år'] >= (2021-alder)]
        if 'Udeluk forslagstyper' in constraint:
            df = df[~df['Teknikområde'].isin(fors)]
        if 'Sikkerhedsfaktor ganges på investering' in constraint:
            df['Investering'] = df['Investering']*((faktor/100)+1)


        #df_mid = df
        #df = df.groupby(['EnergimærkeID', 'håndværkstype'], dropna=False).sum()[['Besparelse [kr.]', 'Besparelse CO2 [g]', 'Besparelse [kWh]', 'Rentabilitet', 'Investering', 'Levetid', 'TBT']]

        prob = LpProblem("The Optimization Problem", LpMaximize)
        df['var'] = [LpVariable('x' + str(i), cat='Binary')
                       for i in range(df.shape[0])]

        if 'Økonomisk besparelse' in optimer:
            prob += lpSum(df['Besparelse [kr.]']*df['var']), 'optimering DKK'
        elif 'Klimamæssig besparelse' in optimer:
            prob += lpSum(df['Besparelse CO2 [g]']*df['var']), 'optimering CO2'
        else:
            st.warning('Der er ikke valgt optimering')

        # The constraints are entered
        if 'Investering' in constraint:
            prob += lpSum(df['Investering']*df['var']) <= invest, "Investering"
        if 'Tilbagebetalingstid' in constraint:
            prob += lpSum(df['TBT']*df['var']) <= lpSum(tilbage * np.sum(df['var'])), "Tilbagebetalingstid"
        if 'Minimum CO2' in constraint:
            prob += lpSum(df['Besparelse CO2 [g]']*df['var']) >= minCO2, "Minimum CO2"
        if 'Maximum CO2' in constraint:
            prob += lpSum(df['Besparelse CO2 [g]']*df['var']) <= maxCO2, "Maximum CO2"
        # if 'Rentabilitet' in constraint:
        #     prob += lpSum(df['Rentabilitet']*df['var']) <= rent, 'Rentabilitet'


        prob.writeLP("OptimizationModel.lp")
        prob.solve()

        # The status of the solution is printed to the screen
        status = LpStatus[prob.status]
        value = []
        for v in prob.variables():
            value.append(v.varValue)

        df['values'] = [i.varValue for i in df['var']]
        df['var'] = df['var'].astype(str)

        df1 = df[df['values']==1]
        #df = df.reset_index()
        #df1 = df1.reset_index()
        #df2 = df_mid[df1['EnergimærkeID'].isin(df_mid['EnergimærkeID']) & df1['håndværkstype'].isin(df_mid['håndværkstype'])]
        return df1, status




####################################### PuLP ###############################################

    df1, status = OptimizationModel(df)

    st.write('Udvælgelsen er: ' + status)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric('Investering [kr.]', '{:,.2f} kr.'.format(df1['Investering'].sum()))
    #col1.metric('Gennemsnitlig investering [kr.]', '{:,.2f} kr.'.format(df1['Investering'].sum()/df1.shape[0]))
    col1.metric('Investering af fuld potentiale i %', '{:,.1f} %'.format(df1['Investering'].sum()/df['Investering'].sum()*100))
    col1.metric('Antal teknikområder', df1['Teknikområde'].nunique())

    col2.metric('Besparelse [kr./år]', '{:,.2f} kr./år'.format(df1['Besparelse [kr.]'].sum()))
    #col2.metric('Gennemsnitlig besparelse [kr./år]', '{:,.2f} kr./år'.format(df1['Besparelse [kr.]'].sum()/df1.shape[0]))
    col2.metric('Besparelse af fuld potentiale i %', '{:,.1f} %'.format(df1['Besparelse [kr.]'].sum()/df['Besparelse [kr.]'].sum()*100))
    col2.metric('Energisparepris', '{:,.2f} kr./kWh'.format(df1['Investering'].sum()/df1['Besparelse [kr.]'].sum()))
    col3.metric('Besparelse CO2 [ton/år]', '{:,.2f} ton/år'.format(df1['Besparelse CO2 [g]'].sum()/1000000))
    #col3.metric('Gennemsnitlig besparelse CO2 [ton/år]', '{:,.2f} ton/år'.format(df1['Besparelse CO2 [g]'].sum()/1000000/df1.shape[0]))
    col3.metric('Besparelse CO2 af fuld potentiale i %', '{:,.1f} %'.format(df1['Besparelse CO2 [g]'].sum()/df['Besparelse CO2 [g]'].sum()*100))
    col3.metric('CO2 sparepris', '{:,.2f} kr./kg CO2'.format(df1['Investering'].sum()/(df1['Besparelse CO2 [g]'].sum()/1000)))
    col4.metric('Antal forslag', df1.shape[0])
    col4.metric('Gennemsnitlig tilbagebetalingstid [år]', '{:,.2f} år'.format(df1['TBT'].sum()/df1.shape[0]))
    col4.metric('Median rentabilitet', '{:,.2f}'.format(df1['Rentabilitet'].median()))
    st.write(df1[['Adresse', 'Adressenavn', 'by', 'år', 'Teknikområde', 'Tekst', 'Investering', 'Besparelse [kr.]', 'Besparelse CO2 [g]', 'Rentabilitet', 'TBT']]\
             .style.background_gradient(cmap='Blues').set_precision(2))



####################################### PuLP ###############################################



        # def hent_forslag_tekst(df):
        #     start_time = time.time()
        #     SERVER = "redshift.bi.obviux.dk"
        #     PORT = '5439'  # Redshift default
        #     USER = "mrs"
        #     PASSWORD = "j89Foijf8fIJFAD8dsIFJA8DFMasf_D7fa9df"
        #     DATABASE = "redshift"
        #
        #     cnxn = db.connect(host=SERVER, database=DATABASE, user=USER, password=PASSWORD, port=PORT)
        #
        #     print("Connected to Redshift")
        #
        #     query = """
        #             WITH build AS
        #             (
        #             SELECT energylabel_id, CAST(build.propertynumber AS INT), build.building_id, build.ownership, build.reviewdate,  CAST(build.municipalitynumber AS INT), build.streetname, build.housenumber,
        #             build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
        #             FROM energylabels.building_data AS build
        #             WHERE ownership != 'Private'
        #
        #             )
        #             SELECT build.energylabel_id,
        #             prop_group.shorttext, prop_group.longttext, prop_group.seebclassification,
        #
        #             build.propertynumber, build.building_id, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
        #             build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
        #
        #             FROM energylabels.proposal_groups AS prop_group
        #             RIGHT JOIN build ON build.energylabel_id = prop_group.energylabel_id
        #
        #             ORDER BY build.energylabel_id
        #             """
        #
        #     n = 10000
        #     dfs = []
        #     for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        #         dfs.append(chunk)
        #     df_prop = pd.concat(dfs)
        #     print("--- df %s seconds ---" % (time.time() - start_time))


@st.experimental_memo
def hent_input(kommune,ejerskab):
    start_time = time.time()
    SERVER = "redshift.bi.obviux.dk"
    PORT = '5439'  # Redshift default
    USER = "mrs"
    PASSWORD = "j89Foijf8fIJFAD8dsIFJA8DFMasf_D7fa9df"
    DATABASE = "redshift"

    cnxn = db.connect(host=SERVER, database=DATABASE, user=USER, password=PASSWORD, port=PORT)

    print("Connected to Redshift")

    print('Normal datahentning')
    query = """
            WITH build AS
            (
            SELECT energylabel_id, CAST(build.propertynumber AS INT), build.building_id, build.ownership, build.reviewdate,  CAST(build.municipalitynumber AS INT), build.streetname, build.housenumber,
            build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
            FROM energylabels.building_data AS build
            WHERE (municipalitynumber IN {}) AND (ownership IN {})
            )
            SELECT build.energylabel_id, input.building_id AS Bygnr, input.zone_id,
            input.seebclassification, input.shorttext AS tekst, input.data_category as data_kategori, input.data_value as data_værdi

            FROM energylabels.input_data AS input
            RIGHT JOIN build ON build.energylabel_id = input.energylabel_id

            ORDER BY build.energylabel_id, input.building_id, input.zone_id, input.shorttext
            """.format(str(tuple(kommune['municipalitynumber'])).replace(',','') if len(kommune['municipalitynumber']) == 1 else tuple(kommune['municipalitynumber']),
                       str(tuple(ejerskab['ownership'])).replace(',','') if len(ejerskab['ownership']) == 1 else tuple(ejerskab['ownership']))
    n = 10000
    dfs = []
    for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        dfs.append(chunk)
    df = pd.concat(dfs)
    print("--- df %s seconds ---" % (time.time() - start_time))
    data_time = time.time() - start_time

    return df, data_time  #

@st.experimental_memo
def input_data(df1, df_input):
    df_i = pd.merge(df1, df_input, on=['EnergimærkeID'])
    df_i = df_i[['Adresse', 'Adressenavn',  'zone_id', 'postnr', 'by', 'EnergimærkeID', 'Teknikområde', 'tekst', 'data_kategori', 'data_værdi']]
    df_i = df_i.groupby(['Adresse', 'Adressenavn',  'zone_id', 'data_kategori']).first().reset_index()
    df_i = df_i[['Adresse', 'Adressenavn',  'zone_id', 'postnr', 'by', 'EnergimærkeID', 'Teknikområde', 'tekst', 'data_kategori', 'data_værdi']]
    return df_i



@st.experimental_memo
def hent_forslag(kommune,ejerskab):
    start_time = time.time()
    SERVER = "redshift.bi.obviux.dk"
    PORT = '5439'  # Redshift default
    USER = "mrs"
    PASSWORD = "j89Foijf8fIJFAD8dsIFJA8DFMasf_D7fa9df"
    DATABASE = "redshift"

    cnxn = db.connect(host=SERVER, database=DATABASE, user=USER, password=PASSWORD, port=PORT)

    print("Connected to Redshift")

    print('Normal datahentning')
    query = """
            WITH build AS
            (
            SELECT energylabel_id, CAST(build.propertynumber AS INT), build.building_id, build.ownership, build.reviewdate,  CAST(build.municipalitynumber AS INT), build.streetname, build.housenumber,
            build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
            FROM energylabels.building_data AS build
            WHERE (municipalitynumber IN {}) AND (ownership IN {})
            )
            SELECT build.energylabel_id, prop.building_id AS Bygnr, prop.zone_id, prop.lifetime, prop.investment,
            prop.seebclassification, prop.shorttext AS tekst, prop.data_category as data_kategori, prop.data_value as data_værdi,
            prop.proposal_id

            FROM energylabels.proposals AS prop
            RIGHT JOIN build ON build.energylabel_id = prop.energylabel_id

            ORDER BY build.energylabel_id, prop.building_id, prop.zone_id, prop.shorttext
            """.format(str(tuple(kommune['municipalitynumber'])).replace(',','') if len(kommune['municipalitynumber']) == 1 else tuple(kommune['municipalitynumber']),
                       str(tuple(ejerskab['ownership'])).replace(',','') if len(ejerskab['ownership']) == 1 else tuple(ejerskab['ownership']))
            # , ref.proposal_id AS proposal_id_ref, ref.proposal_group_id as proposal_group
            # LEFT JOIN energylabels.proposal_group_references AS ref ON build.energylabel_id = ref.energylabel_id AND prop.proposal_id = ref.proposal_id

    n = 10000
    dfs = []
    for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        dfs.append(chunk)
    df = pd.concat(dfs)
    print("--- df %s seconds ---" % (time.time() - start_time))
    data_time = time.time() - start_time

    return df, data_time  #

@st.experimental_memo
def forslag_data(df1, df_input):
    df_f = pd.merge(df1, df_input, on=['EnergimærkeID', 'Tekniknr'])
    df_f = df_f[['Adresse', 'Adressenavn',  'zone_id', 'postnr', 'by', 'EnergimærkeID', 'Teknikområde', 'tekst', 'data_kategori', 'data_værdi']]
    df_f = df_f.groupby(['Adresse', 'Adressenavn',  'zone_id', 'data_kategori']).first().reset_index()
    #df_f['data_værdi'] = pd.to_numeric(df_f['data_værdi'], errors='coerce')
    df_f = df_f[['Adresse', 'Adressenavn',  'zone_id', 'postnr', 'by', 'EnergimærkeID', 'Teknikområde', 'tekst', 'data_kategori', 'data_værdi']]
    return df_f

if kommune is not None:
    df_input, data_time_input = hent_input(kommune, ejerskab)
    df_forslag, data_time_input = hent_forslag(kommune, ejerskab)
    df_input = oversat_kolonnenavne(df_input)
    df_forslag = oversat_kolonnenavne(df_forslag)


    df_i = input_data(df1, df_input)
    df_f = forslag_data(df1, df_forslag)

    df_f = df_i.merge(df_f, on=['Adresse', 'Adressenavn',  'zone_id', 'postnr', 'by', 'EnergimærkeID', 'Teknikområde', 'data_kategori'], suffixes=(' input', ' forslag'))



# with st.expander("Input data oversigt"):
#     container = st.container()
#     coldown_1, coldown_2 = container.columns(2)
#     container.write(df_i.head(10000))
#
# with st.expander("Forslag data oversigt"):
#     container = st.container()
#     coldown_1, coldown_2 = container.columns(2)
#     container.write(1)
#     #df_input = data_cleaning(df_input)
#     #df_input = udregn_co2(df_input)
#     container.write(2)
#     container.write(df_forslag.shape[0])
#     container.write(df_forslag.columns)
#     # df_input['zone_id'] = pd.to_numeric(df_input['zone_id'])
#     # df_input['data_input_type'] = df_input['data_input_type'].to_string()
    # container.write(df_f.head(10000))

################### Rapport med energiforslag #################################

with st.expander("Rapport med energiforslag"):
    container = st.container()
    coldown_1, coldown_2 = container.columns(2)
    #coldown_3, coldown_4 = container.columns((3,2))

    container = coldown_2.container()
    all4 = coldown_2.checkbox("Vælg alle kolonner")

    if all4:
         kolonne_valg = container.multiselect("Vælg de kolonner der skal vises",
            options=list(df1.columns),
            default=list(df1.columns))
    else:
        kolonne_valg =  container.multiselect("Vælg de kolonner der skal vises",
            options=df1.columns,
            default=('EnergimærkeID', 'Forslagets gruppe ID', 'Teknikområde', 'Tekst', 'Investering', 'Besparelse [kr.]', 'Besparelse CO2 [g]', 'Besparelse [kWh]',
                     'levetid_ny', 'Rentabilitet', 'TBT', 'Kommune', 'ejendomsnr', 'Adresse', 'Adressenavn', 'by', 'Anvendelse', 'ejerskab', 'areal', 'besigtigelsesdato')
        )

    forslag = df1[df1.columns.intersection(kolonne_valg)]
    forslag.sort_values(['EnergimærkeID'])

    @st.cache
    def convert_df_csv(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8-sig')

    @st.cache
    def convert_df_excel(df, liste):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        workbook = writer.book

        df.to_excel(writer, index=False, sheet_name='Rådata' )
        worksheet = writer.sheets['Rådata']
        (max_row, max_col) = df.shape
        column_settings = [{'header': column} for column in df.columns]
        worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
        worksheet.set_column(0, max_col - 1, 12)

        worksheet1 = workbook.add_worksheet('Filter information')
        worksheet1.write_string('A1', 'Overordnet filtrering')
        worksheet1.write_string('A2','Kommuner')
        worksheet1.write_row('B2', municipalities)
        worksheet1.write_string('A3','Ejerskabformer')
        worksheet1.write_row('B3', bygningstyper)
        worksheet1.write_string('A4','Anvendelser')
        worksheet1.write_row('B4', brugskode)
        worksheet1.write_string('A5','Adresser')
        worksheet1.write_row('B5', adresse)
        worksheet1.write_string('A6','Teknikområder')
        worksheet1.write_row('B6', teknik)

        worksheet1.write_string('A8', 'Optimering ift. forslag')
        worksheet1.write_string('A9','Optimeret efter')
        worksheet1.write('B9', optimer)
        worksheet1.write_string('A10','Begrænsninger')
        worksheet1.write_row('B10', constraint)
        if 'Investering' in constraint:
            worksheet1.write_string('A11','Investering begrænsning')
            worksheet1.write('B11', invest)
        if 'Tilbagebetalingstid' in constraint:
            worksheet1.write_string('A12','Maks gennemsnitlig tilbagebetalingstid')
            worksheet1.write('B12', tilbage)
        if 'Rentabilitet' in constraint:
            worksheet1.write_string('A13','Mindste rentabilitet')
            worksheet1.write('B13', rent)

        worksheet1.write_string('A19', 'Opdaterede brændselspriser og udledning')
        worksheet1.write_string('A20','Elpris [kr/kWh]')
        worksheet1.write('A21', elpris)
        worksheet1.write_string('A23','El udledning [g/kWh]')
        worksheet1.write('A24', elCO2)
        worksheet1.write_string('B20','Naturgaspris [kr/kWh]')
        worksheet1.write('B21', NGpris)
        worksheet1.write_string('B23','Naturgas udledning [g/kWh]')
        worksheet1.write('B24', NGCO2)
        worksheet1.write_string('C20','Træpris [kr/kWh]')
        worksheet1.write('C21', woodpris)
        worksheet1.write_string('C23','Træ udledning [g/kWh]')
        worksheet1.write('C24', woodCO2)
        worksheet1.write_string('D20','Bygaspris [kr/kWh]')
        worksheet1.write('D21', CGpris)
        worksheet1.write_string('D23','Bygas udledning [g/kWh]')
        worksheet1.write('D24', CGCO2)
        worksheet1.write_string('E20','Fjernvarmepris [kr/kWh]')
        worksheet1.write('E21', DHpris)
        worksheet1.write_string('E23','Fjernvarme udledning [g/kWh]')
        worksheet1.write('E24', DHCO2)
        worksheet1.write_string('F20','Fyringsgasoliepris [kr/kWh]')
        worksheet1.write('F21', FGOpris)
        worksheet1.write_string('F23','Fyringsgasolie udledning [g/kWh]')
        worksheet1.write('F24', FGOCO2)
        worksheet1.write_string('G20','Oliepris [kr/kWh]')
        worksheet1.write('G21', Oliepris)
        worksheet1.write_string('G23','Olie udledning [g/kWh]')
        worksheet1.write('G24', OlieCO2)
        worksheet1.write_string('H20','Træpillepris [kr/kWh]')
        worksheet1.write('H21', WPpris)
        worksheet1.write_string('H23','Træpiller udledning [g/kWh]')
        worksheet1.write('H24', WPCO2)




        teknikomr = list(df['Teknikområde'].unique())
        for tek in teknikomr:
            df_f[df_f['Teknikområde'] == tek].to_excel(writer, index=False, sheet_name='Data ' + str(tek))
            worksheet = writer.sheets['Data ' + str(tek)]
            (max_row, max_col) = df_f[df_f['Teknikområde'] == tek].shape
            column_settings = [{'header': column} for column in df_f[df_f['Teknikområde'] == tek].columns]
            worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
            worksheet.set_column(0, max_col - 1, 18)

            df[df['Teknikområde'] == tek][liste].to_excel(writer, index=False, sheet_name=str(tek))
            worksheet = writer.sheets[str(tek)]
            (max_row, max_col) = df[df['Teknikområde'] == tek][liste].shape
            column_settings = [{'header': column} for column in df[df['Teknikområde'] == tek][liste].columns]
            worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
            worksheet.set_column(0, max_col - 1, 18)

            # df_i[df_i['Teknikområde'] == tek].to_excel(writer, index=False, sheet_name='input ' + str(tek))
            # worksheet = writer.sheets['input ' + str(tek)]
            # (max_row, max_col) = df_i[df_i['Teknikområde'] == tek].shape
            # column_settings = [{'header': column} for column in df_i[df_i['Teknikområde'] == tek].columns]
            # worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
            # worksheet.set_column(0, max_col - 1, 18)
        #format1 = workbook.add_format({'num_format': '0.00'})
        #worksheet.set_column('A:A', None, format1)

        writer.save()
        processed_data = output.getvalue()
        return processed_data

    # output = BytesIO()
    # writer = pd.ExcelWriter(output, engine='xlsxwriter')
    # workbook = writer.book
    #
    # df.to_excel(writer, index=False, sheet_name='Rådata' )
    # worksheet = writer.sheets['Rådata']
    # (max_row, max_col) = df1.shape
    # column_settings = [{'header': column} for column in df1.columns]
    # worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
    # worksheet.set_column(0, max_col - 1, 12)
    #
    # writer.save()
    # processed_data = output.getvalue()

    # pvt_all = df1.groupby(['Adresse'])
    # st.write(pvt_all)






    # csv_i   = convert_df_csv(df_i)
    csv_f   = convert_df_csv(df_f)
    csv     = convert_df_csv(forslag)
    liste = ['Adresse', 'Adressenavn', 'by',  'Teknikområde', 'Tekst',  'Investering', 'Besparelse [kr.]', 'Besparelse CO2 [g]', 'Rentabilitet', 'EnergimærkeID', 'Forslagets gruppe ID']
    excel = convert_df_excel(df1, liste)
    liste = ['Adresse', 'Adressenavn', 'by',  'Teknikområde', 'Tekst',  'Investering', 'Besparelse [kr.]', 'Rentabilitet', 'EnergimærkeID', 'Forslagets gruppe ID']
    orig = convert_df_excel(df_s, liste)

    coldown_1.subheader('Download udtræk af forslag')
    coldown_1.download_button(
                              label='📥 Download forslag som excel fil',
                              data=excel,
                              file_name='Energimærkeforslag ' + ', '.join(map(str, municipalities)) + '.xlsx',
                              mime='xlsx'
    )
    # coldown_1.download_button(
    #                           label='📥 Download input data som CSV fil',
    #                           data=csv_i,
    #                           file_name='Energimærkeinput_data.csv',
    #                           mime='text/csv'
    # )
    coldown_1.download_button(
                              label='📥 Download overordnet forslag info som CSV fil',
                              data=csv,
                              file_name='Energimærkeforslag_overordnet ' + ', '.join(map(str, municipalities)) + '.csv',
                              mime='text/csv'
    )
    coldown_1.download_button(
                              label='📥 Download forslags data som CSV fil',
                              data=csv_f,
                              file_name='Energimærkeforslag_data ' + ', '.join(map(str, municipalities)) + '.csv',
                              mime='text/csv'
    )
    coldown_1.download_button(
                              label='📥 Download oprindelig data som excel fil',
                              data=orig,
                              file_name='Energimærkedata ' + ', '.join(map(str, municipalities)) + '.xlsx',
                              mime='xlsx'
    )
    # coldown_1.download_button(
    #                           label='📥 Download original som CSV fil',
    #                           data=orig,
    #                           file_name='Energimærkeforslag_original.csv',
    #                           mime='text/csv'
    # )

with st.expander("info"):
    container = st.container()
    col_1, col_2 = container.columns(2)
    #st.write(df_f.head(1000))
    # col_2.write(df_i)
    # col_1.write(df_f)
    #container.write(df_f.head(1000))

    @st.experimental_memo
    def samlet_info(df_f):
        #df_f = df_f[df_f['data_værdi'].str.isnumeric()]
        df_f['data_værdi input'] = pd.to_numeric(df_f['data_værdi input'], errors='coerce')
        df_f['data_værdi forslag'] = pd.to_numeric(df_f['data_værdi forslag'], errors='coerce')
        df_f = df_f[(df_f['data_kategori'] == 'Length') |
                    (df_f['data_kategori'] == 'NumberOfPumps') |
                    (df_f['data_kategori'] == 'NumberOfEntities') |
                    (df_f['data_kategori'] == 'Area')]
        #df_f['data_værdi'] = pd.to_numeric(df_f['data_værdi'])
        #st.write(df_f.head(1000))
        tekst = df_f.groupby(['tekst input', 'tekst forslag']).agg({'data_kategori': 'first', 'data_værdi forslag':'sum', 'data_værdi input':'sum',  'EnergimærkeID': 'count'}).reset_index()
        df_f = df_f.groupby(['Adresse', 'Teknikområde', 'data_kategori']).sum().reset_index()


        samlet = df_f.groupby(['Teknikområde', 'data_kategori']).agg({'data_værdi input':'sum', 'EnergimærkeID': 'count'}).reset_index()

        return samlet, df_f, tekst
    #
    # samlet_i, df_i, tekst_i = samlet_info(df_i)
    #
    #
    #
    # col_2.write('Input info')
    # col_2.write(samlet_i)
    # col_2.write('Samlet antal bygninger: ' + str(df_i['EnergimærkeID'].nunique()))
    # col_2.write(tekst_i)


    samlet_f, df_f, tekst_f = samlet_info(df_f)

    st.write('Forslag info')
    st.write(samlet_f)
    st.write('Samlet antal bygninger: ' + str(df_f['EnergimærkeID'].nunique()))
    st.write(tekst_f)


    tjek = st.checkbox('Vis data på enkelte bygninger')

    if tjek:
        st.write(df_f.head(1000))


    csvvvv = convert_df_csv(tekst_f)
    st.download_button(
                              label='📥 Download oprindelig data',
                              data=csvvvv,
                              file_name='samlet.csv',
                              mime='csv'
    )



# cloud = forslag['Teknikområde'].value_counts()
# cloud = cloud.reset_index()
#cloud['Teknikområde'] = df['Teknikområde']
df = df[df['Teknikområde'] != 'Kedler']
cloud = df.groupby('Teknikområde').sum()#df['Besparelse [kr.]']/df['Investering']
cloud['besp'] = cloud['Besparelse [kr.]']/cloud['Investering']
cloud = cloud['besp']
cloud = cloud.reset_index()
# st.write(cloud)
cloud = [tuple(x) for x in cloud.to_numpy()]


wcloud = (WordCloud()
.add(series_name="Teknikområde", data_pair=cloud, word_size_range=[10, 66])
.set_global_opts(
    title_opts=opts.TitleOpts(
        title="", title_textstyle_opts=opts.TextStyleOpts(font_size=23)
    ),
    tooltip_opts=opts.TooltipOpts(is_show=True),
    toolbox_opts=opts.ToolboxOpts(is_show=True),
))

st_pyecharts(wcloud, height='500px')


#st.balloons()

################ Løft til næste niveau #########################################
# st.header('Løft til næste niveau')
# with st.expander("Løft til næste niveau"):
#     col1, col2 = st.columns(2)
#     col2.image('EMO_grænse.png')
