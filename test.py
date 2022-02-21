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
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb
import math
from pyecharts import options as opts
from pyecharts.charts import Bar, Pie, Grid, Line, Scatter, Sankey, WordCloud
from streamlit_echarts import st_pyecharts
import psycopg2 as db
import pulp
from pulp import *
import time

pio.templates.default = "simple_white"


st.set_page_config(layout="wide", page_title="EMO iPortefølje", page_icon="andel_e.png")

st.image('EMO_iportefølje.png', width=800)

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

@st.experimental_memo
def kommune_og_ejerskab():
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

st.write(kommune)
st.write(ejerskab)



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


if not municipalities:
    st.info('Du skal vælge en kommune')
    st.stop()
if not bygningstyper:
    st.info('Du skal vælge en ejerskabsform')
    st.stop()


kommune = kommune[kommune['Kommune'].isin(municipalities)]
ejerskab = ejerskab[ejerskab['ejerskab'].isin(bygningstyper)]

st.write(kommune)
st.write(tuple(kommune['municipalitynumber']))
st.write(ejerskab)
st.write(str(tuple(ejerskab['ownership'])).replace(',',''))


# %%
@st.experimental_memo
def hent_data(BBR0, BBR1, ener_list, kommune, ejerskab):
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
                SELECT energylabel_id, CAST(build.propertynumber AS INT), build.building_id, build.ownership, build.reviewdate, CAST(build.municipalitynumber AS INT), build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE (municipalitynumber IN {}) AND (propertynumber IN {})
                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.longttext, prop_group.seebclassification,
                build.propertynumber, build.building_id, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
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
                SELECT energylabel_id, build.propertynumber, build.building_id, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE energylabel_id IN {}
                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.longttext, prop_group.seebclassification,
                build.propertynumber, build.building_id, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
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
                SELECT energylabel_id, CAST(build.propertynumber AS INT), build.building_id, build.ownership, build.reviewdate,  CAST(build.municipalitynumber AS INT), build.streetname, build.housenumber,
                build.postalcode, build.postalcity, build.usecode, build.dwellingarea, build.commercialarea
                FROM energylabels.building_data AS build
                WHERE (municipalitynumber IN {}) AND (ownership IN {})

                )
                SELECT build.energylabel_id,
                fuel.proposal_group_id, fuel.fuelsaved, fuel.material, fuel.unit,
                fuel.energyperunit, fuel.co2perunit, fuel.costperunit, fuel.fixedcostperyear, fuel.original_cost,
                prof.profitability, prop_group.investment, prof.investmentlifetime,
                prop_group.shorttext, prop_group.longttext, prop_group.seebclassification,
                build.propertynumber, build.building_id, build.ownership, build.reviewdate, build.municipalitynumber, build.streetname, build.housenumber,
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

df, data_time = hent_data(BBR0, BBR1, ener_list, kommune, ejerskab)
st.write(df)
