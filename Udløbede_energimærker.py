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

st.set_page_config(layout="wide", page_title="EMO udløbede energimærker", page_icon="andel_u.png")

st.header('Udløbede energimærker')
start_time = time.time()
st.sidebar.image("andel_logo_white_rgb.png")
st.sidebar.write("Version 0.1")





@st.experimental_memo
def hent_data():
    start_time = time.time()
    SERVER = "redshift.bi.obviux.dk"
    PORT = '5439'  # Redshift default
    USER = "mrs"
    PASSWORD = "j89Foijf8fIJFAD8dsIFJA8DFMasf_D7fa9df"
    DATABASE = "redshift"

    cnxn = db.connect(host=SERVER, database=DATABASE, user=USER, password=PASSWORD, port=PORT)

    print("Connected to Redshift")

    query = """

            SELECT energylabel_id, CAST(build.propertynumber AS INT), build.buildingnumber, build.ownership, build.reviewdate,  CAST(build.municipalitynumber AS INT),
            build.postalcode, build.postalcity,  build.dwellingarea, build.commercialarea
            FROM energylabels.building_data AS build



            """ #WHERE ownership != 'Private'
    n = 10000
    dfs = []
    for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        dfs.append(chunk)
    df = pd.concat(dfs)
    print("--- df %s seconds ---" % (time.time() - start_time))
    data_time = time.time() - start_time

    return df, data_time  #
df, data_time = hent_data()

@st.experimental_memo
def magi(df):
    df["Kommune"] = df["municipalitynumber"].astype(str)
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
    df["beboelsesareal"] = pd.to_numeric(df["dwellingarea"])
    df["kommercielt_areal"] = pd.to_numeric(df["commercialarea"])
    df['areal'] = df['beboelsesareal'] + df["kommercielt_areal"]
    df['besigtelsesdato'] = pd.to_datetime(df['reviewdate'], errors='coerce')
    df['år'] = pd.DatetimeIndex(df['besigtelsesdato']).year
    basedate = pd.Timestamp.today()
    df['antal_år'] = ((basedate-df['besigtelsesdato']).dt.days/365).apply(np.floor)
    df["ownership"].replace(
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
    return df
df = magi(df)

with st.sidebar.expander('Vælg kommune og ejerskab'):
    container = st.container()
    all = st.checkbox("Vælg alle kommuner")

    if all:
        municipalities = container.multiselect("Vælg dine yndlingskommuner",
            options=list(np.unique(df["Kommune"])),
            default=list(np.unique(df["Kommune"])))
    else:
        municipalities =  container.multiselect("Vælg dine yndlingskommuner",
            options=list(np.unique(df["Kommune"])))

    container = st.container()
    all2 = st.checkbox("Vælg alle ejerskabsformer")

    if all2:
        bygningstyper = container.multiselect("Hvilken ejerskabsform skal medtages",
            options=list(np.unique(df["ownership"])),
            default=list(np.unique(df["ownership"])))
    else:
        bygningstyper =  container.multiselect("Hvilken ejerskabsform skal medtages",
            options=list(np.unique(df["ownership"])))
    st.info('Vælg kun det data der skal bruges')

if not municipalities:
    st.info('Du skal vælge en kommune')
    st.stop()
if not bygningstyper:
    st.info('Du skal vælge en ejerskabsform')
    st.stop()

@st.experimental_memo
def bygningstype(bygningstyper, municipalities, df):
    """ Filters if the data is in the municipalities chosen and in the types of buildings chosen.
    Also merges the proposals and building_data tables. """

    df = df[df["Kommune"].isin(municipalities)]
    df = df[df["ownership"].isin(bygningstyper)]
    return df

df = bygningstype(bygningstyper, municipalities, df)

st.write(df.head(10000))
st.write(df.shape[0])
df.loc[df['antal_år'] <= 0,'antal_år'] = np.nan



val = df.groupby(['Kommune', 'antal_år'], dropna=False).count().unstack(fill_value=0).stack()
val = val.reset_index()
st.subheader('Antal bygninger i hver alderskategori fordelt på kommune')
st.write(val)


@st.experimental_singleton
def figur(df, grader):
    b1 = (
        Bar()
        .add_xaxis(list(df.Kommune.unique()))
        .add_yaxis('0 år gammelt', list(df[df['antal_år']==0].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('1 år gammelt', list(df[df['antal_år']==1].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('2 år gammelt', list(df[df['antal_år']==2].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('3 år gammelt', list(df[df['antal_år']==3].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('4 år gammelt', list(df[df['antal_år']==4].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('5 år gammelt', list(df[df['antal_år']==5].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('6 år gammelt', list(df[df['antal_år']==6].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('7 år gammelt', list(df[df['antal_år']==7].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('8 år gammelt', list(df[df['antal_år']==8].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('9 år gammelt', list(df[df['antal_år']==9].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('10 år gammelt', list(df[df['antal_år']==10].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('11 år gammelt', list(df[df['antal_år']==11].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('12 år gammelt', list(df[df['antal_år']==12].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        #.add_yaxis('Besparelse i kg CO2/år', list(df.besparelse_CO2/1000), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
        #.add_yaxis('Antal projekter per år', list(pd.to_numeric(df['values'])), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
         .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="right", is_show=True),
            #label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=grader)),
            title_opts=opts.TitleOpts(

                title='Opvarmningstyper per kommune', subtitle="Fordeling af opvarminingstyper per kommune", pos_left="center"
            ),
            toolbox_opts=opts.ToolboxOpts(orient='vertical', is_show=False),
        )
        .set_series_opts(

    )
    )
    return b1
b1 = figur(val, 90)
st_pyecharts(b1, height='500px')


val = df.groupby(['antal_år', 'ownership']).count().unstack(fill_value=0).stack()
val = val.reset_index()
st.subheader('Antal bygninger i hver alderskategori')
st.write(val)

@st.experimental_singleton
def figur(df, grader):
    b1 = (
        Bar()
        .add_xaxis(list(df['antal_år'].unique()))
        .add_yaxis('Privat', list(df[df['ownership']=='Privat'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Privat andelsboligforening', list(df[df['ownership']=='Privat andelsboligforening'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Anden kommune', list(df[df['ownership']=='Anden kommune'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Firmaer og selskaber', list(df[df['ownership']=='Firmaer og selskaber'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Forening, legat eller selvejende institution', list(df[df['ownership']=='Forening, legat eller selvejende institution'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Iboliggende kommune', list(df[df['ownership']=='Iboliggende kommune'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Region', list(df[df['ownership']=='Region'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Staten', list(df[df['ownership']=='Staten'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Almennyttig boligselskab', list(df[df['ownership']=='Almennyttig boligselskab'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Andet', list(df[df['ownership']=='Andet'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
         .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="right", is_show=True),
            #label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=grader)),
            title_opts=opts.TitleOpts(

                title='Opvarmningstyper per kommune', subtitle="Fordeling af opvarminingstyper per kommune", pos_left="center"
            ),
            toolbox_opts=opts.ToolboxOpts(orient='vertical', is_show=False),
        )
        .set_series_opts(

    )
    )
    return b1
b1 = figur(val, 0)
st_pyecharts(b1, height='500px')
