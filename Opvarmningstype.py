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

st.set_page_config(layout="wide", page_title="EMO Opvarmningstype", page_icon="andel_o.png")

st.header('Opvarmningstype')
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

    print('Forslagsdata')

    query = """
            WITH prop_group AS
            (
            SELECT prop_group.energylabel_id, prop_group.shorttext, prop_group.longttext, prop_group.seebclassification
            FROM energylabels.proposal_groups AS prop_group
            WHERE seebclassification IN ('2-1-5-0')
            )
            SELECT prop_group.energylabel_id,
            build.propertynumber,  build.ownership, build.municipalitynumber,
            build.postalcode, build.postalcity,

            prop_group.shorttext, prop_group.seebclassification, prop_group.longttext

            FROM energylabels.building_data AS build
            RIGHT JOIN prop_group ON build.energylabel_id = prop_group.energylabel_id

            ORDER BY build.energylabel_id
            """


    n = 10000
    dfs = []
    for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        dfs.append(chunk)
    df_prop = pd.concat(dfs)
    print("--- df %s seconds ---" % (time.time() - start_time))

    print('Input data')

    query = """
            WITH input AS
            (
            SELECT input.energylabel_id, input.shorttext, input.seebclassification, input.data_input_type, input.data_category, input.data_value
            FROM energylabels.input_data AS input
            WHERE seebclassification IN ('2-0-0-0', '2-1-0-0', '2-1-1-0', '2-1-2-0', '2-1-3-0', '2-1-4-0', '2-1-5-0', '2-1-6-0')
            )
            SELECT input.energylabel_id,
            build.propertynumber,  build.ownership, build.municipalitynumber,
            build.postalcode, build.postalcity,

            input.shorttext, input.seebclassification, input.data_input_type

            FROM energylabels.building_data AS build
            RIGHT JOIN input ON build.energylabel_id = input.energylabel_id

            ORDER BY build.energylabel_id
            LIMIT 1000000
            """


    n = 10000
    dfs = []
    for chunk in tqdm(pd.read_sql(query, con=cnxn, chunksize=n)):
        dfs.append(chunk)
    df = pd.concat(dfs)
    print("--- df %s seconds ---" % (time.time() - start_time))

    return df, df_prop  #

df, df_prop = hent_data()

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
    df["teknikområde"] = df["seebclassification"]
    df["teknikområde"].replace(
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
            "2-0-0-0": "Varmeanlæg-200",
            "2-1-0-0": "Varmeanlæg-210",
            "2-1-1-0": "Varmeanlæg-211",
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
    return df

df = magi(df)
df_prop = magi(df_prop)

if st.checkbox('Vis rådata'):
    st.subheader('df')
    st.write(df.head(1000))
    st.subheader('df_prop')
    st.write(df_prop.head(1000))



value = df_prop.groupby('Kommune').count()
value = value.reset_index()
st.subheader('Antal varmepumpeprojekter fordelt på kommunerne')
st.write(value)

@st.experimental_singleton
def figur(df, grader):
    b1 = (
        Bar()
        .add_xaxis(list(df.Kommune))
        .add_yaxis('Antal varmepumpeprojekter', list(df.energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
        #.add_yaxis('Besparelse i kg CO2/år', list(df.besparelse_CO2/1000), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
        #.add_yaxis('Antal projekter per år', list(pd.to_numeric(df['values'])), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),)
         .set_global_opts(
            legend_opts=opts.LegendOpts(orient='vertical', pos_left="left", is_show=True),
            #label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"),
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=grader)),
            title_opts=opts.TitleOpts(

                title='Varmepumper per kommune', subtitle="Fordeling af antal forslag på {}".format('varmepumper'), pos_left="center"
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
b1 = figur(value, 90)
st_pyecharts(b1, height='500px')

# val = df.groupby('energylabel_id').unique('teknikområde')
# val = val.reset_index()
val = df.groupby(['Kommune', 'teknikområde']).count().unstack(fill_value=0).stack()
val = val.reset_index()
st.subheader('Antal opvarmningstype indenfor kategorien per kommune')
st.write(val)


@st.experimental_singleton
def figur(df, grader):
    b1 = (
        Bar()
        .add_xaxis(list(df.Kommune.unique()))
        .add_yaxis('Solvarme', list(df[df['teknikområde']=='Solvarme'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Varmepumpe', list(df[df['teknikområde']=='Varmepumper'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Fjernvarme', list(df[df['teknikområde']=='Fjernvarme'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Kedler', list(df[df['teknikområde']=='Kedler'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Ovne', list(df[df['teknikområde']=='Ovne'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Varmeanlæg-200', list(df[df['teknikområde']=='Varmeanlæg-200'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Varmeanlæg-210', list(df[df['teknikområde']=='Varmeanlæg-210'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
        .add_yaxis('Varmeanlæg-211', list(df[df['teknikområde']=='Varmeanlæg-211'].energylabel_id), label_opts=opts.LabelOpts(is_show=False, formatter="{b}: {c}"), stack='stack1')
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
        # markpoint_opts=opts.MarkPointOpts(
        #     data=[
        #         opts.MarkPointItem(type_="max", name="Maximum"),
        #         opts.MarkPointItem(type_="min", name="Minimum"),
        #     ]
        # ),
        # markline_opts=opts.MarkLineOpts(
        #     data=[
        #         opts.MarkLineItem(type_="max", name="Maximum"),
        #     ]
        # ),
    )
    )
    return b1
b1 = figur(val, 90)
st_pyecharts(b1, height='500px')
