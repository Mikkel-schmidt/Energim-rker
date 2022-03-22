#import pyodbc
import numpy as np
import pandas as pd
from tqdm import tqdm
import psycopg2 as db

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

elpris   = 2.08
elCO2    = 128.
DHpris   = 650.
DHCO2    = 141.
NGpris   = 9.0
NGCO2    = 204.
CGpris   = 9.0
CGCO2    = 204.
FGOpris  = 9.0
FGOCO2   = 266.
Oliepris = 9.0
OlieCO2  = 281.
woodpris = 9.0
woodCO2  = 0.0
WPpris   = 9.0
WPCO2    = 0.0
Rapspris = 9.0
RapsCO2  = 0.0
Halmpris = 9.0
HalmCO2  = 0.0

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

def udregn_pris(df):
    """
    Justerer pris på brændselstyper

        Parameters
        ----------
        df              Rå Pandas DataFrame

        Returns
        -------
        df              Pandas DataFrame
    """
    df['besparelse kr. per enhed_orig'] = df['Besparelse kr. per enhed']

    #df['Besparelse kr. per enhed'][(df['Materiale'] == 'El')] = elpris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'El')] = elpris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'Fjernvarme')] = DHpris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'Naturgas')] = NGpris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'Bygas')] = CGpris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'Fyringsgasolie')] = FGOpris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'Olie')] = Oliepris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'Træ')] = woodpris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'Træpiller')] = WPpris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'Rapsolie')] = Rapspris
    df['Besparelse kr. per enhed'][(df['Materiale'] == 'Halm')] = Halmpris
    return df

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
