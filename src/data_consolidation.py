import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2 

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


def consolidate_station_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Charger les codes des villes depuis CONSOLIDATE_CITY
    city_codes_df = con.execute("SELECT id, name FROM CONSOLIDATE_CITY").df()
    print("Colonnes dans city_codes_df :", city_codes_df.columns.tolist())

    # Créer le mapping en utilisant les noms de colonnes en minuscules
    city_code_mapping = dict(zip(city_codes_df['name'], city_codes_df['id']))

    # Normalisation des nom de villes pour le mapping
    city_codes_df['name'] = city_codes_df['name'].str.strip().str.lower()
    city_code_mapping = dict(zip(city_codes_df['name'], city_codes_df['id']))

    # ----------- Paris -----------
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data_paris = json.load(fd)
    paris_raw_data_df = pd.json_normalize(data_paris)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"1-{x}")
    paris_raw_data_df["created_date"] = date.today()
    
    # Normalisation des noms de villes pour correspondre au mapping
    paris_raw_data_df["city_name"] = paris_raw_data_df["nom_arrondissement_communes"].str.strip().str.lower()
    paris_raw_data_df["city_code"] = paris_raw_data_df["city_name"].map(city_code_mapping)

    required_columns = ['address']
    for col in required_columns:
        if col not in paris_raw_data_df.columns:
            paris_raw_data_df[col] = None

    paris_station_data_df = paris_raw_data_df[[
        "id",
        "stationcode",
        "name",
        "city_name",
        "city_code",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]].copy()
    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status"
    }, inplace=True)

    # ----------- Nantes -----------
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data_nantes = json.load(fd)
    nantes_raw_data_df = pd.json_normalize(data_nantes)
    nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"2-{x}")
    nantes_raw_data_df["created_date"] = date.today()
    nantes_raw_data_df["city_name"] = "nantes"  # Déjà en minuscules
    nantes_raw_data_df["city_code"] = nantes_raw_data_df["city_name"].map(city_code_mapping)


    required_columns = ['address']
    for col in required_columns:
        if col not in nantes_raw_data_df.columns:
            nantes_raw_data_df[col] = None

    nantes_station_data_df = nantes_raw_data_df[[
        "id",
        "number",
        "name",
        "city_name",
        "city_code",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]].copy()
    nantes_station_data_df.rename(columns={
        "number": "code",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "bike_stands": "capacity"
    }, inplace=True)

    # ----------- Toulouse -----------
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data_toulouse = json.load(fd)
    toulouse_raw_data_df = pd.json_normalize(data_toulouse)  # Ajustez en fonction de la structure réelle

    toulouse_raw_data_df["id"] = toulouse_raw_data_df["number"].apply(lambda x: f"3-{x}")
    toulouse_raw_data_df["created_date"] = date.today()
    toulouse_raw_data_df["city_name"] = "toulouse"
    toulouse_raw_data_df["city_code"] = toulouse_raw_data_df["city_name"].map(city_code_mapping)


    required_columns = ['address']
    for col in required_columns:
        if col not in toulouse_raw_data_df.columns:
            toulouse_raw_data_df[col] = None

    toulouse_station_data_df = toulouse_raw_data_df[[
        "id",
        "number",
        "name",
        "city_name",
        "city_code",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]].copy()

    toulouse_station_data_df.rename(columns={
    "number": "code",
    "position.lon": "longitude",
    "position.lat": "latitude",
    "bike_stands": "capacity"
    }, inplace=True)


    # Combiner les données
    combined_station_data_df = pd.concat([paris_station_data_df, nantes_station_data_df, toulouse_station_data_df], ignore_index=True)
    print("Columns in combined_station_data_df:", combined_station_data_df.columns.tolist())
    # Insérer dans la base de données
    con.register("combined_station_data_df", combined_station_data_df)
    con.execute("""
        INSERT OR REPLACE INTO CONSOLIDATE_STATION
        SELECT * FROM combined_station_data_df
    """)



def consolidate_city_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # Charger les données des communes
    with open(f"data/raw_data/{today_date}/communes_data.json") as fd:
        communes_data = json.load(fd)

    # Convertir en DataFrame
    communes_df = pd.json_normalize(communes_data)

    # Renommer les colonnes pour correspondre au schéma
    communes_df.rename(columns={
        'nom': 'name',
        'code': 'id',
        'population': 'nb_inhabitants'
    }, inplace=True)

    # Ajouter la date de création
    communes_df['created_date'] = date.today()

    # Sélectionner les colonnes nécessaires
    city_data_df = communes_df[['id', 'name', 'nb_inhabitants', 'created_date']]

    # Insérer dans la base de données
    con.register("city_data_df", city_data_df)
    con.execute("""
        INSERT OR REPLACE INTO CONSOLIDATE_CITY
        SELECT * FROM city_data_df
    """)


def consolidate_station_statement_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    # ----------- Paris Station Statement Data -----------
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data_paris = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data_paris)

    required_columns = {
        'stationcode': None,
        'numdocksavailable': 0,
        'numbikesavailable': 0,
        'duedate': None
    }

    for col, default_value in required_columns.items():
        if col not in paris_raw_data_df.columns:
            paris_raw_data_df[col] = default_value

    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = date.today()

    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]]

    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date"
    }, inplace=True)

    # ----------- Nantes Station Statement Data -----------
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data_nantes = json.load(fd)

    nantes_raw_data_df = pd.json_normalize(data_nantes)

    # Ensure required columns are present
    required_columns_nantes = {
        'number': None,
        'available_bike_stands': 0,
        'available_bikes': 0,
        'last_update': None
    }

    for col, default_value in required_columns_nantes.items():
        if col not in nantes_raw_data_df.columns:
            nantes_raw_data_df[col] = default_value

    nantes_raw_data_df["station_id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["created_date"] = date.today()

    nantes_station_statement_data_df = nantes_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]]

    nantes_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date"
    }, inplace=True)

    # ----------- Toulouse -----------
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data_toulouse = json.load(fd)
    toulouse_raw_data_df = pd.json_normalize(data_toulouse)  # Ajustez en fonction de la structure réelle

    toulouse_raw_data_df["station_id"] = toulouse_raw_data_df["number"].apply(lambda x: f"3-{x}")
    toulouse_raw_data_df["created_date"] = date.today()

    toulouse_station_statement_data_df = toulouse_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]].copy()

    toulouse_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date"
    }, inplace=True)

    # Combiner les données
    combined_station_statement_data_df = pd.concat([paris_station_statement_data_df, nantes_station_statement_data_df, toulouse_station_statement_data_df], ignore_index=True)

    # ----------- Insertion dans la base -----------
    con.register("combined_station_statement_data_df", combined_station_statement_data_df)
    con.execute("""
        INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT
        SELECT * FROM combined_station_statement_data_df
    """)

