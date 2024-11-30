# README - Pipeline ETL pour les données des vélos en libre-service

## Description

Le but de ce projet est de créer un pipeline ETL (Extraction, Transformation, Chargement) pour récupérer, transformer, et stocker les données des stations de vélos en libre-service dans les grandes villes de France. Ce projet permet de mettre en pratique les connaissances acquises en data ingénierie et se base sur des données open-source disponibles en temps réel.

### Le pipeline intègre :

    Paris comme base initiale.
    Les villes supplémentaires Nantes, Toulouse, et potentiellement d'autres grandes villes françaises.
    Les données descriptives des communes françaises, via une API de l'État.
    
    
## Fonctionnalités principales

Le projet suit une architecture ETL classique avec trois étapes principales :


    Ingestion des données : Récupération des données via des APIs open-source et stockage sous forme de fichiers JSON localement.
    
    
    Consolidation des données : Chargement des données consolidées dans une base de données DuckDB, permettant une organisation et une historisation des données.
    
    
    Agrégation des données : Création de tables dimensionnelles et factuelles pour une modélisation analytique.
    
    
## Structure des fichiers

### Ingestion des données

Le fichier data_ingestion.py contient les fonctions permettant de récupérer les données en temps réel et de les enregistrer localement :

    get_paris_realtime_bicycle_data() : Récupère les données pour Paris.
    get_nantes_realtime_bicycle_data() : Récupère les données pour Nantes.
    get_toulouse_realtime_bicycle_data() : Récupère les données pour Toulouse.
    get_communes_data() : Récupère les données des communes françaises via l'API gouvernementale.

Les fichiers JSON sont stockés localement dans des dossiers horodatés dans data/raw_data/YYYY-MM-DD.


### Consolidation des données

Le fichier data_consolidation.py contient :

    Les fonctions pour créer les tables de consolidation via le fichier SQL create_consolidate_tables.sql.
    Des fonctions pour transformer et charger les données consolidées dans une base DuckDB :
        consolidate_station_data()
        consolidate_station_statement_data()
        consolidate_city_data()

Les données sont historisées dans DuckDB pour permettre des analyses temporelles.


### Agrégation des données

Le fichier data_agregation.py contient :

    Une fonction pour créer les tables agrégées via create_agregate_tables.sql.
    Des fonctions pour alimenter les tables dimensionnelles (DIM_STATION, DIM_CITY) et la table factuelle (FACT_STATION_STATEMENT).

Les tables sont conçues pour une modélisation dimensionnelle, facilitant l'analyse des données consolidées et leur visualisation.


### Exécution principale

Le fichier main.py orchestre toutes les étapes du pipeline :

    Récupération des données avec les fonctions d’ingestion.
    Transformation et chargement des données dans les tables consolidées.
    Agrégation des données dans des tables dimensionnelles et factuelles. 
    
    
## Prérequis

Pour exécuter ce projet, vous avez besoin :

    Python 3.8 ou une version plus récente.
    Les bibliothèques Python suivantes :
        requests
        pandas
        duckdb
    Un environnement virtuel Python pour gérer les dépendances.
    
    
## Installation 

Créez un environnement virtuel Python :

python3 -m venv .venv
source .venv/bin/activate

Installez les dépendances nécessaires :

pip install -r requirements.txt

Exécutez le pipeline :

python3 src/main.py

## Fonctionnement 

    Étape d'ingestion :
        Les données des stations et des communes sont récupérées via les APIs et enregistrées en local sous forme de fichiers JSON.

    Étape de consolidation :
        Les données JSON sont chargées dans une base DuckDB et organisées dans des tables CONSOLIDATE_*.

    Étape d'agrégation :
        Les données consolidées sont transformées en tables dimensionnelles et factuelles (DIM_*, FACT_*) pour une analyse ultérieure.
        
        
## Exemple de requêtes analytiques

Une fois le pipeline exécuté, vous pouvez effectuer des requêtes sur la base DuckDB. Par exemple :

    Nombre de vélos disponibles par ville :
    
  
  SELECT DIM_CITY.NAME, SUM(FACT_STATION_STATEMENT.BICYCLE_AVAILABLE) AS TOTAL_BICYCLES
FROM FACT_STATION_STATEMENT
JOIN DIM_CITY ON DIM_CITY.ID = FACT_STATION_STATEMENT.CITY_ID
GROUP BY DIM_CITY.NAME;

    Occupation moyenne des stations :
    
SELECT DIM_STATION.NAME, AVG(BICYCLE_AVAILABLE / CAPACITY) AS OCCUPANCY_RATE
FROM FACT_STATION_STATEMENT
JOIN DIM_STATION ON DIM_STATION.ID = FACT_STATION_STATEMENT.STATION_ID
GROUP BY DIM_STATION.NAME;

Pour accéder à la base DuckDB vous devez d'abord, après avoir exécuté le pipeline, exécuter la commande suivante : 
./duckdb data/duckdb/mobility_analysis.duckdb

## Note

Les données sont historisées dans les tables consolidées (CONSOLIDATE_*) pour permettre un suivi temporel.


## Contact

Pour toute question ou remarque, contactez nous via l'adresse mail suivante : 
yastene.guendouz@polytech-lille.net
