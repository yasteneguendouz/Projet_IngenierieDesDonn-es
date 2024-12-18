-- Table des stations dimensionnelles
CREATE TABLE IF NOT EXISTS DIM_STATION (
    ID VARCHAR PRIMARY KEY,
    CODE VARCHAR,
    NAME VARCHAR,
    ADDRESS VARCHAR,
    LONGITUDE DOUBLE,
    LATITUDE DOUBLE,
    STATUS VARCHAR,
    CAPACITY INTEGER
);

-- Table des villes dimensionnelles
CREATE TABLE IF NOT EXISTS DIM_CITY (
    ID VARCHAR PRIMARY KEY,
    NAME VARCHAR,
    NB_INHABITANTS INTEGER
);

-- Table des faits des relevés de stations
CREATE TABLE IF NOT EXISTS FACT_STATION_STATEMENT (
    STATION_ID VARCHAR NOT NULL,
    CITY_ID VARCHAR NOT NULL,
    BICYCLE_DOCKS_AVAILABLE INTEGER,
    BICYCLE_AVAILABLE INTEGER,
    LAST_STATEMENT_DATE TIMESTAMP,
    CREATED_DATE DATE DEFAULT current_date,
    PRIMARY KEY (STATION_ID, CITY_ID, CREATED_DATE),
    FOREIGN KEY (STATION_ID) REFERENCES DIM_STATION (ID),
    FOREIGN KEY (CITY_ID) REFERENCES DIM_CITY (ID)
);

