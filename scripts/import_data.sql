-- import with plane SQL (also slow...)

USE covid_flights;

SET autocommit = 0;
SET unique_checks = 0;
SET foreign_key_checks = 0;

CREATE TABLE csv_airline
(
    Code        VARCHAR(255),
    Description VARCHAR(255)
);

CREATE TABLE csv_airport
(
    Code        VARCHAR(255),
    Description VARCHAR(255)
);

CREATE TABLE csv_state
(
    Code        VARCHAR(255),
    Description VARCHAR(255)
);

CREATE TABLE csv_flights
(
    FL_DATE            VARCHAR(255),
    MKT_CARRIER        VARCHAR(255),
    MKT_CARRIER_FL_NUM VARCHAR(255),
    TAIL_NUM           VARCHAR(255),
    ORIGIN             VARCHAR(255),
    ORIGIN_CITY_NAME   VARCHAR(255),
    ORIGIN_STATE_ABR   VARCHAR(255),
    DEST               VARCHAR(255),
    DEST_CITY_NAME     VARCHAR(255),
    DEST_STATE_ABR     VARCHAR(255),
    CANCELLED          VARCHAR(255),
    DUP                VARCHAR(255),
    AIR_TIME           VARCHAR(255),
    DISTANCE           VARCHAR(255),
    placeholder        VARCHAR(255)
);

CREATE TABLE csv_covidcases
(
    date   VARCHAR(255),
    county VARCHAR(255),
    state  VARCHAR(255),
    fips   VARCHAR(255),
    cases  VARCHAR(255),
    deaths VARCHAR(255)
);

-- get allowed path with: SHOW VARIABLES LIKE 'secure_file_priv'

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/lookup-tables/AIRLINE_IATA_CODE.csv' INTO TABLE csv_airline COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/lookup-tables/AIRPORT.csv' INTO TABLE csv_airport COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/lookup-tables/STATE_ABR_AVIATION.csv' INTO TABLE csv_state COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_01.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_02.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_03.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_04.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_05.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_06.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_07.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_08.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_09.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_10.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_11.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/flightdata/FLIGHTS_12.csv' INTO TABLE csv_flights COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/covid/covid-us-counties.csv' INTO TABLE csv_covidcases COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;

COMMIT;

INSERT INTO airline (Code, Name)
SELECT Code, Description
FROM csv_airline;

INSERT INTO airport (Code, Name)
SELECT Code, Description
FROM csv_airport;

INSERT INTO state (Code, Name)
SELECT Code, Description
FROM csv_state;

INSERT INTO flight (Date, FlightNumber, TailNumber, Cancelled, Duplicate, Distance, AirTime, Airline_Id,
                    OriginAirport_Id, OriginState_Id, DestinationAirport_Id, DestinationState_Id)
SELECT FL_DATE,
       MKT_CARRIER_FL_NUM,
       TAIL_NUM,
       CANCELLED,
       IF(DUP = 'Y', 1, 0),
       IF(DISTANCE = '', 0, DISTANCE),
       IF(AIR_TIME = '', 0, AIR_TIME),
       al.Airline_Id,
       apo.Airport_Id,
       so.State_Id,
       apd.Airport_Id,
       sp.State_Id
FROM csv_flights f
         JOIN airline al ON al.Code = f.MKT_CARRIER
         JOIN airport apo ON apo.Code = f.ORIGIN
         JOIN state so ON so.Code = f.ORIGIN_STATE_ABR
         JOIN airport apd ON apd.Code = f.DEST
         JOIN state sp ON sp.Code = f.DEST_STATE_ABR;

COMMIT;

INSERT INTO county(Name)
SELECT DISTINCT county
FROM csv_covidcases;

INSERT INTO covidcaserecord (Date, Cases, Deaths, State_Id, County_Id)
SELECT date,
       IF(cases = '', 0, cases),
       IF(deaths = '', 0, deaths),
       s.State_Id,
       co.County_Id
FROM csv_covidcases cases
         JOIN state s ON s.Name = cases.state
         JOIN county co ON cases.county = co.Name;

DROP TABLE csv_airline;
DROP TABLE csv_airport;
DROP TABLE csv_state;
DROP TABLE csv_flights;
DROP TABLE csv_covidcases;


SET autocommit = 1;
SET unique_checks = 1;
SET foreign_key_checks = 1;

COMMIT;