CREATE SCHEMA covid_flights;
USE covid_flights;

CREATE TABLE Airport
(
    Airport_Id INT          NOT NULL AUTO_INCREMENT,
    Code       VARCHAR(255) NOT NULL,
    Name       VARCHAR(255) NOT NULL,
    PRIMARY KEY (Airport_Id)
);

CREATE TABLE Airline
(
    Airline_Id INT          NOT NULL AUTO_INCREMENT,
    Code       VARCHAR(255) NOT NULL,
    Name       VARCHAR(255) NOT NULL,
    PRIMARY KEY (Airline_Id)
);

CREATE TABLE State
(
    State_Id INT          NOT NULL AUTO_INCREMENT,
    Code     VARCHAR(255) NOT NULL,
    Name     VARCHAR(255) NOT NULL,
    PRIMARY KEY (State_Id)
);

CREATE TABLE County
(
    County_Id INT          NOT NULL AUTO_INCREMENT,
    Name      VARCHAR(255) NOT NULL,
    PRIMARY KEY (County_Id)
);

CREATE TABLE Flight
(
    Flight_Id             INT         NOT NULL AUTO_INCREMENT,
    Date                  DATE        NOT NULL,
    FlightNumber          INT         NOT NULL,
    TailNumber            VARCHAR(10) NOT NULL,
    Cancelled             BOOL,
    Duplicate             BOOL,
    Distance              INT,
    AirTime               INT,
    Airline_Id            INT         NOT NULL,
    OriginAirport_Id      INT         NOT NULL,
    OriginState_Id        INT         NOT NULL,
    DestinationAirport_Id INT         NOT NULL,
    DestinationState_Id   INT         NOT NULL,
    PRIMARY KEY (Flight_Id),
    FOREIGN KEY (Airline_Id) REFERENCES Airline (Airline_Id),
    FOREIGN KEY (OriginAirport_Id) REFERENCES Airport (Airport_Id),
    FOREIGN KEY (OriginState_Id) REFERENCES State (State_Id),
    FOREIGN KEY (DestinationAirport_Id) REFERENCES Airport (Airport_Id),
    FOREIGN KEY (DestinationState_Id) REFERENCES State (State_Id)
);

CREATE TABLE CovidCaseRecord
(
    Record_Id INT  NOT NULL AUTO_INCREMENT,
    Date      DATE NOT NULL,
    Cases     INT  NOT NULL,
    Deaths    INT  NOT NULL,
    State_Id  INT  NOT NULL,
    County_Id INT  NOT NULL,

    PRIMARY KEY (Record_Id),
    FOREIGN KEY (State_Id) REFERENCES State (State_Id),
    FOREIGN KEY (County_Id) REFERENCES County (County_Id)
);