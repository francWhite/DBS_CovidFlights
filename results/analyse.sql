USE covid_flights;

#-------- Cumulative number of cases per state and day --------------
SELECT cvr.date, s.Name, SUM(Cases) AS cases
FROM covidcaserecord cvr
         JOIN state s ON cvr.State_Id = s.State_Id
GROUP BY cvr.Date, cvr.State_Id
ORDER BY cvr.Date, cvr.State_Id;

#-------- Number of new cases per state and day --------------
#cases_per_state1 und cases_per_state2 sind identisch, aber MySql braucht scheinbar zwei querys um korrekt zu funktionieren...
WITH cases_per_state1 AS (
    SELECT cvr.date, cvr.State_Id, SUM(Cases) AS cases
    FROM covidcaserecord cvr
    GROUP BY cvr.Date, cvr.State_Id)
   , cases_per_state2 AS (
    SELECT cvr.date, cvr.State_Id, SUM(Cases) AS cases
    FROM covidcaserecord cvr
    GROUP BY cvr.Date, cvr.State_Id
)
SELECT c_current.Date, s.Name, c_current.Cases - COALESCE(c_previous.Cases, 0) AS new_cases
FROM cases_per_state1 c_current
         JOIN state s ON c_current.State_Id = s.State_Id
         LEFT JOIN cases_per_state2 c_previous ON c_previous.State_Id = c_current.State_Id AND
                                                  c_previous.Date = DATE_SUB(c_current.Date, INTERVAL 1 DAY)
ORDER BY c_current.Date, c_current.State_Id;

#-------- Number of flights per state and day --------
SELECT f.Date, s.Name, COUNT(*) number_of_flights
FROM flight f
         JOIN state s ON f.OriginState_Id = s.State_Id
GROUP BY f.Date, f.OriginState_Id
ORDER BY f.Date, f.OriginState_Id;


#-------- Final query: number of new cases and flights per state and day --------
WITH cases_per_state AS (
    WITH cases_per_state1 AS (
        SELECT cvr.date, cvr.State_Id, SUM(Cases) AS cases
        FROM covidcaserecord cvr
        GROUP BY cvr.Date, cvr.State_Id)
       , cases_per_state2 AS (
        SELECT cvr.date, cvr.State_Id, SUM(Cases) AS cases
        FROM covidcaserecord cvr
        GROUP BY cvr.Date, cvr.State_Id
    )
    SELECT c_current.Date,
           c_current.State_Id,
           c_current.Cases - COALESCE(c_previous.Cases, 0) AS new_cases
    FROM cases_per_state1 c_current
             LEFT JOIN cases_per_state2 c_previous ON c_previous.State_Id = c_current.State_Id AND
                                                      c_previous.Date = DATE_SUB(c_current.Date, INTERVAL 1 DAY)
    ORDER BY c_current.Date, c_current.State_Id
),
     flights_per_state AS (
         SELECT f.Date,
                f.OriginState_Id AS State_Id,
                COUNT(*)            number_of_flights
         FROM flight f
         GROUP BY f.Date, f.OriginState_Id
         ORDER BY f.Date, f.OriginState_Id
     )
SELECT f.Date,
       s.Name                   AS state,
       f.number_of_flights,
       COALESCE(c.new_cases, 0) AS number_of_new_cases
FROM flights_per_state f
         JOIN state s ON f.State_Id = s.State_Id
         LEFT OUTER JOIN cases_per_state c ON f.State_Id = c.State_Id AND f.Date = c.Date
ORDER BY f.Date, f.State_Id;



#------------------------------------ Random facts ------------------------------------
# most popular airline
SELECT a.Name, COUNT(f.Airline_Id) number_of_flights
FROM flight f
         JOIN airline a ON a.Airline_Id = f.Airline_Id
GROUP BY f.Airline_Id
ORDER BY 2 DESC;

# airport with most traffic
SELECT a.Code, a.Name, COUNT(f.OriginAirport_Id) number_of_flights_departing
FROM flight f
         JOIN airport a ON f.OriginAirport_Id = a.Airport_Id
GROUP BY f.OriginAirport_Id
ORDER BY 3 DESC;

#average airtime
SELECT AVG(AirTime) average_airtime
FROM flight;

# flights per day
SELECT date, COUNT(*) flights_per_day
FROM flight
GROUP BY Date
ORDER BY 2 DESC;

#total miles
select sum(Distance) total_miles from flight;