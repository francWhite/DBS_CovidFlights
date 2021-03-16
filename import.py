import mysql.connector
import csv
from os import walk

def main():
  db = mysql.connector.connect(host="localhost",  user="hslu_user",  password=")dz,K^K=_=qqWd487=JR.T@=V#pg!7!K", database="covid_flights")
  cursor = db.cursor()
  
  import_lookupTable(cursor, 'Airport', './datasets/lookup-tables/AIRPORT.csv')
  import_lookupTable(cursor, 'Airline', './datasets/lookup-tables/AIRLINE_IATA_CODE.csv')
  import_lookupTable(cursor, 'State', './datasets/lookup-tables/STATE_ABR_AVIATION.csv')
  db.commit()
  
  import_flights(cursor, db, './datasets/flightdata')
  import_covidCaseRecords(cursor, db, './datasets/covid/covid-us-counties.csv')

#-----------------------------------import lookup-tables---------------------------------------
def import_lookupTable(cursor, tablename, filename):
  print('importing: {}'.format(filename))

  with open(filename) as csv_file:
    csv_file.readline() #skip header
    csv_reader = csv.reader(csv_file, delimiter=',')

    for row in csv_reader:
      sql = "INSERT INTO {} (Code, Name) VALUES (%s, %s)".format(tablename)
      values = (row[0], row[1])
      cursor.execute(sql, values)

#--------------------------------------import flights------------------------------------------
def import_flights(cursor, db, directory):
   _, _, filenames = next(walk(directory))   
   for filename in filenames:
     import_flight(cursor, db, "{}/{}".format(directory, filename))

def import_flight(cursor, db, filename):
  print('importing: {}'.format(filename))
  
  airlines_cache = {}
  airports_cache = {}
  states_cache = {}
  values = []

  with open(filename) as csv_file:
    csv_file.readline()
    csv_reader = csv.reader(csv_file, delimiter=',')

    for row in csv_reader:
      date = row[0]
      airline_code = row[1]
      flightnumber = row[2]
      tailnumber = row[3]
      originAirport_code = row[4]
      originState_code = row[6]
      destAirport_code = row[7]
      destState_code = row[9]
      cancelled = row[10]
      duplicate = 1 if row[11] == "Y" else 0
      airTime =  row[12] if row[12] else None
      distance = row[13] if row[13] else None

      airline_id = get_cached_airline_id(cursor, airlines_cache, airline_code)
      originAirport_id = get_cached_airport_id(cursor, airports_cache, originAirport_code)
      originState_id = get_cached_state_id_byCode(cursor, states_cache, originState_code)
      destAirport_id = get_cached_airport_id(cursor, airports_cache, destAirport_code)
      destState_id = get_cached_state_id_byCode(cursor, states_cache, destState_code)

      row_values = (date, flightnumber, tailnumber, cancelled, duplicate, distance, airTime, airline_id, originAirport_id, originState_id, destAirport_id, destState_id)
      values.append(row_values)

  value_chunks = chunks(values, 10000)
  chunk_count = len(list(chunks(values, 10000)))
  index = 0
  for chunk in value_chunks:
    sql = "INSERT INTO Flight(Date, FlightNumber, TailNumber, Cancelled, Duplicate, Distance, AirTime, Airline_Id, OriginAirport_Id, OriginState_Id, DestinationAirport_Id, DestinationState_Id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    cursor.executemany(sql, chunk)
    db.commit()

    index += 1
    print("{:.2f}%".format(index / chunk_count * 100))

def get_cached_airline_id(cursor, airlines_cache, airline_code):
  if airline_code in airlines_cache:
    return airlines_cache[airline_code]
  else:
    cursor.execute("SELECT Airline_Id FROM airline WHERE Code = %s", (airline_code,))
    airline_id = cursor.fetchone()[0]
    airlines_cache[airline_code] = airline_id
    return airline_id

def get_cached_airport_id(cursor, airports_cache, airport_code):
  if airport_code in airports_cache:
    return airports_cache[airport_code]
  else:
    cursor.execute("SELECT Airport_Id FROM airport WHERE Code = %s", (airport_code,))
    airport_id = cursor.fetchone()[0]
    airports_cache[airport_code] = airport_id
    return airport_id

def get_cached_state_id_byCode(cursor, states_cache, state_code):
  if state_code in states_cache:
    return states_cache[state_code]
  else:
    cursor.execute("SELECT State_Id FROM state WHERE Code = %s", (state_code,))
    state_id = cursor.fetchone()[0]
    states_cache[state_code] = state_id
    return state_id

#--------------------------------------import covid case records------------------------------------------
def import_covidCaseRecords(cursor, db, filename):
  print('importing: {}'.format(filename))
  values = []
  states_cache = {}
  county_cache = {}

  with open(filename) as csv_file:
    csv_file.readline()
    csv_reader = csv.reader(csv_file, delimiter=',')

    for row in csv_reader:
      date = row[0]
      county_name = row[1]
      state_name = row[2]
      cases = row[4] if row[4] else 0
      deaths = row[5] if row[5] else 0
      
      state_id = get_cached_state_id_byName(cursor, states_cache, state_name)
      county_id = get_cached_county_id(cursor, county_cache, county_name)

      row_values = (date, cases, deaths, state_id, county_id)
      values.append(row_values)

  value_chunks = chunks(values, 10000)
  chunk_count = len(list(chunks(values, 10000)))
  index = 0
  for chunk in value_chunks:
    sql = "INSERT INTO covidcaserecord(Date, Cases, Deaths, State_Id, County_Id) VALUES (%s, %s, %s, %s, %s)"
    cursor.executemany(sql, chunk)
    db.commit()

    index += 1
    print("{:.2f}%".format(index / chunk_count * 100))

def get_cached_state_id_byName(cursor, states_cache, state_name):
  if state_name in states_cache:
    return states_cache[state_name]
  else:
    cursor.execute("SELECT State_Id FROM state WHERE Name = %s", (state_name,))
    state_id = cursor.fetchone()[0]
    states_cache[state_name] = state_id
    return state_id

def get_cached_county_id(cursor, county_cache, county_name):
  if county_name in county_cache:
    return county_cache[county_name]
  else:
    cursor.execute("SELECT County_Id FROM county WHERE Name = %s", (county_name,))
    result = cursor.fetchone()
    county_id = result[0] if result else None
    if not county_id:    
      cursor.execute("INSERT INTO county (Name) VALUES (%s)", (county_name,))
      cursor.execute("SELECT County_Id FROM county WHERE Name = %s", (county_name,))
      county_id = cursor.fetchone()[0]

    county_cache[county_name] = county_id
    return county_id

#--------------------------------------helper methods#----------------------------------------

  if state_code in states_cache:
    return states_cache[state_code]
  else:
    cursor.execute("SELECT State_Id FROM state WHERE Code = %s", (state_code,))
    state_id = cursor.fetchone()[0]
    states_cache[state_code] = state_id
    return state_id

def file_len(filename):
    with open(filename) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

main()