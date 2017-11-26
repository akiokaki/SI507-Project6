# Import statements
import psycopg2
import psycopg2.extras
import sys
import csv
from config_proj6 import *

### Write code / functions to set up database connection and cursor here.
try:
    conn = psycopg2.connect("dbname = '{0}' user = '{1}' password='{2}'".format(db_name, db_user, db_password))
    print("Success connecting to the database")

except:
    print("Unable to connect to the database")
    sys.exit(1)

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

### Write code / functions to create tables with the columns you want and all database setup here.
def setup_database():

    cur.execute("""CREATE TABLE IF NOT EXISTS States(
        ID SERIAL PRIMARY KEY NOT NULL,
        Name VARCHAR(128) UNIQUE NOT NULL
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS Sites(
        ID SERIAL NOT NULL,
        Name VARCHAR(128) UNIQUE NOT NULL,
        Type VARCHAR(128),
        State_ID INTEGER REFERENCES States(ID),
        Location VARCHAR(225),
        Description TEXT
    )""")
    conn.commit()


# Write code / functions to deal with CSV files and insert data into the database here.
def write_database():
# Arkansas data
    with open('arkansas.csv', 'r') as ak_data:
        ak = csv.DictReader(ak_data)
        cur.execute("""INSERT INTO States(Name) VALUES('Arkansas') RETURNING ID""")
        result = cur.fetchone()
        for row in ak:
            row['STATE_ID'] = result[0]
            cur.execute("""INSERT INTO 
                Sites(Name, Type, State_ID, Location, Description) 
                VALUES(%(NAME)s, %(TYPE)s, %(STATE_ID)s, %(LOCATION)s, %(DESCRIPTION)s) on conflict do nothing""", 
                row)

# California data
    with open('california.csv', 'r') as cal_data:
        cal = csv.DictReader(cal_data)
        cur.execute("""INSERT INTO States(Name) VALUES('California') RETURNING ID""")
        result = cur.fetchone()
        for row in cal:
            row['STATE_ID'] = result[0]
            cur.execute("""INSERT INTO 
                Sites(Name, Type, State_ID, Location, Description) 
                VALUES(%(NAME)s, %(TYPE)s, %(STATE_ID)s, %(LOCATION)s, %(DESCRIPTION)s) on conflict do nothing""", 
                row)

# Michigan data
    with open('michigan.csv', 'r', encoding = 'utf-8') as mi_data:
        mi = csv.DictReader(mi_data)
        cur.execute("""INSERT INTO States(Name) VALUES('Michigan') RETURNING ID""")
        result = cur.fetchone()
        for row in mi:
            row['STATE_ID'] = result[0]
            cur.execute("""INSERT INTO 
                Sites(Name, Type, State_ID, Location, Description) 
                VALUES(%(NAME)s, %(TYPE)s, %(STATE_ID)s, %(LOCATION)s, %(DESCRIPTION)s) on conflict do nothing""", 
                row)

# Make sure to commit your database changes with .commit() on the database connection.
    conn.commit()


# Write code to be invoked here (e.g. invoking any functions you wrote above)
def execute_and_print(query, numer_of_results=1):
    cur.execute(query)
    results = cur.fetchall()
    for r in results[:numer_of_results]:
        print(r)
    print('--> Result Rows:', len(results))

def run_query():
    print('==> Getting all locations')
    all_locations = execute_and_print("""SELECT Location FROM Sites""")
    print('\n')

    print('==> Getting all beautiful sites')
    beautiful_sites = execute_and_print("""SELECT Name FROM Sites WHERE Description ILIKE '%beautiful%'""")
    print('\n')

    print('==> Getting all the National Lakeshore Parks')
    natl_lakeshores = execute_and_print("""SELECT count(Type) from Sites WHERE Type = 'National Lakeshore'""")
    print('\n')

    print('==> Getting all National Parks in Michigan')
    michigan_names = execute_and_print("""SELECT States.Name AS State_name, Sites.Name AS Site_name FROM Sites INNER JOIN States ON Sites.State_ID = States.ID WHERE States.Name LIKE '%Michigan%'""")
    print('\n')

    print('==> Getting a count of National Parks in Arkansas')
    total_number_arkansas = execute_and_print("""SELECT count(Sites.Name) FROM Sites INNER JOIN States ON Sites.State_ID = States.ID WHERE States.Name LIKE '%Arkansas%'""")
    print('\n')

# if __name__ == '__main__':
    # command = None
    # if len(sys.argv) > 1:
        # command = sys.argv[1]

    # if command == 'setup':
        # print('-- Setting up database --')
        # setup_database()
    # elif command == 'write':
        # print('-- Writting database --')
        # write_database()
    # else:
        # print('-- Running Queries --')
        # run_query()
    

    # if command == 'setup':
        # print('setting up database')
        # setup_database()
    # elif command == 'search':
        # load_cache()
        # print('searching', search_term)
        # search_songs(search_term)
    # else:
        # print('nothing to do')

if __name__ == '__main__':
    command = None
    # if len(sys.argv) > 1:
        # command = sys.argv[1]
    print('-- Setting up database --')
    setup_database()
    print('-- Writting database --')
    write_database()
    print('-- Running Queries --')
    run_query()

# We have not provided any tests, but you could write your own in this file or another file, if you want.
