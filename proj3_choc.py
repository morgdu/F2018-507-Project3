import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

def create_tables():

    # try to conenct to database and create tables for Bars and Countries
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        # Drop tables 
        statement = '''DROP TABLE IF EXISTS 'Bars';'''
        cur.execute(statement)

        statement = '''DROP TABLE IF EXISTS 'Countries';'''
        cur.execute(statement)

        conn.commit()

        statement = '''
            CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT NOT NULL,
            'Alpha3' TEXT NOT NULL,
            'EnglishName' TEXT NOT NULL,
            'Region' TEXT NOT NULL,
            'Subregion' TEXT NOT NULL,
            'Population' INTEGER,
            'Area' REAL
            );
        '''
        cur.execute(statement)

        statement = '''
            CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' TEXT NOT NULL,
            'REF' TEXT NOT NULL,
            'ReviewDate' TEXT NOT NULL,
            'CocoaPercent' REAL,
            'CompanyLocationId' INTEGER,
            'Rating' REAL,
            'BeanType' TEXT NOT NULL,
            'BroadBeanOriginId' INTEGER
            );
        '''
        cur.execute(statement)

        conn.commit()
        conn.close()
    
    except:
        print("Could not create tables.")

def populate_tables():

    # try to connect to database and populate tables with data from CSV and JSON files
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()

        # open Bars file
        with open(BARSCSV) as f:
            csvReader = csv.reader(f)
            
            # skips header row
            next(csvReader, None)

            # populate table
            for row in csvReader:
                cur.execute("INSERT INTO 'Bars' (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocationId, Rating, BeanType, BroadBeanOriginId) VALUES (?,?,?, ?, ?, ?, ?, ?, ?)", (row[0], row[1], row[2], row[3], row[4][:-1], row[5], row[6], row[7], row[8]))
        f.close()

        # open countries file and make it a python object
        f = open(COUNTRIESJSON)
        content = f.read()
        countries_py_obj = json.loads(content)

        # populate table and update Bars foreign key columns referencing Countries table
        for country in countries_py_obj:
            cur.execute("INSERT INTO 'Countries' (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area) VALUES (?, ?, ?, ?, ?, ?, ?)", (country['alpha2Code'], country['alpha3Code'], country['name'], country['region'], country['subregion'], country['population'], country['area']))
            country_id = cur.execute("SELECT id from 'Countries' WHERE EnglishName=?", (country['name'],)).fetchone()[0]
            cur.execute("UPDATE 'Bars' SET CompanyLocationId = ? WHERE CompanyLocationId = ?", (country_id, country['name']))
            cur.execute("UPDATE 'Bars' SET BroadBeanOriginId = ? WHERE BroadBeanOriginId = ?", (country_id, country['name']))
        f.close()

        conn.commit()
        conn.close()
    
    except:
        print("Could not populate tables.")


create_tables()
populate_tables()
