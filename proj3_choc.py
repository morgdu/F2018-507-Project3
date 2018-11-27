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

# Part 2: Implement logic to process user commands
def process_command(command):

    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    # separate primary command from parameters
    cmnd = command.split()[0]
    params = command.split()[1:]

    # bars prompt
    if cmnd == "bars":

        # set defaults
        country_region = ""
        order_by = "Rating"
        order = "DESC"
        num = 10

        # check for parameters in the command line and adjust sql query accordingly
        for param in params:
            if "sellcountry" in param:
                country_region = param.split('=')[1]
                where_sql = " WHERE c.alpha2 = " + "'" + country_region + "'"
            elif "sourcecountry" in param:
                country_region = param.split('=')[1]
                where_sql = " WHERE c2.alpha2 = " +  "'" + country_region + "'"
            elif "sellreigion" in param:
                country_region = param.split('=')[1]
                where_sql = " WHERE c.Region = " + "'" + country_region + "'"
            elif "sourceregion" in param:
                country_region = param.split('=')[1]
                where_sql = " WHERE c2.Region = " + "'" + country_region + "'"
            elif "ratings" in param:
                order_by = "Rating"
            elif "cocoa" in param:
                order_by = "CocoaPercent"
            elif "top" in param:
                order = "DESC"
                num = param.split('=')[1]
            elif "bottom" in param:
                order = "ASC"
                num = param.split('=')[1]
            else:
                return "Command not recognized: " + command
    
        # base statement that selects values from table joins
        statement = "SELECT b.SpecificBeanBarName, b.Company, c.EnglishName, b.Rating, b.CocoaPercent, c2.EnglishName FROM Bars b JOIN Countries c on b.CompanyLocationId = c.Id JOIN Countries c2 on b.BroadBeanOriginId = c2.Id"

        # if country value is a parameter, add where clause to SQL statement
        if country_region != "":
            statement += where_sql

        # add order by, direction, and limit to SQL statement (either default or specified by params)
        statement += (" ORDER BY " + order_by + " " + order + " LIMIT " + str(num))

        # get results
        results = cur.execute(statement)
        results = cur.fetchall()

        return results

    # companies prompt
    elif cmnd == "companies":
        
        # set defaults
        country_region = ""
        order_by = "Rating"
        order = "DESC"
        num = 10
        
        # check for parameters in the command line and adjust sql query accordingly
        for param in params:
            if "country" in param:
                country_region = param.split('=')[1]
                where_sql = " WHERE c.alpha2 = " + "'" + country_region + "'"
            elif "region" in param:
                country_region = param.split('=')[1]
                where_sql = " WHERE c.Region = " +  "'" + country_region + "'"
            elif "ratings" in param:
                order_by = "Rating"
                aggregate = "AVG(b.Rating)"
            elif "cocoa" in param:
                order_by = "CocoaPercent"
                aggregate = "AVG(b.CocoaPercent)"
            elif "bars_sold" in param:
                order_by = "COUNT(*)"
                aggregate = "COUNT(*)"
            elif "top" in param:
                order = "DESC"
                num = param.split('=')[1]
            elif "bottom" in param:
                order = "ASC"
                num = param.split('=')[1]
            else:
                return "Command not recognized: " + command
    
        # base statement that selects correct values from table and joins
        statement = "SELECT b.Company, c.EnglishName," + aggregate + " FROM Bars b JOIN Countries c on b.CompanyLocationId = c.Id"
        # if country value is a parameter, add where clause to SQL statement
    
        if country_region != "":
            statement += where_sql

        # update with group by statement 
        statement += " GROUP BY b.Company HAVING COUNT(*) > 4"

        # add order by, direction, and limit to SQL statement (either default or specified by params)
        statement += (" ORDER BY " + aggregate + " " + order + " LIMIT " + str(num))

        # get results
        results = cur.execute(statement)
        results = cur.fetchall()

        return results

    # countries prompt
    elif cmnd == "countries":
        
        # set defaults
        country_region = ""
        join_by = " FROM Bars b JOIN Countries c on b.CompanyLocationId = c.Id"
        order_by = "Rating"
        order = "DESC"
        num = 10
        
        # check for parameters in the command line and adjust sql query accordingly
        for param in params:
            if "region" in param:
                country_region = param.split('=')[1]
                where_sql = " WHERE c.Region = " +  "'" + country_region + "'"
            elif "sellers" in param:
                join_by = join_by
            elif "sources" in param:
                join_by = " FROM Bars b JOIN Countries c on b.BroadBeanOriginId = c.Id"
            elif "ratings" in param:
                order_by = "Rating"
                aggregate = "AVG(b.Rating)"
            elif "cocoa" in param:
                order_by = "CocoaPercent"
                aggregate = "AVG(b.CocoaPercent)"
            elif "bars_sold" in param:
                order_by = "COUNT(*)"
                aggregate = "COUNT(*)"
            elif "top" in param:
                order = "DESC"
                num = param.split('=')[1]
            elif "bottom" in param:
                order = "ASC"
                num = param.split('=')[1]
            else:
                return "Command not recognized: " + command
    
        # base statement that selects correct values from table and joins
        statement = "SELECT c.EnglishName, c.Region," + aggregate + join_by
        
        # if country/region value is a parameter, add where clause to SQL statement
        if country_region != "":
            statement += where_sql

        # update with group by statement 
        statement += " GROUP BY c.EnglishName HAVING COUNT(*) > 4"

        # add order by, direction, and limit to SQL statement (either default or specified by params)
        statement += (" ORDER BY " + aggregate + " " + order + " LIMIT " + str(num))

        # get results
        results = cur.execute(statement)
        results = cur.fetchall()

        return results

    # regions prompt
    elif cmnd == "regions":
        
        # set defaults
        country_region = ""
        join_by = " FROM Bars b JOIN Countries c on b.CompanyLocationId = c.Id"
        order_by = "Rating"
        order = "DESC"
        num = 10
        
        # check for parameters in the command line and adjust sql query accordingly
        for param in params:
            if "sellers" in param:
                join_by = join_by
            elif "sources" in param:
                join_by = " FROM Bars b JOIN Countries c on b.BroadBeanOriginId = c.Id"
            elif "ratings" in param:
                order_by = "Rating"
                aggregate = "AVG(b.Rating)"
            elif "cocoa" in param:
                order_by = "CocoaPercent"
                aggregate = "AVG(b.CocoaPercent)"
            elif "bars_sold" in param:
                order_by = "COUNT(*)"
                aggregate = "COUNT(*)"
            elif "top" in param:
                order = "DESC"
                num = param.split('=')[1]
            elif "bottom" in param:
                order = "ASC"
                num = param.split('=')[1]
            else:
                return "Command not recognized: " + command
    
        # base statement that selects correct values from table and joins
        statement = "SELECT c.Region," + aggregate + join_by

        # update sql with group by statement 
        statement += " GROUP BY c.Region HAVING COUNT(*) > 4"

        # add order by, direction, and limit to SQL statement (either default or specified by params)
        statement += (" ORDER BY " + aggregate + " " + order + " LIMIT " + str(num))

        # get results
        results = cur.execute(statement)
        results = cur.fetchall()

        return results
    
    # if any other word present in command, return bad input error
    else:
        return "Command not recognized: " + command


def load_help_text():
    with open('help.txt') as f:
        return f.read()
