import psycopg2
import pandas as pd
from config import config


# ------------------------------------------------ SETUP QUERIES ------------------------------------------------ #
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def drop_tables():
    """ drop tables in the PostgreSQL database"""
    commands = (
        'drop table if exists population;',
        'drop table if exists year;',
        'drop table if exists country;',
        'drop table if exists continent;'
        )
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        '''
        create table IF NOT EXISTS continent(
            continent_id INT GENERATED ALWAYS AS IDENTITY,
            iso VARCHAR(64),
            PRIMARY KEY(continent_id)
        );
        ''',
        '''
        create table IF NOT EXISTS country(
            country_id INT GENERATED ALWAYS AS IDENTITY,
            continent_id INT,
            name VARCHAR(64),
            iso2 VARCHAR(3),
            iso3 VARCHAR(4),
            PRIMARY KEY(country_id),
            CONSTRAINT fk_continent
                FOREIGN KEY(continent_id)
                    REFERENCES continent(continent_id)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE
        );
        ''',
        '''
        create table if not exists year(
            year_id INT GENERATED ALWAYS AS IDENTITY,
            year INT,
            PRIMARY KEY(year_id)
        );
        ''',
        '''
        create table IF NOT EXISTS population(
            population_id INT GENERATED ALWAYS AS IDENTITY,
            country_id INT,
            year_id INT,
            population INT,
            natality DECIMAL,
            mortality DECIMAL,
            gdp DECIMAL,
            gdp_pc DECIMAL,
            PRIMARY KEY(population_id),
            CONSTRAINT fk_country
                FOREIGN KEY(country_id)
                    REFERENCES country(country_id)
                        ON UPDATE CASCADE
                        ON DELETE CASCADE,
            CONSTRAINT fk_year
                FOREIGN KEY(year_id)
                    REFERENCES year(year_id)
                        ON UPDATE CASCADE
                        ON DELETE CASCADE
        );
        ''')
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



# ------------------------------------------------ INSERT QUERIES ------------------------------------------------ #
def insert_continent_list(continents_iso):
    """ insert multiple continents into the continent table """
    
    sql = """ INSERT INTO continent(iso) VALUES """
    conn = None    
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        # execute the INSERT statement
        args_str = ','.join(cur.mogrify("(%s)", x).decode('utf-8') for x in continents_iso)
        cur.execute(sql + (args_str))
        
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_country_list(countries_list):
    """ insert multiple countries into the continent table """ 
    sql = """ 
    INSERT INTO country(name, iso2, iso3, continent_id) 
    VALUES (%s,%s,%s, (SELECT continent_id FROM continent WHERE iso=(%s)));"""
    conn = None    
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        # execute the INSERT statement
        for country in countries_list:
            cur.execute(sql, country)
        
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_year_list(years_list):
    """ insert multiple years into the years table """ 
    sql = """ 
    INSERT INTO year(year) 
    VALUES (%s);"""
    conn = None    
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        # execute the INSERT statement
        for year in years_list:
            cur.execute(sql, year)
        
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_population_list(population_list):
    """ insert multiple population rows into the population table """ 
    sql = """ 
    INSERT INTO population(population, natality, mortality, gdp, gdp_pc, country_id, year_id) 
    VALUES (%s,%s,%s,%s,%s, (SELECT country_id FROM country WHERE name=(%s)),
                            (SELECT year_id FROM year WHERE year=(%s)));"""
    conn = None    
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        # execute the INSERT statement
        for population_row in population_list:
            cur.execute(sql, population_row)
        
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



# ------------------------------------------------ SELECT QUERIES ------------------------------------------------ #
def select_population():
    sql = """ 
    SELECT c.name, y.year, p.population 
    FROM population AS p
        JOIN country AS c ON c.country_id = p.country_id
        JOIN year AS y ON y.year_id = p.year_id"""
    conn = None    
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        # execute the SELECT statement
        cur.execute(sql)
        result = cur.fetchall()
        
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return result
        else:
            return None


def select_population_vs_rates():
    sql = """ 
    SELECT c.name, y.year, p.population, p.natality, p.mortality 
    FROM population AS p
        JOIN country AS c ON c.country_id = p.country_id
        JOIN year AS y ON y.year_id = p.year_id"""
    conn = None    
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        # execute the SELECT statement
        cur.execute(sql)
        result = cur.fetchall()
        
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return result
        else:
            return None


def select_population_gdp():
    sql = """ 
    SELECT c.name, y.year, p.population, p.gdp, p.gdp_pc
    FROM population AS p
        JOIN country AS c ON c.country_id = p.country_id
        JOIN year AS y ON y.year_id = p.year_id"""
    conn = None    
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()

        # execute the SELECT statement
        cur.execute(sql)
        result = cur.fetchall()
        
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return result
        else:
            return None
