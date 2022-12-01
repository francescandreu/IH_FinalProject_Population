import pandas as pd
import numpy as np

from config import config
import gathering as gt
import clean as cl
import sql


# --------------------- MAIN EXECUTION --------------------- #
if __name__ == '__main__':
    sql.connect()
    sql.drop_tables()
    sql.create_tables()

    df = gt.build_countries_iso()
    df = gt.build_countries_name(df)
    df = gt.build_continents_iso(df)

    # Insert continents
    continents = [tuple([x]) for x in list(df.Continent.unique())]
    sql.insert_continent_list(continents)
    print("[PostgreSQL - Continents added]")


    # Insert countries
    row_list = []
    for index, rows in df.iterrows():
        my_list = [rows.Country, rows.ISO2, rows.ISO3, rows.Continent]
        row_list.append(my_list)
    countries = [tuple(x) for x in row_list]
    sql.insert_country_list(countries)
    print("[PostgreSQL - Countries added]")


    # Insert years
    population_df = pd.read_csv("data\input\population_total_long.csv")
    years = [tuple([int(x)]) for x in list(population_df.Year.unique())]
    sql.insert_year_list(years)
    print("[PostgreSQL - Years added]")


    # Insert population: mortality, natality, population, gdp, gdp_pc
    df_pop = gt.get_population()
    df_mor = gt.get_moratility()
    df_result = df_pop.join(df_mor)

    df_fer = gt.get_fertility()
    df_fer = cl.refactor_fertility(df_fer)
    df_result = df_result.join(df_fer)

    df_gdp = gt.get_gdp()
    df_gdp = cl.refactor_gdp(df_gdp)
    df_result = df_result.join(df_gdp)

    df_gdp_pc = gt.get_gdp_pc()
    df_gdp_pc = cl.refactor_gdp_pc(df_gdp_pc)
    df_result = df_result.join(df_gdp_pc)
    df_result.to_csv("data\output\df_train.csv", index=False)

    row_list = []
    df_result = df_result.reset_index()
    df_result = df_result.replace(np.nan, None)
    for index, rows in df_result.iterrows():
        my_list = [rows.Population, rows.NatalityRate, rows.MortalityRate, rows.GDP, rows.GDP_PC, rows.Country, rows.Year]
        row_list.append(my_list)
    population_list = [tuple(x) for x in row_list]
    #print(population_list)
    sql.insert_population_list(population_list)
    print("[PostgreSQL - Population added]")


    # SELECT QUERIES TO IMPLEMENT AND BUILD CSV FILES TO BUILD TABLEAU
    # 1. Select population total for each country (to build histplot)
    response = sql.select_population()
    df = pd.DataFrame(response, columns=['Country', 'Year', 'Population'])
    df.to_csv("data\output\population_hist.csv", index=False)

    # 2. Select population vs rates
    response = sql.select_population_vs_rates()
    df = pd.DataFrame(response, columns=['Country', 'Year', 'Population', 'Natality', 'Mortality'])
    df.to_csv("data\output\population_rates_hist.csv", index=False)

    # 3. pop vs mortality/natality
    response = sql.select_population_gdp()
    df = pd.DataFrame(response, columns=['Country', 'Year', 'Population', 'GDP', 'GDP_PC'])
    df.to_csv("data\output\population_gdp_hist.csv", index=False)