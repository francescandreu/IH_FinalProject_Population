import pandas as pd
import requests


# ----------------------------------- Countries/Continents ----------------------------------- #
def get_country_names():
    response = requests.get(f"http://country.io/names.json")
    json_response = response.json()
    return json_response

def get_country_iso():
    response = requests.get(f"http://country.io/iso3.json")
    json_response = response.json()
    return json_response

def get_continents_iso():
    response = requests.get(f"http://country.io/continent.json")
    json_response = response.json()
    return json_response

def build_countries_iso():
    country_iso = get_country_iso()
    iso2 = country_iso.keys()
    iso3 = country_iso.values()
    data = {'ISO2':iso2, 'ISO3':iso3}
    df = pd.DataFrame.from_dict(data)
    return df

def build_countries_name(df):
    country_names = get_country_names()
    lst = list(country_names.values())
    df['Country'] = lst
    return df

def build_continents_iso(df):
    continents_iso = get_continents_iso()
    continents_iso
    lst = list(continents_iso.values())
    df['Continent'] = lst
    return df


# ----------------------------------- Mortality ----------------------------------- #
def get_moratility():
    df = pd.read_csv("data\input\child_mortality_rate.csv")
    df = df.groupby(['Country', 'Year']).sum()
    df = df.drop(["Unnamed: 0","Mortality Rate"], axis=1)
    df["MortalityRate"] = df["Child Mortality(1 to 4)"] / df["Total Population"]
    df.to_csv("data\output\clean_mortality.csv",index=True)
    df = df.drop(['Child Mortality(1 to 4)', 'Total Population'], axis=1)
    return df


# ----------------------------------- Fertility ----------------------------------- #
def get_fertility():
    df = pd.read_csv("data\input\child_natality_rate.csv")
    df.set_index("Country")
    return df


# ----------------------------------- Population ----------------------------------- #
def get_population():
    df = pd.read_csv("data\input\population_total_long.csv")
    new_columns = {'Country Name':'Country', 'Year':'Year', 'Count':'Population'}
    df.rename(columns=new_columns, inplace=True, errors='raise')

    df = df.groupby(['Country', 'Year']).sum()
    return df


# ----------------------------------- GDP ----------------------------------- #
def get_gdp():
    df = pd.read_csv("data\input\gdp.csv")
    new_columns = {'Country Name':'Country', 'Code':'ISO'}
    df.rename(columns=new_columns, inplace=True, errors='raise')
    df.set_index("Country")
    df.drop(['Unnamed: 65'], axis=1, inplace=True)
    return df
    

# ----------------------------------- GDP PC ----------------------------------- #
def get_gdp_pc():
    df = pd.read_csv("data\input\gdp_per_capita.csv")
    new_columns = {'Country Name':'Country', 'Code':'ISO'}
    df.rename(columns=new_columns, inplace=True, errors='raise')
    df.set_index("Country")
    df.drop(['Unnamed: 65'], axis=1, inplace=True)
    return df