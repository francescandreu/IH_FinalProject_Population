import pandas as pd
import numpy as np

import gathering as gt


def refactor_fertility(df):
    df = df.T
    new_header = df.iloc[0] 
    df = df[1:] 
    df.columns = new_header 
    df.drop(['_World'], axis=1, inplace=True)
    df = df.T

    lst = []
    for index, rows in df.iterrows():
        for year in df.columns:
            my_list = [index, int(year), rows[year]]
            lst.append(my_list)
    df_2 = pd.DataFrame(lst, columns=['Country', 'Year', 'NatalityRate'])
    df_2.set_index(['Country','Year'], inplace=True)
    return df_2
    

def refactor_gdp(df):
    df.drop(['ISO'], axis=1, inplace=True)
    df = df.T
    new_header = df.iloc[0] 
    df = df[1:] 
    df.columns = new_header 
    df = df.T

    lst = []
    for index, rows in df.iterrows():
        for year in df.columns:
            my_list = [index, int(year), rows[year]]
            lst.append(my_list)    
    df_2 = pd.DataFrame(lst, columns=['Country', 'Year', 'GDP'])
    df_2.set_index(['Country','Year'], inplace=True)
    return df_2


def refactor_gdp_pc(df):
    df.drop(['ISO'], axis=1, inplace=True)
    df = df.T
    new_header = df.iloc[0] 
    df = df[1:] 
    df.columns = new_header 
    df = df.T

    lst = []
    for index, rows in df.iterrows():
        for year in df.columns:
            my_list = [index, int(year), rows[year]]
            lst.append(my_list)    
    df_2 = pd.DataFrame(lst, columns=['Country', 'Year', 'GDP_PC'])
    df_2.set_index(['Country','Year'], inplace=True)
    return df_2