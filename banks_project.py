import glob
import pandas as pd
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import numpy as np
from datetime import datetime


def log_progress(message):
    with open("code_log.txt", "a") as log_file:
        log_file.write(f"{datetime.now().strftime('%H:%M:%S')}:{message}\n")


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    table = data.find("table")

    if table is None :
        return df

    rows = table.find_all("tr")

    for row in rows[1:]:
        cols = row.find_all("td")
        values = [col.text.strip() for col in cols]

        if len(values) == len(table_attribs):
            df.loc[len(df)] = values

    return df

def transform(df,csv_path):
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index(exchange_df.columns[0]).squeeze().to_dict()

    df['Market_Cap'] = df['Market_Cap'].astype(float)

    df['MC_EUR_Billion'] = np.round(df['Market_Cap'] * exchange_rate.get('EUR', 1), 2)
    df['MC_GBP_Billion'] = np.round(df['Market_Cap'] * exchange_rate.get('GBP', 1), 2)
    df['MC_INR_Billion'] = np.round(df['Market_Cap'] * exchange_rate.get('INR', 1), 2)

    return df


#def load_to_csv(df, output_path):


#def load_to_db(df, sql_connection, table_name):


#def run_query(query_statement, sql_connection):


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Rank','Bank name','Market_Cap']
code_log = 'code_log.txt'
csv_path = 'exchange_rate.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df_transformed = transform(df,csv_path)

print(df_transformed)
