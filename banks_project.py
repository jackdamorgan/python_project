import glob
import pandas as pd
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import numpy as np
from datetime import datetime
pd.set_option('display.max_columns', None)



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

    log_progress('Data extraction complete. Initiating Transformation process')
    return df


def transform(df, csv_path):
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index(exchange_df.columns[0]).to_dict()[exchange_df.columns[1]]

    df['MC_USD_Billion'] = df['MC_USD_Billion'].replace('[\$,]', '', regex=True).astype(float)

    # 3. Add converted columns (rounded to 2 decimal places)
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    log_progress('Data transformation complete. Initiating Loading process')
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Executing code by order

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Rank','Bank name','Market_Cap']
code_log = 'code_log.txt'
csv_path = 'exchange_rate.csv'
csv_transformed = ('largest_banks_data.csv')

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
df = df.rename(columns={'Market_Cap': 'MC_USD_Billion'})

df_transformed = transform(df,csv_path)
csv_trans = load_to_csv(df_transformed,csv_transformed)
print(df_transformed)

print((df['MC_EUR_Billion'][4]))