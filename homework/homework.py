"""
Escriba el codigo que ejecute la accion solicitada.
"""
import pandas as pd
import os
import glob
from datetime import datetime
import numpy as np
# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    FILES_DIR = "files"
    INPUT_DIR = os.path.join(FILES_DIR, "input")
    OUTPUT_DIR = os.path.join(FILES_DIR, "output")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    file_pattern = os.path.join(FILES_DIR, "input", "bank-marketing-campaing-*.csv.zip")
    zip_files = glob.glob(file_pattern)

    all_data = []

    for file in zip_files:
        try:
            df = pd.read_csv(file, compression='zip')
            all_data.append(df)
        except Exception:
            continue

    if not all_data:
        df_full = pd.DataFrame()
    else:
        df_full = pd.concat(all_data, ignore_index=True)
        
        if not df_full.empty and df_full.columns[0].startswith('Unnamed: 0'):
            df_full = df_full.drop(columns=[df_full.columns[0]])

    if df_full.empty:
        cols_client = ['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage'] # <--- USAR 'mortgage'
        cols_campaign = ['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'last_contact_date']
        cols_economics = ['client_id', 'cons_price_idx', 'euribor_three_months']
        
        pd.DataFrame(columns=cols_client).to_csv(os.path.join(OUTPUT_DIR, "client.csv"), index=False)
        pd.DataFrame(columns=cols_campaign).to_csv(os.path.join(OUTPUT_DIR, "campaign.csv"), index=False)
        pd.DataFrame(columns=cols_economics).to_csv(os.path.join(OUTPUT_DIR, "economics.csv"), index=False)
        return


    df_client = df_full[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    
    df_client['job'] = df_client['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    
    df_client['education'] = df_client['education'].str.replace('.', '_', regex=False).replace('unknown', pd.NA)
    
    df_client['credit_default'] = df_client['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    
    df_client['mortgage'] = df_client['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
    
    df_client = df_client[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']]

    df_campaign = df_full[['client_id', 'contact_duration', 'number_contacts', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'month', 'day']].copy()
    
    df_campaign['previous_outcome'] = df_campaign['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    
    df_campaign['campaign_outcome'] = df_campaign['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)

    month_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    df_campaign['month_num'] = df_campaign['month'].str.lower().map(month_map)
    
    def create_date(row):
        try:
            return datetime(2022, int(row['month_num']), int(row['day'])).strftime('%Y-%m-%d')
        except ValueError:
            return pd.NA

    df_campaign['last_contact_date'] = df_campaign.apply(create_date, axis=1)

    df_campaign.drop(columns=['month', 'day', 'month_num'], inplace=True)
    df_campaign = df_campaign[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'last_contact_date']]

    df_economics = df_full[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    
    df_economics = df_economics[['client_id', 'cons_price_idx', 'euribor_three_months']]

    df_client.to_csv(os.path.join(OUTPUT_DIR, "client.csv"), index=False)
    df_campaign.to_csv(os.path.join(OUTPUT_DIR, "campaign.csv"), index=False)
    df_economics.to_csv(os.path.join(OUTPUT_DIR, "economics.csv"), index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()
