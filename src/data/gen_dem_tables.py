import argparse
import timeit
import os
from datetime import date

import numpy as np
import pandas as pd
import pyodbc
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows


def create_tables(db_path, db_name, files_date):
    conn = pyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
        f"Dbq={db_path}/{db_name};"
        )
    cur = conn.cursor()
    print(f"--- Creating GENERATION db_key to data dictionary of {db_name} ---")
    gen_key_to_properties = get_key_to_properties_dict(cur, type=("G"))  # "Line", "Flow";  "Node", "Genration"; "Node", "Marginal Loss Factor"
    print(f"--- Creating DEMAND db_key to name dictionary of {db_name} ---")
    dem_key_to_properties = get_key_to_properties_dict(cur, type=("L", "L_D", "R"))  # "Line", "Flow";  "Node", "Genration"; "Node", "Marginal Loss Factor"
    print(f"--- Creating GENERATION table of {db_name} ---")
    gen_table = db_data_to_table(cur, gen_key_to_properties, files_date)
    print(f"--- Creating DEMAND table of {db_name} ---")
    dem_table = db_data_to_table(cur, dem_key_to_properties, files_date)

    cur.close()
    conn.close()

    return gen_table, dem_table


def get_key_to_properties_dict(cur, type):
    # Get class_id
    cur.execute(f"SELECT clave, nombre_barra, Zona, tipo1 FROM BASE where tipo1 in{tuple(type)}")
    key_to_properties = {}
    while True:
        row = cur.fetchone()
        if row is None:
            break
        key_to_properties[row.clave] = [row.tipo1, row.nombre_barra, row.Zona]
        # print(u"{0} is class id {1}".format(row.name, row.class_id))

    return key_to_properties


def db_data_to_table(cur, key_to_properties, files_date):
    t_data = "MEDIDAS"
    year = int(f'20{files_date[-4:-2]}')
    month = int(files_date[-2:])    
    if month < 12:
        num_days = (date(year, month + 1, 1) - date(year, month, 1)).days        
    elif month == 12:
        num_days = 31
    num_hours = num_days * 24
    dtype = [('clave_ds', 'U100'), ('hora_mensual', int), ('valor', float)]
    data_mx = np.zeros((len(key_to_properties.keys()) * num_hours), dtype=dtype)

    # Get data values
    cur.execute(f"SELECT Hora_Mensual, MedidaHoraria, clave FROM {t_data} where clave in{tuple(key_to_properties.keys())} and Hora_Mensual < {num_hours + 1}")
    mx_row = 0
    while True:
        row = cur.fetchone()
        if row is None:
            break
        data_mx[mx_row] = (row.clave, row.Hora_Mensual, row.MedidaHoraria)
        mx_row += 1
    data_mx = np.sort(data_mx, order=['clave_ds', 'hora_mensual'])
    data_table = pd.DataFrame(data_mx)
    data_table = data_table.drop_duplicates()  # Drop all empty rows
    if data_table.iloc[0, 0] == '':
        data_table = data_table.drop(0)
    data_table_wide = pd.pivot(data_table, index="clave_ds", columns='hora_mensual', values='valor')
    # data_table_wide.to_csv(f"{out_folder}/{files_name}.csv", index=True, sep=";", decimal=",")

    properties_table = pd.DataFrame.from_dict(key_to_properties, orient='index',
                       columns=['tipo1', 'nombre_barra', 'Zona'])
    properties_table = properties_table.sort_index() 
    properties_table['clave_ds'] = properties_table.index
    properties_table = properties_table.reset_index(drop=True)

    data_table_wide = data_table_wide.reset_index()  # add "clave_ds" to the dataframe
    complete_table = pd.merge(properties_table, data_table_wide, on='clave_ds')
    
    return complete_table


def save_table(table, out_path, file_name):
    table.to_csv(f"{out_path}/{file_name}.csv", index=False, sep=";", decimal=",")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_folder', type=str, default='src/data', help='data folder')
    parser.add_argument('--db_folder_name', type=str, default='db_ds', help='database folder name')
    parser.add_argument('--data_year', type=str, default='2022', help='database year')
    parser.add_argument('--data_month', type=str, default='09', help='database year')
    parser.add_argument('--out_folder', type=str, default='gen_dem_tables', help='.csv output folder')    

    args = parser.parse_args()

    # convert to dictionary
    params = vars(args)

    pyodbc.lowercase = False
    db_path = f"{params['data_folder']}/{params['db_folder_name']}/{params['data_year']}"
    files_date=f'{params["data_year"][-2:]}{params["data_month"]}'
    db_name = f'BD_balance_valorizado_{files_date}_Data.accdb'
    
    out_path = f"{params['data_folder']}/{params['out_folder']}"
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    # out_path = f"{out_path}/{params['data_year']}"
    # if not os.path.exists(out_path):
    #     os.mkdir(out_path)
    
    out_path = f"{out_path}/{files_date}"
    if not os.path.exists(out_path):
        os.mkdir(out_path)

    time_start = timeit.default_timer()
    gen_table, dem_table = create_tables(db_path, db_name, files_date)
    gen_file_name = f"GENERATION_{files_date}"
    save_table(gen_table, out_path, gen_file_name)
    dem_file_name = f"DEMAND_{files_date}"
    save_table(dem_table, out_path, dem_file_name)
    time_end = timeit.default_timer()
    print(f"time run = {time_end - time_start}")
