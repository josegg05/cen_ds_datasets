import argparse
import timeit

import numpy as np
import pandas as pd
import pypyodbc
from geopy.geocoders import Nominatim


def create_geolocation_table(class_name, db_path, out_path, files_name, db_name="Model PRGdia_Full_Definitivo Solution.accdb"):
    conn = pypyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
        f"Dbq={db_path}/{db_name};"
        )
    cur = conn.cursor()
    print(f"--- Creating {files_name} db_obj to {class_name} dictionary of {db_path[-66:-50]} ---")
    attribute_ids, object_to_class = get_key_to_obj_dict(cur, class_name)  # "Line", "Flow";  "Node", "Genration"; "Node", "Marginal Loss Factor"
    print(f"--- Creating {files_name} geolocation table of {db_path[-66:-50]} ---")
    geoloc_table_wide = database_geolocation_to_csv(attribute_ids, object_to_class, cur)
    geoloc_table_wide = get_loaction_info(geoloc_table_wide.copy(), out_path, files_name)
    print(f"Total of {class_name}s with data = {geoloc_table_wide.shape[0]}; Total of {class_name}s with No data = {len(object_to_class.keys()) - geoloc_table_wide.shape[0]}")
    cur.close()
    conn.close()


def get_key_to_obj_dict(cur, class_name=""):
    # Get class_id
    cur.execute(f"SELECT class_id, name FROM t_class where name like '{class_name}'")
    while True:
        row = cur.fetchone()
        if row is None:
            break
        class_id = row.get("class_id")
        # print(u"{0} is class id {1}".format(row.get("name"), row.get("class_id")))

    # Get attribute_ids
    attributes_names = ("Latitude", "Longitude")
    cur.execute(f"SELECT attribute_id, class_id, name FROM t_attribute where class_id like {class_id} and name in{attributes_names}")
    attribute_ids = {}
    while True:
        row = cur.fetchone()
        if row is None:
            break
        attribute_ids[row.get("attribute_id")] = row.get("name")
        # print(u"Object with ID {0} is class {1} and name {2}".format(row.get("object_id"), row.get("class_id"), row.get("name")))

    # Get object_id and class object names
    cur.execute(f"SELECT name, object_id, class_id FROM t_object where class_id like {class_id}")
    object_to_class = {}
    while True:
        row = cur.fetchone()
        if row is None:
            break
        object_to_class[row.get("object_id")] = row.get("name")
        # print(u"Object with ID {0} is class {1} and name {2}".format(row.get("object_id"), row.get("class_id"), row.get("name")))

    return attribute_ids, object_to_class


def database_geolocation_to_csv(attribute_ids, object_to_class, cur):
    t_attribute_data = "t_attribute_data"
    dtype = [('BARRA_PLEXOS', 'U100'), ('coord_type', 'U100'), ('value', float)]
    geoloc_mx = np.zeros((len(object_to_class.keys())) * (len(attribute_ids.keys())), dtype=dtype)

    # Get property values
    cur.execute(f"SELECT value, object_id, attribute_id FROM {t_attribute_data} where object_id in{tuple(object_to_class.keys())} and attribute_id in{tuple(attribute_ids.keys())}")
    mx_row = 0
    while True:
        row = cur.fetchone()
        if row is None:
            break
        geoloc_mx[mx_row] = (object_to_class[row.get("object_id")].upper(), attribute_ids[row.get("attribute_id")].upper(), row.get("value"))
        mx_row += 1
    geoloc_mx = np.sort(geoloc_mx, order=['BARRA_PLEXOS', 'coord_type'])
    geoloc_table = pd.DataFrame(geoloc_mx)
    geoloc_table_wide = pd.pivot(geoloc_table, index="BARRA_PLEXOS", columns='coord_type', values='value')
    
    return geoloc_table_wide


def get_loaction_info(geoloc_table_wide, out_folder, files_name):
    # Country Internet data
    chile_center_coords = (-35.675147, -71.542969)
    chile_length = 34  # aprox

    center_lat = chile_center_coords[0]
    delta_lat = chile_length/4
    cardinal_categories_names = ["0_Muy_Norte", "1_Norte", "2_Sur", "3_Muy_Sur"]
    cardinal_categories_limits = [center_lat + delta_lat, center_lat, center_lat - delta_lat]

    cardinal_values = []
    for lat in geoloc_table_wide["LATITUDE"]:
        if lat >  cardinal_categories_limits[0]:
            cardinal_value = cardinal_categories_names[0]
        elif cardinal_categories_limits[0] >= lat > cardinal_categories_limits[1]:
            cardinal_value = cardinal_categories_names[1]
        elif cardinal_categories_limits[1] >= lat > cardinal_categories_limits[2]:
            cardinal_value = cardinal_categories_names[2]
        else:
            cardinal_value = cardinal_categories_names[3]
        cardinal_values.append(cardinal_value)

    geoloc_table_wide["CARD"] = cardinal_values

    geoLoc = Nominatim(user_agent="GetLoc")
    loc_names = []
    for bar_idx in geoloc_table_wide.index.to_list():
        loc_names.append(geoLoc.reverse(f"{geoloc_table_wide.loc[bar_idx, 'LATITUDE']}, {geoloc_table_wide.loc[bar_idx, 'LONGITUDE']}").raw['address']['state'])

    geoloc_table_wide['REGION'] = loc_names
    geoloc_table_wide.to_csv(f"{out_folder}/{files_name}.csv", index=True, sep=";", decimal=",")
    return geoloc_table_wide




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_folder', type=str, default='src/data/filesdb', help='data folder')
    parser.add_argument('--out_folder', type=str, default='src/data/filesdb', help='.csv output folder')
    parser.add_argument('--db_folder_name', type=str, default='PLEXOS20220818', help='database folder name')
    parser.add_argument('--db_name', type=str, default='Model PRGdia_Full_Definitivo Solution.accdb', help='data base name')
    parser.add_argument('--class_name', type=str, default='Node', help='class Name. Can be 1. Node; 2. Generator')
    parser.add_argument('--key_to_class_dict_path', type=str, default=None, help='path to key_to_class dictionary')

    args = parser.parse_args()

    # convert to dictionary
    params = vars(args)

    pypyodbc.lowercase = False
    db_path = f"{params['data_folder']}/{params['db_folder_name']}"
    out_path = f"{params['out_folder']}/{params['db_folder_name']}"
    db_name = params['db_name']
    class_name = params['class_name']
    files_name = "bars_geolocation"

    time_start = timeit.default_timer()
    create_geolocation_table(class_name, db_path, out_path, files_name, db_name)
    time_end = timeit.default_timer()
    print(f"time run = {time_end - time_start}")
