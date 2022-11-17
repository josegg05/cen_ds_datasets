import argparse
import timeit

import numpy as np
import pandas as pd
import pypyodbc


def create_property_table_dummy(db_path, out_path, files_name, db_name):
    # TODO: Implement using another library.
    print('Implementation Pending')
    with open(f'{out_path}/{files_name}.csv', 'w') as fp:
        pass


def create_property_table(class_name, property_name, db_path, out_path, files_name, db_name="Model PRGdia_Full_Definitivo Solution.accdb"):
    conn = pypyodbc.connect(
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
        f"Dbq={db_path}/{db_name};"
        )
    cur = conn.cursor()
    print(f"--- Creating {files_name} db_key to {class_name}_{property_name} dictionary of {db_path[-66:-50]} ---")
    key_to_class = get_key_to_class_dict(cur, class_name, property_name)  # "Line", "Flow";  "Node", "Genration"; "Node", "Marginal Loss Factor"
    print(f"--- Creating {files_name} {property_name} table of {db_path[-66:-50]} ---")
    property_table_wide = database_to_csv(key_to_class, cur, out_path, files_name)
    print(f"Total of {class_name}s with {property_name} data = {property_table_wide.shape[0]}; Total of {class_name}s with No data = {len(key_to_class.keys()) - property_table_wide.shape[0]}")
    print(f"--- {property_name} Table of {db_path[-66:-50]} created ---")
    print("\n")
    cur.close()
    conn.close()


def get_key_to_class_dict(cur, class_name="", property_name=""):
    # Get class_id
    cur.execute(f"SELECT class_id, name FROM t_class where name like '{class_name}'")
    while True:
        row = cur.fetchone()
        if row is None:
            break
        class_id = row.get("class_id")
        # print(u"{0} is class id {1}".format(row.get("name"), row.get("class_id")))

    # Get object_id and class object names
    cur.execute(f"SELECT name, object_id, class_id FROM t_object where class_id like {class_id}")
    object_to_class = {}
    while True:
        row = cur.fetchone()
        if row is None:
            break
        object_to_class[row.get("object_id")] = row.get("name")
        # print(u"Object with ID {0} is class {1} and name {2}".format(row.get("object_id"), row.get("class_id"), row.get("name")))

    # Get membership_id
    child_class_id = class_id
    parent_class_id = 1
    cur.execute(f"SELECT membership_id, child_object_id, child_class_id, parent_class_id FROM t_membership where child_class_id like {child_class_id} and parent_class_id like {parent_class_id}")
    membership_to_class = {}
    while True:
        row = cur.fetchone()
        if row is None:
            break
        membership_to_class[row.get("membership_id")] = object_to_class[row.get("child_object_id")]
        # print(u"Object with ID {0} is membership {1} and class {2} and parent_class {3}".format(row.get("child_object_id"), row.get("membership_id"), row.get("child_class_id"), row.get("parent_class_id")))

    # Get collection_id
    child_class_id = class_id
    parent_class_id = 1
    cur.execute(f"SELECT collection_id, child_class_id, parent_class_id, name FROM t_collection where child_class_id like {child_class_id} and parent_class_id like {parent_class_id}")
    while True:
        row = cur.fetchone()
        if row is None:
            break
        collection_id = row.get("collection_id")
        # print(u"{0} is collection_id {1}".format(row.get("name"), row.get("collection_id")))

    # Get property_id
    cur.execute(f"SELECT property_id, collection_id, name FROM t_property where name like '{property_name}' and collection_id like {collection_id}")
    while True:
        row = cur.fetchone()
        if row is None:
            break
        class_property_id = row.get("property_id")
        # print(u"{0} is property_id {1} and collection_id {2}".format(row.get("name"), row.get("property_id"),  row.get("collection_id")))

    # Get key_id
    period_type_id = 0
    cur.execute(f"SELECT key_id, membership_id, property_id FROM t_key where property_id like {class_property_id} and period_type_id like {period_type_id}")
    key_to_class = {}
    while True:
        row = cur.fetchone()
        if row is None:
            break
        key_to_class[row.get("key_id")] = membership_to_class[row.get("membership_id")]
        # print(u"Key with ID {0} is membership {1} and property_id {2}".format(row.get("key_id"), row.get("membership_id"), row.get("property_id")))

    print(f"{class_name}_class_id={class_id}; {class_name}_collection_id={collection_id}; {class_name}_{property_name}_property_id={class_property_id}")
    print(f"# {class_name}s={len(object_to_class)}; # Memberships={len(membership_to_class)};  # Keys={len(key_to_class)}")
    # TODO: Save the key_to_class dictionary
    return key_to_class


def database_to_csv(key_to_class, cur, out_folder, files_name):
    t_data = "t_data_0"
    num_hours = 24
    dtype = [('BARRA_PLEXOS', 'U100'), ('period_id', int), ('value', float)]
    property_mx = np.zeros((len(key_to_class.keys()) * num_hours), dtype=dtype)

    # Get property values
    cur.execute(f"SELECT period_id, value, key_id FROM {t_data} where key_id in{tuple(key_to_class.keys())} and period_id < {num_hours + 1}")
    mx_row = 0
    while True:
        row = cur.fetchone()
        if row is None:
            break
        property_mx[mx_row] = (key_to_class[row.get("key_id")].upper(), row.get("period_id"), row.get("value"))
        mx_row += 1
    property_mx = np.sort(property_mx, order=['BARRA_PLEXOS', 'period_id'])
    property_table = pd.DataFrame(property_mx)
    property_table = property_table.drop_duplicates()  # Drop all empty rows
    if property_table.iloc[0, 0] == '':
        property_table = property_table.drop(0)
    property_table_wide = pd.pivot(property_table, index="BARRA_PLEXOS", columns='period_id', values='value')
    property_table_wide.to_csv(f"{out_folder}/{files_name}.csv", index=True, sep=";", decimal=",")
    
    return property_table_wide


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_folder', type=str, default='src/data/files', help='data folder')
    parser.add_argument('--out_folder', type=str, default='src/data/files', help='.csv output folder')
    parser.add_argument('--db_folder_name', type=str, default='PROGRAMA20190607 copy', help='database folder name')
    parser.add_argument('--db_name', type=str, default='Model PRGdia_Full_Definitivo Solution.mdb', help='data base name')
    parser.add_argument('--class_name', type=str, default='Line', help='class Name. Can be 1. Line; 2. Node')
    parser.add_argument('--property_name', type=str, default='Flow', help='property name. Can be: 1. Flow; 2. Generation; 3. Marginal Loss Factor')
    parser.add_argument('--key_to_class_dict_path', type=str, default=None, help='path to key_to_class dictionary')

    args = parser.parse_args()

    # convert to dictionary
    params = vars(args)

    pypyodbc.lowercase = False
    db_path = f"{params['data_folder']}/{params['db_folder_name']}"
    out_path = f"{params['out_folder']}/{params['db_folder_name']}"
    db_name = params['db_name']
    class_name = params['class_name']
    property_name = params['property_name']
    files_name = property_name.upper() + params["db_folder_name"][-8:]

    time_start = timeit.default_timer()
    create_property_table(class_name, property_name, db_path, out_path, files_name, db_name)
    time_end = timeit.default_timer()
    print(f"time run = {time_end - time_start}")
