import argparse
import timeit
import os
import sys
 
# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))
 
# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)
 
# adding the parent directory to
# the sys.path.
sys.path.append(parent)
 
# importing
import data.gen_dem_tables as gdt


def get_years(databases_path):
    months = {}
    for root, dirs, files in os.walk(databases_path, topdown=False):
        if root == databases_path:
            years = dirs
        else:            
            months[root[-4:]] = []
            for f in files:
                months[root[-4:]].append(f[-13:-11])

    return years, months


def get_months(years, databases_path):
    months = []
    for year in years:
        pass
    
    return months


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_folder', type=str, default='src/data', help='data folder')
    parser.add_argument('--db_folder_name', type=str, default='db_ds', help='database folder name')
    parser.add_argument('--data_year', type=str, default='all', help='database year')
    parser.add_argument('--data_month', type=str, default='all', help='database year')
    parser.add_argument('--out_folder', type=str, default='gen_dem_tables', help='.csv output folder')   

    args = parser.parse_args()

    # convert to dictionary
    params = vars(args)

    months_dic = None
    if params['data_year'] == 'all':
        years, months_dic = get_years(f"{params['data_folder']}/{params['db_folder_name']}")
    else:
        years = [params['data_year']]

    if params['data_month'] != 'all':
        months = [params['data_month']]

    out_folder = f"{params['data_folder']}/{params['out_folder']}"
    if not os.path.exists(out_folder):
        os.mkdir(out_folder)

    for year in years:
        if months_dic:
            months = months_dic[year]
        for month in months:
            db_path = f"{params['data_folder']}/{params['db_folder_name']}/{year}"
            files_date=f'{year[-2:]}{month}'
            db_name = f'BD_balance_valorizado_{files_date}_Data.accdb'

            # out_path = f"{out_folder}/{year}"
            # if not os.path.exists(out_path):
            #     os.mkdir(out_path)

            out_path = f"{out_path}/{files_date}"
            if not os.path.exists(out_path):
                os.mkdir(out_path)

            time_start = timeit.default_timer()
            gen_table, dem_table = gdt.create_tables(db_path, db_name, files_date)
            gen_file_name = f"GENERATION_{files_date}"
            gdt.save_table(gen_table, out_path, gen_file_name)
            dem_file_name = f"DEMAND_{files_date}"
            gdt.save_table(dem_table, out_path, dem_file_name)
            time_end = timeit.default_timer()
            print(f"time run = {time_end - time_start}")
