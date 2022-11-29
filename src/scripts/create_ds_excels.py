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
import data.db_ds_excels as dde


def get_years(databases_path):
    months = {}
    for root, dirs, files in os.walk(databases_path, topdown=True):
        if root == databases_path:
            years = dirs
            years_paths = [os.path.join(databases_path, y) for y in years]
        elif root in years_paths:      
            if dirs:
                months[root[-4:]] = [d[-2:] for d in dirs]

    return years, months


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_folder', type=str, default='src/data', help='data folder')
    parser.add_argument('--tables_folder_name', type=str, default='gen_dem_tables', help='database folder name')
    parser.add_argument('--excel_folder_name', type=str, default='model_ds', help='model and excel folder name')
    parser.add_argument('--data_year', type=str, default='all', help='table year')
    parser.add_argument('--data_month', type=str, default='all', help='table month')
    parser.add_argument('--out_folder', type=str, default='model_ds', help='.csv output folder')

    args = parser.parse_args()

    # convert to dictionary
    params = vars(args)

    months_dic = None
    if params['data_year'] == 'all':
        years, months_dic = get_years(f"{params['data_folder']}/{params['tables_folder_name']}")
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
            files_date=f'{year[-2:]}{month}'
            data_path = f"{params['data_folder']}/{params['tables_folder_name']}/{year}/{files_date}"
            excel_path = f"{params['data_folder']}/{params['excel_folder_name']}/{year}/FPen_{files_date}_def"

            out_path = f"{out_folder}/{year}"
            if not os.path.exists(out_path):
                os.mkdir(out_path)

            out_path = f"{out_path}/FPen_{files_date}_def"
            if not os.path.exists(out_path):
                os.mkdir(out_path)

            out_path = f"{out_path}/BD_DS_days"
            if not os.path.exists(out_path):
                os.mkdir(out_path)

            time_start = timeit.default_timer()
            gen_table, dem_table = dde.load_tables(data_path, files_date)
            dde.generate_days_excel(gen_table, dem_table, excel_path, out_path, files_date)
            time_end = timeit.default_timer()
            print(f"time run = {time_end - time_start}")
