import os
import sys
import argparse
import timeit

from datetime import date

os.environ["PATH"] = r"C:\Program Files\DIgSILENT\PowerFactory 2021 SP5"+os.environ["PATH"]
sys.path.append(r"C:\Program Files\DIgSILENT\PowerFactory 2021 SP5\Python\3.9")

import powerfactory as pf


def start_pf():
        # Connect with Power Factory
    app = pf.GetApplication()

    # Show the app IDE. Not necessary
    app.Show()

    # Get current user
    user = app.GetCurrentUser()    

    return app, user


def import_pfd(app, pfd_path, project_name):
    #script=app.GetCurrentScript()
    importObj=app.CreateObject('CompfdImport','Import')

    importObj.SetAttribute("e:g_file", pfd_path)

    location=app.GetCurrentUser()
    importObj.g_target=location

    importObj.Execute()
    act_error_flag = app.ActivateProject(project_name)


def close_pf(app):
    # Show the app IDE. Not necessary
    app.Hide()

    return


def run_fp_cal_loop(app, user, project_name, data_folder, days, first_day=1):
    # Activate a specific project
    for day in range(first_day, days+1):  # BORRAR si se inicializa afuera
        print(f' --------------  Calculating FP for day {day} --------------------- ')
        app, user = start_pf()  # BORRAR si se inicializa afuera
        act_error_flag = app.ActivateProject(project_name)
        if act_error_flag != 0:            
            with open('error_out.txt', 'w+') as f:
                print(f"{project_name} can't be activated")
                f.write(f"{project_name} can't be activated")
            break
            # pfd_path = f"{data_folder}/{project_name}.pfd"
            # import_pfd(app, pfd_path, project_name)
        else:
            print(f'************** Project {project_name} activated succesfully *****************')
        project = app.GetActiveProject()

        # Activate Study case
        std_case = app.GetActiveStudyCase()
        print(f'Study Case {std_case.loc_name} active')
        if std_case.loc_name != 'Base SEN':
            FolderWhereStudyCasesAreSaved= app.GetProjectFolder('study')
            AllStudyCasesInProject= FolderWhereStudyCasesAreSaved.GetContents()
            for StudyCase in AllStudyCasesInProject:
                if StudyCase.loc_name == 'Base SEN':
                    std_case = StudyCase
                    std_case.Activate()
                    print(f'Study Case {std_case.loc_name} now activated')
                    break

    #for day in range(first_day, days+1):  # DESCOMENTAR si se inicializa afuera
        time_start = timeit.default_timer()
        load_data_dpl = app.GetFromStudyCase("Carga Excel consumos y generación hora.ComDpl")
        err = load_data_dpl.SetInputParameterString('path', f'{data_folder}/BD_DS_dia_{day}\BD_DS')
        err = load_data_dpl.SetInputParameterString('fpen', 'hour')
        err = load_data_dpl.Execute()
        print(f'Error de load_data_dpl = {err}')
        solver_dpl = app.GetFromStudyCase("Solver cuadre Dda y Gx hora.ComDpl")
        err = solver_dpl.SetInputParameterString('path', f'{data_folder}/BD_DS_dia_{day}')
        err = solver_dpl.SetInputParameterString('fpen', 'hour')
        err = solver_dpl.Execute()
        print(f'Error de solver_dpl = {err}')
        fp_calc_dpl = app.GetFromStudyCase("PenaltyFactorCalc2 hora.ComDpl")
        err = fp_calc_dpl.SetInputParameterString('path', f'{data_folder}/BD_DS_dia_{day}/')
        err = fp_calc_dpl.SetInputParameterString('fpen', 'hour')
        err = fp_calc_dpl.SetInputParameterInt('i_allbus', 0)
        err = fp_calc_dpl.Execute()
        print(f'Error de fp_calc_dpl = {err}')
        time_end = timeit.default_timer()
        print(f"time run day {day}= {time_end - time_start}")

        close_pf(app)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_name_preffix', type=str, default='BD 2018_DTE', help='PF project name preffix')
    parser.add_argument('--project_name_suffix', type=str, default='', help='PF project name suffix')
    parser.add_argument('--data_folder', type=str, default='C:/Users/note306/Documents/cen_ds_datasets/src/data/model_ds', help='data folder')
    parser.add_argument('--data_years', nargs='+', type=str, default=['22'])
    parser.add_argument('--data_months', nargs='+', type=str, default=['07', '06', '05', '04', '03'])
    parser.add_argument('--start_day', type=str, default='01', help='database year')

    args = parser.parse_args()

    # convert to dictionary
    params = vars(args)

    years = params['data_years']
    months = params['data_months']
    for y in years:
        for m in months:
            month =  int(m)  #int(params['data_month'])
            year =  int(y)  #int(params['data_year'])
            start_day = int(params['start_day'])
            project_name = f"{params['project_name_preffix']}_{y}{m}_def{params['project_name_suffix']}"
            data_folder = f"{params['data_folder']}/20{y}/FPen_{y}{m}_def"

            if month < 12:
                num_days = (date(year, month + 1, 1) - date(year, month, 1)).days        
            elif month == 12:
                num_days = 31

            
            app = 0
            user = 0
            # app, user = start_pf()  # DESCOMENTAR para inicializar afuera
            print(f' ******************* Start FP calc script for project {project_name} **************************')
            run_fp_cal_loop(app, user, project_name, data_folder, num_days, start_day)