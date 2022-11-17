from dataclasses import dataclass
import datetime as dt
from dateutil.relativedelta import relativedelta
import os
import socket
import zipfile
from bs4 import BeautifulSoup
from urllib.request import urlretrieve, Request, urlopen
import requests
from typing import List
import shutil

from download.property_table_gen import create_property_table
from download.utils import excel_to_csv

BASE_URL = "https://www.coordinador.cl/mercados/documentos/transferencias-economicas/antecedentes-de-calculo-para-las-transferencias-economicas"
INIT_YEAR = 2020 # 2019
INIT_MONTH = 6 # 6
INIT_DAY = 7 # 7
INIT_DATE = dt.datetime(year=INIT_YEAR, month=INIT_MONTH, day=INIT_DAY)
DATA_PATH = "src/data/files_zip/"
DATA_UNZIP_PATH = "src/data/files_ds/"
DATA_NOT_FOUND_PATH = DATA_PATH + "not_found_ds.txt"

# https://www.coordinador.cl/wp-content/uploads/2022/05/PROGRAMA20220503.zip
# https://www.coordinador.cl/wp-content/uploads/2021/09/PROGRAMA20211001-1.zip

mes_a_nombre = {1: "enero", 2:"febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio", 7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"}

@dataclass
class URL:
    url_base: str
    accdb_name: str

    @property
    def accdb(self) -> str:
        return f"{self.url_base}/{self.accdb_name}"


def get_download_path(current_date: dt.datetime):
    y = str(current_date.year)
    m = str(current_date.month).zfill(2)
    d = str(current_date.day).zfill(2)

    url = f"{BASE_URL}/{y}-antecedentes-de-calculo-para-las-transferencias-economicas"
    # https://www.coordinador.cl/mercados/documentos/transferencias-economicas/antecedentes-de-calculo-para-las-transferencias-economicas/2019-antecedentes-de-calculo-para-las-transferencias-economicas/julio-definitivo/
    # https://www.coordinador.cl/wp-content/uploads/2019/08/A-1907-D1-Resultados-y-CMg.zip
    # https://www.coordinador.cl/mercados/documentos/transferencias-economicas/antecedentes-de-calculo-para-las-transferencias-economicas/2020-antecedentes-de-calculo-para-las-transferencias-economicas/enero-definitivo/
    # https://www.coordinador.cl/wp-content/uploads/2020/02/01-Resultados-2.zip
    # https://www.coordinador.cl/wp-content/uploads/2020/02/01-Resultados.zip
    # https://www.coordinador.cl/wp-content/uploads/2020/03/01-Resultados.zip
    # https://www.coordinador.cl/wp-content/uploads/2020/03/01-Resultados-2.zip
    # https://www.coordinador.cl/wp-content/uploads/2019/07/A-1906-D1-Resultados-y-CMg.zip
    # https://www.coordinador.cl/wp-content/uploads/2019/07/A-1906-P1-Resultados-y-CMg.zip
    # https://www.coordinador.cl/wp-content/uploads/2022/02/01-Resultados-2.zip
    # https://www.coordinador.cl/wp-content/uploads/2022/02/01-Resultados-3.zip
    # https://www.coordinador.cl/mercados/documentos/transferencias-economicas/antecedentes-de-calculo-para-las-transferencias-economicas/2022-antecedentes-de-calculo-para-las-transferencias-economicas/enero-preliminar-2022-antecedentes-de-calculo-para-las-transferencias-economicas
    # https://www.coordinador.cl/wp-content/uploads/2022/02/01-Resultados-1.zip

    # https://www.coordinador.cl/wp-content/uploads/2019/05/A-1904-D1-Resultados-y-CMg.zip
    # https://www.coordinador.cl/wp-content/uploads/2020/08/A-Resultados-1811-R03D.zip
    # https://www.coordinador.cl/wp-content/uploads/2019/04/A-1812-D1-Resultados-y-CMg.zip
    
    req = Request(url)
    html_page = urlopen(req)
    soup = BeautifulSoup(html_page, 'html.parser')
    
    urls_rel = []
    urls_def = []
    for docs_list_obj in soup.find_all('ul', {'class':'cen_list-documentos'}):
        for links_obj in docs_list_obj.find_all('a'):
            link = links_obj.get('href')
            if('tel' in link):
                break
            if('-rel' in link):
                urls_rel.append(link)
            if('-definitivo' in link):
                urls_def.append(link)

    urls_rel.sort()
    urls_def.sort()
    #for link in ur
            
    
    
    
    accdb = f"01-Resultados-2.zip"



    urls = URL(f"{BASE_URL}/{y}/{m}", accdb)
    return urls

    # https://www.coordinador.cl/wp-content/uploads/2022/08/PLEXOS20220818.zip


def add_not_found_file(file_zip: str):
    """Add the file_zip name into the non-existing-file database.

    Checks if the file exists and if the name is present and if not, it is added.
    """

    file_exists = os.path.exists(DATA_NOT_FOUND_PATH)
    if file_exists:
        with open(DATA_NOT_FOUND_PATH, 'r') as f:
            lines = f.readlines()
            lines = [line.rstrip() for line in lines]
        if file_zip not in lines:
            with open(DATA_NOT_FOUND_PATH, "a") as f:
                f.write("\n")
                f.write(file_zip)
        else:
            pass

    else:
        with open(DATA_NOT_FOUND_PATH, "w") as f:
            f.write(file_zip)


def download_files(url: List[str], time_out: int = 5) -> None:
    socket.setdefaulttimeout(time_out)

    try:
        urlretrieve(url.prg, os.path.join(DATA_PATH, url.prg_name))
    except Exception as e:
        print(f"File {url.prg_name} not found - URL: {url.prg}, {e}")
        add_not_found_file(url.prg)
        return None

    try:
        urlretrieve(url.accdb, os.path.join(DATA_PATH, url.accdb_name))
    except Exception as e:
        print(f"File {url.accdb_name} not found - URL: {url.accdb}, {e}")
        add_not_found_file(url.accdb)
        os.remove(os.path.join(DATA_PATH, url.prg_name))
        return None

    folder = os.path.splitext(url.accdb_name)[0]
    
    return folder


def unzip_files(url, folder) -> None:
    # unzip prg
    unzip_error = unzip_prg(url.prg_name, zip_path=DATA_PATH, targetdir=DATA_UNZIP_PATH, folder=folder)
    if unzip_error:
        shutil.rmtree(os.path.join(DATA_UNZIP_PATH, folder))
        return

    # unzip db
    unzip_error = unzip_db(url.accdb_name, zip_path=DATA_PATH, targetdir=DATA_UNZIP_PATH, folder=folder)
    if unzip_error:
        shutil.rmtree(os.path.join(DATA_UNZIP_PATH, folder))
        return


def unzip(file: str, path: str, targetdir: str, folder: str = None) -> None:
    """Unzip a file saved in targetdir, then it saves it in the targetdir."""
    if folder is None:
        folder = os.path.splitext(file)[0]
    file_path = os.path.join(path, file)
    target_path = os.path.join(targetdir, folder)
    try:
        with zipfile.ZipFile(file_path, "r") as z:
            z.extractall(target_path)
        os.remove(file_path)
        return
    except Exception as e:
        print(e)
        # os.remove(file_path)
        return e


def unzip_db(file: str, zip_path: str, targetdir: str, folder: str = None):
    path = os.path.join(targetdir, folder)
    # unzip PLEXOS
    unzip_error = unzip(file, path=zip_path, targetdir=targetdir, folder=folder)
    if unzip_error:
        return 1
    _, _, files = next(os.walk(path))
    files = [f for f in files if f.endswith(".zip")]
    for f in files:
        if ('res' in f.lower()) or ('Model PRGdia_Full_Definitivo Solution' in f):
            # unzip Resultas
            folder_res = os.path.splitext(f)[0]
            unzip_error = unzip(f, path, targetdir=os.path.join(path, folder_res), folder='')
            if unzip_error:
                return 2

            _, db_dir1, _ = next(os.walk(path))
            for dir in db_dir1:
                if ('res' in dir.lower()) or ('Model PRGdia_Full_Definitivo Solution' in dir):
                    path_dir1 = os.path.join(path, dir)
                    _, db_dir2, db_files = next(os.walk(path_dir1))
                    if len(db_dir2) == 1:
                        if 'solution' in db_dir2[0].lower():
                            path_dir2 = os.path.join(path_dir1, db_dir2[0])
                            _, _, db_files = next(os.walk(path_dir2))
                            db_files = sorted([f for f in db_files if f.endswith(".mdb") or f.endswith(".accdb")])
                            create_property_table("Line", "Flow", path_dir2, out_path=path, files_name=f'FLOW{folder[-6:]}', db_name=db_files[-1])
                            create_property_table("Node", "Generation", path_dir2, out_path=path, files_name=f'PRG{folder[-6:]}', db_name=db_files[-1])
                            create_property_table("Node", "Marginal Loss Factor", path_dir2, out_path=path, files_name=f'MLF{folder[-6:]}', db_name=db_files[-1])
                            shutil.rmtree(os.path.join(path_dir1))
                        else:
                            raise NotImplementedError()
                    else:
                        db_files = [f for f in db_files if f.endswith(".mdb") or f.endswith(".accdb")]
                        create_property_table("Line", "Flow", path_dir1, out_path=path, files_name=f'FLOW{folder[-6:]}', db_name=db_files[-1])
                        create_property_table("Node", "Generation", path_dir1, out_path=path, files_name=f'PRG{folder[-6:]}', db_name=db_files[-1])
                        create_property_table("Node", "Marginal Loss Factor", path_dir1, out_path=path, files_name=f'MLF{folder[-6:]}', db_name=db_files[-1])
                        shutil.rmtree(os.path.join(path_dir1))
                else:
                    os.remove(os.path.join(path, f))
        else:
            os.remove(os.path.join(path, f))

def unzip_prg(file: str, zip_path: str, targetdir: str, folder: str = None) -> None:
    unzip_error = unzip(file, path=zip_path, targetdir=targetdir, folder=folder)
    if unzip_error:
        return 1

    target_path = os.path.join(targetdir, folder)
    excel_to_csv(target_path)



def get_urls():
    date = dt.datetime.utcnow()
    current_date = INIT_DATE
    urls = []
    while current_date <= date:
        url = get_download_path(current_date)
        urls.append(url)
        current_date += relativedelta(months=+1)

    return urls


def create_files_paths():
    if not os.path.exists(DATA_PATH):
        os.mkdir(DATA_PATH)
    if not os.path.exists(DATA_UNZIP_PATH):
        os.mkdir(DATA_UNZIP_PATH)


if __name__ == "__main__":
    create_files_paths()
    urls = get_urls()   
    for url in urls:
        folder = download_files(url, time_out=1)
        if folder:
            unzip_files(url, folder)
