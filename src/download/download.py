from dataclasses import dataclass
import datetime as dt
import os
import socket
import zipfile
from urllib.request import urlretrieve
from typing import List
import shutil

from src.data.download.property_table_gen import create_property_table

BASE_URL = "https://www.coordinador.cl/wp-content/uploads"
INIT_YEAR = 2019 # 2019
INIT_MONTH = 6 # 6
INIT_DAY = 7 # 7
INIT_DATE = dt.datetime(year=INIT_YEAR, month=INIT_MONTH, day=INIT_DAY)
DATA_PATH = "src/data/files_zip/"
DATA_UNZIP_PATH = "src/data/files/"
DATA_NOT_FOUND_PATH = DATA_PATH + "not_found.txt"

# https://www.coordinador.cl/wp-content/uploads/2022/05/PROGRAMA20220503.zip
# https://www.coordinador.cl/wp-content/uploads/2021/09/PROGRAMA20211001-1.zip


@dataclass
class URL:
    url_base: str
    prg_name: str
    accdb_name: str

    @property
    def prg(self) -> str:
        return f"{self.url_base}/{self.prg_name}"

    @property
    def accdb(self) -> str:
        return f"{self.url_base}/{self.accdb_name}"


def get_download_path(current_date: dt.datetime):
    y = str(current_date.year)
    m = str(current_date.month).zfill(2)
    d = str(current_date.day).zfill(2)

    file = f"PROGRAMA{y}{m}{d}.zip"
    accdb = f"PLEXOS{y}{m}{d}.zip"
    urls = URL(f"{BASE_URL}/{y}/{m}", file, accdb)
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


def download_files(urls: List[str], time_out: int = 5) -> None:
    if not os.path.exists(DATA_PATH):
        os.mkdir(DATA_PATH)

    for url in urls:
        socket.setdefaulttimeout(time_out)
        # TODO: Check if file is already not found.
        try:
            urlretrieve(url.prg, os.path.join(DATA_PATH, url.prg_name))
        except Exception as e:
            print(f"File {url.prg_name} not found - URL: {url.prg}, {e}")
            add_not_found_file(url.prg)
            continue

        try:
            urlretrieve(url.accdb, os.path.join(DATA_PATH, url.accdb_name))
        except Exception as e:
            print(f"File {url.accdb_name} not found - URL: {url.accdb}, {e}")
            add_not_found_file(url.accdb)
            os.remove(os.path.join(DATA_PATH, url.prg_name))
            continue

        folder = os.path.splitext(url.prg_name)[0]
        unzip_file(url, folder)


def unzip_file(url, folder) -> None:
    # unzip prg
    unzip_error = unzip(url.prg_name, path=DATA_PATH, targetdir=DATA_UNZIP_PATH, folder=folder)
    if unzip_error:
        shutil.rmtree(os.path.join(DATA_UNZIP_PATH, folder))
        return

    # unzip db
    unzip_error = unzip_db(url, folder)
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
        os.remove(file_path)
        return e


def unzip_db(url: URL, folder):
    path = os.path.join(DATA_UNZIP_PATH, folder)
    # unzip PLEXOS
    unzip_error = unzip(url.accdb_name, path=DATA_PATH, targetdir=DATA_UNZIP_PATH, folder=folder)
    if unzip_error:
        return 1
    _, _, files = next(os.walk(path))
    files = [f for f in files if f.endswith(".zip")]
    for f in files:
        if 'res' in f.lower():
            # unzip Resultas
            folder = os.path.splitext(f)[0]
            unzip_error = unzip(f, path, targetdir=os.path.join(path, folder), folder='')
            if unzip_error:
                return 2

            _, db_dir1, _ = next(os.walk(path))
            for dir in db_dir1:
                if 'res' in dir.lower():
                    path_dir1 = os.path.join(path, dir)
                    _, db_dir2, db_files = next(os.walk(path_dir1))
                    if len(db_dir2) == 1:
                        if 'solution' in db_dir2[0].lower():
                            path_dir2 = os.path.join(path_dir1, db_dir2[0])
                            _, _, db_files = next(os.walk(path_dir2))
                            db_files = sorted([f for f in db_files if f.endswith(".mdb") or f.endswith(".accdb")])
                            create_property_table("Line", "Flow", path_dir2, out_path=path, files_name='FLOW', db_name=db_files[-1])
                            shutil.rmtree(os.path.join(path_dir1))
                        else:
                            raise NotImplementedError()
                    else:
                        db_files = [f for f in db_files if f.endswith(".mdb") or f.endswith(".accdb")]
                        create_property_table("Line", "Flow", path_dir1, out_path=path, files_name='FLOW', db_name=db_files[0])
                        shutil.rmtree(os.path.join(path_dir1))
                elif 'Model PRGdia_Full_Definitivo Solution' in dir:
                    path_dir1 = os.path.join(path, dir)
                    _, _, db_files = next(os.walk(path_dir1))
                    db_files = [f for f in db_files if f.endswith(".mdb") or f.endswith(".accdb")]
                    create_property_table("Line", "Flow", path_dir2, out_path=path, files_name='FLOW', db_name=db_files[0])
                    shutil.rmtree(os.path.join(path_dir1))
                else:
                    os.remove(os.path.join(path, f))
            # if len(db_dir1) == 1 and 'res' in db_dir1[0].lower():
            #     path_dir1 = os.path.join(path, db_dir1[0])
            #     _, db_dir2, _ = next(os.walk(path_dir1))
            #     if len(db_dir2) == 1 and 'solution' in db_dir2[0].lower():
            #         path_dir2 = os.path.join(path_dir1, db_dir2[0])
            #         _, _, db_files = next(os.walk(path_dir2))
            #         db_files = [f for f in db_files if f.endswith(".mdb") or f.endswith(".accdb")]
            #         create_property_table_dummy(path_dir2, out_path=path, files_name='chaotabla', db_name=db_files[0])
            #         shutil.rmtree(os.path.join(path_dir1))

            #     else:
            #         raise NotImplementedError()
            # else:

            #     raise NotImplementedError()

            # if len(db_dir1) == 1 and 'res' in db_dir1[0].lower():
            #     path_dir1 = os.path.join(path, db_dir1[0])
            #     _, db_dir2, _ = next(os.walk(path_dir1))
            #     if len(db_dir2) == 1 and 'solution' in db_dir2[0].lower():
            #         path_dir2 = os.path.join(path_dir1, db_dir2[0])
            #         _, _, db_files = next(os.walk(path_dir2))
            #         db_files = [f for f in db_files if f.endswith(".mdb") or f.endswith(".accdb")]
            #         create_property_table_dummy(path_dir2, out_path=path, files_name='chaotabla', db_name=db_files[0])
            #         shutil.rmtree(os.path.join(path_dir1))

            #     else:
            #         raise NotImplementedError()
            # else:

            #     raise NotImplementedError()
        else:
            os.remove(os.path.join(path, f))


def unzip_files() -> None:
    if not os.path.exists(DATA_UNZIP_PATH):
        os.mkdir(DATA_UNZIP_PATH)

    _, _, files = next(os.walk(DATA_PATH))
    files = [f for f in files if f.endswith(".zip")]
    files.sort()
    for f in files:
        unzip(file=f, path=DATA_PATH, targetdir=DATA_UNZIP_PATH)
        check_correct_files(f)


def check_correct_files(file) -> None:
    folder = os.path.splitext(file)[0]
    unzipped_path = os.path.join(DATA_UNZIP_PATH, folder)
    _, _, files = next(os.walk(unzipped_path))

    prg = [f for f in files if "PRG" in f and f.endswith(".xlsx")]
    fp = [f for f in files if "PO" in f and f.endswith(".xlsx")]

    if not (prg and fp):
        print(f"Removing {file}")
        os.remove(os.path.join(DATA_PATH, file))
        shutil.rmtree(unzipped_path)


def main_download():
    date = dt.datetime.utcnow()
    current_date = INIT_DATE
    urls = []
    while current_date <= date:
        url = get_download_path(current_date)
        urls.append(url)
        current_date += dt.timedelta(days=1)

    download_files(urls, time_out=1)


if __name__ == "__main__":
    main_download()
    unzip_files()
