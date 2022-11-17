from dataclasses import dataclass
import difflib
import os
import time
from typing import Dict, List, Optional, Tuple
import warnings

import numpy as np
import pandas as pd
import torch as th
import dgl
import matplotlib.pyplot as plt
import networkx as nx

from src.data.preprocess.my_warnings import CentralNotInDictWarning

warnings.simplefilter('always', UserWarning)
warnings.simplefilter('always', CentralNotInDictWarning)


FP_EXCEL_ROW_INIT = 2
FP_EXCEL_COLS = "B:Z"
FP_SHEET = "FP diario"

PROGRAMA_EXCEL_COLS = "C:AC"
PRG_SHEET = "PROGRAMA"
CENTRAL = "CENTRAL"
CENTRAL_PLEXOS = "CENTRAL_PLEXOS"
BARRA_PLEXOS = "BARRA_PLEXOS"
HOURS_COLS = [i for i in range(1, 25)]

AVAILABLE_TECHS = [
    "Hidroeléctricas de Pasada",
    "Eólicas",
    "Solares",
    "Centrales de concentración solar",
    "Térmicas",
    "Embalses y Reguladas",
]

DATA_PATH = "src/data/files/"
DICT_PATH = "src/data/bar2central_dictionary.xlsx"

NOT_IN_DICT = "src/data/notindict.txt"


@dataclass
class FileData:
    path: str   # path to the files
    prg: str    # program file name
    fp: str     # fp file name
    g: str      # flow graph file name


def read_central2bar_dict(path: Optional[str] = None) -> Dict[str, str]:
    """Read the excel containing the map from "Central" to "Barra".

    Args:
        path: path to the excel file containing the dictionary defined in the
            "Centrales" sheet.

    Returns:
        A dictionary mapping from "Central" to "Bar".
    """

    path = path if path else DICT_PATH

    xls = pd.ExcelFile(path)
    df = pd.read_excel(xls, "Centrales",)
    df[CENTRAL_PLEXOS] = df[CENTRAL_PLEXOS].str.upper()
    df[BARRA_PLEXOS] = df[BARRA_PLEXOS].str.upper()
    d = dict(zip(df[CENTRAL_PLEXOS], df[BARRA_PLEXOS]))
    return d


def read_prg_tables(
    path: str,
    techs: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """Read the excel containing the Program Generations by central.

    The function works by find the row index where the technology is find in the
    spread sheet. Then it iterates over the next rows until it finds the next
    NaN value, which defines the end index of the table corresponding to that
    technology. The former table is transformed into a dataframe and it is saved
    into a dictionary as the pair "tech: df". Finally it iterates over all the
    techs and return the dictionary.

    Args:
        path: path to the excel file containing the hourly generations defined
            in the "PROGRAMA" sheet.
        techs: A list containing all the available techonologies of generation.

    Returns:
        A dictionary containing in its entries a dataframe for each techonology
        avaiable in ```techs```. Each dataframe contains a columns with the
        central name, the central name in plexos and its respective hourly
        generation in [MW].
    """

    xls = pd.ExcelFile(path)

    # Read all the sheet inside PROGRAMA
    df = pd.read_excel(
        xls,
        PRG_SHEET,
        header=None,
        usecols=PROGRAMA_EXCEL_COLS
    )
    # drop column AC
    df.drop(df.columns[-1], axis=1, inplace=True)

    # Find a column with nan indices used to detect where tables ends.
    nan_values = df.iloc[:, 0].isnull()
    nan_indices = df.iloc[:, 0][nan_values].index.tolist()

    dfs = {}

    techs = techs if techs else AVAILABLE_TECHS
    for tech in techs:
        mask = df.values == tech
        if mask.any():
            # find the first and last index of the table associated to tech
            start_idx = df[mask].index.tolist()[0]
            end_idx = next(idx for idx in nan_indices if idx > start_idx)

            # create and format dataframe
            df_aux = pd.DataFrame(df.iloc[start_idx:end_idx, :].values)
            header = df_aux.iloc[0]
            header[:2] = [CENTRAL, CENTRAL_PLEXOS]
            header[2:] = HOURS_COLS
            df_aux.columns = header

            # drop the first row of the table of total data.
            idx = 3 if df_aux.values[0, 0].lower() == "total" else 2
            df_aux = df_aux.iloc[idx:, :]

            # add to tech dictionary.
            dfs[tech] = df_aux

        else:
            print(f"{tech} not avaiable")

    return dfs


def add_bar2df(
    df: pd.DataFrame,
    central2bar: Dict[str, str],
    path: str
) -> pd.DataFrame:
    """Add the bar associated to the central into the main dataframe.

    The function adds to the new column the bar associated to the central using the PLEXOS name to each row.

    TODO: try to add a warning instead
    .. note::
        If the central has no bar pair in the dictionary, the central is deleted from the output dataframe and a warning is raised for each central with bar pair unavailable.

        The main reason behind this is mainly due to a dictionary outdated. In these case, please update the centrals to bar dictionary.

        Additionally, take in consideration that if the PLEXOS central's name is
        not specified, a closest match find and used for the dictionary. Which
        may cause a wrong name matching. In these cases, correct the dataset.

    Args:
        df: dataframe that comes with the format of the function
            :func:`~src.data.preprocces.utils.read_prg`
        central2bar: a dictionary that associates the centrals and its
            corresponding bar coming from
            :func:`~src.data.preprocces.utils.read_central2bar_dict`
        path: path to the excel file containing the hourly generations defined
            in the "PROGRAMA" sheet. Use when ```CentralNotInDictWarning``` is
            triggerd.

    Returns:
        The input dataframe with a new column containing the bar of the central.
    """
    notindict = []

    for i, central_name in enumerate(df[CENTRAL_PLEXOS].tolist()):
        # if central present in dict, add to bar name to df.

        if central_name in central2bar:
            df[BARRA_PLEXOS].iloc[i] = central2bar[central_name]

        elif pd.isna(central_name) or pd.isnull(central_name):
            c_name = df[CENTRAL][i]
            c = difflib.get_close_matches(c_name, central2bar)

            m1 = """\nCentral with nan PLEXOS name. \n"""
            m2 = f"""Check the current PRG, column D for central {c_name}. {path}\n"""
            if c:
                closest_plexos_name = c[0]
                df[BARRA_PLEXOS].iloc[i] = central2bar[closest_plexos_name]
                m3 = f"""Using closest math (plexos) name {c[0]} found on dictionary.\n"""

                warnings.warn(m1 + m2 + m3, stacklevel=2)
            else:
                m3 = """Closest match not found on dictionary.\n"""
                raise ValueError(m1 + m2 + m3)

        # add to a list of non avaiable central-bar pair
        else:
            notindict.append(central_name)
            continue

    # raise warning for non available centrals and drop them from df.

    if notindict:
        m1 = f"\nUpdate Central to Bar Dictionary {DICT_PATH}\n"
        m2 = "The following centrals are not present in central2bar dictionary and their data is dropped."
        m3 = f"\n{notindict}\n"
        warnings.warn(m1 + m2 + m3, CentralNotInDictWarning, stacklevel=2)
        for central in notindict:
            idx = df.index[df[CENTRAL_PLEXOS] == central].tolist()[0]
            df.drop(idx, axis=0, inplace=True)
    return df


def sum_bar_data(df: pd.DataFrame) -> pd.DataFrame:
    """Create a dataframe with the hourly generation by bar.

    Args:
        df: a dataframe comming containing the central names, the corresponding
            bar and the hourly generation coming from
            :func:`~src.data.preprocess.utils.add_bar2df`

    Returns:
        A new dataframe containing the bar and its corresponding generation by
        hour.
    """

    # Create a list all the bars to iterate.
    bars = list(dict.fromkeys(df[BARRA_PLEXOS].tolist()))
    bars.sort()

    data = []
    for bar in bars:
        # Select the central that are present at the bar. Select its hour
        # colums. Add all the centrals hourly. Append to a list.
        d = df[df[BARRA_PLEXOS] == bar][HOURS_COLS].to_numpy().sum(0)
        data.append([bar, *d])

    # transform the list into a dataframe.
    df_out = pd.DataFrame(data, columns=[BARRA_PLEXOS, *HOURS_COLS])
    return df_out


def read_prg(
    path: str,
    central2bar: Dict[str, str],
    techs: Optional[List[str]] = None
) -> pd.DataFrame:
    """Read and transform the program generation to generation by bars.

    The function reads the excel containing the Program Generations by central, associates each central to its corresponding bar and returns a dataframe with the hourly generation by bar.

    Args:
        path: path to the excel file containing the hourly generations defined
            in the "PROGRAMA" sheet.
        central2bar: a dictionary containing the bar for each central.
        techs: A list containing all the available techonologies of generation.
    """

    # Read the generation by technology and get a dataframe for each tech.
    prg_dict = read_prg_tables(path, techs)

    # Concat tech dataframes and reformat df.
    dfs = [v for _, v in prg_dict.items()]
    df = pd.concat(dfs)
    df[BARRA_PLEXOS] = ""
    df.reset_index(drop=True, inplace=True)
    df[CENTRAL_PLEXOS] = df[CENTRAL_PLEXOS].str.upper()

    # add a column containg the corresponding bar for each central.
    df = add_bar2df(df, central2bar, path)

    df_bars = sum_bar_data(df)

    return df_bars


def read_fp(path: str) -> pd.DataFrame:
    """Read the excel containing the FP by bars.

    Args:
        path: path to the excel file containing the FP defined in the
            "FP diario" sheet.

    Returns:
        A dataframe that contains a columns with the bar name and its respective
        hourly FP.
    """
    xls = pd.ExcelFile(path)
    df = pd.read_excel(xls, FP_SHEET, header=FP_EXCEL_ROW_INIT - 1, usecols=FP_EXCEL_COLS)
    header = df.columns.tolist()
    header[0] = BARRA_PLEXOS
    df.columns = header
    df[BARRA_PLEXOS] = df[BARRA_PLEXOS].str.upper()
    return df


def check_prg_bars_in_fp(fp: pd.DataFrame, prg: pd.DataFrame) -> None:
    """Assert that all the bars with generation has FP."""
    fp_bars_list = fp[BARRA_PLEXOS].tolist()
    prg_bars_list = prg[BARRA_PLEXOS].tolist()
    assert all(elem in fp_bars_list for elem in prg_bars_list)


def read_data(paths: FileData,
              dict_path: Optional[str] = None,
              techs: Optional[str] = None
              ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read excel files in paths and return a FP and Bar Generation dataframes.

    Args:
        paths: A ``FileData``` object that contains the path, to PRG and PO
            files, and its corresponding names.
        dict_path: path to the excel file containing the dictionary defined in
            the "Centrales" sheet.
        techs: A list containing all the available techonologies of generation.

    Returns:
        Two dataframes, the first containing the bars with its respective hourly generation and the second one containing the bars with its respective hourly FPs.
    """

    fp = read_fp(os.path.join(paths.path, paths.fp))
    central_dict = read_central2bar_dict(dict_path)
    prg = read_prg(os.path.join(paths.path, paths.prg), central_dict, techs)
    check_prg_bars_in_fp(fp, prg)
    prg = complete_empty_bars(prg, fp)
    prg, fp = sort_dfs(prg, fp)
    return prg, fp


def sort_dfs(prg, fp):
    prg.sort_values(BARRA_PLEXOS, inplace=True)
    prg.reset_index(drop=True, inplace=True)
    fp.sort_values(BARRA_PLEXOS, inplace=True)
    fp.reset_index(drop=True, inplace=True)
    assert (prg[BARRA_PLEXOS] == fp[BARRA_PLEXOS]).all()
    return prg, fp


def complete_empty_bars(prg, fp):
    """Complete bars with zero generation."""
    prg_bars = prg[BARRA_PLEXOS].values
    fp_bars = fp[BARRA_PLEXOS].values
    new_prg = fp.copy()
    new_prg[HOURS_COLS] = np.zeros_like(new_prg[HOURS_COLS])

    for i, bar in enumerate(fp_bars):
        if bar in prg_bars:
            assert new_prg[BARRA_PLEXOS][i] == bar
            new_prg.iloc[i, -24:] = prg[prg[BARRA_PLEXOS] == bar][HOURS_COLS]

    return new_prg


def get_paths(path: Optional[str] = None, ext: str = ".csv") -> List[FileData]:
    """Get the path of the dataset where PRG and PO files are located.

    Args:
        path: where to find the files.
        ext: whether to read a .csv or an .xlsx file

    Returns:
        A list containing ```FileData``` objects that contains the path, to PRG
        and PO files, and its corresponding names.
    """
    assert ext == ".csv" or ".xlsx"

    files_paths = []
    path = path if path else DATA_PATH
    _, dirs, _ = next(os.walk(path))
    dirs.sort()
    for d in dirs:
        p = os.path.join(path, d)
        try:
            _, _, files = next(os.walk(p))
            prg = next(f for f in files if "PRG" in f and f.endswith(ext))
            fp = next(f for f in files if "PO" in f and f.endswith(ext))
            g = next(f for f in files if "FLOW" in f and f.endswith(ext))
            files_paths.append(FileData(p, prg, fp, g))
        except Exception:
            pass
    return files_paths


def split_data(dataset: List[FileData], split_percentage: int, split_offset: int) -> List[FileData]:
    len_data = len(dataset)
    split_size = int(len_data/(100/split_percentage))
    total_offset = split_size*split_offset
    d_split = dataset[total_offset:total_offset+split_size]
    return d_split


def read_csv(path: str) -> pd.DataFrame:
    """Read the preprocess dataframes from csv."""
    df = pd.read_csv(path, sep=";", decimal=",")
    header = df.columns.tolist()
    header[-24:] = HOURS_COLS
    df.columns = header
    return df


def dataset_to_csv(path: Optional[str] = None, test: bool = True) -> None:
    """Transform preprocess dataframes to csv's.

    Args:
        test: flag to assert the equality between the initial df and the one
            load from the create csv.
    """
    file_paths = get_paths(path=path, ext=".xlsx")

    for p in file_paths:
        print(p)
        prg, fp = read_data(p)

        prg_name = os.path.join(p.path, p.prg)
        fp_name = os.path.join(p.path, p.fp)

        prg_csv_name = os.path.splitext(prg_name)[0] + ".csv"
        fp_csv_name = os.path.splitext(fp_name)[0] + ".csv"

        prg.to_csv(prg_csv_name, index=False, sep=";", decimal=",")
        fp.to_csv(fp_csv_name, index=False, sep=";", decimal=",")

        if test:
            df_prg = read_csv(prg_csv_name)
            df_fp = read_csv(fp_csv_name)

            # dataframe are not exactly equal due to quantization errors.

            # Checking columns names
            assert (prg.columns == df_prg.columns).all()
            assert (fp.columns == df_fp.columns).all()
            # Checking bar names
            assert (prg[BARRA_PLEXOS] == df_prg[BARRA_PLEXOS]).all()
            assert (fp[BARRA_PLEXOS] == df_fp[BARRA_PLEXOS]).all()
            # Checking numerical values
            assert all((prg[HOURS_COLS] - df_prg[HOURS_COLS]) < 1e-12)
            assert all((fp[HOURS_COLS] - df_fp[HOURS_COLS]) < 1e-12)


def excel_to_csv(path: Optional[str] = None, test: bool = True) -> None:
    """Transform preprocess dataframes to csv's.

    Args:
        test: flag to assert the equality between the initial df and the one
            load from the create csv.
    """
    _, _, files = next(os.walk(path))
    prg_excel = next(f for f in files if "PRG" in f and f.endswith(".xlsx"))
    fp_excel = next(f for f in files if "PO" in f and f.endswith(".xlsx"))
    p = FileData(path, prg_excel, fp_excel, None)

    prg, fp = read_data(p)

    #prg_name = os.path.join(p.path, p.prg)
    fp_name = os.path.join(p.path, p.fp)

    #prg_csv_name = os.path.splitext(prg_name)[0] + ".csv"
    fp_csv_name = os.path.splitext(fp_name)[0] + ".csv"

    #prg.to_csv(prg_csv_name, index=False, sep=";", decimal=",")
    fp.to_csv(fp_csv_name, index=False, sep=";", decimal=",")

    # Remove excel files
    os.remove(f"{p.path}/{prg_excel}")
    os.remove(f"{p.path}/{fp_excel}")

    if test:
        #df_prg = read_csv(prg_csv_name)
        df_fp = read_csv(fp_csv_name)

        # dataframe are not exactly equal due to quantization errors.

        # Checking columns names
        #assert (prg.columns == df_prg.columns).all()
        assert (fp.columns == df_fp.columns).all()
        # Checking bar names
        #assert (prg[BARRA_PLEXOS] == df_prg[BARRA_PLEXOS]).all()
        assert (fp[BARRA_PLEXOS] == df_fp[BARRA_PLEXOS]).all()
        # Checking numerical values
        #assert all((prg[HOURS_COLS] - df_prg[HOURS_COLS]) < 1e-12)
        assert all((fp[HOURS_COLS] - df_fp[HOURS_COLS]) < 1e-12)


def read_csv_data(p: FileData) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read PRG and FP dataframes from preprocessed csv's."""
    prg_name = os.path.join(p.path, p.prg)
    fp_name = os.path.join(p.path, p.fp)

    prg = read_csv(prg_name)
    fp = read_csv(fp_name)

    return prg, fp


def day_to_hour(
    data: pd.DataFrame,
    value_name: str,
):
    columns = data.columns
    if value_name.lower() == "flow":
        return [data[["IN", "OUT", i]].copy().rename(columns = {i:value_name}) for i in HOURS_COLS]
    else:
        return [data[[columns[0], i]].copy().rename(columns = {i:value_name}) for i in HOURS_COLS]


def gen_sym_graph(g):
    edges_in = [e[1] for e in g['IN']]
    edges_out = [e[1] for e in g['OUT']]
    edges_weights = g['flow'].to_list()
    edges_weights_neg = [-i for i in edges_weights]

    edges_in_sym = edges_in + edges_out
    edges_out_sym = edges_out + edges_in
    edges_weights_sym = edges_weights + edges_weights_neg

    graph = dgl.graph((th.tensor(edges_in_sym), th.tensor(edges_out_sym)))
    graph.edata['w'] = th.tensor(edges_weights_sym)

    # graph = dgl.add_self_loop(graph)

    return graph


def set_self_loop_values(num_nodes):
    # TODO: Define self loop values
    return th.ones(num_nodes)

def gen_graph(flow, prg, self_loop=False):
    edges_in = [int(e[1]) for e in flow['IN']]
    edges_out = [int(e[1]) for e in flow['OUT']]
    edges_weights = flow['flow'].to_list()
    edges_signs = flow['sign'].to_list()

    for ew_idx in range(len(edges_signs)):
        if edges_signs[ew_idx] < 0:
            edge_in_temp = edges_in[ew_idx]
            edges_in[ew_idx] = edges_out[ew_idx]
            edges_out[ew_idx] = edge_in_temp

    #### Initial Adjacency
    # graph = dgl.graph((th.tensor(edges_in), th.tensor(edges_out)))
    # graph.edata['w'] = th.tensor(edges_weights)
    # graph = dgl.remove_self_loop(graph)

    #### Transpose Initial Adjacency
    # graph = dgl.graph((th.tensor(edges_out), th.tensor(edges_in)))  # Transpose
    # graph.edata['w'] = th.tensor(edges_weights)

    #### Initial Adjacency with self-loop flag
    # graph = dgl.graph((th.tensor(edges_in), th.tensor(edges_out)))
    # if self_loop:
    #     graph = dgl.add_self_loop(graph)
    #     sl_values = set_self_loop_values(graph.num_nodes())
    #     graph.edata['w'] = th.cat((th.tensor(edges_weights), sl_values))
    # else:
    #     graph.edata['w'] = th.tensor(edges_weights)

    ### Antisymmetric Adjacency
    edgein = th.cat((th.tensor(edges_in), th.tensor(edges_out)))
    edgeout = th.cat((th.tensor(edges_out), th.tensor(edges_in)))
    edges_weights = th.cat((th.tensor(edges_weights), -th.tensor(edges_weights)))
    graph = dgl.graph((edgein, edgeout))
    if self_loop:
        graph = dgl.add_self_loop(graph)
        sl_values = set_self_loop_values(graph.num_nodes())
        graph.edata['w'] = th.cat((edges_weights, sl_values))
    else:
        graph.edata['w'] = edges_weights

    #### Just Ones Adjacency
    # edgein = th.cat((th.tensor(edges_in), th.tensor(edges_out)))
    # edgeout = th.cat((th.tensor(edges_out), th.tensor(edges_in)))
    # edges_weights = th.cat((th.tensor(edges_weights), -th.tensor(edges_weights)))
    # graph = dgl.graph((edgein, edgeout))
    # graph.edata['w'] = th.ones(len(edges_weights))

    return graph


def run_time_test(max_n_files: int = 20) -> None:
    """Run a test time comparing loading xlsx and csv files.

    Args:
        n: max files to read.
    """

    # just one iteration because loading xlsx files is slow
    file_paths = get_paths()
    max_n_files = min(max_n_files, len(file_paths))
    print(f"Computing time test for {max_n_files}")

    # loading xlsx
    t0 = time.perf_counter()
    file_paths = get_paths(ext='.xlsx')
    for i, p in enumerate(file_paths):
        _ = read_data(p)
        if i >= max_n_files:
            break

    # loading csv
    t1 = time.perf_counter()
    file_paths = get_paths()
    for i, p in enumerate(file_paths):
        _ = read_csv_data(p)
        if i >= max_n_files:
            break
    t2 = time.perf_counter()

    print(f"xlsx time: {t1 - t0}")
    print(f"csv time: {t2 - t1}")


def plot_graph(graph):
    nodes_names = list(graph.nodes())
    nodes_attributes = list(graph.ndata["x"])
    nodes = [(int(nodes_names[node_idx]), {"pow": int(nodes_attributes[node_idx])}) for node_idx in range(len(nodes_names))]

    in_edges = list(graph.edges()[0])
    out_edges = list(graph.edges()[1])
    edges_weights = list(graph.edata["w"])
    edges = [(int(in_edges[edge_idx]), int(out_edges[edge_idx]), np.float32(edges_weights[edge_idx])) for edge_idx in range(len(in_edges))]

    DG = nx.DiGraph()
    DG.add_nodes_from(nodes)
    DG.add_weighted_edges_from(edges)

    subax = plt.subplot(111)
    nx.draw(DG, with_labels=True)
    plt.show()


if __name__ == "__main__":
    dataset_to_csv(test=True)
    # dataset_to_csv(path="src/data/test/", test=True)
    # run_time_test()