import os
import pandas as pd

from typing import List, Optional
from src.data.download.utils import get_paths, HOURS_COLS

edges_t = List[List[str]]


def split_edges(edges: List[str]) -> edges_t:
    return [e.split('->') for e in edges]


def read_edges(path) -> edges_t:
    df = pd.read_csv(path, sep=";", decimal=",")
    header = [df.columns[0]]
    header.extend(HOURS_COLS)
    df.columns = header
    df = df[df[header[0]].str.contains("->")].reset_index(drop=True)
    df = df.drop_duplicates(subset=[header[0]])
    df = df.sort_values(by=[header[0]]) 
    df = df.fillna(0) 
    edges = df[header[0]].tolist()
    edges = split_edges(edges)
    df[['bar_in', 'bar_out']] = pd.DataFrame(edges)
    return df, edges


def read_csv(path: str) -> pd.DataFrame:
    """Read the preprocess dataframes from csv."""
    df = pd.read_csv(path, sep=";", decimal=",")
    header = df.columns.tolist()
    header[-24:] = HOURS_COLS
    df.columns = header
    return df


if __name__ == "__main__":    
    data_path = "src/data/filesdb/"
    csv_bars_name = "BARRA_PLEXOS"
    file_paths = get_paths(path=data_path)

    bars_flow = pd.Series()
    for path in file_paths:
        p = os.path.join(path.path, path.g)
        flow, edges = read_edges(p)
        bars_flow = pd.concat((bars_flow, flow["bar_in"], flow["bar_out"]))
        bars_flow = pd.Series(bars_flow.unique())

    bars_flow.name = "bars"
    bars_flow = bars_flow.sort_values(ascending=True).reset_index(drop=True)
    bars_flow.to_csv(f"{data_path}line_bars.csv", index=False)

    bars_prg = pd.Series()
    for path in file_paths:
        p = os.path.join(path.path, path.prg)
        prg = read_csv(p)
        bars_prg = pd.concat((bars_prg, prg[csv_bars_name]))
        bars_prg = pd.Series(bars_prg.unique())

    bars_prg.name = "bars"
    bars_prg = bars_prg.sort_values(ascending=True).reset_index(drop=True)
    bars_prg.to_csv(f"{data_path}prg_bars.csv", index=False)
    
    # bars_line_to_prg_df = pd.read_csv("src/data/line_bar_to_prg_bar.csv")
    # bars_line_to_prg_values = bars_line_to_prg_df.values
    # bars_line_to_prg_dict = {bars_par[0]: bars_par[1] for bars_par in bars_line_to_prg_values}
    # print(bars_line_to_prg_dict)

    # Change first column name    
    # for path in file_paths:
    #     files = [path.prg, path.fp, path.g]
    #     for f in files:
    #         p = os.path.join(path.path,f)
    #         df = pd.read_csv(p, sep=";", decimal=",")
    #         columns = ["BARRA_PLEXOS"] + df.columns[1:].to_list()
    #         df.columns = columns
    #         df.to_csv(p, index=False, sep=";", decimal=",")





