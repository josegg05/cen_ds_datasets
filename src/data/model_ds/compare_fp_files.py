import pandas as pd

year = "2022"
month = '05'
scenario = 'Baja'

file_allbus0 = pd.read_csv(f"{year}/FPen_{year[-2:]}{month}_def_ok/Dda {scenario} FP_def_all0.csv", delimiter=';', encoding='latin-1')
file_allbus1 = pd.read_csv(f"{year}/FPen_{year[-2:]}{month}_def_ok/Dda {scenario} FP_def_all1.csv", delimiter=';', encoding='latin-1')

compare = file_allbus0.compare(file_allbus1)

print(compare.head(20))