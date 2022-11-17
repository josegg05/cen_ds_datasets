import os
import sys

os.environ["PATH"] = r"C:\Program Files\DIgSILENT\PowerFactory 2021 SP5"+os.environ["PATH"]
sys.path.append(r"C:\Program Files\DIgSILENT\PowerFactory 2021 SP5\Python\3.9")

import powerfactory as pf

# Connect with Power Factory
app = pf.GetApplication()

# Show the app IDE. Not necessary
app.Show()

# Get current user
user = app.GetCurrentUser()

# Activate a specific project
act_error_flag = app.ActivateProject("2209-BD-OP-COORD-DMAP")
project = app.GetActiveProject()

# Get SEN bars
sen_all_bars = []
sen_all_bars_dict = {}
count_bars = []
max_bar = 4  # 4 = Max value wen 20 evaluated
for i in range(1, max_bar+1):
    bars_bi = app.GetCalcRelevantObjects(f"* B{i}.ElmTerm")  # StaBar y ElmTerm = Barras,  ElmSym = Generadores 
    sen_all_bars.extend(bars_bi)
    count_bars.append(len(bars_bi))

bars_readed = []
for bar in sen_all_bars:
    sen_all_bars_dict[bar.loc_name] = bar
    if bar.loc_name in bars_readed:
        print(f"Repeated bar: {bar.loc_name}")
    else:
        bars_readed.append(bar.loc_name)


sen_line_dict = {}
sen_lines = app.GetCalcRelevantObjects("*.ElmBranch")  # ElmBranch = Line?

for i in sen_lines:
    sen_line_dict[i.loc_name] = i



while True:
    pass






