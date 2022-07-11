import numpy as np
import pandas as pd
import hvplot.streamz
import matplotlib.pyplot as plt
import numpy as np
import panel as pn
from streamz.dataframe import PeriodicDataFrame
pn.extension()

#object from which data is collected:
class data_gen:
    def __init__(self,name,size=1024,sets=4):
        self.name = name
        self.size = size
        self.sets = sets

    def get_data(self):
        return np.random.randn(self.sets,self.size)
    
#Have a dictionary of items with name:
data_dict = {
    "a" : data_gen("a"),
    "b" : data_gen("b"),
    "c" : data_gen("c"),
    "d" : data_gen("d"),
    "e" : data_gen("e"),
    "f" : data_gen("f")
}

#Generate dataframe
def name_dataFrame(**kwargs):
    dct = {}
    for name,dg in data_dict.items():
        d = dg.get_data()
        sets, size = d.shape
        t_dict ={}
        for i in range(sets):
            t_dict[i] = {
                c : d[i,c] for c in range(size)
            }
        t_df = pd.DataFrame(t_dict).transpose()
        dct[name] = t_df
    df = pd.concat(dct).transpose()
    return df

#Have it be streamed
df = PeriodicDataFrame(name_dataFrame, interval='5s')

#Compose panel layout
pn_realtime = pn.Column("# Data Dashboard")
for name in data_dict:
    pn_realtime.append(
            (pn.Row(f"""##Name: {name}""")))
    pn_realtime.append(pn.Row(
                df[name].hvplot.line(backlog=1024, width = 600, height=500, xlabel="n", ylabel="f(n)", grid=True)
            ))

pn_realtime.servable()
