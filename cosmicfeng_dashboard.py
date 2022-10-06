import numpy as np
import pandas as pd
import hvplot.streamz
from cosmic.hashpipe_aux import redis_obj
from cosmic.fengines import ant_remotefeng_map
import numpy as np
import panel as pn
from streamz.dataframe import PeriodicDataFrame
pn.extension()

#fetch the ant2feng dict
ant_feng_dict = ant_remotefeng_map.get_antennaFengineDict(redis_obj)

#Please select antennas you want listed here:
antnames = ["ea08", "ea03"]

NUM_ANTS = len(ant_feng_dict)
NUM_CHANNELS = 1024
NUM_TUNINGS = 4
YMin = -100
YMax = -20

def ant_dataFrame(**kwargs):
    dct = {}
    for ant,feng in ant_feng_dict.items():
        if ant in antnames:
            autocorr = np.array(feng.autocorr.get_new_spectra(),dtype=np.float64)
            autocorr = 10*np.log10(autocorr)
            np.nan_to_num(autocorr, copy=False, nan=0.0, posinf=2.0, neginf = 0.0)
            n_ifs, n_chans = autocorr.shape
            t_dict = {}
            for i in range(n_ifs):
                t_dict[i] = {
                        c : autocorr[i,c] for c in range(n_chans)
                    }
            t_df = pd.DataFrame(t_dict).transpose()
            t_df.index.name='ifs'
            dct[ant] = t_df
    df = pd.concat(dct).transpose()
    return df

df = PeriodicDataFrame(ant_dataFrame, interval='5s')

pn_realtime = pn.Column("# Autocorrelation Dashboard")
for ant_name in ant_feng_dict:
    if ant_name in antnames:
        pn_realtime.append(
                (pn.Row(f"""##Antenna: {ant_name}""")))
        pn_realtime.append(pn.Row(
                    df[ant_name].hvplot.line(backlog=1024, width = 800, height=700, xlim=(0,1024), ylim = (YMin, YMax), xlabel="frequency channels", ylabel="Spectral Power [dB]", grid=True)
                ))

pn_realtime.servable()