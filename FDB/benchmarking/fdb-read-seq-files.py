import os
import earthkit.data
import numpy as np
import time

tot_time_sel=0
tot_time=0

files = ["/scratch/mch/cosuna/mars/COSMO-1E/1h/ml_sl/000/lfff00000000","/scratch/mch/cosuna/mars/COSMO-1E/1h/ml_sl/000/lfff00010000","/scratch/mch/cosuna/mars/COSMO-1E/1h/ml_sl/000/lfff00020000"]
num_rec = 0

request = {
    #"date":"20230201",
    #"time":"0300",
    #"class":"od",
    #"stream":"enfo",
    #"type":"ememb",
    #"model":"COSMO-1E",
    #"expver":"0001",
    "step":[0,1,2],
    #"number":["1"],
    #"levtype":"ml",
    #"levelist":list(range(81)),
    #T,P,FI
    #"param": param
    }

start = time.time()
ds = earthkit.data.from_source("file", files)
end = time.time()
tot_time_sel += end-start

field_map: dict[tuple[int, ...], np.ndarray] = {}

start = time.time()
for f in ds:
    key = f.metadata(('number','step','level','param'))
    field_map[key] = f.to_numpy(dtype=np.float32)
    num_rec += 1
end = time.time()

tot_time += end-start

for f in ds: 
    print("Ni, Nj", f.metadata(('Ni', 'Nj')))
    break
print("Sel time:", tot_time_sel)
print("Load time:", tot_time)
print("Num records: ", num_rec)

