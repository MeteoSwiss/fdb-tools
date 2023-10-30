import os
import earthkit.data
import numpy as np
import time

os.environ['FDB_HOME'] = '/scratch/mch/vcherkas/vcherkas/spack-root/linux-sles15-zen3/gcc-11.3.0/fdb-5.11.17-4hcp6n5lien4rzi4tqu2roa4zvsrfeur'
os.environ['FDB5_HOME'] = os.environ['FDB_HOME']
os.environ['FDB5_CONFIG'] = "{'type':'local','engine':'toc','schema':'/opr/vcherkas/fdb-schema','spaces':[{'handler':'Default','roots':[{'path':'/opr/vcherkas/fdb_root'}]}]}"
os.environ['ECCODES_DEFINITION_PATH'] = '/scratch/mch/vcherkas/eccodes-cosmo-resources/definitions:/scratch/mch/vcherkas/eccodes/definitions'

class MissingEnvironmentVariable(Exception):
    pass

if os.getenv('FDB_HOME') is None:
    raise MissingEnvironmentVariable('FDB_HOME needs to be set (for pyfdb). Find with `spack location -i fdb`.')
    
if os.getenv('FDB5_HOME') is None:
    raise MissingEnvironmentVariable('FDB5_HOME needs to be set (for earthkit.data). Should be identical to FDB_HOME.')
    
if (os.getenv('FDB5_CONFIG') is None and os.getenv('FDB5_CONFIG_FILE') is None):
    raise MissingEnvironmentVariable('Either FDB5_CONFIG or FDB5_CONFIG_FILE needs to be set (for FDB).')
    
if os.getenv('ECCODES_DEFINITION_PATH') is None:
    raise MissingEnvironmentVariable('ECCODES_DEFINITION_PATH needs to be set (for reading COSMO data)')


paramlist_all = [["500014", "500001", "500006", "500028","500030","500032","500035"]]
paramlist_all_files = [["T", "P", "FI", "U","V","W","QV"]]

tot_time_sel=0
tot_time=0
num_rec = 0

for param in paramlist_all:
    request = {
        "date":"20230201",
        "time":"0300",
        "class":"od",
        "stream":"enfo",
        "type":"ememb",
        "model":"COSMO-1E",
        "expver":"0001",
        "step":["0","1","2"],
        "number":["0"],
        "levtype":"ml",
        "levelist":list(range(81)),
        #T,P,FI
        "param": param
        }

    start = time.time()
    ds = earthkit.data.from_source("fdb", request, batch_size=0)
    end = time.time()
    tot_time_sel += end-start

    field_map: dict[tuple[int, ...], np.ndarray] = {}

    start = time.time()
    for f in ds:
        key = f.metadata(('number','step','levelist','param'))
        num_rec+=1
        field_map[key] = f.to_numpy(dtype=np.float32)
    end = time.time()

    tot_time += end-start

    for f in ds: 
        print("Ni, Nj", f.metadata(('Ni', 'Nj')))
        break
    
print("Sel time:", tot_time_sel)
print("Load time:", tot_time)
print("Num records: ", num_rec)

tot_time_sel=0
tot_time=0

files = ["/scratch/mch/cosuna/mars/COSMO-1E/1h/ml_sl/000/lfff00000000","/scratch/mch/cosuna/mars/COSMO-1E/1h/ml_sl/000/lfff00010000","/scratch/mch/cosuna/mars/COSMO-1E/1h/ml_sl/000/lfff00020000"]
num_rec = 0

for param in paramlist_all_files:
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
        "levelist":list(range(81)),
        #T,P,FI
        "param": param
        }

    start = time.time()
    ds = earthkit.data.from_source("file", files).sel(request)
    end = time.time()
    tot_time_sel += end-start

    field_map: dict[tuple[int, ...], np.ndarray] = {}

    start = time.time()
    for f in ds:
        key = f.metadata(('number','step','levelist','param'))
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

