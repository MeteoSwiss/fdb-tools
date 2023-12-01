import dask
import logging
import os
import json
import time
from typing import Any
from pathlib import Path
from dataclasses import dataclass
from datetime import timedelta
from typing import List

import earthkit.data
import numpy as np
import matplotlib.pyplot as plt
from distributed import Client
from dask_jobqueue import SLURMCluster
from distributed.deploy.cluster import Cluster


logging.basicConfig(encoding='utf-8', filename=Path(os.path.abspath(__file__)).parent/'dask_fdb.log', level=logging.DEBUG)
logger = logging.getLogger('matplotlib')
logger.setLevel(logging.WARNING)



def start_cluster(*cluster_args: Any, **cluster_kwargs: Any) -> Client:
    """ Starts dask scheduler and Slurm cluster """

    global client

    _env_vars = {'OMP_NUM_THREADS': '1', 'OMP_THREAD_LIMIT': '1'}

    exclusive_jobs = False

    walltime = "00:10:00"
    walltime_delta = timedelta(seconds=3600) 
    worker_lifetime = int((walltime_delta - timedelta(minutes=3)).total_seconds())

    node_memory: str = "10GB"
    node_memory_bytes = dask.utils.parse_bytes(node_memory)

    node_cores = 1
    job_cpu = 1 # cores per job
    job_mem = dask.utils.format_bytes(node_memory_bytes // (node_cores // job_cpu))


    cluster = SLURMCluster(
        walltime=walltime,
        cores=node_cores,
        memory=job_mem,
        # processes=1, 
        job_cpu=job_cpu, 
        job_extra_directives=["--exclusive", "--nodelist nid002137"] if exclusive_jobs else ["--nodelist nid002137"],
        scheduler_options={
            "dashboard_address": ":8877",
        },
        worker_extra_args=["--lifetime", f"{worker_lifetime}s", "--lifetime-stagger", "2m", "--lifetime-restart"],
        local_directory='/scratch/mch/vcherkas/dask',
        log_directory='/scratch/mch/vcherkas/dask/logs',
        job_script_prologue=[f"export {e}={v}" for e, v in _env_vars.items()],
        nanny=True,
        queue='debug'
    )

    client = Client(cluster)

    logging.info(f"Started new SLURM cluster. Dashboard available at {cluster.dashboard_link}")

    if "SLURMD_NODENAME" in os.environ:
        nodename = os.environ["SLURMD_NODENAME"]
    else:
        nodename = "local"
    logging.info(f"Current node name: {nodename}")
    logging.debug(cluster.job_script())

    return client


def scale_and_wait(n: int):
    """ Scales number of dask workers to n workers."""

    logging.info(f"Scaling Dask workers to {n}")
    if client:
        assert isinstance(client.cluster, Cluster)
        client.cluster.scale(n)
        if n > 0:
            client.wait_for_workers(n)


def read_data(ens: int, steps: List[int], param: str):
    """ Reads data from FDB"""
    
    os.environ['FDB_HOME'] = '/scratch/mch/vcherkas/vcherkas/spack-root/linux-sles15-zen3/gcc-11.3.0/fdb-5.11.17-4hcp6n5lien4rzi4tqu2roa4zvsrfeur'
    os.environ['FDB5_HOME'] = os.environ['FDB_HOME']
    os.environ['FDB5_CONFIG'] = "{'type':'local','engine':'toc','schema':'/opr/vcherkas/fdb-schema-ordered','spaces':[{'handler':'Default','roots':[{'path':'/opr/vcherkas/fdb_root_ordered'}]}]}"
    os.environ['ECCODES_DEFINITION_PATH'] = '/scratch/mch/vcherkas/eccodes-cosmo-resources/definitions:/scratch/mch/vcherkas/eccodes/definitions'

    tot_time_sel=0
    tot_time=0
    num_rec = 0

    request = {
        "date":"20230727",
        "time":"0000",
        "class":"od",
        "stream":"enfo",
        "type":"ememb",
        "model":"COSMO-2E",
        "expver":"0001",
        "step":list(steps),
        "number":f'{ens}',
        "levtype":"ml",
        "levelist": list(range(81)),
        "param": f'{param}',
        }

    start = time.time()
    logging.info('Sending FDB request.')
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
        ni, nj = f.metadata(('Ni', 'Nj'))
        break

    # Calculate bandwidth GB/s
    gbps = ni*nj*num_rec*32/(10**9)/(8*(tot_time+tot_time_sel))

    print("Request:", json.dumps(request, indent=2))
    print("Sel time:", f"{tot_time_sel}")
    print("Load time:", f"{tot_time}")
    print("Num records: ",f"{num_rec}")
    print("Total rate: ", gbps, "Gigabytes/s")

    return gbps



def run_exp(num_workers: int, ens: int):
    """Run FDB retrieval in parallel in same node on the Slurm cluster."""

    logging.info(f"Running benchmark on {num_workers} workers")

    # n-sized chunks from list/
    def divide_chunks(l, n): 
        for i in range(0, len(l), n):  
            yield l[i:i + n] 
    
    steplist = list(divide_chunks(l=list(range(1,41)), n=8)) 

    paramlist = [
        "500014", # T
        "500001", # P
        "500028", # U
        "500030", # V
        "500032", # W
        "500035", # QV Specific Humidity
        "500100", # QC Cloud Mixing Ratio
        "500148", # PP 
        ]
    
    requests = [[steps,param] for steps in steplist for param in paramlist]

    lazy_results  = []

    for node in range(0, num_workers):
        steps, param = requests[node]
        lazy_result = dask.delayed(read_data)(ens=ens, steps=steps, param=param)
        lazy_results.append(lazy_result)

    results = dask.compute(*lazy_results)
    logging.debug(f"Results: {results}")

    all_results[str(num_workers)] = results



def plot():
    """Plot results from all scaling experiments."""

    data = [[len(worker),result] for worker in [list(i) for i in list(all_results.values())] for result in worker]

    sum_data = {len(j):sum(j) for j in [list(i) for i in list(all_results.values())]}
    sum_data_nodes = list(sum_data.keys())
    sum_data_bandwidth = list(sum_data.values())

    print(data)
    print(sum_data)

    nodes = [i[0] for i in data] # X axis data
    bandwidth = [i[1] for i in data] # Y axis data

    plt.style.use('seaborn-v0_8-whitegrid')
    fig = plt.figure()
    ax = plt.axes()

    ax.plot(np.asarray(nodes, dtype='int'), bandwidth, '_')
    ax.set_xlabel('Nodes')
    ax.set_ylabel('Gigabytes/s')

    ax.plot(np.asarray(sum_data_nodes, dtype='int'), sum_data_bandwidth, '-r')
    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)

    plt.savefig(Path(os.path.dirname(__file__)) / "dask_plot")


all_results = {}

def main(): 

    total_cores=40

    start_cluster()

    assert len([1]+list(range(2,total_cores+1,2))) <= 21 # total members
    
    for idx, i in enumerate([1]+list(range(2,total_cores+1,2))):

        scale_and_wait(i)

        # run_exp(num_workers=i, ens=idx)

        scale_and_wait(0)

    # Shut down the Dask cluster
    client.cluster.close()
    client.close()

    plot()


if __name__=="__main__": 
    main() 