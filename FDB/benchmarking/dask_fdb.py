import dask
import logging
import os
import json
import time
import gc
from typing import Any, TypeVar
from pathlib import Path
from dataclasses import dataclass
from datetime import timedelta

import earthkit.data
import numpy as np
import matplotlib.pyplot as plt
from distributed import Client
from dask_jobqueue import SLURMCluster  # type: ignore
from distributed.deploy.cluster import Cluster

logging.basicConfig(encoding='utf-8', filename='dask_fdb.log', level=logging.DEBUG)

node_name_env_var = "SLURMD_NODENAME"

debug = False
"""Debug mode toggle"""


T = TypeVar("T")

def arg2list(x: T | list[T]) -> list[T]:
    """Return a single-element list if x is not a list. Otherwise return x."""
    if not isinstance(x, list):
        x = [x]
    return x


class WorkerEnvironment:
    """
    This class manages environments for dask workers.
    """

    omp_threads: int = 1
    """The number of threads available to openmp"""

    env_var_map: dict[str, str | list[str]] = {"omp_threads": ["OMP_NUM_THREADS", "OMP_THREAD_LIMIT"]}
    """Maps attributes of this class to environment variables"""

    @classmethod
    def load(cls) -> "WorkerEnvironment":
        """Returns a new worker environment whose attributes are initialized according to the current environment."""
        out = cls()
        for k, v in out.env_var_map.items():
            if isinstance(v, list):
                v = v[0]
            out.__dict__[k] = out.__dict__[k].__class__(os.environ[v])
        return out

    @property
    def _env_vars(self) -> dict[str, str]:
        """This worker environment as a dictionary mapping environment variable names to values"""
        return {e: str(self.__dict__[v]) for v, es in self.env_var_map.items() for e in arg2list(es)}

    def set(self) -> None:
        """Sets environment variables according to this worker environment"""
        os.environ.update(self._env_vars)

    def get_job_script_prologue(self) -> list[str]:
        """Returns a list of bash commands setting up the environment of a worker."""
        return [f"export {e}={v}" for e, v in self._env_vars.items()]




def dask_config_get_not_none(key: str, default: Any) -> Any:
    """
    Returns the value of `dask.config.get(key, default)` and returns the default if `None` would be returned.

    Group:
        Util
    """
    out = dask.config.get(key, default)
    if out is None:
        return default
    else:
        return out


@dataclass
class ClusterConfig:
    """
    A configuration class for configuring a dask SLURM cluster.

    Group:
        Dask
    """

    workers_per_job: int = 1
    """The number of workers to spawn per SLURM job"""
    cores_per_worker: int = dask_config_get_not_none("jobqueue.slurm.cores", 1)
    """The number of cores available per worker"""
    omp_parallelism: bool = False
    """
    Toggle whether the cores of the worker should be reserved to the implementation of the task.
    If true, a worker thinks it has only one one thread available and won't run tasks in parallel.
    Instead, zebra is configured with the given number of threads.
    """
    exclusive_jobs: bool = False
    """Toggle whether to use a full node exclusively for one job."""
    queuing: bool = False
    """If True, queuing will be used by dask. If False, it will be disabled."""


client: Client | None = None
"""
The currently active dask client.

Group:
    Dask
"""
_active_config: ClusterConfig | None = None



def parse_slurm_time(t: str) -> timedelta:
    """
    Returns a timedelta from the given duration as is being passed to SLURM

    Args:
        t: The time in SLURM format

    Returns:
        A timedelta object representing the passed SLURM time.

    Group:
        Util
    """
    has_days = "-" in t
    d = "0"
    if has_days:
        d, t = t.split("-")
        tl = t.split(":")
        h, m, s = tl + ["0"] * (3 - len(tl))
    else:
        tl = t.split(":")
        if len(tl) == 1:
            tl = ["0", *t, "0"]
        elif len(tl) == 2:
            tl = ["0", *tl]
        h, m, s = tl
    return timedelta(days=int(d), hours=int(h), minutes=int(m), seconds=int(s))



def start_slurm_cluster(cfg: ClusterConfig = ClusterConfig()) -> Client:
    """
    Starts a new SLURM cluster with the given config and returns a client for it.
    If a cluster is already running with a different config, it is shut down.

    Args:
        cfg: The configuration of the cluster to start

    Returns:
        A client connected to the newly started SLURM cluster.

    Group:
        Dask
    """
    global client, _active_config

    if cfg == _active_config:
        assert client is not None
        return client

    if client is not None:
        cluster = client.cluster
        client.close()
        assert isinstance(cluster, SLURMCluster)
        cluster.close()
        print("Closed SLURM cluster")

    worker_env = WorkerEnvironment()

    walltime = dask_config_get_not_none("jobqueue.slurm.walltime", "01:00:00")
    node_cores = dask_config_get_not_none("jobqueue.slurm.cores", 1)
    node_memory: str = dask_config_get_not_none("jobqueue.slurm.memory", "100GB")
    node_memory_bytes = dask.utils.parse_bytes(node_memory)

    job_cpu = cfg.cores_per_worker * cfg.workers_per_job # cores per job
    jobs_per_node = node_cores // job_cpu
    job_mem = dask.utils.format_bytes(node_memory_bytes // jobs_per_node)

    cores = (
        job_cpu if not cfg.omp_parallelism else cfg.workers_per_job
    )  # the number of cores dask believes it has available per job
    worker_env.omp_threads = 1 if not cfg.omp_parallelism else cfg.cores_per_worker

    walltime_delta = parse_slurm_time(walltime)
    worker_lifetime_td = walltime_delta - timedelta(minutes=3)
    worker_lifetime = int(worker_lifetime_td.total_seconds())

    dashboard_address = ":8877"

    if cfg.queuing:
        dask.config.set({"distributed.scheduler.worker-saturation": 1.0})
    else:
        dask.config.set({"distributed.scheduler.worker-saturation": "inf"})

    cluster = SLURMCluster(
        # resources
        walltime=walltime,
        cores=cores,
        memory=job_mem,
        processes=cfg.workers_per_job,
        job_cpu=job_cpu,
        job_extra_directives=["--exclusive"] if cfg.exclusive_jobs else [],
        # scheduler / worker options
        scheduler_options={
            "dashboard_address": dashboard_address,
        },
        worker_extra_args=["--lifetime", f"{worker_lifetime}s", "--lifetime-stagger", "2m", "--lifetime-restart"],
        # filesystem config
        local_directory='/scratch/mch/vcherkas/dask',
        # shared_temp_directory=,
        log_directory='/scratch/mch/vcherkas/dask/logs',
        # other
        job_script_prologue=worker_env.get_job_script_prologue(),
        nanny=True,
        queue='postproc'
    )
    client = Client(cluster)
    _active_config = cfg
    logging.info(f"Started new SLURM cluster. Dashboard available at {cluster.dashboard_link}")
    if node_name_env_var in os.environ:
        nodename = os.environ[node_name_env_var]
    else:
        nodename = "local"
    logging.info(f"Current node name: {nodename}")
    logging.debug(cluster.job_script())
    return client


def start_scheduler(debug: bool = debug, *cluster_args: Any, **cluster_kwargs: Any) -> Client | None:
    """
    Starts a new scheduler either in debug or run mode.

    Args:
        debug (bool):
            If `False`, a new SLURM cluster will be started and a client connected to the new cluster is returned.
            If `True`, `None` is returned and dask is configured to run a synchronous scheduler.
        cluster_args: The positional arguments passed to :py:func:`start_slurm_cluster`
        cluster_kwargs: The keyword arguments passed to :py:func:`start_slurm_cluster`

    Returns:
        A client connected to the new cluster / scheduler or `None`, depending on `debug`.

    Group:
        Dask
    """
    if debug:
        dask.config.set(scheduler="synchronous")
        return None
    else:
        return start_slurm_cluster(*cluster_args, **cluster_kwargs)

def scale_and_wait(n: int) -> None:
    """
    Scales the current registered cluster to `n` workers and waits for them to start up.

    Group:
        Dask
    """
    logging.info(f"Scaling Dask workers to {n}")
    if client:
        assert isinstance(client.cluster, Cluster)
        client.cluster.scale(n)
        client.wait_for_workers(n)

# Function that reads data from FDB
def read_data(eps: int):
    os.environ['FDB_HOME'] = '/scratch/mch/vcherkas/vcherkas/spack-root/linux-sles15-zen3/gcc-11.3.0/fdb-5.11.17-4hcp6n5lien4rzi4tqu2roa4zvsrfeur'
    os.environ['FDB5_HOME'] = os.environ['FDB_HOME']
    os.environ['FDB5_CONFIG'] = "{'type':'local','engine':'toc','schema':'/opr/vcherkas/fdb-schema-ordered','spaces':[{'handler':'Default','roots':[{'path':'/opr/vcherkas/fdb_root_ordered'}]}]}"
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
    # paramlist_all = [["500014"]]

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
            # "step":["0"],
            "step":list(range(10)),
            "number":[f"{eps}"],
            "levtype":"ml",
            "levelist": list(range(81)),
            #T,P,FI
            "param": param
            }

        start = time.time()
        logging.info('Sending FDB request.')
        ds = earthkit.data.from_source("fdb", request, batch_size=0)
        end = time.time()
        tot_time_sel += end-start

        field_map: dict[tuple[int, ...], np.ndarray] = {}

        start = time.time()
        for f in ds:
            # key = f.metadata(('number','step','levelist','param'))
            key = f.metadata(('number','param'))
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

        del field_map

    print("Request:", json.dumps(request, indent=2))
    print("Sel time:", f"{tot_time_sel}")
    print("Load time:", f"{tot_time}")
    print("Num records: ",f"{num_rec}")
    print("Total rate: ", gbps, "Gigabytes/s")

    return gbps


def run_exp(num_workers: int, eps: int):

    logging.info(f"Running benchmark on {num_workers} workers")
    lazy_results  = []

    for node in range(0, num_workers):
        lazy_result = dask.delayed(read_data)(eps)
        lazy_results.append(lazy_result)

    results = dask.compute(*lazy_results)

    all_results[str(num_workers)] = results
    logging.info(f"Results: {results}")

all_results = {}

def main(): 

    start_scheduler(debug)

    total_eps=11

    for i in range(1,total_eps+1):

        scale_and_wait(i)

        run_exp(num_workers=len(client.cluster.workers), eps=i-1)

        gc.collect()

    # Shut down the Dask cluster
    client.cluster.close()
    client.close()

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




if __name__=="__main__": 
    main() 