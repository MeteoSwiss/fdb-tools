import xarray as xr
import time


start = time.time()
zarr = xr.open_dataset("/scratch/mch/cosuna/mars/COSMO-1E/nc/1h/ml_sl/000/lfff.zarr")
end = time.time()
print("sel time: ", end-start)
params = ["T", "P", "U","V","W","QV"]

start = time.time()
for param  in params:
    zarr[param].load()

end = time.time()
print("time:", end-start)