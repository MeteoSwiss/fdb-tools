#!/bin/bash -l
#SBATCH --partition=postproc	
#SBATCH --time=0-00:30:00	# 30 minute time limit
#SBATCH --ntasks=1		# 1 tasks (i.e. processes)

echo $(date)
echo FDB5_CONFIG: $FDB5_CONFIG
fdb-info --all

export PATH=$PATH:`spack location -i fdb-fortran`

echo $GRIB_DEFINITION_PATH

echo "Files to process:" $@
python3 FDB_archive.py $@
