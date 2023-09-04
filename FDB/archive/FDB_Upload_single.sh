#!/bin/bash -l
#SBATCH --partition=postproc	
#SBATCH --output=/scratch/e1000/meteoswiss/scratch/vcherkas/fdb-setup/mars/logs/%x_%j.out	# Output file
#SBATCH --time=0-00:30:00	# 30 minute time limit
#SBATCH --ntasks=1		# 1 tasks (i.e. processes)

echo $(date)
echo $FDB5_CONFIG
fdb-info --all

export PATH=$PATH:`$SPACK_ROOT/bin/spack location -i fdb-fortran`
export PATH=$PATH:`$SPACK_ROOT/bin/spack location -i fdb`

export GRIB_DEFINITION_PATH=/scratch/e1000/meteoswiss/scratch/vcherkas/miniconda3/envs/fdb/share/eccodes/definitions/:/scratch/e1000/meteoswiss/scratch/vcherkas/eccodes-cosmo-resources/definitions
echo $GRIB_DEFINITION_PATH

echo "File:" $FILE_TO_PROCESS
python3 FDB_Upload_run_single.py $FILE_TO_PROCESS