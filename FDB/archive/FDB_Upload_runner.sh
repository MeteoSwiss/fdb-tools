#!/bin/bash

#SBATCH --job-name="fdb_upload"
#SBATCH --time=00:05:00
#SBATCH --partition=postproc 
#SBATCH --output=/scratch/e1000/meteoswiss/scratch/vcherkas/fdb-setup/mars/logs/%x.%j.o
#SBATCH --nodes=10
#SBATCH --ntasks-per-node=10



echo $(date)

eval "$(conda shell.bash hook)"

conda activate fdb

. $SCRATCH/spack-c2sm/setup-env.sh
spack env activate $SCRATCH/spack-env

export PATH=$PATH:$FDB5_DIR/bin


export CODING=mars

FDB_ROOT_PARENT=/opr/vcherkas/COSMO-1E/$CODING
FDB_ROOT=$FDB_ROOT_PARENT/fdb_root

echo Emptying FDB Root
rm -rfv $FDB_ROOT
mkdir -p $FDB_ROOT

SETUP_FOLDER=$SCRATCH/fdb-setup/$CODING
export LOG_FOLDER=$SETUP_FOLDER/logs
export RUN_LOG_FOLDER=$LOG_FOLDER/full-run

echo Emptying Log Folder
rm -rf $LOG_FOLDER
mkdir -p $RUN_LOG_FOLDER
rm -rf $RUN_LOG_FOLDER/*

export FDB5_CONFIG='{'type':'local','engine':'toc','schema':'$SETUP_FOLDER/fdb-schema','spaces':[{'handler':'Default','roots':[{'path':'$FDB_ROOT'}]}]}'
fdb-info --all

export DATA_DIR=/opr/vcherkas/COSMO-1E/23020103_409

for FOLDER in $DATA_DIR/*/; 
do
    echo "$FOLDER"
    for FILE in $FOLDER/*; 
    do
        export FILE_TO_PROCESS=$FILE
        sbatch FDB_Upload_single.sh 
    done
done

