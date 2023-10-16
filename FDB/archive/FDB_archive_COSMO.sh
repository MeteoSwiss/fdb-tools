#!/bin/bash

# This script is an example script to demonstrate how you could archive a whole model run of COSMO to FDB.
#
# Various paths in this script might need to change for your setup:
#
# FDB_ROOT is the directory which the FDB will use to archive data to. You should adapt to a directory to which you have write access.
# eccodes-cosmo-resources is saved at $COSMO_DEFINITIONS_PATH. You may need to change the branch/fork depending on your needs.
# It is expected that there is a fdb schema file within $SETUP_FOLDER. You need to provide the schema. There is one you could use at FDB/mars/fdb_schema
# Your spack may not be saved in this location, and your spack environment may have another name.

echo $(date)

. $SCRATCH/spack-c2sm/setup-env.sh
spack env activate $SCRATCH/spack-env

export FDB5_DIR=`spack location -i fdb`
if [ -z "$FDB5_DIR" ]; then
  echo "FDB is not installed. Load your spack environment containing an FDB installation."
  return
fi

export PATH=$PATH:$FDB5_DIR/bin

FDB_ROOT=$SCRATCH/fdb_root

while true; do
    read -p "Delete FDB Root at: $FDB_ROOT? [y/N]" yn
    case $yn in
        [Yy]* ) delete=1; break;;
        [Nn]* ) delete=0; break;;
        * ) echo "Please answer yes or no.";;
    esac
done

if [ "$delete" -eq "1" ]; then
    echo Emptying FDB Root
    rm -rfv $FDB_ROOT
fi
mkdir -p $FDB_ROOT

SETUP_FOLDER=$SCRATCH/fdb-setup
export LOG_FOLDER=$SETUP_FOLDER/logs
export RUN_LOG_FOLDER=$LOG_FOLDER/full-run

while true; do
    read -p "Delete Logs at: $LOG_FOLDER? [Y/N]" yn
    case $yn in
        [Yy]* ) deletelogs=1; break;;
        [Nn]* ) deletelogs=0; break;;
        * ) echo "Please answer yes or no.";;
    esac
done

if [ "$deletelogs" -eq "1" ]; then
    echo Emptying Log Folder
    rm -rf $LOG_FOLDER
    mkdir -p $RUN_LOG_FOLDER
fi

export ECCODES_PATH=`spack location -i eccodes@2:25`
if [ -z "$ECCODES_PATH" ]; then
  echo "eccodes 2:25 is not installed. Load your spack environment containing an eccodes installation."
  return
fi

export COSMO_DEFINITIONS_PATH=$SETUP_FOLDER/eccodes-cosmo-resources

if [ ! -d "$COSMO_DEFINITIONS_PATH" ]; then
  git clone git@github.com:cosunae/eccodes-cosmo-resources.git -b revise_mars_model $COSMO_DEFINITIONS_PATH
fi

export GRIB_DEFINITION_PATH=$COSMO_DEFINITIONS_PATH/definitions:$ECCODES_PATH/share/eccodes/definitions/

export FDB5_CONFIG='{'type':'local','engine':'toc','schema':'$SETUP_FOLDER/fdb-schema','spaces':[{'handler':'Default','roots':[{'path':'$FDB_ROOT'}]}]}'
fdb-info --all

# Directory containing COSMO-1E full run of GRIB data.
export DATA_DIR=/opr/vcherkas/COSMO-1E/23020103_409
archive=0

while true; do
    read -p "Archive to FDB from: $DATA_DIR (all members)? [Y/N]" yn
    case $yn in
        [Yy]* ) archive=1; break;;
        [Nn]* ) archive=0; break;;
        * ) echo "Please answer yes or no.";;
    esac
done

if [ "$archive" -eq "1" ]; then
    for FOLDER in $DATA_DIR/*/; 
    do
        echo "$FOLDER"
        for FILE in $FOLDER/*; 
        do
            filename=`basename $FILE`
            if [[ $filename = laf* ]]
            then
                echo Skipping $FILE
            else
                export FILE_TO_PROCESS=$FILE
                sbatch ./FDB_archive.sh 
            fi
        done
    done
fi

if [ "$archive" -eq "0" ]; then
    test=0

    while true; do
        read -p "Archive to FDB from: $DATA_DIR/001 (single member)? [Y/N]" yn
        case $yn in
            [Yy]* ) test=1; break;;
            [Nn]* ) test=0; break;;
            * ) echo "Please answer yes or no.";;
        esac
    done

    if [ "$test" -eq "1" ]; then
        for FILE in  $DATA_DIR/001/*; 
        do
            filename=`basename $FILE`
            if [[ $filename = laf* ]]
            then
                echo Skipping $FILE
            else
                export FILE_TO_PROCESS=$FILE
                sbatch ./FDB_archive.sh --output $LOG_FOLDER/%x_%j.out
            fi
        done
    fi
fi
