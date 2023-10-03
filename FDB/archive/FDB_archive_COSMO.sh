#!/bin/bash

echo $(date)

. $SCRATCH/spack-c2sm/setup-env.sh
spack env activate $SCRATCH/spack-env
export FDB5_DIR=`spack location -i fdb`

export PATH=$PATH:$FDB5_DIR/bin

export CODING=mars

FDB_ROOT_PARENT=/opr/vcherkas/COSMO-1E/$CODING
FDB_ROOT=$FDB_ROOT_PARENT/fdb_root

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
    mkdir -p $FDB_ROOT
fi

SETUP_FOLDER=$SCRATCH/fdb-setup/$CODING
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
    rm -rf $RUN_LOG_FOLDER/*
fi

export ECCODES_PATH=`spack location -i eccodes`
export COSMO_DEFINITIONS_PATH=$SCRATCH/eccodes-cosmo-resources

if [ ! -d "$SCRATCH/eccodes-cosmo-resources" ]; then
  git clone git@github.com:cosunae/eccodes-cosmo-resources.git $COSMO_DEFINITIONS_PATH
fi  

export GRIB_DEFINITION_PATH=$COSMO_DEFINITIONS_PATH/definitions:$ECCODES_PATH/share/eccodes/definitions/

export FDB5_CONFIG='{'type':'local','engine':'toc','schema':'$SETUP_FOLDER/fdb-schema','spaces':[{'handler':'Default','roots':[{'path':'$FDB_ROOT'}]}]}'
fdb-info --all


archive=0

while true; do
    read -p "Archive to FDB from: $DATA_DIR? [Y/N]" yn
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
        read -p "Test archive to FDB from: $DATA_DIR/001? [Y/N]" yn
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
                sbatch ./FDB_archive.sh 
            fi
        done
    fi
fi
