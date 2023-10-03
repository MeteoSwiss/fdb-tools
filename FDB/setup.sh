#!/bin/bash


export FDB5_DIR=`spack location -i fdb`
if [[ $FDB5_DIR == *"matches no installed packages"* ]]; then
  echo "FDB is not installed."
  return
fi

export PATH=$PATH:$FDB5_DIR/bin

FDB_ROOT=$SCRATCH/fdb-root

if [ ! -d "$FDB_ROOT" ]; then
  mkdir -p $FDB_ROOT
fi

SETUP_FOLDER=$SCRATCH/fdb-setup

if [ ! -d "$SETUP_FOLDER" ]; then
  mkdir -p $SETUP_FOLDER
fi

# Default schema
wget https://raw.githubusercontent.com/ecmwf/fdb/master/tests/fdb/etc/fdb/schema --output-document=$SETUP_FOLDER/fdb-schema

export ECCODES_PATH=`spack location -i eccodes`
if [[ $ECCODES_PATH == *"matches no installed packages"* ]]; then
  echo "eccodes is not installed."
  return
fi

export COSMO_DEFINITIONS_PATH=$SCRATCH/eccodes-cosmo-resources

if [ ! -d "$SCRATCH/eccodes-cosmo-resources" ]; then
  git clone git@github.com:cosunae/eccodes-cosmo-resources.git $COSMO_DEFINITIONS_PATH
fi  

export GRIB_DEFINITION_PATH=$COSMO_DEFINITIONS_PATH/definitions:$ECCODES_PATH/share/eccodes/definitions/

export FDB5_CONFIG='{'type':'local','engine':'toc','schema':'$SETUP_FOLDER/fdb-schema','spaces':[{'handler':'Default','roots':[{'path':'$FDB_ROOT'}]}]}'
fdb-info --all
echo FDB5_CONFIG: $FDB5_CONFIG

