# fdb-tools
Various tools to assist with FDB/MARS setup.

## FDB Installation

Before running the FDB scripts you should have FDB installed via spack.

```
git clone --depth 1 --recurse-submodules --shallow-submodules -b v0.20.1.0 https://github.com/C2SM/spack-c2sm.git
```
```
. spack-c2sm/setup-env.sh
```
```
mkdir spack-env
```
```
cat > spack-env/spack.yaml << EOF
# This is a Spack Environment file.
#
# It describes a set of packages to be installed, along with
# configuration settings.
spack:
  # add package specs to the `specs` list
  specs: [fdb ^eckit@1.20.2 ~mpi ^eccodes@2.25 jp2k=none +fortran ^hdf5 ~mpi] 
  view: false
  concretizer:
    unify: true
  config:
    install_tree:
      root: $SCRATCH/spack-root
EOF
```
```
spack env activate -p spack-env
```
```
spack install
```
To make sure you have FDB installed, make sure a path is returned by
```
spack location -i fdb
```

## pyFDB Installation

To run the notebook to read from FDB with python, you also need to install pyFDB from local a clone (not yet available on PyPI) and earthkit-data. See below steps.

```
conda create -n fdb
conda activate fdb
python -m pip install --upgrade git+https://github.com/ecmwf/pyfdb.git@master
pip install earthkit-data
```

## Required Environment Variables

The setup.sh script sets up the following variables, for a new FDB on your $SCRATCH.

- `FDB_HOME` needs to be set (for pyfdb). Identical to `FDB5_HOME`. Find with `spack location -i fdb`.
    
- `FDB5_HOME` needs to be set (for earthkit.data). Identical to `FDB_HOME`. Find with `spack location -i fdb`.

- Either `FDB5_CONFIG` or `FDB5_CONFIG_FILE` needs to be set (for FDB). `FDB5_CONFIG` should have json version of config, whereas `FDB5_CONFIG_FILE` should be set to a file path where the config file (YAML) lives. eg 
    ```
    export FDB5_CONFIG='{'type':'local','engine':'toc','schema':'$SETUP_FOLDER/fdb-schema','spaces':[{'handler':'Default','roots':[{'path':'$FDB_ROOT'}]}]}'
    ```

- `ECCODES_DEFINITION_PATH` needs to be set for reading COSMO data. Use the revise_mars_model branch of eccodes_cosmo_definitions.

