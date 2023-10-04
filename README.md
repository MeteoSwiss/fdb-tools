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
  specs: [fdb ^eckit@1.20.2 ~mpi ^eccodes@2.19.0 jp2k=none +fortran ^hdf5 ~mpi] 
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
