#!/bin/bash

junopath="/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/"

if [ $# -le 1 ]
then
    junopath="$(junopath)/Pre-Release/J18v1r1-Pre1/"
else
    junopath=$(junopath)$1
# setup JUNO software
if [ -z $JUNOTOP ]
then
    source "$(junopath)/setup.sh"
fi

# setup mpi env

export PATH=/junofs/users/zhaobq/mpich-install/bin:$PATH

# setup python env
export PYTHONPATH=/junofs/users/zhaobq/pylib/lib/python2.7/site-packages:$PYTHONPATH

# setup DistJET env
export DistJETPATH="/junofs/users/zhaobq/DistJET_v0.3"
export DistJET_TMP="$(HOME)/.DistJET"