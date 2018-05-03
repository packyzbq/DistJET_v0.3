#!/bin/bash

mpic++ -std=c++11 -I/junofs/users/zhaobq/mpi-install/include -L/junofs/users/zhaobq/mpi-install/lib -I/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Release/J16v1r4/ExternalLibs/Boost/1.55.0/include -L/afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Release/J16v1r4/ExternalLibs/Boost/1.55.0/lib -I/usr/include/python2.6 -L/usr/lib64/python2.6 -lboost_python --shared -fPIC $1 -o $2
#-I/usr/include/mpich2-x86_64
