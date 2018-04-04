#!/bin/bash
topdir="$DistJETPATH/Backend/HTCONDOR"
confdir="$HOME/.DistJET"
if [ ! -d $confdir ]; then
	mkdir $confdir
fi
#topdir=/junofs/users/zhaobq/DistJET_Test
#dir="$topdir/$1"
#if [ ! -d $dir ]; then
#	mkdir "$dir"
#fi
#cd /junofs/users/zhaobq/mpi_HTCondor/res
dir="$confdir/ssh-auth"

if [ ! -d $dir ]; then
    mkdir $dir
fi

cd $dir
if [ ! -f "ip.txt" ]; then
	touch ip.txt
fi
host=`hostname`
#comm="condor_status -l $host"
#echo $comm
#ip=`$comm`
echo $host >> ip.txt
auth="$dir/authority"
if [ ! -d $auth ]; then
	mkdir "$auth"
fi
cd "$auth"
while [ ! -f "STOP" ]; do
    echo "--------------------"
    bash $topdir/start-sshd.sh
done
