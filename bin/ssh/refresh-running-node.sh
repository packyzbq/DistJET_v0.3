#!/bin/bash
NODES_LIST="$1/hosts.txt"
if [ $# -gt 1 ]; then
    condor_q $1 -af RemoteHost | grep slot | cut -d '@' -f 2 > $NODES_LIST #| cut -d '.' -f 1 > $NODES_LIST
else
    condor_q $USER -af RemoteHost | grep slot | cut -d '@' -f 2 > $NODES_LIST #| cut -d '.' -f 1 > $NODES_LIST
fi

#sed -i 's/$/&:1/g' $NODES_LIST
python $DistJETPATH/bin/ssh/gen_host.py $1

