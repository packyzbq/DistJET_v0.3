import os

tmp_dir = os.environ['HOME']+'/.DistJET_tmp'
if not os.path.exists(tmp_dir):
    print "No DistJET App found, exit"
    exit()

with open(os.environ['HOME']+'/.diane_tmp/worker','r') as worker:
    lines = worker.readlines()
for line in lines:
    print line