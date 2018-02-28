import os,sys
dir = sys.argv[1]
num = 0
h_r = open(dir+'/hosts.txt','r')
readline=h_r.readlines()
h_r.close()
hosts = open(dir+'/hosts.txt','w+')
host_dict={}
for line in readline:
    num+=1
    if host_dict.has_key(line):
        host_dict[line]+=1
    else:
        host_dict[line] = 1
hosts.seek(0)
for k, v in host_dict.items():
    hosts.write('%s:%s\n'%(k[:-1],str(v)))
hosts.flush()
hosts.close()
print num
