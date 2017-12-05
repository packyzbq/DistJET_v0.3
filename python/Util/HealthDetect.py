import time

def getCpuInfo():
    info = {}
    with open('/proc/stat') as f:
        for line in f:
            if line.split()[0].startswith('cpu'):
                total = 0
                d = line.split()
                for i in xrange(1, len(d)):
                    total += long(d[i])
                idle = d[4]
                info[d[0]] = {'total': total, 'idle': idle}
    return info


def getCpuUsage(cpuid=None):
    """
    get cpu usage: 100*(total-idle)/total
    :return: {cpuk:usage}
    """
    info1 = getCpuInfo()
    time.sleep(0.01)
    info2 = getCpuInfo()
    tmpdict = {}
    for k in info1.keys():
        idle = float(info2[k]['idle'])-float(info1[k]['idle'])
        total = float(info2[k]['total'])-float(info1[k]['total'])
        if total == 0.0:
            usage = 0.0
        else:
            usage = 100-(idle*100/total)
        #usage = 100-(int(info2[k]['idle'])-int(info1[k]['idle'])*100)/(long(info2[k]['total'])-long(info1[k]['total']))
        tmpdict[k] = usage
    if cpuid and tmpdict.has_key('cpu'+cpuid):
        return {'cpu'+cpuid:tmpdict['cpu'+cpuid]}
    return tmpdict

def getMemoUsage():
    meminfo = {}
    with open('/proc/meminfo') as f:
        for line in f:
            meminfo[line.split(':')[0]] = line.split(':')[1].strip()[:-2]
    if not 'MemFree' in meminfo.keys() or not 'MemTotal' in meminfo.keys():
        return {'MemUsage': 0}
    else:
        return {'MemUsage':round(float(meminfo['MemFree'])/float(meminfo['MemTotal']),2)*100, 'MemFree':meminfo['MemFree'], 'MemTotal':meminfo['MemTotal']}
