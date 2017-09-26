from Process import Process_withENV
import threading
import time

comm = ['echo $HOME\n', 'python ./wait.py\n']

def hook():
    print 'hook method called'

def add(proc):
    for com in comm:
        proc.set_exe(com)
        print '@thread add comm %s'%com
        time.sleep(2)

    proc.set_exe(['echo "hello"\n','echo "world"\n'])
    time.sleep(1)
    proc.set_exe('exit')

setup = 'source /afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Pre-Release/J17v1r1-Pre2/setup.sh'
with open('output.txt','w+') as output:
    proc = Process_withENV(setup,output,timeout=5,hook=hook)
    print "@proc setup recode=%d"%proc.initialize()
    thread = threading.Thread(target=add,args=proc)
    proc.run()

print '@proc finished'
