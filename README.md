# DistJET_0.3
## 1. 系统环境
- 物理环境(测试环境)
CPU型号：Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz\
物理核数：2   每个物理CPU的核数：12
- 软件环境
    - JUNO Offline software
    - Boost.Python
    - Python
        - psutil
    - MPICH3

## 2. 框架结构
这部分主要讲系统的主要架构，各模块部分功能及实现

### 目录结构
目录结构分为两部分，代码目录结构与运行时目录结构

代码目录的根目录在环境变量中，$DistJETPATH
- $DistJETPATH 目录
    - bin/ -- 命令集
        - setup -- 配置运行时环境
        - start -- 启动
        - jobls -- 监控任务
        - workerls -- 监控调度
    - config/ -- 默认配置目录
        - config.ini
    - python/ -- 源代码
    - Application/ -- 作业流目录(新增作业流需要在此添加)
        - ProdApp/ -- 数据产生处理
        - AnaApp/ -- 数据分析
        - UnitTestApp/ -- 单元测试
    - Backend/ -- 后端脚本目录
        - Backend.py
        - HTCONDOR/
        - LOCAL/
    - MPI_MODULE -- MPI通信层接口

运行时目录结构分为$Rundir和$DistJET。其中$Rundir主要存储产生的日志，$DistJET主要存储临时文件
- $Rundir(默认 **./Rundir_\***)
- $DistJET_TMP 目录 ( **$USER/.DistJET** )  
    - config.ini -- 临时配置文件，当系统启动后，将默认配置与用户配置结合生成的配置文件，用于系统读取
    - worker -- 调度情况，以**< worker status taskid lasttime>**四元组表示（临时）
    - ssh_auth/ ( 保存用户SSH免密登录--HTCondor后端使用 )
        - authority/ ( 认证相关文件 包括公钥、私钥、authorized_keys、ssh_config )
    - hostfile -- 启动MPI环境时所需的主机列表

### 系统架构
系统主要分为两部分，核心层：用于控制系统逻辑与执行作业；接口层：用于自由定义作业流和系统扩展
#### 核心层
包括MPI通信模块，Master模块和Slave模块
##### MPI通信模块
1. **MPI环境**\
需要在各个节点间实现相互的SSH免密登录，从而为MPICH的Hydra进程管理系统创建环境。且在启动前，需要在主节点（登录节点）使用`hydra_nameserver &`启动服务。
2. **MPI模块编写**\
MPI通信模块使用C++编写，使用Boost.python对类和接口进行封装。使用`mpic++`指令编译为`.o`文件，Python部分可以直接载入调用相应类和方法，`mpic++`=`g++ -I$PATH/mpi-install/include -L$PATH/mpi-install/lib -lmpicxx -Wl,-rpath -Wl,$PATH/mpi-install/lib -Wl,--enable-new-dtags -lmpi` 
3. **通信协议/标签**\
通信模块使用C/S架构，消息通过打标签的形式进行区别，主要设计了一下几个标签:

|tags|用途|
|---|---|
|MPI_PING|心跳通信，传递通信时间、节点状态、完成的任务等信息。|
|MPI_REGISTER|	节点注册，用于工作节点启动后主动连接管理节点。|
|MPI_REGISTER_ACK|	注册确认，用于管理节点与工作节点成功连接后进行确认。|
|APP_INI|	作业流初始化，管理节点向工作节点传递作业流初始化操作。|
|APP_INI_ACK|	作业流初始化确认，工作节点向管理节点反馈初始化操作结果。|
|TASK_ADD|	添加任务，用于管理节点向工作节点派发任务。|
|TASK_REMOVE|	删除任务，用于管理节点从工作节点撤回任务。|
|MPI_HALT|	挂起工作节点，当无任务可被执行时使工作节点闲置|
|APP_FIN|	作业流清理，所有任务执行结束后，管理节点向工作节点传递的清理操作。|
|LOGOUT|	工作节点登出。|
|LOGOUT_ACK|	管理节点删除工作节点信息，反馈登出确认。|
|MPI_DISCONNECT|	工作节点与管理节点断开。|
|EXTRA|	用于扩展或提供其他信息。|

4. **通信子**\
系统中，数据交互只发生在主节点（登录节点）与从节点（计算节点）之间，从节点间无通信。因此主节点与每一个从节点构成一个通信子，相互独立。
5. **容错性**\
MPI不具有很强的容错性，即当多个节点通信时，某一个节点因系统或其他原因崩溃，则会导致整个MPI环境崩溃退出。

##### Master模块
- ApplicationMgr：管理并加载作业流
- Scheduler：决定任务与工作节点的映射关系
- WorkerRegistry：管理工作节点的相关信息
- WatchDog线程：每隔一段时间扫描`WorkerRegistry`，对异常的工作节点进行处理。

##### Worker模块
- Agent:负责与Master通信，并处理信息
    - HeartBeat：固定时间间隔向Master发送心跳包，包括运行状态、正在执行的任务、已完成的任务、资源利用率等。
- Worker:负责启动、监管并结束`Process`
- Process:负责执行任务，使用`subprocess`库创建子进程，并通过`PIPE`与子进程通信。`Process`向子进程传递任务指令，子进程向`Process`传递任务执行的日志信息

#### 接口层
接口层主要包括四种接口：用户接口、作业流接口、调度接口、后端接口。其中用户接口与作业流接口详细介绍见应用部分
- 用户接口\
包含四个指令，setup（初始化环境），start（启动系统），jobls（列出作业执行情况），workerls（列出作业调度情况），具体见**3.使用方法**中
- 作业流接口\
系统允许开发者通过**IApplication接口**和**Task类** 来构建不同的作业流，详见**3.使用方法**
- 调度接口\
调度算法主要设计为实现不同的调度策略，需要通过重写SimpleTaskScheduler类的selectTask()方法实现。默认策略为分配task_todo_queue中第一个任务。
- 后端接口\
后端调度接口依赖于**Backend类**与**script脚本**。具体后端操作编写名为**script的脚本**，Backend类则依据实现接口的类名来加载对应的后端类。
    - Backend类使用动态加载，假设后端名为HTCondor，则加载Backend目录下的HTCONDOR/script.py文件，调用apply()和release()方法
    - 具体后端实现需要编写名为**script的脚本**，并实现apply(num)和release()方法
        - apply(num)：申请资源，num是申请数量，返回值为字典**{节点域名:可分配个数}**
        - release()：释放资源，返回值True/False

### 系统执行流程
![system workflow](https://github.com/techzbq/DistJET_v0.3/blob/dev/images/system%20workflow.png)
1. 申请资源流程（针对HTCondor）

![resource apply](https://github.com/techzbq/DistJET_v0.3/blob/dev/images/resource%20apply.png)

MPICH的HYDRA进程管理系统运行于SSH，需要配置SSH免密登录
- 修改过系统默认ssh配置文件(~/.ssh/config) --> 对使用ssh有影响
    1. 端口改为2222
    2. identity文件改为 ~/.DistJET/ssh-auth/authority/*key
- 在Backend/HTCONDOR 中使用 run_ssh.sh启动ssh

2. 任务流动
在系统中，任务分配采取"拉"的模式，即从节点向主节点申请任务。主要流程如下

![task assign](https://github.com/techzbq/DistJET_v0.3/blob/dev/images/task%20assign%20control.jpg)

## 3.使用方法
### 指令集&参数
- setup —— 配置系统运行环境
    - $JUNOTOP -- JUNO离线软件主目录
    - $DistJETPATH -- 系统主目录 ( /junofs/users/zhaobq/DistJET_v0.3 )
    - $DistJET_TMP -- 系统运行时临时目录( $HOME/.DistJET )
    - MPICH依赖库路径

- start —— 启动系统
```bash
-h, --help                  //show this help message and exit
-c, --conf CONFIG_PATH      //configure file path
--server SERVER_HOST        //where to start name service
-m, --app APP_MODULE        //the app you want to run
--app-conf APP_CONFIG       //the configure file of Application
-n, --worker-num WORKER_NUM //the number of worker you want to start
-d                          //debug mode
-s, --screen                //print log output into screen
--backend {condor,local}    //Backend choice

```
- master.py（内置指令，不对用户开放，可供测试使用）
```bash
argv[1] = appfile, 
argv[2] = config, 
argv[3]=log_level, 
argv[4] = app_config_file, 
argv[5] = log_screen, 
argv[6] = Rundir

```
- worker.py（内置指令，不对用户开放，可供测试使用）
```bash
argv[1]=capacity, 
argv[2]=conf_file, 
argv[3]=log_level, 
argv[4]=log_console, 
argv[5]=rundir

```

- jobls —— 查询任务执行情况\
屏幕显示样式：
```bash
-----task status-----
running: %d tasks
success: %d tasks
error: %d tasks
total: %d tasks
```
- workerls —— 查询任务调度情况\
读取 **$HOME/.DistJET/worker** 文件，并在屏幕中以四元组的方式显示:
```bash
#工作节点号  节点状态    运行任务号     最近一次心跳通信时间
wid         status      running     lasttime
1           FINALIZED   None        11:39:12

#节点状态与对应的状态码如下
NEW                 0
INITIALIZED         1
INITIALIZING        2
INITIALIZE_FAIL     3
SCHEDULED           4
RUNNING             5
ERROR               6
LOST                7
RECONNECT           8
FINALIZED           9
FINALIZING          10
FINALIZE_FAIL       11
IDLE                12


```

### 用户启动脚本
用户启动脚本需要作为参数输入，系统调用脚本中的 **run()** 方法来创建和载入作业流。以数据产生处理作业流（ProdApp）为例:
```python
#引入所需的调度器
from python.IScheduler import SimpleTaskScheduler
import ProdApp

#定义run方法，作业流配置文件路径作为参数传入
def run(app_config_path):
    #定义产生作业流对象
    app = ProdApp.ProdApp("/junofs/users/zhaobq/DistJET_v0.3/Application/ProdApp/","ProdApp",config_path=app_config_path)
    #设置作业流执行结果存放目录
    app.set_resdir("/junofs/users/zhaobq/DistJET_v0.3/ProdAppTest/res")
    #设置调度器
    app.set_scheduler(SimpleTaskScheduler)
    #以列表的形式返回
    return [app]
```
IApplication接口主要API
```python
__init__(rootdir, name, config_path=None) # 创建作业流对象
set_resdir(resdir) #设置作业流执行结果存放目录
set_boot(exe_list) #设置作业流执行指令, 一般由开发者定义
set_input_path(input_path) # 设置输入数据路径
set_rootdir(rootdir) #设置作业流根目录，用于获取作业流目录下的默认设定
set_scheduler(scheduler) #设置调度器
```

### 定义/开发作业流
此部分主要依赖于`IApplication`接口、`Task`类以及`ChainTask`类。具体的作业流需要派生自IApplication，作业流对象中包括：执行的指令(boot)，执行参数(args)，输入数据(data(可选))，结果存放路径(res_dir)等。同时需要实现必要的split()、merge()、setup()、uninstall()方法。
- split()\
产生Task对象列表，并将task列表作为返回值
- merge()\
合并所有Task对象的执行结果
- setup()\
为作业流执行建立相关环境，返回True/False
- uninstall()\
清理作业流的执行环境/释放内存等操作,返回True/False

作业流被加载后，系统会依据boot、args、data、res_dir和scheduler判断作业流是否可执行，之后才会对作业流进行拆分。

作业流需要依据一定规则拆分为`Task`对象，其中包括这个任务对象执行的**完整指令**以及**结果存放路径**。`ChainTask`派生自`Task`，包涵了`father_list`和`child_list`，用以描述该任务对象的前置后置关系。开发者也可以通过派生`ChainTask`和`Task`添加更多的任务属性。
