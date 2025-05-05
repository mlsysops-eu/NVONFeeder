import subprocess
import multiprocessing
import os
import argparse
import sys
import matplotlib.pyplot as plt
import time
import datetime
from collections import OrderedDict
import json
import shutil


def processStats(stats):
    """ Processes statistics regarding a certain permutation window.
        Args:
            stats: statistcs returned by the NVON Simulator. 

        Returns: statistics in a dict data structure.
    """
    totalSec = {}
    meanUtil = {}
    traceFile = {}

    for stat in stats:
        id = int(stat[0])
        totalSec[id]=int(stat[1][1])
        meanUtil[id]=float(stat[1][2])
        traceFile[id]=stat[1][0]
    meanUtil = OrderedDict(sorted(meanUtil.items()))
    totalSec = OrderedDict(sorted(totalSec.items()))
    tStats = {}
    tStats['meanUtil'] = meanUtil
    tStats['totalSec'] = totalSec
    tStats['traceFile'] = traceFile
    return tStats

def getBest(tStats: dict[dict]):
    """ Returns the best performing trace.
        Args:
            tStats: a dict containing mean utilization, total execution time, and trace-file names
                    of all permutations in a window.   

        Returns: 
            - best performing trace file name
            - the respective min execution time
            - the first trace that finished its execution in the minimum time
    """
    totalSec = tStats['totalSec']    
    mintotalSec = min(totalSec.values())
    for key, val in totalSec.items():
        if val == mintotalSec:
            minTimeRunsPnt = key
            break 
    return tStats['traceFile'][minTimeRunsPnt], mintotalSec, minTimeRunsPnt
    

def saveGraphs(graphStats: dict[dict],  pathBatchResults: str, policyName: str, bpoint: int, bTime: int):
    """ Creates graphs for mean utilization and total execution time. It accounts all permutations in a window.
        Args:
            graphStats: a dict containing mean utilization, total execution time, and trace-file names
                        of all permutations in a window. 
            pathBatchResults: results directory to save the graphs
            policyName: the name of the policy used to get next window (default is best)
            bpoint: the first trace that finished its execution in the minimum time
            bTime: the respective minimun execution time
    """
    # Plot the utilization graph per window run
    utilPlot = plt.figure(1)
    plt.plot(graphStats['meanUtil'].keys(),graphStats['meanUtil'].values())
    # Add labels
    plt.title('Mean utilization')
    plt.xlabel('Run')
    plt.ylabel('Utilization')
    # Save to png
    utilPlot.savefig(pathBatchResults + os.sep + "util_" + policyName + '.png')
    utilPlot.clear()

    # Plot the total run time graph per window run
    totalTimePlot = plt.figure(2)
    plt.plot(graphStats['totalSec'].keys(), graphStats['totalSec'].values())
    plt.plot(bpoint,bTime,"o")
    plt.annotate(bTime,(bpoint,bTime+10))
    # Add labels
    plt.title('Total run time')
    plt.xlabel('Run')
    plt.ylabel('Time')
    # Save to png
    totalTimePlot.savefig(pathBatchResults + os.sep + "totalSec_" + policyName + '.png')
    totalTimePlot.clear()


def spawnProcess(tracefile: str, initTrace: str, lck: any, stats: list, id: int, pathResults: str): 
    """ 
        It spawns an instance of NVONSim Simulator every time is executed.
        Args:
            tracefile: trace file will be used in the simulation
            initTrace: a trace file containing the best permuations of the previous windows
            lck: lock for protecting the shared statistics list
            stats: an empy list
            id: execution id
            pathResults: the directory NVSON simulator should use to pesrist the detailed simulation results
    """
    exec = []
    exec = [sys.executable, "main.py","-t",tracefile, "-f", pathResults]
    if len(initTrace) != 0: 
        exec.append("-i")
        exec.append(initTrace)    
    result = subprocess.run(exec, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True)
    print(result.stdout)
    print(result.stderr)
    with lck:
        stats.append((id,result.stdout.split()))

def mycall(_):
    "Call back function, executed once the worker thread finishes its execution. Left for future use."
    pass
    

def startSim(cores: int, runDir: str, traces: list[str], winInitTrace: str, pathResults: str):
    """" Creates a pool of worker processes. The processes execute the spawnProcess function for each trace file.
        Args:
            cores: the number of the assigned cpu cores
            runDir: corrent working directory
            traces: a list of the trace files in the current window
            wininitTrace: a trace file containing the best permuations of the previous windows
            pathResults: the directory NVSON simulator should use to pesrist the detailed simulation results
    """
    pool = multiprocessing.Pool(cores)
    mgr = multiprocessing.Manager()
    lck = mgr.Lock()
    stats = mgr.list()
    id = 0
    for trace in traces:
        time.sleep(0.5)
        tracefullname = os.path.join(runDir,trace)
        pool.apply_async(spawnProcess, (tracefullname, winInitTrace, lck, stats, id, pathResults),callback=mycall)
        id = id + 1    
    pool.close()
    pool.join()
    return stats
     
if __name__ == '__main__':

    aparser = argparse.ArgumentParser(prog="python nvonsim.py",description='starts execution of multiple instances of the NVON Simulator')
    aparser.add_argument("-t", "--traces", required=True, help="path to traces directory")
    aparser.add_argument("-n","--procs", type=int, default=4, help="number of running NVON simulator instances (default 4)")
    args = aparser.parse_args()

    policyName = "best"  

    # Create run-time directory structure
    # All run-time files are stored in the 'run' directory 
    runTime = datetime.datetime.now().strftime("%d%m%Y.%H%M%S")
    runDir = 'run' + os.sep + 'run_' + runTime + "_" + policyName
    os.mkdir(runDir)
    print("Creating directory: ",runDir)
    # Log files directory, contains results from every NVON Simulator run
    logs = runDir + os.sep + 'logs'    
    # Create directory for results (window)
    results = runDir + os.sep + 'results'
    os.mkdir(results)
    print("Creating directory: ", results)
    # Create directory for intermediate workloads (window history) 
    tmp = runDir + os.sep + 'tmp'
    os.mkdir(tmp)
    print("Creating directory: ", tmp)
    
    if not os.path.exists(args.traces):
        sys.exit("Traces list file does not exist. Execution aborted...")
    traces_dir = args.traces.split('\\').pop()
    tracesPath = runDir + os.sep + traces_dir
    print("Trace files will be stored in: ",tracesPath)
    print("Copying trace files...")
    shutil.copytree(args.traces, tracesPath)  
    
    # get workload definiton
    if os.path.exists(tracesPath + os.sep + 'workload.json'):
            with open(tracesPath + os.sep + 'workload.json') as json_file:
                workloadInfo = json.load(json_file)

    else:
        sys.exit("Workload definition file (workload.json) not found. Exitting ...")
    print(workloadInfo)
    stats = []

    winInitTracesFile = ""
    
    ## Process the first window
    # Initialize
    print("\n processing window: 0")
    traceList = os.path.join(tracesPath + os.sep + workloadInfo['winDirPrefix'] + str(0) + os.sep + 'job_permut_list.txt')
    with open(traceList) as fp:
        traces = fp.readlines()      
    traces = [x.strip() for x in traces]    
    winResults = results + os.sep + workloadInfo['winDirPrefix'] + str(0)
    os.mkdir(winResults)
    winLogs = logs + os.sep + workloadInfo['winDirPrefix'] + str(0)
    # Start simulation. Run simulations for all traces (permutations) of the first window.
    stats = startSim(args.procs, runDir, traces, "", os.path.abspath(winLogs))
    print("Simulation finished. Saving results....")
    # Process results
    traceStats = processStats(stats)
    bestTrace, bTime, bpoint = getBest(traceStats)         
    print("Best performing trace file is: ", bestTrace, bTime, bpoint)    
    with open(results + os.sep + "win_0" + os.sep +'win_0_'+ policyName +'.log', "w") as wfp:
        log = '\n'.join(str(ln) for ln in stats)        
        log = log + "\nBest performing trace file is: " + str(bestTrace) + " time: " + str(bTime)
        wfp.write(log)
    saveGraphs(traceStats, winResults, policyName, bpoint, bTime)
    # Pick the best performing window and use it as seed for the next simulation
    nextWinInitTraces = {}   
    if os.path.exists(bestTrace):
        with open(bestTrace) as btf1:
            nextWinInitTraces = json.load(btf1)
    else:
        print("windows initialization traces file not found",bestTrace)
        exit(-2)
    nextWinInitTraceFile = tmp+os.sep+"winInitTrace_"+str(1)+".json"
    with open(nextWinInitTraceFile, "w") as output_file:
        json.dump(nextWinInitTraces, output_file)

    
    ## Process next windows    
    for i in range(1, workloadInfo['nrWindows']):
        # Initialize
        print("\n processing window: ",i)
        traceList = os.path.join(tracesPath + os.sep + workloadInfo['winDirPrefix'] + str(i) + os.sep + 'job_permut_list.txt')
        with open(traceList) as fp:
            traces = fp.readlines()      
        traces = [x.strip() for x in traces]
        winResults = results + os.sep + workloadInfo['winDirPrefix'] + str(i)
        print(winResults)
        os.mkdir(winResults)
        winLogs = logs + os.sep + workloadInfo['winDirPrefix'] + str(i)
        # Start simulation. Run simulations for all traces (permutations) of the current window.
        stats = startSim(args.procs, runDir, traces, nextWinInitTraceFile, os.path.abspath(winLogs))
        print("Simulation finished. Saving results....")
        # Process results
        prevWinInitTracesFile = nextWinInitTraceFile
        traceStats = processStats(stats)
        bestTrace, bTime, bpoint = getBest(traceStats)
        print("Best performing trace file is: ", bestTrace, bTime, bpoint)
        with open(results + os.sep + "win_" + str(i) + os.sep + 'win_' + str(i) + '_' + policyName + '.log', "w") as wfp:
            log = '\n'.join(str(ln) for ln in stats)           
            log = log + "\nBest performing trace file is: " + str(bestTrace) + " time: " + str(bTime)
            wfp.write(log)
        saveGraphs(traceStats, winResults, policyName, bpoint, bTime)
        ## Prepare seed for the next simulation
        # First retrieve all previous windows used as seed in the current simulation
        prevWinInitTraces = {}
        nextWinInitTraces = {}        
        if os.path.exists(prevWinInitTracesFile):
            with open(prevWinInitTracesFile) as wtr:
                prevWinInitTraces = json.load(wtr)
        else:
            print("Previous window initialivation traces file not found",prevWinInitTracesFile)
            exit(-1)
        # Pick the best performing window in the current simulation
        if os.path.exists(bestTrace):
            with open(bestTrace) as btf1:
                nextWinInitTraces = json.load(btf1)
        else:
            print("best traces file not found",bestTrace)
            exit(-2)
        # Concatenate previous and current windows as seed for the next simulation 
        prevWinInitTraces.update(nextWinInitTraces)
        nextWinInitTraces = prevWinInitTraces
        nextWinInitTraceFile = tmp+os.sep+"winInitTrace_"+str(i+1)+".json"
        with open(nextWinInitTraceFile, "w") as output_file:
            json.dump(nextWinInitTraces, output_file)
    