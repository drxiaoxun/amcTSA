import logging
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from threading import Semaphore
import concurrent.futures
import os
import psutil
from pathlib import Path
import numpy as np
import glob
import time as cpuTime
import pandas as pd
import pickle
import paramsSystem as sysParams
import df_IO
from myTangle import myDAG
import myTangle



dagFileFolder = sysParams.dagsFileFolder
curDir = Path.cwd()

def _readDag(fullpath_dagFile):
    with open(fullpath_dagFile, 'rb') as f:
        #     # The protocol version used is detected automatically, so we do not
        #     # have to specify it.
            dagReadOut = pickle.load(f)
    return dagReadOut

# custom thread class
class resThread(Thread):
    # constructor
    def __init__(self, stopMeaEvent, cpuInterval=0.1):
        # call the parent constructor
        super(resThread, self).__init__()
        # store the event
        self.stopMeaEvent = stopMeaEvent        
        self.cpuInterval = cpuInterval
        print('In TH, stopEvent Status:', stopMeaEvent.is_set())
        
    # execute task
    def run(self):
        # print('Start a thread to capture res consumptions ...')        
        # curProcess = psutil.Process(os.getpid())
        curProcess = psutil.Process()        
        self.memTrace = []
        self.cpuTrace = []        
        # print('In TH-run, stopEvent Status: ', self.event.is_set())    

        while True:         
            if self.stopMeaEvent.is_set() is False:                
                # print('In TH-run, ticEvent Status: ', self.ticEvent.is_set())         
                cpuPct = curProcess.cpu_percent(interval=self.cpuInterval)
                # cpuPct = curProcess.cpu_percent()              
                # print(f'In TH, cpuPct: {cpuPct}')
                # cpuPct = curProcess.cpu_percent(interval=None)              
                self.cpuTrace.append(cpuPct)
                # print('In TH, system wide cpuPct:', psutil.cpu_percent(percpu=True, interval=self.cpuInterval))
                mem = curProcess.memory_info()[0] / float(2 ** 20)
                self.memTrace.append(mem)
            else:
                print('measureRes Worker closing down ...')
                break        
        

    
def runTsaOnDags(tsa, dagTipSelected, msgCase, saveRlt=False):
    filename = 'Res_Opts_dagParams_DagSizes-[500,3500,50]_Rates-[20,40,20]_nTips-{a}_Cases-100.csv'.format(a=dagTipSelected)
    # filename = 'Opts_dagParams_DagSizes-[500,3500,50]_Rates-[20,100,20]_nTips-{a}_Cases-50.csv'.format(a=dagTipSelected)
    fullpath = Path.joinpath(curDir, dagFileFolder,  filename)

    # Read the dag instance and convert it to the new tangle class instance
    dagParamDf = pd.read_csv(fullpath)
    dagSizeRange = range(2000, 3001, 200)
    print(list(dagSizeRange))
    dagParamDf = dagParamDf[dagParamDf['dagSize'].isin(list(dagSizeRange))]

    dagSizeList = list(dagParamDf['dagSize'].unique())
    dagSizeRange = '[{a},{b},{c}]'.format(a=min(dagSizeList), b=max(dagSizeList), c=50)
    gRateList = list(dagParamDf['rate'].unique())
    gRateRange = '[{a},{b},{c}]'.format(a=min(gRateList), b=max(gRateList), c=20)
    nTipList = list(dagParamDf['nTipSelected'].unique())
    # dagParamDf = dagParamDf.iloc[::-1]
    # dagParamDf = dagParamDf[dagParamDf['dagSize']==200]
    # dagParamDf = dagParamDf.iloc[2000:]
    # print('dagParams', dagParamDf)


    dagParamDf_r = dagParamDf.iloc[::-1]
    cpuInterval = 0.1

    stopMeasureEvent = Event()    
    res_th = resThread(stopMeasureEvent, cpuInterval)
    # print('Before TH start, stopEvent Status:', res_th.event.is_set())
    res_th.start()

    for row in dagParamDf_r.itertuples():        
        gpIdx = row.Index
        # dagSize,rate,caseIdx
        dagSize = row.dagSize
        rate = row.rate
        idx = row.caseIdx
        nTips = row.nTipSelected

        # # if (idx+1) % 2 != 0:   
        if idx>=50:                                    
            continue       

        matchingExp = './dagsFile/dagDatas/dag-*_size-{a}_rate-{b}_nTips-{c}_caseIdx-{d}.pickle'.format(a=dagSize, b=rate, c=nTips, d=idx)
        print('Searching dag-*_size-{a}_rate-{b}_nTips-{c}_caseIdx-{d}.pickle'.format(a=dagSize, b=rate, c=nTips, d=idx))
        searchRlt = glob.glob(matchingExp)
        # print('Dag file search results: ', searchRlt)
        if len(searchRlt) == 0:
            print('NOT FOUND: ', matchingExp)
            continue

        for rltFile in glob.glob(matchingExp):
            # print('Get DAG instance ... >>>>> ', rltFile)
            dagReadOut = _readDag(rltFile)

            tangle_g = myDAG(msgLambda=rate, tsaMethod='mcmc', bAttaching=False, plot=True)
            tangle_g.time           = dagReadOut.time
            tangle_g.transactions   = dagReadOut.transactions
            tangle_g.arrivalTimes   = dagReadOut.arrivalTimes
            tangle_g.nTipSelected   = sysParams.nTip
            # tangle_g.tip_selection  = dagReadOut.tip_selection
            tangle_g.G              = dagReadOut.G

            # compuatations of using RW tsa
            compNum = 0        
            
            # print('stopEvent Status:', stopEvent.is_set())            
            # print('Current process#:', psutil.Process().pid)           
            # print('After TH start, stopEvent Status:', res_th.event.is_set())            
                    
            if tsa == 'srw':                            
                for k in range(msgCase):
                    print('Using {a} for nMsgCase#-{b}'.format(a=tsa, b=k+1))                    
                    tangle_g.tsaRW(k, 1)
                    edgeEvalNums = np.sum(myTangle.rwPathEdgeEvalsNum)
                    # print('Evaluated Edge#: ', edgeEvalNums)
                    compNum += edgeEvalNums
                    myTangle.rwPathEdgeEvalsNum.clear()
            elif tsa == 'prw':                
                futureList = []                            
                start_tic = cpuTime.time()
                # # callback for completed tasks
                # def task_complete_callback(future):
                #     global semaphore
                #     # release the semaphore
                #     semaphore.release()
                MAX_EXE_NUM = msgCase
                with ThreadPoolExecutor(max_workers=MAX_EXE_NUM, thread_name_prefix='rwExeTh') as exePool:
                    # logging.info(f'prepare to submit tasks ...')
                    for k in range(msgCase):        
                        k += 1
                        # logging.info(f'Acquiring for TH-{k}...')
                        # semaphore.acquire()
                        # logging.info(f"Submit TH-{k}")
                        # logging.info(f'Th#-{k}, Using {tsa} for nMsgCase#-{k}')
                        future = exePool.submit(tangle_g.tsaRW, k, 1)                        
                        # future.add_done_callback(task_complete_callback)
                        futureList.append(future)
                
                    # for future in concurrent.futures.as_completed(futureList):
                    #     # get the result for the next completed task
                    #     # logging.info(f'<<<<<<<<- of FU: {semaphore}')
                    #     result = future.result() # blocks
                    #     # logging.info(f'worker {result[0]} actual stop time: {result[1]}')
                    #     logging.info(f'Multiprocessing ---> FINISHED RESULT: {result}')
                stop_tic = cpuTime.time()
                logging.info(f'Multiprocessing TotalTime: {stop_tic - start_tic}')

                edgeEvalNums = np.sum(myTangle.rwPathEdgeEvalsNum)
                # print('Evaluated Edge#: ', edgeEvalNums)
                compNum += edgeEvalNums
                myTangle.rwPathEdgeEvalsNum.clear()
            elif tsa == 'amc':
                print('Run calTSPD. ...') 
                tangle_g.updatePi()                          
                # print('finish cal Pi ...')
                for k in range(msgCase):
                    # print('sampling for msg#:',k)
                    tangle_g.tsaSampling()            
            
            # print('trigger stopMeasureEvent ... xxx')
            
            # print('CPU% Trace: ', res_th.memTrace)
            # print('MEM Trace: ', res_th.cpuTrace)
            memTrace = list(filter(lambda x: x != 0, res_th.memTrace)) 
            cpuTrace = list(filter(lambda x: x != 0, res_th.cpuTrace)) 
            print(f'clear res trace data ...')
            res_th.memTrace.clear()
            res_th.cpuTrace.clear()
            print(f'after cleaned, cpuTrace: {res_th.cpuTrace}')
            

            # print('CPU% Trace: ', cpuTrace)
            # print('MEM Trace: ', memTrace)

            memMean = np.mean(memTrace)
            memMedian = np.median(memTrace)
            memPeak = np.max(memTrace)

            cpuMean = np.mean(cpuTrace)
            cpuMedian = np.median(cpuTrace)
            cpuPeak = np.max(cpuTrace)

                       

            print('CPU%: {a},{b},{c}'.format(a=cpuMean,b=cpuMedian,c=cpuPeak))
            print('MEM: {a},{b},{c}'.format(a=memMean,b=memMedian,c=memPeak))
            nEdges = tangle_g.G.size()
            compNum = nEdges + rate + msgCase*tangle_g.nTipSelected*np.log(rate)
            
            # print('Realistic rlt: {a}'.format(a=compNum))
            
            # print('Theoretical rlt: ', end='')
            # theoCompNum = 0
            # if tsa == 'rw':
            #     theoCompNum = msgCase*dagSize*(sysParams.nTip)**2/rate
            #     print(theoCompNum)
            # elif tsa == 'amc':
            #     # print('calculating theoretical value: ')
            #     theoCompNum = {}
            #     theoCompNum['lw']       = dagSize-1
            #     theoCompNum['up']       = rate + (dagSize-rate)*(tangle_g.nTipSelected-1)
            #     theoCompNum['sampling'] = rate + msgCase*tangle_g.nTipSelected*np.log(rate)
            #     print(theoCompNum)

            # 'dagIdx','caseIdx','size','rate','nTipSelected','tsa','nMsg','theoCompNum'
            compNumCols = [ 'dagIdx', 'caseIdx', 'size', 'rate', 'nTipSelected', 'tsa', 'nMsg', 'compNum'
                            , 'memMean', 'memMedian', 'memPeak'
                            , 'cpuMean', 'cpuMedian', 'cpuPeak'
                            # , 'memTrace', 'cpuTrace'
                            ]
        
            logData = [ gpIdx, idx, dagSize, rate, nTips, tsa, msgCase, compNum
                        , memMean, memMedian, memPeak
                        , cpuMean, cpuMedian, cpuPeak
                        # , memTrace, cpuTrace
                        ]

            entryDf = pd.DataFrame([logData], columns=compNumCols)
            logDf = pd.concat([entryDf], ignore_index=True)              
            
            nDagCases = 50
            compNumFileName = f'./simResults/resCostMeasurements/detailed_RES_optNums_dagSize-{dagSizeRange}_rate-{gRateRange}_nTip-{nTipList}_nDagCase-{nDagCases}_nMsg-{msgCase}_cpuInterval-{cpuInterval}_tsa-{tsa}.csv'

            if saveRlt == True:
                df_IO.writeToCVS(compNumFileName, logDf)
            print('Test#:{a} || Finished {b}'.format(a=gpIdx, b=rltFile), end='\n\n')
    stopMeasureEvent.set()
            # res_th.join()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s-%(levelname)s-%(message)s',level=logging.INFO)
    nTipSelectedCases = [3]
    msgCaseList = [20]
    saveRlt = False
    for nTips in nTipSelectedCases:
        for tsa in [ 'prw', 'amc', 'srw']:
        # for tsa in [ 'amc',  'srw']:
            for msgCase in msgCaseList:                  
                runTsaOnDags(tsa, nTips, msgCase, saveRlt)