import logging
from pathlib import Path
from multiprocessing import Process
import sys
import glob
import numpy as np
import ast
import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
import df_IO
import pickle
import paramsSystem as sysparams
import tsaSimul as tsaSim
sys.setrecursionlimit(1000000)

currentDir = Path.cwd()
testRltFolder = sysparams.testRltFolder
evalParamsFiles = sysparams.evalParamsFolder

def runRW_TestsWithFile(evalParam_filename, bStart, saveRlt=False):
    # bSingleRun = False
    # specifiedRowIdx = tStart
    evalParamFullPath = evalParam_filename  
    rwRltCols = [   
                    'testProfile', 'tsa'
                    , 'totalFinishingTime'
                    , 'totalMsgRwTime'
                    , 'meanMsgUnitRwTime'                 
                    , 'meanMad', 'medianMad', 'maxMad'
                    # , 'msgTrafs', 'msgMadsTrace'
                    ]
    # evalParam file columns: ['dagSize','rate','nTipSelected','caseIdx']['timeSeq','nMsg','rate','duration','trafCaseIdx']
    cks_evals = 1000

   
    for chunk in pd.read_csv(evalParamFullPath, chunksize=cks_evals):
        chunk = chunk[chunk['dagSize'].isin([2000,2500,3000])]
        chunk = chunk[chunk['duration'] == 10]        

        for row in chunk.itertuples():
            evalIdx = row.Index          
            dagSize = row.dagSize
            growRate = row.rate_x
            nTipSelected = row.nTipSelected
            dagCaseIdx = row.caseIdx
            # print('\ndag_size-{a}_rate-{b}_nTips-{c}_caseIdx-{d}.pickle'.format(a=dagSize, b=growRate, c=nTipSelected, d=dagCaseIdx), end=' ')

            # Traf Params:
            msgQueue = row.timeSeq
            # print('Before || msgQueue:', msgQueue)
            msgQueue = ast.literal_eval(msgQueue)
            # print('Get || msgQueue:', type(msgQueue))
            msgRate = row.rate_y
            msgDuration = row.duration
            msgTrafCaseIdx = row.trafCaseIdx

            # pick dag file
            # dag-0_size-500_rate-20_nTips-2_caseIdx-0.pickle
            dagFileName = './dagsFile/dagDatasLess/dag-*_size-{a}_rate-{b}_nTips-{c}_caseIdx-{d}.pickle'.format(a=dagSize, b=growRate, c=nTipSelected, d=dagCaseIdx)
            # dagFileName_fullpath = Path.joinpath(currentDir, sysparams.dagsFileFolder, dagFileName)
            NameList = glob.glob(dagFileName)
            # print('Searchwed Results:', NameList)
            for name in NameList:
                # print('found Dag:', name)
                dagReadOut = _readDag(name)

            with open(name, 'rb') as f:
            #     # The protocol version used is detected automatically, so we do not
            #     # have to specify it.
                dagReadOut = pickle.load(f)
            # print(dagReadOut)

            testProfile = {}
            testProfile['gIdx'] = evalIdx
            testProfile['dagSize'] = dagSize
            testProfile['growRate'] = growRate
            testProfile['nTipSelected'] =nTipSelected
            testProfile['dagCaseIdx'] = dagCaseIdx
            testProfile['msgRate'] = msgRate
            testProfile['nMsg'] = int(len(msgQueue))
            testProfile['msgDuration'] = msgDuration
            testProfile['msgTrafCaseIdx'] = msgTrafCaseIdx

            logDf = pd.DataFrame(columns=rwRltCols)

            # logDf['msgTrafs'] = logDf['msgTrafs'].astype('object')
            # logDf.at[0, 'msgTrafs'] = msgQueue
            # print('msgTrafs', logDf['msgTrafs'])

            msgQueueNew = msgQueue.copy()
            _, msgProcDoneTimeTickList, msgProcTimeList, totalMsgRwTime, totalFinishingTime = tsaSim.simRWwithDag(dagReadOut, msgQueueNew)            
            _, meanMad, medianMad, maxMad, _, meanMsgUnitRwTime = tsaSim.rwKPIs(msgQueueNew,
                                                                msgProcDoneTimeTickList,
                                                                msgProcTimeList)

            
            print(f'TotalFinishingSpan: {totalFinishingTime}')
            totalMsgRwTime = round(totalMsgRwTime, 5)
            # ['totalMsgRwTime', 'totalRWTime', 'meanMad', 'medianMad', 'maxMad', 'msgMadsTrace', 'testProfile']
            logDf.at[0, 'tsa'] = 'srw'
            logDf.at[0, 'totalMsgRwTime'] = totalMsgRwTime
            logDf.at[0, 'meanMsgUnitRwTime'] = meanMsgUnitRwTime
            logDf.at[0, 'totalFinishingTime'] = round(totalFinishingTime, 5)
            # logDf.at[0, 'totalRWTime'] = totalRWTime
            logDf.at[0, 'meanMad'] = meanMad
            logDf.at[0, 'medianMad'] = medianMad
            logDf.at[0, 'maxMad'] = maxMad
            logDf.at[0, 'testProfile'] = testProfile
            # logDf.at[0, 'msgMadsTrace'] = list(msgMads)

            
            print('Finished Test#: batch#-{a}-{b}:{c} '.format(a=bStart, b=evalIdx, c=logDf.at[0, 'testProfile']), end='\n')
           
            # logData = [totalMsgRwTime, totalRWTime, meanMad, medianMad, maxMad, msgMads, testProfile]
            # logTest_i = pd.DataFrame([logData], columns=rwRltCols)
            # logDf = pd.concat([logTest_i], ignore_index=True)
            rltPartIdx = evalIdx // cks_evals
            logFileName = './withBatchFile5_sequential/srw_TestLogs__batch#-{a}_part-{b}.csv'.format(a=bStart, b=rltPartIdx)
            print('rltPart#: ', rltPartIdx)
            rltfullPath = Path.joinpath(currentDir, testRltFolder, logFileName)            
            if saveRlt is True:
                # print("Write to log file {a}".format(a=rltfullPath))
                df_IO.writeToCVS(rltfullPath, logDf)
        # rltPartIdx += 1
        # print('------------Create a new logfile part-', rltPartIdx)

def runRWprallel_TestsWithFile(evalParam_filename, bStart, nThreads = 10, saveRlt=False):    
    evalParamFullPath = evalParam_filename   

    rwRltCols = [   
                    'testProfile', 'totalFinishingTime'
                    , 'totalMsgRwTime'
                    , 'meanMsgUnitRwTime'
                    # , 'totalRWTime'
                    , 'meanMad', 'medianMad', 'maxMad'
                    , 'totalLaunch'
                    , 'meanLaunch', 'medianLaunch', 'maxLaunch'
                    # , 'msgTrafs', 'msgMadsTrace'
                    ]
    # evalParam file columns: ['dagSize','rate','nTipSelected','caseIdx']['timeSeq','nMsg','rate','duration','trafCaseIdx']
    cks_evals = 1000
    for chunk in pd.read_csv(evalParamFullPath, chunksize=cks_evals):
        # lastRow = chunk.iloc[-1:]
       
        # if lastRow.index < specifiedRowIdx:
        #     print('Skip ... Not yet the specified Test#:', specifiedRowIdx)
        #     continue

        logging.info(f'Only test cases with dagSize in [2000,2500,3000] and duration = 10s, skipping wrong tests ...')
        chunk = chunk[chunk['dagSize'].isin([2000, 2500, 3000])]
        chunk = chunk[chunk['duration'] == 10]

        for row in chunk.itertuples():
            evalIdx = row.Index
            
            # if evalIdx < 5215:
            #     continue
            # elif evalIdx > 5215:
            #     break 
            
            dagSize = row.dagSize
            growRate = row.rate_x
            nTipSelected = row.nTipSelected
            dagCaseIdx = row.caseIdx
            # Traf Params:
            msgQueue = row.timeSeq
            # print('Before || msgQueue:', msgQueue)
            msgQueue = ast.literal_eval(msgQueue)
            # print('Get || msgQueue:', type(msgQueue))
            msgRate = row.rate_y
            msgDuration = row.duration
            msgTrafCaseIdx = row.trafCaseIdx

            # if ((msgTrafCaseIdx+1) % 2) != 0:
            #     continue

            # pick dag file
            # dag-0_size-500_rate-20_nTips-2_caseIdx-0.pickle
            dagFileName = './dagsFile/dagDatasLess/dag-*_size-{a}_rate-{b}_nTips-{c}_caseIdx-{d}.pickle'.format(a=dagSize, b=growRate, c=nTipSelected, d=dagCaseIdx)
            # dagFileName_fullpath = Path.joinpath(currentDir, sysparams.dagsFileFolder, dagFileName)
            NameList = glob.glob(dagFileName)
            # print('Searchwed Results:', NameList)
            for name in NameList:
                # print('found Dag:', name)
                dagReadOut = _readDag(name)

            with open(name, 'rb') as f:
            #     # The protocol version used is detected automatically, so we do not
            #     # have to specify it.
                dagReadOut = pickle.load(f)
            # print(dagReadOut)

            testProfile = {}
            testProfile['gIdx'] = evalIdx
            testProfile['dagSize'] = dagSize
            testProfile['growRate'] = growRate
            testProfile['nTipSelected'] =nTipSelected
            testProfile['dagCaseIdx'] = dagCaseIdx
            testProfile['msgRate'] = msgRate
            testProfile['nMsg'] = int(len(msgQueue))
            testProfile['msgDuration'] = msgDuration
            testProfile['msgTrafCaseIdx'] = msgTrafCaseIdx
            logging.info(f'{testProfile}')

            logDf = pd.DataFrame(columns=rwRltCols)
            # logDf['msgTrafs'] = logDf['msgTrafs'].astype('object')
            # logDf.at[0, 'msgTrafs'] = msgQueue
            # print('msgTrafs', logDf['msgTrafs'])

            msgQueueCp = msgQueue.copy()
            
            msgThLaunchTimeList, _, msgProcDoneTimeTickList, msgProcTimeList, totalMsgRwTime, totalFinishingTime = tsaSim.simRwwithDag_parallel(dagReadOut, msgQueueCp, nThreads)            
            _, meanMad, medianMad, maxMad, _, meanMsgUnitRwTime = tsaSim.rwKPIs(msgQueueCp,                                                                msgProcDoneTimeTickList, msgProcTimeList)

            totalMsgRwTime = round(totalMsgRwTime, 5)           
            meanLaunch = round(np.mean(msgThLaunchTimeList), 5)
            medianLaunch = round(np.median(msgThLaunchTimeList), 5)
            maxLaunch = round(np.max(msgThLaunchTimeList), 5)
            totalLaunch = round(sum(msgThLaunchTimeList), 5)

            print(f'TotalFinishingSpan: {totalFinishingTime}')
            print(f'mean, median, max-LaunchTime: {meanLaunch} | {medianLaunch} | {maxLaunch}')
            print(f'tLaunch time vs. tProc Time: {totalLaunch} vs {totalMsgRwTime}, ratio: {(round(totalLaunch / (totalLaunch + totalMsgRwTime), 3))*100}%')

            # ['totalMsgRwTime', 'totalRWTime', 'meanMad', 'medianMad', 'maxMad', 'msgMadsTrace', 'testProfile']
            logDf.at[0, 'tsa'] = 'prw'
            logDf.at[0, 'totalMsgRwTime'] = totalMsgRwTime
            logDf.at[0, 'meanMsgUnitRwTime'] = meanMsgUnitRwTime
            logDf.at[0, 'totalFinishingTime'] = round(totalFinishingTime, 3)            
            # logDf.at[0, 'totalRWTime'] = totalRWTime
            logDf.at[0, 'meanMad'] = meanMad
            logDf.at[0, 'medianMad'] = medianMad
            logDf.at[0, 'maxMad'] = maxMad            
            # logDf.at[0, 'msgMadsTrace'] = list(msgMads)
            logDf.at[0, 'meanLaunch'] = meanLaunch
            logDf.at[0, 'medianLaunch'] = medianLaunch
            logDf.at[0, 'maxLaunch'] = maxLaunch
            logDf.at[0, 'totalLaunch'] = totalLaunch
            logDf.at[0, 'testProfile'] = testProfile
  
            print('Finished Test#: batch#-{a}-{b}:{c} '.format(a=bStart, b=evalIdx, c=logDf.at[0, 'testProfile']), end='\n\n')
           
            # logData = [totalMsgRwTime, totalRWTime, meanMad, medianMad, maxMad, msgMads, testProfile]
            # logTest_i = pd.DataFrame([logData], columns=rwRltCols)
            # logDf = pd.concat([logTest_i], ignore_index=True)
            rltPartIdx = evalIdx // cks_evals            
            
            logFileName = f'./withBatchFile5_sequential/prw_TestLogs_nTh-{nThreads}_batch#-{bStart}_part-{rltPartIdx}.csv'
            # print('rltPart#: ', rltPartIdx)
            rltfullPath = Path.joinpath(currentDir, testRltFolder, logFileName)
            
            if saveRlt is True:
                # print("Write to log file {a}".format(a=rltfullPath))
                df_IO.writeToCVS(rltfullPath, logDf)            
        # rltPartIdx += 1
        # print('------------Create a new logfile part-', rltPartIdx)


def runRwTests(batchIdx_start):    
    # batchIdx_start = int(sys.argv[1])    
    print('Run tests with batch#-{a}'.format(a=batchIdx_start))

    # for batchIdx in range(batchIdx_start,batchIdx_end,1):
    # evalFile = './batchTestFiles2/EvalParams_TrafRates-[20, 40, 60, 80, 100]_durations-[0.2, 0.4, 0.6]_nCases-10_dagSize-[500, 1000, 1500, 2000, 2500, 3000]_growRates-[20, 40, 60, 80, 100]_nTips-[2, 4, 6]_dagCases-10_Batch#-{a}.csv'.format(a=batchIdx_start)

    # evalFile = './batchTestFiles2/EvalParams_TrafRates-[20, 40, 60, 80, 100]_durations-[10, 20, 30]_nCases-10_dagSize-[500, 1000, 1500, 2000, 2500, 3000]_growRates-[20, 40, 60, 80, 100]_nTips-[2, 4]_dagCases-10_Batch#-{a}.csv'.format(a=batchIdx_start)

    evalFile = './batchTestFiles4/EvalParams_TrafRates-[20, 40, 60, 80, 100]_durations-[10, 30]_nCases-50_dagSize-[500, 1000, 1500, 2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{a}.csv'.format(a=batchIdx_start)
    nThreads = 10
    saveRlt = True
    runRWprallel_TestsWithFile(evalFile, batchIdx_start, nThreads, saveRlt)
    # runRW_TestsWithFile(evalFile, batchIdx_start)    
    print('Finished batch:', evalFile)

def run_sRw_Tests_1by1(evalParamFileFldIdx):
    batchFileFolderIdx = evalParamFileFldIdx[0]
    saveRlt = True
    for batchIdx_start in range(6):
        evalFile = f'./evalParamsFiles/batchTestFiles{batchFileFolderIdx}/EvalParams_TrafRates-[10, 20, 40, 60, 80, 100, 120]_durations-[10]_nCases-100_dagSize-[2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{batchIdx_start}.csv'
        runRW_TestsWithFile(evalFile, batchIdx_start,   saveRlt)    
        print('Finished batch:', evalFile)



def run_sRw_Tests_Mp(evalParamFileFldIdx):
    batchFileFolderIdx = evalParamFileFldIdx[0]
    processList = []    
    saveRlt = True
    # for batchIdx_start in range(6):
    for batchIdx_start in [5]:
        # evalFile = './batchTestFiles2/EvalParams_TrafRates-[20, 40, 60, 80, 100]_durations-[10, 20, 30]_nCases-10_dagSize-[500, 1000, 1500, 2000, 2500, 3000]_growRates-[20, 40, 60, 80, 100]_nTips-[2, 4]_dagCases-10_Batch#-{a}.csv'.format(a=batchIdx_start)
        # evalFile = './batchTestFiles{a}/EvalParams_TrafRates-[20, 40, 60, 80, 100]_durations-[10, 30]_nCases-50_dagSize-[500, 1000, 1500, 2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{b}.csv'.format(a=evalParamFileFldIdx[0], b=batchIdx_start)

        evalFile = f'./evalParamsFiles/batchTestFiles{batchFileFolderIdx}/EvalParams_TrafRates-[10, 20, 40, 60, 80, 100, 120]_durations-[10]_nCases-100_dagSize-[2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{batchIdx_start}.csv'       
        p_i = Process(target=runRW_TestsWithFile, args=(evalFile, batchIdx_start, saveRlt))
        processList.append(p_i)
        print('Process Instance {b} for batch#-{a}'.format(a=batchIdx_start, b=p_i))
    for p_i in processList:
        p_i.start()
        print('Launch Process ', p_i)
    for p_i in processList:
        p_i.join()
        print('Waiting Process ', p_i)
    
def run_pRw_Tests_1by1(evalParamFileFldIdx):
    batchFileFolderIdx = evalParamFileFldIdx[0]
    nThreads = 10
    saveRlt = False
    for batchIdx_start in range(6):
        evalFile = f'./evalParamsFiles/batchTestFiles{batchFileFolderIdx}/EvalParams_TrafRates-[10, 20, 40, 60, 80, 100, 120]_durations-[10]_nCases-100_dagSize-[2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{batchIdx_start}.csv'
        runRWprallel_TestsWithFile(evalFile, batchIdx_start, nThreads, saveRlt)    
        print('Finished batch:', evalFile)


def run_pRw_Tests_Mp(evalParamFileFldIdx):
    batchFileFolderIdx = evalParamFileFldIdx[0]
    processList = []
    nThreads = 10
    saveRlt = True
    for batchIdx_start in range(6):
        # evalFile = './batchTestFiles2/EvalParams_TrafRates-[20, 40, 60, 80, 100]_durations-[10, 20, 30]_nCases-10_dagSize-[500, 1000, 1500, 2000, 2500, 3000]_growRates-[20, 40, 60, 80, 100]_nTips-[2, 4]_dagCases-10_Batch#-{a}.csv'.format(a=batchIdx_start)
        # evalFile = './batchTestFiles{a}/EvalParams_TrafRates-[20, 40, 60, 80, 100]_durations-[10, 30]_nCases-50_dagSize-[500, 1000, 1500, 2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{b}.csv'.format(a=evalParamFileFldIdx[0], b=batchIdx_start)
        evalFile = f'./evalParamsFiles/batchTestFiles{batchFileFolderIdx}/EvalParams_TrafRates-[10, 20, 40, 60, 80, 100, 120]_durations-[10]_nCases-100_dagSize-[2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{batchIdx_start}.csv'      
        p_i = Process(target=runRWprallel_TestsWithFile, args=(evalFile, batchIdx_start, nThreads, saveRlt))
        processList.append(p_i)
        print('Process Instance {b} for batch#-{a}'.format(a=batchIdx_start, b=p_i))
    for p_i in processList:
        p_i.start()
        print('Launch Process ', p_i)
    for p_i in processList:
        p_i.join()
        print('Waiting Process ', p_i)
    # print('Finished Batch#-{a}'.format(a=batchIdx_start))
   
def _readDag(fullpath_dagFile):
    with open(fullpath_dagFile, 'rb') as f:
        #     # The protocol version used is detected automatically, so we do not
        #     # have to specify it.
            dagReadOut = pickle.load(f)
    return dagReadOut


if __name__ == '__main__':
    evalParamFileFldIdx = [5]
    run_sRw_Tests_1by1(evalParamFileFldIdx)
    # run_pRw_Tests_1by1(evalParamFileFldIdx)
    # run_sRw_Tests_Mp(evalParamFileFldIdx)
    # run_pRw_Tests_Mp(evalParamFileFldIdx)