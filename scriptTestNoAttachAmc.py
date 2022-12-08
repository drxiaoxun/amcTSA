from pathlib import Path
from multiprocessing import Process
import glob
import numpy as np
import ast
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pickle
import df_IO
import paramsSystem as sysparams
import tsaSimul as tsaSim

currentDir = Path.cwd()
testRltFolder = sysparams.testRltFolder
evalParamsFiles = sysparams.evalParamsFolder

def runAmcTestsWithFile(evalParam_filename, tau, bStart, savedRlt=False):    
    # bSingleRun = False    
    evalParamFullPath = evalParam_filename
    # evalParamFullPath = Path.joinpath(currentDir, evalParamsFiles, filename)
    # evalParamsDf = readFromCVS(evalParamFullPath)

    # [totalFinishingTime, PitCalTime, SamplingTime, meanMad, medianMad, maxMad, msgAttachDelayList, testProfile]
    amcRltCols = [
                    'testProfile','tsa','totalFinishingTime'
                    , 'PitCalTime','PitCalNums','SamplingTime'
                    ,'meanMsgUnitProcTime'
                    ,'meanMad','medianMad','maxMad'
                    # , 'msgTrafs', 'msgMadsTrace'
                    ]
    # evalParam file columns: ['dagSize','rate','nTipSelected','caseIdx']['timeSeq','nMsg','rate','duration','trafCaseIdx']
    cks_evals = 1000
    
    for chunk in pd.read_csv(evalParamFullPath, chunksize=cks_evals):        
        print(f'only test specified dagSize and trafDuration ...')
        
        dagSizeRange = [2000,2500,3000]
        chunk = chunk[chunk['dagSize'].isin(dagSizeRange)]
        chunk = chunk[chunk['duration'] == 10]

        for row in chunk.itertuples():
            # print(row)
            evalIdx = row.Index
            dagSize = row.dagSize
            growRate = row.rate_x
            nTipSelected = row.nTipSelected
            dagCaseIdx = row.caseIdx
            # print('\ndag_size-{a}_rate-{b}_nTips-{c}_caseIdx-{d}.pickle'.format(a=dagSize, b=growRate, c=nTipSelected, d=dagCaseIdx), end=' ')

            # Traf Params:
            msgQueue = row.timeSeq
            msgQueue = ast.literal_eval(msgQueue)
            msgRate = row.rate_y
            msgDuration = row.duration
            msgTrafCaseIdx = row.trafCaseIdx
            
            # specified_msgRate = [20, 60, 80, 100]
            # msgRateCondition = msgRate in specified_msgRate
            # if msgRateCondition is False:
            #     print(f'Not specified msgRate, skipped ...')
            #     continue

            # pick dag file
            # dag-0_size-500_rate-20_nTips-2_caseIdx-0.pickle
            dagFileName = './dagsFile/dagDatasLess/dag-*_size-{a}_rate-{b}_nTips-{c}_caseIdx-{d}.pickle'.format(a=dagSize, b=growRate, c=nTipSelected, d=dagCaseIdx)
            NameList = glob.glob(dagFileName)
            # print('Searchwed Results:', NameList)
            for name in NameList:
                # print('found Dag:', name)
                dagReadOut = _readDag(name)
            with open(name, 'rb') as f:
                dagReadOut = pickle.load(f)
            # print(dagReadOut)

            # logDf.at[0, 'msgTrafs'] = msgQueue
            testProfile = {}
            testProfile['gIdx'] = evalIdx
            testProfile['dagSize'] = dagSize
            testProfile['growRate'] = growRate
            testProfile['nTipSelected'] = nTipSelected
            testProfile['dagCaseIdx'] = dagCaseIdx
            testProfile['msgRate'] = msgRate
            testProfile['msgDuration'] = msgDuration
            testProfile['nMsg'] = int(len(msgQueue))
            testProfile['msgTrafCaseIdx'] = msgTrafCaseIdx
            testProfile['tau'] = tau

            msgQueueNew = msgQueue.copy()
            windowPiCalTimeList, msgProcStartTimeTickList, msgProcDoneTimeTickList, nPiCal, totalFinishingTime = tsaSim.simAmcwithDag(dagReadOut, tau, msgQueueNew)
            meanMad, medianMad, maxMad, msgAttachDelayList, PitCalTime, SamplingTime, meanMsgUnitProcTime = tsaSim.amcKPIs(msgQueueNew,
                                                                              windowPiCalTimeList,
                                                                              msgProcStartTimeTickList,
                                                                              msgProcDoneTimeTickList)
            
            logDf = pd.DataFrame(columns=amcRltCols)       
            logDf.at[0, 'tsa'] = 'amc'
            logDf.at[0, 'totalFinishingTime'] = totalFinishingTime
            logDf.at[0, 'PitCalTime'] = PitCalTime
            logDf.at[0, 'PitCalNums'] = nPiCal
            logDf.at[0, 'SamplingTime'] = SamplingTime
            logDf.at[0, 'meanMsgUnitProcTime'] = meanMsgUnitProcTime
            logDf.at[0, 'meanMad'] = meanMad
            logDf.at[0, 'medianMad'] = medianMad
            logDf.at[0, 'maxMad'] = maxMad     
            logDf.at[0, 'testProfile'] = testProfile
            print('Finished Test#: batch#-{a}-{b}:{c} '.format(a=bStart, b=evalIdx, c=logDf.at[0, 'testProfile']), end='\n')           
            
            print(logDf.at[0, 'totalFinishingTime'], end='||')
            print(logDf.at[0, 'PitCalTime'], end='||')
            print(logDf.at[0, 'PitCalNums'], end='||')
            print(logDf.at[0, 'SamplingTime'], end='||')
            print(logDf.at[0, 'meanMsgUnitProcTime'], end='||')
            print(logDf.at[0, 'meanMad'], end='||')
            print(logDf.at[0, 'medianMad'], end='||')
            print(logDf.at[0, 'maxMad'], end='\n')
            
            
            rltPartIdx = evalIdx // cks_evals
            tau = round(tau, 2)
            logFileName = f'./simResults/withBatchFile5_sequential/amc_TestLogs_batch#-{bStart}_tau-{tau}_part-{rltPartIdx}.csv'
            # rltfullPath = Path.joinpath(currentDir, testRltFolder, logFileName)            
            if savedRlt is True:
                print(f'Write to logfile {logFileName} ...')
                df_IO.writeToCVS(logFileName, logDf)
        # print('------------Create a new logfile part-', rltPartIdx)
    
def _readDag(fullpath_dagFile):
    with open(fullpath_dagFile, 'rb') as f:
        #     # The protocol version used is detected automatically, so we do not
        #     # have to specify it.
        dagReadOut = pickle.load(f)
    return dagReadOut


def runAmcTests(batchIdx_start):    
    savedRlt = False
    batchFileFolderIdx = 5
    tauTestList = sysparams.amcTauList   
    for valTau in tauTestList:
        print(f'Run tests with batch#-{batchIdx_start}_Tau_{valTau}')
        evalFile = f'./evalParamsFiles/batchTestFiles{batchFileFolderIdx}/EvalParams_TrafRates-[10, 20, 40, 60, 80, 100, 120]_durations-[10]_nCases-100_dagSize-[2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{batchIdx_start}.csv'    
        runAmcTestsWithFile(evalFile, valTau, batchIdx_start, savedRlt)


def runAmcTests_1by1():    
    savedRlt = False
    batchFileFolderIdx = 5
    tauTestList = sysparams.amcTauList   
    for batchIdx_start in range(6):
        for valTau in tauTestList:
            print(f'Run tests with batch#-{batchIdx_start}_Tau_{valTau}')
            evalFile = f'./evalParamsFiles/batchTestFiles{batchFileFolderIdx}/EvalParams_TrafRates-[10, 20, 40, 60, 80, 100, 120]_durations-[10]_nCases-100_dagSize-[2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{batchIdx_start}.csv'    
            runAmcTestsWithFile(evalFile, valTau, batchIdx_start, savedRlt)
            print(f'[FINISHED] Run tests with batch#-{batchIdx_start}_Tau_{valTau}')

def runAmcTests_mp(batchIdx_start):
    savedRlt = False
    tauTestList = sysparams.amcTauList
    processList = []
    batchFileFolderIdx = 5
    for valTau in [0.5]:
        evalFile = f'./evalParamsFiles/batchTestFiles{batchFileFolderIdx}/EvalParams_TrafRates-[10, 20, 40, 60, 80, 100, 120]_durations-[10]_nCases-100_dagSize-[2000, 2500, 3000]_growRates-[20, 40]_nTips-[2, 4]_dagCases-10_Batch#-{batchIdx_start}.csv'    

        p_i = Process(target=runAmcTestsWithFile, args=(evalFile, valTau, batchIdx_start, savedRlt))
        processList.append(p_i)
        print('Process Instance {c} for batch#-{a}_Tau_{b}'.format(a=batchIdx_start, b=valTau, c=p_i))

    for p_i in processList:
        p_i.start()
        print('Launch Process ', p_i)
    for p_i in processList:
        p_i.join()
        print('Waiting Process ', p_i)
    print('Finished Batch#{a}'.format(a=batchIdx_start))

if __name__ == '__main__':
    runAmcTests_1by1()