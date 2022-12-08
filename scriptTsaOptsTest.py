from pathlib import Path
import numpy as np
import glob
import time as cpuTime
import pandas as pd
import pickle
import paramsSystem as sysParams
# from tangle import Tangle
from myTangle import myDAG
import myTangle

dagFileFolder = sysParams.dagsFileFolder
curDir = Path.cwd()

def writeToCVS(fullpath, df):
    with open(fullpath, mode='a',newline='') as f:
        df.to_csv(f, sep=',', encoding='utf-8-sig', line_terminator="", header=f.tell() == 0, index=False)


def _readDag(fullpath_dagFile):
    with open(fullpath_dagFile, 'rb') as f:
        #     # The protocol version used is detected automatically, so we do not
        #     # have to specify it.
            dagReadOut = pickle.load(f)
    return dagReadOut


def runTsaOnDags(tsa, dagTipSelected, msgCase):    
    filename = 'Opts_dagParams_DagSizes-[500,3500,50]_Rates-[20,100,20]_nTips-{a}_Cases-50.csv'.format(a=dagTipSelected)
    fullpath = Path.joinpath(curDir, dagFileFolder,  filename)

    # Read the dag instance and convert it to the new tangle class instance
    dagParamDf = pd.read_csv(fullpath)
    dagSizeList = list(dagParamDf['dagSize'].unique())
    dagSizeRange = '[{a},{b},{c}]'.format(a=min(dagSizeList),b=max(dagSizeList),c=50)
    gRateList = list(dagParamDf['rate'].unique())
    gRateRange = '[{a},{b},{c}]'.format(a=min(gRateList),b=max(gRateList),c=20)
    nTipList = list(dagParamDf['nTipSelected'].unique())
    # dagParamDf = dagParamDf.iloc[::-1]
    # dagParamDf = dagParamDf[dagParamDf['dagSize']==200]
    # dagParamDf = dagParamDf.iloc[2000:]
    # print('dagParams', dagParamDf)

    for row in dagParamDf.itertuples():
        print('\n Test Case#_tsa-{a}_msgCase-{b}'.format(a=tsa, b=msgCase))
        gpIdx = row.Index
        # dagSize,rate,caseIdx
        dagSize = row.dagSize
        rate = row.rate
        idx = row.caseIdx
        nTips = row.nTipSelected

        matchingExp = './dagsFile/dagDatas/dag-*_size-{a}_rate-{b}_nTips-{c}_caseIdx-{d}.pickle'.format(a=dagSize, b=rate, c=nTips, d=idx)
        # files = glob.glob(matchingExp)
        # files = glob.glob(matchingExp + "/*.csv", recursive = True)
        for rltFile in glob.glob(matchingExp):
            print('\n\nGet attributes ... >>>>> ', rltFile)
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
            if tsa == 'rw':
                for k in range(msgCase):
                    print('Using {a} for nMsgCase#-{b}'.format(a=tsa, b=k+1))
                    tangle_g.tsaRW(k)
                    edgeEvalNums = np.sum(myTangle.rwPathEdgeEvalsNum)
                    print('Evaluated Edge#: ', edgeEvalNums)
                    compNum += edgeEvalNums
                    myTangle.rwPathEdgeEvalsNum.clear()
            elif tsa == 'amc':
                nEdges = tangle_g.G.size()
                compNum = nEdges + rate + msgCase*tangle_g.nTipSelected*np.log(rate)
                
            print('Realistic rlt: {a}'.format(a=compNum))
            
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
            
            logData = [gpIdx, idx, dagSize, rate, tangle_g.nTipSelected, tsa, msgCase, compNum]
            compNumCols = ['dagIdx','caseIdx','size','rate','nTipSelected','tsa','nMsg','compNum']
            entryDf = pd.DataFrame([logData], columns=compNumCols)
            logDf = pd.concat([entryDf], ignore_index=True)

            compNumFileName = 'optNums_dagSize-{a}_rate-{b}_nTip-{c}_nCase-{d}_nMsg-{e}_tsa-{f}.csv'.format(a=dagSizeRange,
                                                                                                                b=gRateRange,
                                                                                                                c=nTipList,
                                                                                                                d=50,
                                                                                                                e=msgCase,
                                                                                                                f=tsa)
            compNumFullPath = Path.joinpath(curDir, dagFileFolder, compNumFileName)

            # print("Write to ParamsFile {a}".format(a=fullPath))
            isWritten = True
            if isWritten == True:
                writeToCVS(compNumFullPath, logDf)
            print('Test#:{a} || Finished {b}'.format(a=gpIdx, b=rltFile))

if __name__ == '__main__':
    
    nTipSelectedCases = [3,4]
    for nTips in nTipSelectedCases:
        for tsa in ['amc']:
            for msgCase in range(4,11,1):
                runTsaOnDags(tsa, nTips, msgCase)