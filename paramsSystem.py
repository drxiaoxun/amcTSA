# from matplotlib import cm
import numpy as np

testRltFolder = 'simResults'
folder_msgTrafficByDuration = 'msgTrafficByDuration'
evalParamsFolder = 'evalParamsFiles'
procRltFolder = 'procResults'
dagsFileFolder = 'dagsFile'


Alpha = 0.001
nTip = 3
Lambda = 60
nMsg = 2000
subDagSize = 1000
msgVisDelay = 1
# for amc
tau = 0.06


# for traffic generation
lambdaList = [10,20,40,60,80,100]
durationList = [10]
# durationList = np.arange(0.1,2.1,0.1)
# durationList = [round(float(n), 2) for n in durationList]
nCaseTraffic = 100

#### DAG generation parameters
nDagCase = 10
DagSizeList = [500, 1000, 1500, 2000, 2500, 3000]
DagRateList = [20, 40, 60, 80, 100]
dagTipSelectedList = [2, 4]

testnTipSelected = 3
# amcTauList = [0.02, 0.06, 0.1, 0.14, 0.18, 0.2, 0.24, 0.28, 0.3]
# amcTauList = np.arange(0.5,0.29,-0.05)
# amcTauList = [round(tau, 2) for tau in amcTauList]
amcTauList = [0.6, 0.55, 0.5, 0.45, 0.4, 0.35, 0.3]
# print(amcTauList)

#### Operation number parameters
optMsgCaseList = range(4, 11, 4)

