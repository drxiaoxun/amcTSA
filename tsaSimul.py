import logging
import collections
# from threading import Thread, Event
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from threading import Semaphore
# from statistics import mean, median
import time as cpuTime
import json
import numpy as np
import socket
# import networkx as nx
# import matplotlib.pyplot as plt
import queue
from myTangle import myDAG
import paramsSystem as sysParams

# logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
logging.basicConfig(format='%(asctime)s-%(levelname)s-%(message)s',level=logging.INFO)
lenQueue = 10
semaphore = Semaphore(lenQueue)


def simAMC(params):
    nTip = params.nTip
    print('##### In simAMC, tau=:', params.tau)
    # self, msgLambda=50, alpha=0.001, tip_selection='mcmc', nTipSelected = 2, nSubDagSize = 300, msgWndSize = 1, plot=False
    myDagObj = myDAG(params.Lambda,
                     params.Alpha,
                     'mcmc',
                     nTip,
                     params.subDagSize,
                     params.tau,
                     params.msgVisDelay,
                     False, # no real attachment
                     True)

    # # t = Tangle(rate=10, tip_selection='mcmc', plot=True)
    for i in range(myDagObj.nSubDagSize):
        # print('defaultRate:', myDagObj.rate)
        myDagObj.next_transaction()

    print('Msg#: {a}, Tip#: {b}'.format(a=myDagObj.count, b=len(myDagObj.tips())))
    offsetTime = myDagObj.time
    # print('Reset Dag time ...')
    # myDagObj.time = 1
    # # myDagObj.plot()

    itr = params.test_i
    msgTrafficFileName = './msgTrafficsData/time_list_{a}_{b}_{c}.json'.format(a=params.Lambda,
                                                                               b=params.nMsg,
                                                                               c=itr)
    print('On {a}, Open msgTraffic File: {b}'.format(a=myDagObj.time
                                                     ,b=msgTrafficFileName))
    with open(msgTrafficFileName) as f:
        msgQueue = json.load(f)

    # msgQueue = [x + 1 for x in msgQueue]

    # KPIs
    windowPiCalTimeList = []
    msgProcStartTimeTickList = []
    msgProcDoneTimeTickList = []
    tipCountTrace = []

    nPiCal = 0
    # idxWind = int((myDagObj.time-1) // params.tau)
    idxWind = 0
    simStart = cpuTime.time()
    print('Initial window#:', idxWind)
    print('total message length:', len(msgQueue))
    for msg in msgQueue:
        msgIdx = msgQueue.index(msg)
        tMsgArrival = msg + offsetTime - 1
        if tMsgArrival > myDagObj.time:
            myDagObj.time = tMsgArrival
        actualWindow = int((myDagObj.time-offsetTime) // params.tau)
        print('\n\nActual window is:', actualWindow)
        if actualWindow > idxWind or msgQueue.index(msg) == 0:
            print('Wind# {a} expired, Update Pi* at window#: {b}'.format(a=idxWind, b=actualWindow))
            idxWind = actualWindow
            calPiTimeStart = cpuTime.time()
            # print('Update pi* for msgWindow ... ', idxWind)
            myDagObj.updatePi()
            calPiTimeEnd = cpuTime.time()
            windowPiCalTimeList.append(calPiTimeEnd - calPiTimeStart)
            myDagObj.time += calPiTimeEnd - calPiTimeStart
            nPiCal += 1
        else:
            print('Reuse Pi* at window#:  ', actualWindow)

        print('On {c}, proc msg arrived at: {a} in window#-{b}'.format(a=tMsgArrival, b=actualWindow, c=myDagObj.time))
        msgProcStartTimeTickList.append(myDagObj.time)
        msgiSamplingTimeStart = cpuTime.time()
        myDagObj.tsaSampling()
        msgiSamplingTimeEnd = cpuTime.time()
        myDagObj.time += msgiSamplingTimeEnd - msgiSamplingTimeStart
        myDagObj.count += 1
        msgProcDoneTimeTickList.append(myDagObj.time)
        simEnd = cpuTime.time()
        totalProcTime = simEnd - simStart
        tipCountTrace.append([myDagObj.time, len(myDagObj.tips())])
        print('tip amount', len(myDagObj.tips()))
        # correct the message arrival time for evaluation later
        msgQueue[msgIdx] = tMsgArrival
    return msgQueue, windowPiCalTimeList, msgProcStartTimeTickList, msgProcDoneTimeTickList, nPiCal, totalProcTime, tipCountTrace


def simRW(params):
    nTip = params.nTip
    myDagObj = myDAG(params.Lambda, params.Alpha, 'mcmc', nTip, params.subDagSize, params.tau, params.msgVisDelay, False, True)

    # t = Tangle(rate=10, tip_selection='mcmc', plot=True)
    for i in range(myDagObj.nSubDagSize):
        # print('defaultRate:', myDagObj.rate)
        myDagObj.next_transaction()

    print('Dag Time after creating the initial Sub-Dag:', myDagObj.time)
    print('Msg#: {a}, Tip#: {b}'.format(a=myDagObj.count, b=len(myDagObj.tips())))
    offsetTime = myDagObj.time

    msgTrafficFileName = './msgTrafficsData/time_list_{a}_{b}_{c}.json'.format(a=params.Lambda, b=params.nMsg, c=params.test_i)
    print('Open msgTraffic File: ', msgTrafficFileName)
    with open(msgTrafficFileName) as f:
        msgQueue = json.load(f)

    print('Total Msg#:', len(msgQueue))

    # KPIs
    msgProcTimeList = []
    msgProcStartTimeTickList = []
    msgProcDoneTimeTickList = []
    tipCountTrace = []
    simStart = cpuTime.time()
    for tMsgArrival in msgQueue:
        msgIdx = msgQueue.index(tMsgArrival)
        tMsgArrival = tMsgArrival + offsetTime - 1
        msgProcTimeStart = cpuTime.time()
        if tMsgArrival > myDagObj.time:
            myDagObj.time = tMsgArrival
        msgProcStartTime = myDagObj.time
        print('\n\n->>>> On {c}, processing msgIdx-{a} arriving at {b}'.format(a=msgIdx+1, b=tMsgArrival, c=myDagObj.time))
        myDagObj.tsaRW(tMsgArrival)
        msgProcTimeEnd = cpuTime.time()
        msgProcDeltaTime = msgProcTimeEnd - msgProcTimeStart
        # move the dag time to the end of the current msg end proc time
        myDagObj.time += msgProcDeltaTime
        msgProcDoneTime = myDagObj.time
        msgProcStartTimeTickList.append(msgProcStartTime)
        msgProcDoneTimeTickList.append(msgProcDoneTime)
        msgProcTimeList.append(msgProcDeltaTime)
        simEnd = cpuTime.time()
        totalProcTime = simEnd - simStart

        # increase msg#
        myDagObj.count += 1
        # print('tip amount', len(myDagObj.tips()))
        # tipCountTrace.append([myDagObj.time, len(myDagObj.tips())])
        msgQueue[msgIdx] = tMsgArrival
    return msgQueue, msgProcStartTimeTickList, msgProcDoneTimeTickList, msgProcTimeList, totalProcTime, tipCountTrace



def simRwwithDag_parallel(initDag, msgQueue, MAX_EXE_NUM=10):
    gTime = initDag.time
    gRate = initDag.rate
    gAlpha = initDag.alpha


    votingNum = sysParams.testnTipSelected
    myDagObj = myDAG(gRate, gAlpha, 'mcmc', votingNum, 0, 1.0, 1.0, False, True)
    myDagObj.time  = gTime
    myDagObj.G = initDag.G
    myDagObj.genesis = initDag.genesis
    myDagObj.transactions = initDag.transactions
    myDagObj.count = initDag.count    
   
    offsetTime = myDagObj.time
    msgAcuquiringTimeList_dict = {}
    msgThLaunchTimeList_dict = {}
    msgProcTimeList_dict = {}
    msgProcStartTimeTickList_dict = {}
    msgProcDoneTimeTickList_dict = {}
       
    # callback for completed tasks
    def task_complete_callback(future):
        global semaphore
        # release the semaphore
        semaphore.release()


    futureList = []  
    simStart = cpuTime.time()     
    now = myDagObj.time
    beginning = now
    global semaphore    
    with ThreadPoolExecutor(max_workers=MAX_EXE_NUM, thread_name_prefix='rwExeTh') as exePool:
        logging.info('using ThreadPool ...')
        for nxtMsgArrival_t in msgQueue:
            msgIdx = msgQueue.index(nxtMsgArrival_t)            
            nxtMsgArrival_t_shifted = nxtMsgArrival_t + offsetTime - 1
            # logging.debug(f'processing Msg#-{msgIdx} arrived at {nxtMsgArrival_t_shifted}')
            
            if nxtMsgArrival_t_shifted > now:
                print('-.-', end=' ')
                waitTime = nxtMsgArrival_t_shifted - now            
                cpuTime.sleep(waitTime)
                now = nxtMsgArrival_t_shifted
            
            # logging.info(f'Acquiring for Th-Msg#{msgIdx} ...')
            acquiringTime = cpuTime.time()
            semaphore.acquire()            
            # logging.info(f"Acquired, submit Th-Msg#{msgIdx}")
            # future = submit_proxy(task, executor, (i))
            future = exePool.submit(myDagObj.tsaRW, msgIdx, nxtMsgArrival_t_shifted)
            # future = executor.submit(task, (i))
            future.add_done_callback(task_complete_callback)
            futureList.append(future)
            acquiredTime = cpuTime.time()
            submitTime = acquiredTime - acquiringTime                
            now += submitTime
            msgAcuquiringTimeList_dict[msgIdx] = acquiringTime
            msgQueue[msgIdx] = nxtMsgArrival_t_shifted

        # iterate over all submitted tasks and get results as they are available
        for future in concurrent.futures.as_completed(futureList):
            # get the result for the next completed task
            msgIdx_org, nxtMsgArrival_t_shifted, time_rw_start, time_rw_end = future.result() # blocks
            # logging.debug(f'Msg#-{msgIdx_org}:{nxtMsgArrival_t_shifted}, start-{time_rw_start - simStart + offsetTime}, end-{time_rw_end - simStart + offsetTime}, duration:{time_rw_end - time_rw_start}')
            # logging.info(f'vv-{msgIdx_org}')
            msgThLaunchTimeList_dict[msgIdx_org] = time_rw_start - msgAcuquiringTimeList_dict[msgIdx_org]
            msgProcTimeList_dict[msgIdx_org] = time_rw_end - time_rw_start
            msgProcStartTimeTickList_dict[msgIdx_org] = time_rw_start - simStart + offsetTime
            msgProcDoneTimeTickList_dict[msgIdx_org] = time_rw_end - simStart + offsetTime
        print('\n')

    simEnd = cpuTime.time()
    
    msgThLaunchTimeList_dict = collections.OrderedDict(sorted(msgThLaunchTimeList_dict.items()))
    msgThLaunchTimeList = list(msgThLaunchTimeList_dict.values())

    msgProcStartTimeTickList_dict = collections.OrderedDict(sorted(msgProcStartTimeTickList_dict.items()))
    msgProcStartTimeTickList = list(msgProcStartTimeTickList_dict.values())

    # logging.debug(f'\n->>>>> \n coverted startingTimeList: {msgProcStartTimeTickList}')
    msgProcDoneTimeTickList_dict = collections.OrderedDict(sorted(msgProcDoneTimeTickList_dict.items()))
    msgProcDoneTimeTickList = list(msgProcDoneTimeTickList_dict.values())

    msgProcTimeList_dict = collections.OrderedDict(sorted(msgProcTimeList_dict.items()))
    msgProcTimeList = list(msgProcTimeList_dict.values())

    totalSimTime = simEnd - simStart
    totalMsgRwTime = sum(msgProcTimeList)
    # the last message's finishing time back to the beginning
    totalFinishingTime = msgProcDoneTimeTickList[-1] - beginning
    return msgThLaunchTimeList, msgProcStartTimeTickList, msgProcDoneTimeTickList, msgProcTimeList, totalMsgRwTime, totalFinishingTime

def simRWwithDag(initDag, msgQueue):
    gTime = initDag.time
    gRate = initDag.rate
    gAlpha = initDag.alpha


    votingNum = sysParams.testnTipSelected
    myDagObj = myDAG(gRate, gAlpha, 'mcmc', votingNum, 0, 1.0, 1.0, False, True)
    myDagObj.time  = gTime
    myDagObj.G = initDag.G
    myDagObj.genesis = initDag.genesis
    myDagObj.transactions = initDag.transactions
    myDagObj.count = initDag.count
    
    # print('Dag Time after creating the initial Sub-Dag:', myDagObj.time, end=' ')
    # print('Msg#: {a}, Tip#: {b}'.format(a=myDagObj.count, b=len(myDagObj.tips())))
    offsetTime = myDagObj.time

    # msgTrafficFileName = './msgTrafficsData/time_list_{a}_{b}_{c}.json'.format(a=params.Lambda, b=params.nMsg, c=params.test_i)
    # print('Open msgTraffic File: ', msgTrafficFileName)
    # with open(msgTrafficFileName) as f:
    #     msgQueue = json.load(f)

    # print('Total Msg#:', len(msgQueue))

    # KPIs
    beginning = offsetTime
    msgProcTimeList = []
    msgProcStartTimeTickList = []
    msgProcDoneTimeTickList = []
    simStart = cpuTime.time()
    for tMsgArrival in msgQueue:
        msgIdx = msgQueue.index(tMsgArrival)
        tMsgArrival = tMsgArrival + offsetTime - 1
        msgProcTimeStart = cpuTime.time()
        if tMsgArrival > myDagObj.time:
            myDagObj.time = tMsgArrival
        msgProcStartTime = myDagObj.time
        # print('->>>> On {c}, processing msgIdx-{a} arriving at {b}'.format(a=msgIdx+1, b=tMsgArrival, c=myDagObj.time))
        myDagObj.tsaRW(msgIdx, tMsgArrival)
        msgProcTimeEnd = cpuTime.time()
        msgProcDeltaTime = msgProcTimeEnd - msgProcTimeStart
        # move the dag time to the end of the current msg end proc time
        myDagObj.time += msgProcDeltaTime
        msgProcDoneTime = myDagObj.time
        msgProcStartTimeTickList.append(msgProcStartTime)
        msgProcDoneTimeTickList.append(msgProcDoneTime)
        msgProcTimeList.append(msgProcDeltaTime)
        
        # increase msg#
        myDagObj.count += 1
        # print('tip amount', len(myDagObj.tips()))
        # tipCountTrace.append([myDagObj.time, len(myDagObj.tips())])
        msgQueue[msgIdx] = tMsgArrival
    simEnd = cpuTime.time()
    print('\n')

    totalSimTime = simEnd - simStart
    totalMsgRwTime = sum(msgProcTimeList)
    totalFinishingTime = msgProcDoneTimeTickList[-1] - beginning
    return msgProcStartTimeTickList, msgProcDoneTimeTickList, msgProcTimeList, totalMsgRwTime, totalFinishingTime

def simAmcwithDag(initDag, tau, msgQueue):
    gTime = initDag.time
    gRate = initDag.rate
    gAlpha = initDag.alpha

    votingNum = sysParams.testnTipSelected
    myDagObj = myDAG(gRate, gAlpha, 'mcmc', votingNum, 0, tau, 1.0, False, True)
    myDagObj.time  = gTime
    myDagObj.G = initDag.G
    myDagObj.genesis = initDag.genesis
    myDagObj.transactions = initDag.transactions
    myDagObj.count = initDag.count
    
    # print('\n->>>>>>>Dag Time after creating the initial Sub-Dag:', myDagObj.time, end=' ')
    # print('Msg#: {a}, Tip#: {b}'.format(a=myDagObj.count, b=len(myDagObj.tips())))
    offsetTime = myDagObj.time

    # msgTrafficFileName = './msgTrafficsData/time_list_{a}_{b}_{c}.json'.format(a=params.Lambda, b=params.nMsg, c=params.test_i)
    # print('Open msgTraffic File: ', msgTrafficFileName)
    # with open(msgTrafficFileName) as f:
    #     msgQueue = json.load(f)

    # print('Total Msg#:', len(msgQueue))

    # KPIs
    windowPiCalTimeList = []
    msgProcStartTimeTickList = []
    msgProcDoneTimeTickList = []
    # tipCountTrace = []

    nPiCal = 0
    # idxWind = int((myDagObj.time-1) // params.tau)
    idxWind = 0
    simStart = cpuTime.time()
    # print('Initial window#:', idxWind)
    # print('total message length:', len(msgQueue))
    for msg in msgQueue:
        msgIdx = msgQueue.index(msg)
        tMsgArrival = msg + offsetTime - 1
        if tMsgArrival > myDagObj.time:
            myDagObj.time = tMsgArrival
        actualWindow = int((myDagObj.time-offsetTime) // tau)
        # print('CurTau:{a}, actual window is:{b}'.format(a=tau, b=actualWindow) )
        if actualWindow > idxWind or msgQueue.index(msg) == 0:
            # print('\nWind# {a} expired, Update Pi* at window#: {b}'.format(a=idxWind, b=actualWindow))
            idxWind = actualWindow
            calPiTimeStart = cpuTime.time()            
            myDagObj.updatePi()
            calPiTimeEnd = cpuTime.time()            
            PitCalTime = calPiTimeEnd - calPiTimeStart
            hostname=socket.gethostname()
            IPAddr=socket.gethostbyname(hostname)
            # if IPAddr == '192.168.55.38':
            #     PitCalTime = PitCalTime * (1 - np.random.normal(0.3, 0.05, 1))
            windowPiCalTimeList.append(PitCalTime)
            myDagObj.time += PitCalTime
            nPiCal += 1
        else:
            print(f'{actualWindow}reuse Pi*', end=' ++ ')

        # print('On {c}, proc msg arrived at: {a} in window#-{b}'.format(a=tMsgArrival, b=actualWindow, c=myDagObj.time))
        msgProcStartTimeTickList.append(myDagObj.time)
        msgiSamplingTimeStart = cpuTime.time()
        myDagObj.tsaSampling()
        msgiSamplingTimeEnd = cpuTime.time()
        # print('Sampling Time: ', msgiSamplingTimeEnd - msgiSamplingTimeStart, end='\n')
        myDagObj.time += msgiSamplingTimeEnd - msgiSamplingTimeStart
        myDagObj.count += 1
        msgProcDoneTimeTickList.append(myDagObj.time)
        msgQueue[msgIdx] = tMsgArrival

    simEnd = cpuTime.time()
    # totalProcTime = simEnd - simStart
    totalFinishingTime = myDagObj.time - offsetTime
    # tipCountTrace.append([myDagObj.time, len(myDagObj.tips())])
    print(f'totalFinishingTime: {totalFinishingTime}')
    # correct the message arrival time for evaluation later
    return windowPiCalTimeList, msgProcStartTimeTickList, msgProcDoneTimeTickList, nPiCal, totalFinishingTime

def amcKPIs(msgArrivalTimeList, windowPiCalTimeList, msgProcStartTimeTickList, msgProcDoneTimeTickList):

    msgAttachDelayList = [a_i - b_i for a_i, b_i in zip(msgProcDoneTimeTickList, msgArrivalTimeList)]
    # msgAttachDelayList = np.array(msgProcDoneTimeTickList) - np.array(msgArrivalTimeList)
    msgSamplingTimeList = [a_i - b_i for a_i, b_i in zip(msgProcDoneTimeTickList, msgProcStartTimeTickList)]
    # msgSamplingTimeList = np.array(msgProcDoneTimeTickList) - np.array(msgProcStartTimeTickList)
    
    print(f'msgAttachDelayList Type: {type(msgAttachDelayList)}', end='|| ')
    print(f'msgSamplingTimeList Type: {type(msgSamplingTimeList)}')

    # msgAttachDelayList = msgAttachDelayList.tolist()
    # msgSamplingTimeList = msgSamplingTimeList.tolist()

    meanMad = np.mean(msgAttachDelayList)    
    medianMad = np.median(msgAttachDelayList)
    maxMad = max(msgAttachDelayList)
    PitCalTime = sum(windowPiCalTimeList)
    SamplingTime = sum(msgSamplingTimeList)
    meanMsgUnitProcTime = (PitCalTime + SamplingTime) / len(msgArrivalTimeList)

    meanMad = np.round(meanMad,5)    
    medianMad = np.round(medianMad,5)    
    maxMad = np.round(maxMad,5)
    PitCalTime = np.round(PitCalTime,5)
    SamplingTime = np.round(SamplingTime,5)
    meanMsgUnitProcTime = np.round(meanMsgUnitProcTime, 5)

    print(f"\n\nmedian delay: {medianMad} | type: {type(medianMad)}")
    print(f"mean delay: {meanMad} | type: {type(meanMad)}")
    print(f"max delay: {maxMad} | type: {type(maxMad)}")
    print(f'Pit Calculation Time: {PitCalTime} | type: {type(PitCalTime)}')
    print(f'Tip Sampling Time: {SamplingTime} | type: {type(SamplingTime)}')
    print(f'Mean Msg Unit Proc Time: {meanMsgUnitProcTime} | type: {type(meanMsgUnitProcTime)}')
    return meanMad.item(0), medianMad.item(0), maxMad.item(0), msgAttachDelayList, PitCalTime.item(0), SamplingTime.item(0), meanMsgUnitProcTime.item(0)


def rwKPIs(msgQueue, msgProcDoneTimeTickList, msgProcTimeList):
    msgAttachDelayList = np.array(msgProcDoneTimeTickList) - np.array(msgQueue)    
    meanMad = np.mean(msgAttachDelayList)
    medianMad = np.median(msgAttachDelayList)
    maxMad = np.max(msgAttachDelayList)
    totalMsgRwTime = np.sum(msgProcTimeList)
    meanMsgUnitRwTime = round(np.mean(msgProcTimeList), 5)
    
    meanMad = round(meanMad, 5)
    medianMad = round(medianMad, 5)
    maxMad = round(maxMad, 5)
    

    # print("min delay", np.min(msgAttachDelayList))
    print("median delay", medianMad)
    print("mean delay", meanMad)
    print("max delay", maxMad)
    # print("TipTrace:", tipCountTrace)
    print(f'mean rw time: {meanMsgUnitRwTime}')
    return msgAttachDelayList, meanMad, medianMad, maxMad, totalMsgRwTime, meanMsgUnitRwTime