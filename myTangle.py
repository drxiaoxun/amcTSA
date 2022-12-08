import time as cpuTime
import numpy as np
import networkx as nx
from scipy.sparse import csr_matrix, isspmatrix_csr
import matplotlib.pyplot as plt
import threading
from tangle import Tangle, Transaction, Genesis


# used for recording the number of edges evaluated in test
global rwPathEdgeEvalsNum
rwPathEdgeEvalsNum = []
# used for recording the number of multiplications evaluated in test
global amcMultiplyNum
amcMultiplyNum = []

class myDAG(Tangle):
    def __init__(self, msgLambda=50, alpha=0.001, tsaMethod='mcmc', nTipSelected=3, nSubDagSize=300, msgWndSize=1, msgVisDelay=1.0, bAttaching=True, plot=False):
        self.time = 1.0
        self.defaultRate = msgLambda
        self.rate = 50
        self.alpha = alpha

        # added variable
        self.nTipSelected = nTipSelected
        self.nSubDagSize = nSubDagSize
        self.msgWndSize = msgWndSize
        self.msgVisDelay = msgVisDelay
        self.bAttaching  = bAttaching
        # initialized with tspd of genesis in the dag
        self.subDagGraph = nx.DiGraph()
        self.subDagIdxList = []
        self.orgDagIdxList = []
        # used for subDag to Dag msg index mapping lookup
        self.nodeIdxDict = {}
        self.subDagPt = []
        self.Pi_t = [1]

        if plot:
            self.G = nx.DiGraph()

        self.genesis = Genesis(self)
        self.transactions = [self.genesis]
        self.count = 1
        self.tip_selection = tsaMethod

        self.cw_cache = dict()
        self.t_cache = set()
        self.tip_walk_cache = list()

    def tsaRW(self, msgIdx, tMsgArrival):
        time_rw_start = cpuTime.time()
        # approved_tips = set(self._mcmcSubDag())
        if self.bAttaching is False:
        # Xun Xiao: August 1st, does not care about which tips are selected
            self._mcmcSubDag()
        else:
            approved_tips = set(self._mcmcSubDag())
        time_rw_end = cpuTime.time()
        msgTsaRwTime = time_rw_end - time_rw_start
        # print(f'Msg#-{msgIdx}:{tMsgArrival} took {msgTsaRwTime} Done')
       

        if self.bAttaching:
            # print('tsaRW(), msgVisDelay:', self.msgVisDelay)
            transaction = myMsg(self, tMsgArrival, approved_tips, self.count + 1, self.msgVisDelay)
            for tips in approved_tips:
                # tips.approved_time = np.minimum(tMsgArrival + msgTSATime, tips.approved_time)
                tips.approved_time = np.minimum(self.time + msgTsaRwTime, tips.approved_time)
                # print('----------------------time,approve_time',self.time, t.approved_time)
                # t.approved_time = np.maximum(self.time, t.approved_time)
                tips._approved_directly_by.add(transaction)
                if hasattr(self, 'G'):
                    self.G.add_edges_from([(transaction.num, tips.num)])

            self.transactions.append(transaction)
            print("msg-{a} added".format(a=transaction))
        # else:
        #     print("Skip attachment Msg#-{a}".format(a=self.count))
        self.cw_cache = {}
        print(f'{msgIdx}-///', end=' ')        
        return msgIdx, tMsgArrival, time_rw_start, time_rw_end

    def _mcmcSubDag(self):
        # print("----------------------------mcmc subtangle")
        num_particles = self.nTipSelected
        if self.bAttaching:
            lower_bound = int(np.maximum(0, self.count - self.nSubDagSize - 10))
            upper_bound = int(np.maximum(1, self.count - self.nSubDagSize))
        else:
            lower_bound = 0
            upper_bound = 1
        # print("Choose starting site between: {a}-{b}".format(a=lower_bound, b=upper_bound))
        candidates = self.transactions[lower_bound:upper_bound]

        # candidates = self.genesis
        # if self.count -self.sub_tangle > 0:
        #     print(self.count - self.sub_tangle)
        #     candidates = self.transactions[self.count - self.sub_tangle]
        # print('beginning walker is ------------', candidates)
        # at_least_5_cw = [t for t in self.transactions[lower_bound:upper_bound] if t.cumulative_weight_delayed() >= 5]

        particles = list(np.random.choice(candidates, num_particles))
        # print('Starting Points', particles)
        # particles = candidates
        for i, p in enumerate(particles):
            # print('RW#{a} ////'.format(a=i), end=' ')
            # self._constructSubDagGraph(p)
            # sorted(list(nx.predecessor(self.G, p.num)))
            # print('Ancestors from ', p)
            # print(sorted(list(nx.ancestors(self.G, p.num))))
            # t = threading.Thread(target=self._walk2(p))
            # t.start()
            self._walk2xx(p)
            # print('single-RW trace: ', rwPathEdgeEvalsNum)
            # print('RW path edges# ', rwPathEdgeEvalsNum)
        # print('### ')
        ## 返回nTip个tip
        tips = self.tip_walk_cache[:self.nTipSelected]
        # print("--------------selected tips", tips)
        self.tip_walk_cache = list()
        return tips

    def _walk2xx(self, starting_transaction):
        p = starting_transaction
        # if p.is_tip_delayed() or not p.is_visible():
        #     print('is {a} tip delayed {b}'.format(a=p, b=p.is_tip_delayed()))
        #     print('is {a} tip visible {b}'.format(a=p, b=p.is_visible()))
        #     print('Never RW <---')

        while not p.is_tip_delayed() and p.is_visible():
            # while not p.is_tip() and p.is_visible():
            # print('p---------------------------------',p)
            # print('cw cache', self.cw_cache)            
            # if len(self.tip_walk_cache) >= self.nTipSelected:
            #     print('return due to non-empty tip_walk_cache')
            #     return

            next_transactions = p.approved_directly_by()
            rwPathEdgeEvalsNum.append(len(next_transactions)+1)
            # print('From {a} --> Jump candidates: {b}'.format(a=p, b = next_transactions))
            if self.alpha > 0:
                p_cw = p.cumulative_weight_delayed()
                # print('p.cumulative_weight_delayed()--------------------------------', p_cw)
                c_weights = np.array([])
                nEvalEdges = len(next_transactions)
                for transaction in next_transactions:
                    c_weights = np.append(c_weights, transaction.cumulative_weight_delayed())

                deno = np.sum(np.exp(-self.alpha * (p_cw - c_weights)))
                probs = np.divide(np.exp(-self.alpha * (p_cw - c_weights)), deno)
            else:
                probs = None

            # p = np.random.choice(a=next_transactions, p=probs)
            # # print("pro", probs)
            # ## 一直random walk 到符合条件的tip not tip delay 和 is visible
            if next_transactions:
                p = np.random.choice(a=next_transactions, p=probs)
                # print("Jump-->", p)
            else:
                print('Stop at a tip, break ...')
                rwPathEdgeEvalsNum.append(1)
                break
        self.tip_walk_cache.append(p)

    def _constructSubDagGraph(self, subDagSrc):
        ancestor_start = cpuTime.time()
        ancestors = sorted(list(nx.ancestors(self.G, subDagSrc.num)))
        subDag = [subDagSrc.num] + ancestors
        ancestor_end = cpuTime.time()
        # print('Time Ancestor: ', ancestor_end - ancestor_start)
        
        gConstruct_start = cpuTime.time()
        self.subDagGraph.clear()
        for vIdx in subDag:
            # print('calculating jump prob of:', vIdx)
            # print('num# of txs:', len(self.transactions))
            p = self.transactions[vIdx]
            # print('visit msg:', p)
            # start to traverse all vertices in the subDag to update the transition matrix
            if not p.is_tip_delayed() and p.is_visible():
                # while not p.is_tip() and p.is_visible():
                # print('p---------------------------------',p)
                # print('cw cache', self.cw_cache)

                # next_transactions = p.approved_directly_by()
                approvers = p.approved_directly_by()
                # if self.alpha > 0:
                p_cw = p.cumulative_weight_delayed()
                c_weights = np.array([])
                for j in approvers:
                    c_weights = np.append(c_weights, j.cumulative_weight_delayed())
                deno = np.sum(np.exp(-self.alpha * (p_cw - c_weights)))
                probs = np.divide(np.exp(-self.alpha * (p_cw - c_weights)), deno)
                # print('transition probs of {a}: {b}'.format(a=p, b=probs) )
                # else:
                #     print('alpha value not positive!')
                #     probs = None

                if not approvers:
                    # print('{a} is a tip ..., add an self-loop on: {a}-->{b}'.format(a=p.num, b=p.num))
                    self.subDagGraph.add_weighted_edges_from([(p.num, p.num, 1)])
                else:
                    for j in approvers:
                        idx = approvers.index(j)
                        # print('edge {a}-->{b} jump prob.{c}'.format(a=p.num, b=j.num, c=probs[idx]))
                        self.subDagGraph.add_weighted_edges_from([(p.num, j.num, probs[idx])])
            elif p.is_tip_delayed() and p.is_visible():
                # print('msg {a} is tip delayed'.format(a=p))
                self.subDagGraph.add_weighted_edges_from([(p.num, p.num, 1)])
            # else:
            #     print('msg {a} is not visible'.format(a=p))
            #     # print('p is a tip ..., add an self-loop on: {a}-->{b}'.format(a=p.num, b=p.num))
            #     self.subDagGraph.add_weighted_edges_from([(p.num, p.num, 1)])
        gConstruct_end = cpuTime.time()
        # print('Time gConstruct: ', gConstruct_end - gConstruct_start)

        # print('subDagGraph Edge Set:', self.subDagGraph.edges)

        # save the idx mapping from subDagGraph to orgDagGraph
        curSize = len(self.subDagGraph.nodes)
        self.subDagIdxList = list(range(0, curSize))
        # print('newNodeIdxList', subDagIdxList)
        self.orgDagIdxList = subDag[0:curSize]
        # print('oldNodeIdxList', orgDagIdxList)
        self.nodeIdxDict = dict(zip(self.subDagIdxList, self.orgDagIdxList))

        # print(nx.is_connected(self.subDagGraph))
        # if subDagSrc.num > self.nSubDagSize:

        # print('is csr sparse mx:', isspmatrix_csr(self.subDagPt))
        # print(self.subDagPt.todense())

        # es = nx.dfs_edges(self.subDagGraph, source=0, depth_limit=curSize)
        # print('dfs es:',list(es))
        # if nodeIdxDict:
        #     print(nodeIdxDict)
        #     print('source:', oldNodeIdxList[0])
        #     print('target', oldNodeIdxList[-1])
        #     longestPath = nx.shortest_path_length(self.subDagGraph, source=list(nodeIdxDict)[0], target=list(nodeIdxDict)[-1])
        # print('longest path in subdaggraph', longestPath)

            #
            # for jump in range(longestPath):
            #     pi0 = csr_matrix.dot(pi0, self.subDagPt)
            # print(pi0)

        # self._calPi_t(nodeIdxDict)

        # print(nx.is_connected(self.subDagGraph))
        # if subDagSrc.num > self.nSubDagSize:
        # self.subDagPt = nx.to_scipy_sparse_matrix(self.subDagGraph)
        # print('is csr sparse mx:', isspmatrix_csr(self.subDagPt))
        # print(self.subDagPt.todense())
        # print(self.subDagPt)
        # print(nodeIdxDict)

        # return nodeIdxDict

    def _calPi_t(self):
        curSize = len(self.subDagGraph.nodes)
        # print('initialize pi0 with subDagGraph size ...', curSize)
        pi0 = np.zeros(curSize)
        pi0[0] = 1
        pi0 = np.array(pi0)
        # print('pi0', pi0)

        self.subDagPt = nx.to_scipy_sparse_matrix(self.subDagGraph)

        mxDot_start = cpuTime.time()
        while True:
            pit = csr_matrix.dot(pi0, self.subDagPt)
            # print(pit)
            dist = np.linalg.norm(pit - pi0)
            if dist < 0.001:
                # print('stationary Pi_t:', pit)
                # print('stationary Pi_t derived, Stop!')
                break
            else:
                pi0 = pit
        mxDot_end = cpuTime.time()
        # print('MxDot Time: ', mxDot_end - mxDot_start)

        self.Pi_t = pit
        # print('Pi_t Array:', self.Pi_t)
        return

    def updatePi(self):
        # according to the existing dag topology, update the current Pi
        if self.bAttaching:
            lower_bound = int(np.maximum(0, self.count - self.nSubDagSize - 10))
            upper_bound = int(np.maximum(1, self.count - self.nSubDagSize))
        else:
            lower_bound = 0
            upper_bound = 1
        # print("Choose starting site between: {a}-{b}".format(a=lower_bound, b=upper_bound))
        candidates = self.transactions[lower_bound:upper_bound]
        # print('beginning walker is ------------', candidates)
        subDagSource = np.random.choice(candidates, 1)

        # print('chosen subDagSrc:', subDagSource[0])
        constructG_start = cpuTime.time()
        self._constructSubDagGraph(subDagSource[0])
        constructG_end = cpuTime.time()
        # print('Time to constructG: ', constructG_end - constructG_start, end=' >>')
        self._calPi_t()
        calPit_end = cpuTime.time()
        # print('Time to calPit: ', calPit_end - constructG_end)
        return

    def tsaSampling(self):
        # sampling from pi* and attach the new message to the selected tips
        p = np.random.choice(a=self.subDagIdxList, size=self.nTipSelected, p=self.Pi_t)
        
        return
        selectedTips = []
        for tip_i in range(self.nTipSelected):
            # p = np.random.choice(a=self.subDagIdxList, size=self.nTipSelected, p=self.Pi_t)
            p = np.random.choice(a=self.subDagIdxList, p=self.Pi_t)
            orgMsgIdx = self.nodeIdxDict[p]
            selectedTips.append(self.transactions[orgMsgIdx])
        # print('Selected Tips:', selectedTips, end=' ')

        if self.bAttaching:
            newMsg = myMsg(self, self.time, selectedTips, self.count, self.msgVisDelay)
            for t in selectedTips:
                t.approved_time = np.minimum(self.time, t.approved_time)
                t._approved_directly_by.add(newMsg)
                if hasattr(self, 'G'):
                    self.G.add_edges_from([(newMsg.num, t.num)])
            self.transactions.append(newMsg)
            print("msg-{a} added".format(a=newMsg))
        # else:
            # print("Skip attachment of Msg#-{a}".format(a=self.count))
        return

class myMsg(Transaction):

    def __init__(self, tangle, tCreated, approved_transactions, Idx, msgVisDelay=1.0):
        self.tangle = tangle
        self.time = tCreated
        self.approved_transactions = approved_transactions
        self._approved_directly_by = set()
        self.approved_time = float('inf')
        self.num = Idx
        self._approved_by = set()
        self.msgVisDelay = msgVisDelay

        # print('configured msgVisDelay: {a}'.format(a=self.msgVisDelay))

    def is_visible(self):
        # print('is visible by msgVisDelay', self.tangle.time >= self.time + self.msgVisDelay)
        if self.tangle.count < self.tangle.nSubDagSize:
            bVisiable = self.tangle.time >= self.time + 1.0
        else:
            bVisiable = self.tangle.time >= self.time + self.msgVisDelay
        return bVisiable

    def is_tip_delayed(self):
        # print('is tip delayed by msgVisDelay', self.tangle.time - self.msgVisDelay < self.approved_time)
        if self.tangle.count < self.tangle.nSubDagSize:
            bTipDelayed = self.tangle.time - 1.0 < self.approved_time
        else:
            bTipDelayed = self.tangle.time - self.msgVisDelay < self.approved_time
        return bTipDelayed
