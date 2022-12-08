import pandas as pd

dir_proRlts = './procResults'
dir_simRlts = './simResults/withInitDag'



def writeToCVS(fullpath, df):
    with open(fullpath, mode='a',newline='') as f:
        df.to_csv(f, sep=',', encoding='utf-8-sig', line_terminator="", header=f.tell() == 0, index=False)

def readFromCVS(fullpath):
    readDf = pd.read_csv(fullpath)
    return readDf


# def makelogDf(logData, cols):
#     if logData.empty is True:
#         print('No logData is provided ...')
#         return
#     else:    
#         logDf = pd.DataFrame(columns=cols)
#         logTest_i = pd.DataFrame([logData], columns=cols)
#         logDf = pd.concat([logTest_i], ignore_index=True)
#     return logDf


def ckUniqueValuesInColumn(filename):
    print('Read From: ', filename)

    readDf = readFromCVS(filename)
    colList = readDf.columns.to_list()
    print('Cols: ', colList)

    for col in colList:
        if len(readDf[col].unique()) < 20:
            print('Col:{a} || {b}'.format(a=col, b=readDf[col].unique()))

def ckRowNum(filename):
    print('\n =============================')
    print('Read From: ', filename)
    readDf = readFromCVS(filename)
    print('# of Rows: ',len(readDf.index))



# file = './evalParamsFiles/batchTestFiles2/EvalParams_TrafRates-[20, 40, 60, 80, 100]_durations-[10, 20, 30]_nCases-10_dagSize-[500, 1000, 1500, 2000, 2500, 3000]_growRates-[20, 40, 60, 80, 100]_nTips-[2, 4]_dagCases-10_Batch#-5.csv'
# # file = './simResults/withInitDag2/rwTestLogs_batch#-0_part-1.csv'
# ckRowNum(file)
# ckUniqueValuesInColumn(file)