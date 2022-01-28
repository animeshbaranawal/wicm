import sys

def readFile(filePath):
    file_handler = open(filePath, 'r')
    data = []
    for line in file_handler:
        l = [float(x) for x in line.strip().split(",")]
        data.append(l[0:8])
    file_handler.close()

    return data

def readFilePair(filePath):
    file_handler = open(filePath, 'r')
    dataM = []
    dataL = []
    for line in file_handler:
        l = [float(x) for x in line.strip().split(",")]
        dataM.append(l[0:8])
        dataL.append(l[8:16])
    file_handler.close()

    return dataM, dataL

original = readFile("dmpLarge.txt")
CBW_M, CBW_L = readFilePair("dmpLarge_CBW.txt")
CBPW_M, CBPW_L = readFilePair("dmpLarge_CBPW.txt")
CBAW_M, CBAW_L = readFilePair("dmpLarge_CBAW.txt")

CBW_M_sum = 0
CBW_L_sum = 0
CBPW_M_sum = 0
CBPW_L_sum = 0
CBAW_M_sum = 0
CBAW_L_sum = 0
for i in range(len(original)):
    if original[i][4] == 0:
        continue

    CBW_M_sum += (original[i][4] - CBW_M[i][4]) / original[i][4]
    CBW_L_sum += (original[i][4] - CBW_L[i][4]) / original[i][4]
    CBPW_M_sum += (original[i][4] - CBPW_M[i][4]) / original[i][4]
    CBPW_L_sum += (original[i][4] - CBPW_L[i][4]) / original[i][4]
    CBAW_M_sum += (original[i][4] - CBAW_M[i][4]) / original[i][4]
    CBAW_L_sum += (original[i][4] - CBAW_L[i][4]) / original[i][4]

CBW_M_sum /= len(original)
CBW_L_sum /= len(original)
CBPW_M_sum /= len(original)
CBPW_L_sum /= len(original)
CBAW_M_sum /= len(original)
CBAW_L_sum /= len(original)

print(CBW_M_sum)
print(CBW_L_sum)
print(CBPW_M_sum)
print(CBPW_L_sum)
print(CBAW_M_sum)
print(CBAW_L_sum)
