import numpy as np
from matplotlib import pyplot as plt
from glob import glob


folder = "Data Hidro Febrero/Piezometros"

files = glob("{}/*.csv".format(folder))

for fileName in files:
    with open(fileName, 'r') as fp:
        elemsId = fp.readline()[:-1].split(',')
        fp.readline()
        elemsMod = fp.readline()[:-1].split(',')
        print(elemsId[1], elemsMod[1])