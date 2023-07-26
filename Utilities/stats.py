# Import library
import scipy.stats as stats
import numpy as np


# Conduct Welch's t-Test and print the result
def indicador_cambio_tendencia(data_group1, data_group2):
    """ Calculo de indicador de Welch
    data_group1: arreglo de valores 1
    data_group2: arreglo de valores 2
    return: diccionario con valores 'statistic' y 'pvalue'
    """
    data1 = np.array(data_group1)
    data2 = np.array(data_group2)
    results = stats.ttest_ind(data1, data2, equal_var = False)
    res_dct = {"statistic":results.statistic, "pvalue":results.pvalue}
    return res_dct


# Creating data groups
# data_group1 = np.array([14, 15, 15, 16, 13, 8, 14, 17,16, 14, 19, 20, 21, 15, 15])
# data_group2 = np.array([36, 37, 44, 27, 24, 28, 27,39, 29, 24, 37, 32, 24, 26,33])
