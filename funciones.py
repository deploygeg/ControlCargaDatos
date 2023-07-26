import numpy as np
#funcion para obtener el promedio aplicando la lib numpy
#el parametro a ingresar en la funcion es un objeto array
def promedioArray(lista):
    a=np.array(lista)
    prom=np.mean(a)
    #prom= sum(lista)/float(len(lista))
    return prom