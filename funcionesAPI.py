#!/usr/bin/python
import json
from flask import jsonify
import psycopg2
import yaml
import csv
import pandas as pd
yaml_file=open('C:/Users/gmalaga/Documents/api_ges/app/ControlCargaDatos/config.yml','r')
yaml_content = yaml.load(yaml_file, Loader=yaml.FullLoader)
CONFIG_HOST = yaml_content["host"]
CONFIG_DB = yaml_content["database"]
CONFIG_USER = yaml_content["user"]
CONFIG_PASSWORD = yaml_content["password"]
CONFIG_PORT = yaml_content["port"]
#Los que empiezan con X se refiere a la DB de Test
XCONFIG_HOST = yaml_content["Xhost"]
XCONFIG_DB = yaml_content["Xdatabase"]
XCONFIG_USER = yaml_content["Xuser"]
XCONFIG_PASSWORD = yaml_content["Xpassword"]
XCONFIG_PORT = yaml_content["Xport"]


def conectar():
    """ Conexión al servidor de pases de datos PostgreSQL """
    conexion = None
    try:
        # Lectura de los parámetros de conexion 
        # Conexion al servidor de PostgreSQL
        print('Conectando a la base de datos PostgreSQL con los siguientes datos:  ')
        
        conexion = psycopg2.connect(
            host=CONFIG_HOST, 
            database=CONFIG_DB, 
            user=CONFIG_USER, 
            password=CONFIG_PASSWORD,
            port=CONFIG_PORT
            )
        # creación del cursor
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conexion is not None:
            return conexion.cursor()
    """
    cur = conectar()        
    # Ejecución de una consulta con la version de PostgreSQL
    print('La version de PostgreSQL es la:')
    cur.execute('SELECT version()') 
    # Ahora mostramos la version
    version = cur.fetchone()
    print(version)
       
    # Cierre de la comunicación con PostgreSQL
    cur.close()
    print('Conexión finalizada.')
    """
def get_all_piezometros_activos():
    cur = conectar()
    #Ejecucion de un aconsulta par aobtener loa piezpometros activos
    cur.execute('(SELECT p.nombre_piezometro,t.id,p.field_4 as Tipo FROM piezometros_mel.piezometros_consolidado_test as t,piezometros_mel.piezometros as p WHERE t.id=p.id GROUP BY p.nombre_piezometro,t.id,p.field_4 )ORDER BY t.id ASC')
    r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
    cur.close()
    res= (r[0] if r else None) if True else r
    result= json.dumps(r)
    return result

#funcion para leer lo spiezometros del la carpeta crrent
def get_all_piezometros_current():
    df=pd.read_csv("D:/geoalert-data/mel/Piezometros/current/4108-readings-current.csv", skiprows=9)
    #df.set_index('Date-and-time', inplace=True)
    #print(df.to_json(orient = 'columns'))
    print ("Las cabeceras de la columnas son las siguientes:")
    #print (df.columns.tolist())
    #result = df.head(3)
    print(df[-1:].index.get_value)
    return df.tail(1).to_json(orient = 'columns')
    #return df.columns.tolist()

def get_all_piezometros():
    cur = conectar()        
    # Ejecución de una consulta con la version de PostgreSQL
    print('La consulta de piezometros es :')
    #cur.execute('(SELECT p.nombre_piezometro,t.id,MAX(t.fechahora) FROM piezometros_mel.piezometros_consolidado_test as t,piezometros_mel.piezometros as p WHERE t.id=p.id GROUP BY p.nombre_piezometro,t.id ) ORDER BY t.id ASC') 
    cur.execute('SELECT h.id,h.nombre_piezometro,h.fechahora,k.sensor_1_cota,k.sensor_2_cota,k.sensor_3_cota,k.sensor_4_cota,k.sensor_5_cota,k.sensor_6_cota FROM ((SELECT p.nombre_piezometro,t.id,MAX(t.fechahora) as fechahora FROM piezometros_mel.piezometros_consolidado_test as t,piezometros_mel.piezometros as p WHERE t.id=p.id GROUP BY p.nombre_piezometro,t.id) ORDER BY t.id ASC)as h,piezometros_mel.piezometros_consolidado_test as k WHERE h.id=k.id and h.fechahora=k.fechahora') 
    
    piezos = cur.fetchall()
    res={}
    result={}
    for index,piezo in zip(range(len(piezos)),piezos):
        #res[str(piezo[0])]={str(piezo[1]):str(piezo[2])}
        res["Nombre"]=str(piezo[0])
        res["Id"]=str(piezo[1])
        res["Fecha"]=str(piezo[2])
        res["sensor_1_cota"]=str(piezo[3])
        res["sensor_2_cota"]=str(piezo[4])
        res["sensor_3_cota"]=str(piezo[5])
        res["sensor_4_cota"]=str(piezo[6])
        res["sensor_5_cota"]=str(piezo[7])
        res["sensor_6_cota"]=str(piezo[8])
        result[str(index)]=json.loads(json.dumps(res))
        res.clear()
    # Cierre de la comunicación con PostgreSQL
    cur.close()
    print('Conexión finalizada.')
    return result
"""
def get_all_piezometros_procesados():

    result="<h1>funcion que retorna los piezometros procesados</h1>"
    return result
"""
def get_all_piezometros_staging():
    cur = conectar()        
    # Ejecución de una consulta con la version de PostgreSQL
    print('La consulta de piezometros es :')
    cur.execute('SELECT P.id as Id,S.fecha as fecha,P.nombre_piezometro as Nombre,K.instrument_data as Data FROM (SELECT instrument_id,MAX(fecha) as fecha FROM data_staging.piezometro_src GROUP BY instrument_id) as S,piezometros_mel.piezometros as P,data_staging.piezometro_src as K WHERE (P.id=S.instrument_id AND P.id=K.instrument_id) AND (K.fecha=S.fecha)') 
    piezos = cur.fetchall()
    res={}
    result={}
    for index,piezo in zip(range(len(piezos)),piezos):
        #res[str(piezo[0])]={str(piezo[1]):str(piezo[2])}
        res["Id"]=str(piezo[0])
        res["Fecha"]=str(piezo[1])
        res["Nombre"]=str(piezo[2])
        res["Data"]=str(piezo[3])        
        result[str(index)]=json.loads(json.dumps(res))
        res.clear()
    # Cierre de la comunicación con PostgreSQL
    cur.close()
    print('Conexión finalizada.')

    return result

#funcion para obtener los piezometros ya procesados
def get_all_piezometros_procesados():
    cur = conectar()        
    # Ejecución de una consulta con la version de PostgreSQL
    cur.execute('SELECT Z.nombre_piezometro,Q.id,Q.Fecha as fechahora,Q.data FROM piezometros_mel.piezometros AS Z,(SELECT P.instrument_id as Id,P.fecha as Fecha,P.sensor_data_json as Data FROM data_source.piezometros_data as P,(SELECT instrument_id,max(fecha) as fecha FROM data_source.piezometros_data GROUP BY instrument_id) as T WHERE P.instrument_id=T.instrument_id AND P.fecha=T.fecha) as Q WHERE Z.id=Q.Id ORDER BY Z.id') 
    piezos = cur.fetchall()
    res={}
    result={}
    for index,piezo in zip(range(len(piezos)),piezos):
        #res[str(piezo[0])]={str(piezo[1]):str(piezo[2])}
        res["Nombre_piezometro"]=str(piezo[0])
        res["Id"]=str(piezo[1])
        res["Fechahora"]=str(piezo[2])
        res["Data"]=str(piezo[3])
        
        result[str(index)]=json.loads(json.dumps(res))
        res.clear()
    # Cierre de la comunicación con PostgreSQL
    cur.close()
    print('Conexión finalizada.')
    return result

def get_all_humedad():
    cursor_h = conectar()        
    # Ejecución de una consulta con la version de PostgreSQL
    print('La consulta de sensores de humedad es :')
    #cursor_h.execute('(SELECT c.id_humedad as id,h.pozo as sensor,MAX(c.fechahora) as fecha FROM humedad.humedad_consolidado as c,humedad.humedad as h WHERE c.id_humedad=h.id_humedad GROUP BY c.id_humedad,h.pozo) ORDER BY c.id_humedad ASC') 
    cursor_h.execute('select K.id,K.sensor,K.fecha,C.sensor_1_valor,C.sensor_2_valor,C.sensor_3_valor,C.sensor_4_valor,C.sensor_5_valor,C.sensor_6_valor from humedad.humedad_consolidado as C,((SELECT c.id_humedad as id,h.pozo as sensor,MAX(c.fechahora) as fecha FROM humedad.humedad_consolidado as c,humedad.humedad as h WHERE c.id_humedad=h.id_humedad GROUP BY c.id_humedad,h.pozo) ORDER BY c.id_humedad ASC) as K WHERE K.id=C.id_humedad and K.fecha=C.fechahora')
    sensor_hs = cursor_h.fetchall()
    res={}
    result={}
    for index,sensor_h in zip(range(len(sensor_hs)),sensor_hs):
        #result[str(sensor_h[0])]=str(sensor_h[1])
        res["Id"]=str(sensor_h[0])
        res["Nombre"]=str(sensor_h[1])
        res["Fecha"]=str(sensor_h[2])
        res["Sensor_1"]=str(sensor_h[3])
        res["Sensor_2"]=str(sensor_h[4])
        res["Sensor_3"]=str(sensor_h[5])
        res["Sensor_4"]=str(sensor_h[6])
        res["Sensor_5"]=str(sensor_h[7])
        res["Sensor_6"]=str(sensor_h[8])
        result[str(index)]=json.loads(json.dumps(res))
        res.clear()
    # Cierre de la comunicación con PostgreSQL
    cursor_h.close()
    print('Conexión finalizada.')
    return result
#funcion para obtener los GNSS
def get_all_gnss():
    print("se realiza una consulta en los sensores GNSS")
    cursor_h = conectar()        
    # Ejecución de una consulta con la version de PostgreSQL
    print('La consulta de sensores de humedad es :')
    #cursor_h.execute('(SELECT c.id_humedad as id,h.pozo as sensor,MAX(c.fechahora) as fecha FROM humedad.humedad_consolidado as c,humedad.humedad as h WHERE c.id_humedad=h.id_humedad GROUP BY c.id_humedad,h.pozo) ORDER BY c.id_humedad ASC') 
    cursor_h.execute('SELECT G.gnss_id,G.nombre,P.fecha,H.desp as Heigth,L.desp as Long,T.desp as Trans FROM (SELECT gnss_id,max(fechahora) as fecha FROM gnss.gnss_consolidado_heigth GROUP BY gnss_id) as P,gnss.gnss as G,gnss.gnss_consolidado_heigth as H,gnss.gnss_consolidado_long as L,gnss.gnss_consolidado_trans as T WHERE (P.gnss_id=G.gnss_id) AND (P.gnss_id=H.gnss_id AND P.fecha=H.fechahora) AND (P.gnss_id=L.gnss_id AND P.fecha=L.fechahora) AND (P.gnss_id=T.gnss_id AND P.fecha=T.fechahora) ORDER BY G.gnss_id asc')
    sensor_hs = cursor_h.fetchall()
    res={}
    result={}
    for index,sensor_h in zip(range(len(sensor_hs)),sensor_hs):
        #result[str(sensor_h[0])]=str(sensor_h[1])
        res["Id"]=str(sensor_h[0])
        res["Nombre"]=str(sensor_h[1])
        res["Fecha"]=str(sensor_h[2])
        res["Heigth"]=str(sensor_h[3])
        res["Long"]=str(sensor_h[4])
        res["Trans"]=str(sensor_h[5])
        
        result[str(index)]=json.loads(json.dumps(res))
        res.clear()
    # Cierre de la comunicación con PostgreSQL
    cursor_h.close()
    print('Conexión finalizada.')
    return result
#funcion que obtiene datos de los prismas de la base de datos
def get_all_prismas():
    print("se realiza una consulta en los sensores Prismas")
    cursor_h = conectar()        
    # Ejecución de una consulta con la version de PostgreSQL
    print('La consulta de sensores de humedad es :')
    cursor_h.execute('SELECT P.prisma_id as Id,S.nombre as nombre,P.fecha as fecha,C.desp as Desp FROM (SELECT prisma_id,max(fechahora) as fecha FROM prisma.prisma_consolidado GROUP BY prisma_id) as P, prisma.prisma as S, prisma.prisma_consolidado as C WHERE P.prisma_id=S.prisma_id AND P.fecha=C.fechahora ORDER BY Id')
    sensor_hs = cursor_h.fetchall()
    res={}
    result={}
    for index,sensor_h in zip(range(len(sensor_hs)),sensor_hs):
        #result[str(sensor_h[0])]=str(sensor_h[1])
        res["Id"]=str(sensor_h[0])
        res["Nombre"]=str(sensor_h[1])
        res["Fecha"]=str(sensor_h[2])
        res["Desp"]=str(sensor_h[3])
                
        result[str(index)]=json.loads(json.dumps(res))
        res.clear()
    # Cierre de la comunicación con PostgreSQL
    cursor_h.close()
    print('Conexión finalizada.')
    return result
    
#funcion que obtiene los datos de los clinoextensometros
def get_all_clinoextensometros():
    print ("se realiza la consulta a la DB de los clinos")
    cursor_c=conectar()
    cursor_c.execute('SELECT K.extensometro_id,K.fecha,P."Tipo",sen1_tasa_men,sen2_tasa_men,sen1_tasa_sem,sen2_tasa_sem FROM extensometro.extensometro as P,extensometro.extensometro_tasa as S,(SELECT extensometro_id,MAX(fecha) as fecha FROM extensometro.extensometro_tasa GROUP BY extensometro_id) as K WHERE (P.extid=S.extensometro_id AND S.extensometro_id=K.extensometro_id) AND (K.fecha=S.fecha)')
    sensor_hs = cursor_c.fetchall()
    res={}
    result={}
    for index,sensor_h in zip(range(len(sensor_hs)),sensor_hs):
        #result[str(sensor_h[0])]=str(sensor_h[1])
        res["Id"]=str(sensor_h[0])
        res["Fecha"]=str(sensor_h[1])
        res["Tipo"]=str(sensor_h[2])
        res["sen1_tasa_men"]=str(sensor_h[3])
        res["sen2_tasa_men"]=str(sensor_h[4])
        res["sen1_tasa_sem"]=str(sensor_h[5])
        res["sen2_tasa_sem"]=str(sensor_h[6])
        result[str(index)]=json.loads(json.dumps(res))
        res.clear()
    # Cierre de la comunicación con PostgreSQL
    cursor_c.close()
    print('Conexión finalizada.')
    return result

#funcion que obtiene los datos de los radares y ejes
def get_all_radares():
    print("se realiza una consulta en los sensores de ejes")
    cursor_h = conectar()        
    # Ejecución de una consulta con la version de PostgreSQL
    print('La consulta de sensores de de radares es :')
    cursor_h.execute('SELECT P.eje_id as Id,P.fecha as fecha,S.desp as Desp,S.vel as Vel FROM (SELECT eje_id,max(fechahora) as fecha FROM ejes.eje_consolidado GROUP BY eje_id ORDER BY eje_id) as P,ejes.eje_consolidado as S WHERE P.eje_id=S.eje_id AND P.fecha=S.fechahora')
    sensor_hs = cursor_h.fetchall()
    res={}
    result={}
    for index,sensor_h in zip(range(len(sensor_hs)),sensor_hs):
        #result[str(sensor_h[0])]=str(sensor_h[1])
        res["Id"]=str(sensor_h[0])
        res["Fecha"]=str(sensor_h[1])
        res["Desp"]=str(sensor_h[2])
        res["Vel"]=str(sensor_h[3])
        result[str(index)]=json.loads(json.dumps(res))
        res.clear()
    # Cierre de la comunicación con PostgreSQL
    cursor_h.close()
    print('Conexión finalizada.')
    return result

def get_last_date(file):
    last_date="17-01-2023"
    with open(file,'r') as linea:
        lineas = linea.read().aplitlines()
        print(lineas[-1])
        last_date = lineas[-1] 
    return last_date  

import os

def obtener_nombres_archivos(carpeta):
    nombres_archivos = []
    ruta_carpeta = os.path.abspath(carpeta)  # Obtiene la ruta absoluta de la carpeta
    
    # Verifica si la carpeta existe
    if os.path.exists(ruta_carpeta):
        # Itera sobre los archivos y directorios en la carpeta
        for nombre in os.listdir(ruta_carpeta):
            ruta_archivo = os.path.join(ruta_carpeta, nombre)  # Obtiene la ruta completa del archivo
            if os.path.isfile(ruta_archivo):  # Verifica si es un archivo (no un directorio)
                nombres_archivos.append(nombre)  # Agrega el nombre del archivo a la lista
    
    return nombres_archivos

# Ejemplo de uso
#carpeta = input("Ingrese el nombre de la carpeta: ")
#archivos = obtener_nombres_archivos(carpeta)
#print("Archivos encontrados en la carpeta {}: {}".format(carpeta, archivos))
