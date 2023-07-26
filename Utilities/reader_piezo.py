import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta, date
from os.path import exists
from os import remove
from dateutil import rrule
import csv
import json


PIEZOMETRO_SUBDIR = "../SDM/current" #"piezometros_trigger"

class reader_source_piezo:
    """Lectura de los archivos base entregados por Escondida.
    """
    elemsId = None
    sensors = []

    def __init__(self, fileName=None, node_id=None, dt_month=None, folder=None, sensor_type="CG"):
        self.node_id       = node_id
        self.sensor_type   = sensor_type

        if sensor_type == "TR": #Piezometro de trigger sismico
            if fileName is None:
                fileName =  reader_source_piezo.get_fileName_trigger(node_id, folder=folder)
            with open(fileName, "r") as fp:
                line = fp.readline()
                self.tag_insturmento = line.split(",")[1][1:-1]
                line = fp.readline()
                columns = [column.lstrip("\"").rstrip("\"") for column in line[:-1].split(",")]
                line = fp.readline()
                line = fp.readline()
                self.data =  pd.read_csv(fp, names=columns, header=None)
                self.data.set_index("TIMESTAMP", inplace=True)
                self.times = np.array([datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') for dt in self.data.index])
                self.data.index = self.times
        else:
            if fileName is None:
                fileName = reader_source_piezo.get_fileName(node_id, dt_month=dt_month, folder=folder)
            #print('>Opening', fileName)
            if sensor_type == "CG": #Tec CasaGrande
                with open(fileName, 'r') as fp:
                    self.elemsId   = fp.readline()[:-1].split(',')[1]
                    self.gatewayId = fp.readline()[:-1].split(',')[1]
                    self.model     = fp.readline()[:-1].split(',')[1]
                    for _ in range(6):
                        fp.readline()
                    self.data  = pd.read_csv(fp)
                    self.data.set_index("Date-and-time", inplace=True)
                    #print(self.data.columns)
                    self.times = np.array([datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') for dt in self.data.index]) #data['Date-and-time']
                    self.data.index = self.times
                    sensor_columns = [elem for elem in self.data.columns if elem[-3:-1] == "Ch"]
                    self.sensors = set([sensor_name.split("-")[-1] for sensor_name in sensor_columns])
            elif sensor_type == "VW": #cuerda-vibrante
                with open(fileName, 'r') as fp:
                    self.elemsId   = fp.readline()[:-1].split(',')[1]
                    self.gatewayId = fp.readline()[:-1].split(',')[1]
                    self.model     = fp.readline()[:-1].split(',')[1]
                    for _ in range(6):
                        fp.readline()
                    self.data      = pd.read_csv(fp)
                    self.data.set_index("Date-and-time", inplace=True)
                    self.times     = np.array([datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') for dt in self.data.index])
                    self.data.index = self.times
                    sensor_columns = [elem for elem in self.data.columns if elem[-3:-1] == "Ch"]
                    self.sensors = set([sensor_name.split("-")[-1] for sensor_name in sensor_columns])
        
        self.fileName = fileName
        return


    @property
    def columns(self):
        return self.data.columns


    def get_values_channel(self, channel):
        if self.sensor_type == "CG":
            name = "eng-{}-Ch{}".format(self.node_id, channel)
        elif self.sensor_type == "VW":
            name = "p-{}-Ch{}".format(self.node_id, channel)
        else: # trigger
            name = channel
        return self.data[name]
    

    def get_values_channel_by_day(self, channel, decimals=5):
        values = self.get_values_channel(channel)
        return values.groupby(values.index.date).mean().round(decimals=decimals)


    def get_types_channel(self, channel):
        name = "type-{}-{}".format(self.node_id, channel)
        return self.data[name]


    @staticmethod
    def get_fileName(node_id, dt_month=None, folder=None):
        if dt_month is None:
            fileName = "{}-readings-current.csv".format(node_id)
        else:
            fileName = "{}-readings-{}-{:02d}.csv".format(node_id, dt_month.year, dt_month.month)
        if folder is not None:
            fileName = "{}/{}".format(folder, fileName)
        return fileName


    @staticmethod
    def get_fileName_trigger(intrument_id, folder=None):
        fileName    = "{}/{}_Estatico.dat".format(PIEZOMETRO_SUBDIR, intrument_id)
        if folder is not None:
            fileName = "{}/{}".format(folder, fileName)
        return fileName


    @staticmethod
    def exists_fileName(node_id, dt_month=None, folder=None):
        fileName = reader_source_piezo.get_fileName(node_id, dt_month=dt_month, folder=folder)
        #print("To check", fileName)
        return exists(fileName)


    @staticmethod
    def exists_fileName_trigger(intrument_id, folder=None):
        fileName = reader_source_piezo.get_fileName_trigger(intrument_id, folder=folder)
        return exists(fileName)



class reader_config_piezo:
    """Lectura de los parametros de cada sensor
    """
    def __init__(self, fileName=None):
        self.sensores = pd.read_csv(fileName, delimiter=";", encoding='latin1')
        self.sensores.set_index("id", inplace=True)
        return

    @property
    def nodos(self):
        return self.sensores

    @property
    def dinamicos(self):
        return self.sensores[self.sensores["Umbral Tipo"]=="D"]

    @property
    def estaticos(self):
        return self.sensores[self.sensores["Umbral Tipo"]=="E"]


    def get_nodos_id(self):
        nodos = pd.unique(self.sensores["id_nodo"])
        nodos.sort()
        return nodos


    def get_nodos_id_dinamicos(self):
        #dinamicos = list(set(nodos_dinamicos["id_nodo"]))
        dinamicos = pd.unique(self.dinamicos["id_nodo"])
        dinamicos.sort()
        return dinamicos


    def get_nodos_id_estaticos(self):
        estaticos = pd.unique(self.estaticos["id_nodo"].astype(int))
        estaticos.sort()
        return estaticos

    def get_sensors_nodo(self, node_id):
        rows_sensor = self.sensores[self.sensores["id_nodo"] == node_id]
        return rows_sensor

    def process_nodo_piezo_umbral(self, node_id, folder, sdate, edate): #, depositions
        rows_sensor = self.get_sensors_nodo(node_id)
        process_nodo_piezo_umbral(node_id, folder, sdate, edate, rows_sensor) #, depositions
        return


class reader_fusion_piezo:
    def __init__(self, fileName):
        self.fileName = fileName
        self.nodos   = json.load(open(self.fileName))
        return

    def fusion(self, folder, sdate=None, edate=None, with_date_range=False):
        for pair in self.nodos:
            print("fusion", pair[0], pair[1])
            file1 = "{}/{}-".format(folder, pair[0])
            if with_date_range:
                file1 += "{}-{}.csv".format(sdate.strftime("%Y%m%d"), edate.strftime("%Y%m%d"))
            else:
                file1 += "current.csv"
            data1 = pd.read_csv(file1, parse_dates=["Date-and-time"])
            data1.set_index("Date-and-time", inplace=True)
            file2 = "{}/{}-".format(folder, pair[1])
            if with_date_range:
                file2 += "{}-{}.csv".format(sdate.strftime("%Y%m%d"), edate.strftime("%Y%m%d"))
            else:
                file2 += "current.csv"
            data2 = pd.read_csv(file2, parse_dates=["Date-and-time"])
            data2.set_index("Date-and-time", inplace=True)

            data3 = data1.join(data2)

            data3.to_csv(file1, sep=',', doublequote=False, quoting=csv.QUOTE_NONNUMERIC)
            remove(file2)
        return


class reader_values_umbrales_piezo_daily:
    """Lectura del csv generado. El objetivo es utilizar esta información para poder generar 
    los gráficos de valores y umbrales.
    """

    def __init__(self, fileName):
        self.data = pd.read_csv(fileName, parse_dates=["Date-and-time"])
        self.data.set_index("Date-and-time", inplace=True)
        return

    @property
    def columns(self):
        return self.data.columns
    

    @property
    def table(self):
        return self.data



def make_sensor_umbral_dinamico(sensor, sdate, edate): #, depositions=None
    name   = sensor["Nombre Sensor"]
    delta  = edate - sdate
    tdates = [sdate + timedelta(days=i) for i in range(delta.days + 1)]
    tdt    = np.array([dt.date() for dt in tdates])
    tts    = np.array([time.mktime(tdate.timetuple()) for tdate in tdates])
    dataf  = {"Date-and-time":tdates}
    #print(sensor)
    blue_lin_slope  = np.float64(sensor["blue_line_slope"])
    blue_lin_inter  = np.float64(sensor["blue_line_inter"])
    green_lin_slope = np.float64(sensor["green_line_slope"])
    green_lin_inter = np.float64(sensor["green_line_inter"])
    std_error       = np.float64(sensor["std_error"])
    blue_lin  = blue_lin_slope*tts + blue_lin_inter
    green_lin = green_lin_slope*tts + green_lin_inter
    caution   = green_lin + std_error
    dataf[name+"-blue"]    = blue_lin
    dataf[name+"-green"]   = green_lin
    dataf[name+"-caution"] = caution

    if (sensor["alert_a"] != "") and (not np.isnan(sensor["alert_a"])):
        alert_a  = np.float64(sensor["alert_a"])
        alert_b  = np.float64(sensor["alert_b"])
        alert_c  = np.float64(sensor["alert_c"])
        #print("alerts", alert_a, alert_b, alert_c, "timetuple", tts[0])
        end_trial_dt = datetime.strptime(sensor["alert_intersection_date"],"%Y-%m-%d")
        alert = np.full(len(tts), None)
        if (sensor["depositions"] is None) or (sensor["depositions"] is np.nan):
            day_1_dep_dt = datetime.strptime(sensor["day_1_dep"],"%Y-%m-%d") 
            dif_dtime = day_1_dep_dt - end_trial_dt
            dif_secs  = dif_dtime.total_seconds()
            day_1_dep = time.mktime(day_1_dep_dt.timetuple())
            # end_trial = time.mktime(end_trial_dt.timetuple())
            valid_days = tts >= day_1_dep
            a = alert_a
            b = alert_b - 2*alert_a*dif_secs
            c = alert_c + (green_lin_slope-alert_b)*dif_secs + alert_a*dif_secs*dif_secs
            y  = tts*tts*a
            y += tts*b
            y += c
            alert[valid_days]  = np.round(y, 4)[valid_days]
            
        else: #depositions
            depositions = json.loads(sensor["depositions"])
            #print("depositions", depositions)
            for deposition in depositions:
                day_1_dep_dt = datetime.strptime(deposition,"%Y-%m-%d")
                if day_1_dep_dt > end_trial_dt:
                    dif_dtime = day_1_dep_dt - end_trial_dt
                    dif_secs  = dif_dtime.total_seconds()
                    day_1_dep = time.mktime(day_1_dep_dt.timetuple())
                    # end_trial = time.mktime(end_trial_dt.timetuple())
                    valid_days = tts >= day_1_dep
                    a = alert_a
                    b = alert_b - 2*alert_a*dif_secs
                    c = alert_c + (green_lin_slope-alert_b)*dif_secs + alert_a*dif_secs*dif_secs
                    y  = tts*tts*a
                    y += tts*b
                    y += c
                    alert[valid_days]  = np.round(y, 4)[valid_days]
            # Dia anterior a una deposicion sin dato (para no generar lineas con discontinuidad)
            valid_days = np.zeros(len(tts), dtype=bool)
            for deposition in depositions:
                dep_date_prev = datetime.strptime(deposition,"%Y-%m-%d").date() - timedelta(days=1)
                #print(dep_date_prev, tdt[0],"total by dep", (tdt == dep_date_prev).sum())
                valid_days = valid_days | (tdt == dep_date_prev)
            #print("dias a borrar", valid_days.sum())
            alert[valid_days]  = np.nan

        dataf[name+"-alert"] = alert
        # end_trial = time.mktime(end_trial_dt.timetuple())
        # invalid_days = tts < end_trial
        # y  = tts.copy()
        # y *= tts
        # y *= alert_a
        # y += alert_b*tts
        # y += alert_c
        # trial2 =  np.round(y, 4)
        # trial2[invalid_days]  = None
        # dataf[name+"-trial2"] = trial2

    else:
        dataf[name+"-alert"] = [None] * len(tdates)

    #dataf[name+"-alert"]   = np.ones(len(tdates)) * alert
    table = pd.DataFrame(dataf)
    table.set_index("Date-and-time", inplace=True)
    return table


def make_sensor_umbral_estatico(sensor, sdate, edate):
    delta  = edate - sdate
    tdates = [sdate + timedelta(days=i) for i in range(delta.days + 1)]
    name   = sensor["Nombre Sensor"]
    dataf   = {"Date-and-time":tdates}
    # cota    = np.round(np.float64(sensor["Cota Instalacion Sensor"]),4)
    # err_ins = np.round(np.float64(sensor["Error instrumento (m)"]),4)
    # roof_dr = np.round(np.float64(sensor["roof_drainage"]),4)
    # dam_bod = np.round(np.float64(sensor["dam_body"]),4)
    normal  = sensor["normal"] #np.round(cota + 2.0 * err_ins, 4)
    prevent = sensor["preventive"] #np.maximum(np.round(0.5*(roof_dr-dam_bod)+dam_bod, 4), normal)
    caution = sensor["caution"] #np.maximum(np.round(0.75*(roof_dr-dam_bod) + dam_bod, 4), normal)
    alert   = sensor["alert"] #np.round(cota + 2.0*err_ins if cota > roof_dr else roof_dr, 4)
    dataf[name+"-normal"]     = np.ones(len(tdates)) * normal
    dataf[name+"-preventive"] = np.ones(len(tdates)) * prevent
    dataf[name+"-caution"]    = np.ones(len(tdates)) * caution
    dataf[name+"-alert"]      = np.ones(len(tdates)) * alert
    table = pd.DataFrame(dataf)
    table.set_index("Date-and-time", inplace=True)
    return table


def make_sensor_umbral_wet(sensor, sdate, edate):
    delta   = edate - sdate
    tdates  = [sdate + timedelta(days=i) for i in range(delta.days + 1)]
    name    = sensor["Nombre Sensor"]
    dataf   = {"Date-and-time":tdates}
    max_value = np.round(np.float64(sensor["wet_max"]),4)
    min_value = np.round(np.float64(sensor["wet_min"]),4)
    prevent = max_value
    caution = 1.25 * max_value - 0.25 * min_value
    alert   = 1.5 * max_value - 0.5 * min_value
    dataf[name+"-preventive"] = np.ones(len(tdates)) * prevent
    dataf[name+"-caution"]    = np.ones(len(tdates)) * caution
    dataf[name+"-alert"]      = np.ones(len(tdates)) * alert
    dataf[name+"-normal"]     = [None] * len(tdates)
    table = pd.DataFrame(dataf)
    table.set_index("Date-and-time", inplace=True)
    return table


def make_sensor_umbral_none(sensor, sdate, edate):
    delta   = edate - sdate
    tdates  = [sdate + timedelta(days=i) for i in range(delta.days + 1)]
    name    = sensor["Nombre Sensor"]
    dataf   = {"Date-and-time":tdates}
    dataf[name+"-preventive"] = [None] * len(tdates)
    dataf[name+"-caution"]    = [None] * len(tdates)
    dataf[name+"-alert"]      = [None] * len(tdates)
    dataf[name+"-normal"]     = [None] * len(tdates)
    table = pd.DataFrame(dataf)
    table.set_index("Date-and-time", inplace=True)
    return table


def get_node_channel_values(node_id, sdate, edate, channel, folder=None, with_year_folder=False, sensor_type="CG"):
    #print(node_id, sdate, edate, channel, sensor_type)
    if sensor_type != "TR":
        stack = None
        for dt_month in rrule.rrule(rrule.MONTHLY, dtstart=sdate, until=edate):
            #print(dt_month)
            if with_year_folder:
                folder_path = "{}/{}".format(folder, dt_month.year)
            else:
                folder_path = folder
            #print(reader_source_piezo.get_fileName(node_id, dt_month=dt_month, folder=folder_path))
            if reader_source_piezo.exists_fileName(node_id, dt_month=dt_month, folder=folder_path):
                reader = reader_source_piezo(node_id=node_id, dt_month=dt_month, folder=folder_path, sensor_type=sensor_type)
                values = reader.get_values_channel_by_day(channel)
                #print("values", values)
                if stack is None:
                    stack = values.copy()
                else:
                    stack = pd.concat([stack, values], axis=0)

        folder_current = "{}/current".format(folder)
        if reader_source_piezo.exists_fileName(node_id, folder=folder_current):
            reader = reader_source_piezo(node_id=node_id, folder=folder_current, sensor_type=sensor_type)
            values = reader.get_values_channel_by_day(channel)
            if stack is None:
                stack = values.copy()
            else:
                stack = pd.concat([stack, values], axis=0)
    else:
        print("file SMD", reader_source_piezo.get_fileName_trigger(node_id, folder))
        if reader_source_piezo.exists_fileName_trigger(node_id, folder):
            reader = reader_source_piezo(node_id=node_id, folder=folder, sensor_type=sensor_type)
            stack = reader.get_values_channel_by_day(channel)
        else:
            print("No existe el archivo", reader_source_piezo.get_fileName_trigger(node_id, folder))
            print("En el directorio", folder)
    return stack


def get_sensor_values(sensor, sdate, edate, folder):
    
    channel = sensor["Canal"] # "{}".format(int())
    #print("channel", channel)
    if sensor["Tecnologia del Nodo"] == "TR":
        node_id = sensor["TAG Instrumento"]
        sensor_type = "TR"
        with_year_folder = False
    else:
        node_id = int(sensor["id_nodo"])
        if sensor["Tecnologia del Nodo"] == "VOLT":
            sensor_type = "CG"
            with_year_folder = True
        else:
            sensor_type = "VW"
            with_year_folder = True
    stack   = get_node_channel_values(node_id, sdate, edate, channel, folder=folder, with_year_folder=with_year_folder, sensor_type=sensor_type)

    new_dts = [datetime(dt.year,dt.month,dt.day,12) for dt in stack.index]
    name    = sensor["Nombre Sensor"]
    stack.name  = name+"-cota"
    stack.index = new_dts
    table   = stack.to_frame()
    stack.name  = name+"-columna"
    stack   = stack.add(float(sensor["Cota Instalacion Sensor"])).round(decimals=4)
    table   = table.join(stack)
    return table


def process_nodo_piezo_umbral(node_id, folder, sdate, edate, sensors, with_date_range=False):
    #print(">>node_id", node_id)
    table  = None
    for _, sensor in sensors.iterrows():
        umbral_type = sensor["Umbral Tipo"]
        if ["E","D","W","N"].count(umbral_type):
            if umbral_type == "D":
                new_table = make_sensor_umbral_dinamico(sensor, sdate, edate)
            elif umbral_type == "E":
                new_table = make_sensor_umbral_estatico(sensor, sdate, edate)
            elif umbral_type == "W":
                new_table = make_sensor_umbral_wet(sensor, sdate, edate)
            else:
                new_table = make_sensor_umbral_none(sensor, sdate, edate)
            if table is None:
                table = new_table
            else:
                table = table.join(new_table)

            stack =  get_sensor_values(sensor, sdate, edate, folder)
            if stack is not None:
                table = table.join(stack)
            #pd.merge(left=survey_sub, right=species_sub, how='left', left_on='species_id', right_on='species_id')

    #Guardado de archivo
    folder_out = folder + "/umbrales_diarios"
    fileOut  = "{}/{}-".format(folder_out, int(node_id))
    if with_date_range:
        fileOut += "{}-{}.csv".format(sdate.strftime("%Y%m%d"), edate.strftime("%Y%m%d"))
    else:
        fileOut +="current.csv"
    if table is not None:
        table.to_csv(fileOut, sep=',', doublequote=False, quoting=csv.QUOTE_NONNUMERIC)
        #print(fileOut, "saved")
    else:
        print("No hay umbrales para nodo", node_id)
    return


def plot_umbral(ax, sensor, data1, xlim=None):
    cols_sens = [column for column in data1.columns if (column[:column.rfind("-")]==sensor) and (column[column.rfind("-")+1:]!="cota")]
    colors    = {"columna":"k", "normal":"b", "preventive":"y", "caution":"m", "alert":"r",
                 "blue":"b", "green":"g", "trial2":"k"}
    
    #plt.fill_between(data1.index, data1[cols_sens[0]], data1[cols_sens[2]], facecolor="green", alpha=0.65) #
    ax.fill_between(data1.index, data1[cols_sens[0]], data1[cols_sens[2]], facecolor="yellow", alpha=0.65)
    ax.fill_between(data1.index, data1[cols_sens[2]], data1[cols_sens[3]], facecolor="orange", alpha=0.65)
    for col in cols_sens:
        label = col[col.rfind("-")+1:]
        style = "-" if label == "columna" else ":" if ["normal", "blue"].count(label) else "--"
        style += colors[label] 
        ax.plot(data1.index,data1[col], style, label=label)

    ax.legend()
    ax.set_title(sensor)
    ax.ticklabel_format(useOffset=False, axis='y')
    if xlim is not None:
        ax.set_xlim(xlim)
    return


def generate_plots_nodo(node_id, folder_src, folder_dst, xlim=None, sdate=None, edate=None):
    from matplotlib import pyplot as plt

    file1 = "{}/{}".format(folder_src, node_id)
    if (sdate is None) and (edate is None):
        file1  += "-current"
    else:
        if sdate is not None:
            file1 += "-{}".format(sdate.strftime("%Y%m%d"))
        if edate is not None:
            file1 += "-{}".format(edate.strftime("%Y%m%d"))
    file1  += ".csv"
    data1   = reader_values_umbrales_piezo_daily(file1)
    sensors = set([column[:column.rfind("-")] for column in data1.columns])
    for sensor in sensors:
        fig, ax = plt.subplots(figsize=(12, 6))
        plot_umbral(ax, sensor, data1.table, xlim=xlim)
        fileOut = "{}/{}-{}.png".format(folder_dst, node_id, sensor)
        fig.savefig(fileOut, facecolor="white")
    return
