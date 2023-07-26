from calendar import monthrange
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from os.path import exists
from dateutil import rrule
from scipy.stats import linregress
import csv
import traceback
import json

sens_dir = {"transversal":"_a",
            "longitudinal":"_b",
            "vert":""}
ventanas = ["mes", "sem"]

DIR_DIFERENCIAL = "diferencial"
class reader_source_clino:
    elemsId = None
    sensors = []
    
    def __init__(self, fileName=None, node_id=None, dt_month=None, folder=None, compact=True):
        self.fileName = fileName
        if self.fileName is None:
            self.fileName = reader_source_clino.get_fileName(node_id, dt_month=dt_month, folder=folder)
        else:
            if folder is not None:
                self.fileName = "{}/{}".format(folder, self.fileName)
        #print('>Opening', self.fileName)
        if compact:
            with open(self.fileName, 'r') as fp:
                fp.readline()
                self.data  = pd.read_csv(fp)
                self.data.set_index("TIMESTAMP", inplace=True)
                self.times = np.array([datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') for dt in self.data.index])
                self.data.index = self.times
            self.columns_sensors = [elem for elem in self.data.columns if elem.count("_") > 0]
            sensors = [sensor_name[:sensor_name.rfind("_")] for sensor_name in self.columns_sensors if sensor_name[-1]=="m"]
            self.sensors   = list(set(sensors))
            self.sensors.sort()
        self.heights_sensor = {}
        for sensor in self.sensors:
            self.heights_sensor[sensor] = self.get_heights_sensor(sensor)
        return


    @property
    def columns(self):
        return self.data.columns


    def get_heights_sensor(self, sensor):
        heigths_all = [elem[len(sensor)+1:] for elem in self.columns if elem[-1]=="m" and elem[:len(sensor)] == sensor]
        heights = set(heigths_all)
        heights = [(round(np.float(elem.rstrip("m")),3),elem.rstrip("m")) for elem in heights]
        heights.sort(key=lambda tup: tup[0])
        return {elem[0]:elem[1] for elem in heights}


    def get_values_sensor_height_direction(self, sensor_name, height, direction, offset=0.0):
        # if direction[:3] == "ver":
        #     column_name = "{}_{}m".format(sensor_name, height)
        # else:
        #     column_name = "{}_{:.3f}m{}".format(sensor_name, height, sens_dir[direction])
        
        column_name = "{}_{}m".format(sensor_name, self.heights_sensor[sensor_name][height])
        #print("height", type(height))
        if direction[:3] != "ver":
            column_name += sens_dir[direction]
        if column_name in self.data:
            data = self.data[column_name].add(-offset)
        else:
            print("no column", column_name, "in", sensor_name, height, direction) #self.get_columns_sensor(sensor_name)
            data = None
        return data


    def get_values_sensor_direction(self, sensor_name, direction, offsets=None, difference_depth=True):
        #print("heights_sensor", self.heights_sensor)
        heights = self.heights_sensor[sensor_name]
        if len(heights) == 0:
            print("No heights")
            return

        height_keys = list(heights.keys())
        #print("height_keys[0]", height_keys[0])
        val_acum_prof = self.get_values_sensor_height_direction(sensor_name, height_keys[0],
                                direction, offset=0.0 if offsets is None else offsets[0])
        #print("val_acum_prof", val_acum_prof)
        if val_acum_prof is not None:
            if direction[:3] == "ver":
                val_acum_prof.name = val_acum_prof.name + "-valor-vertical"
            else:
                val_acum_prof.name = val_acum_prof.name.replace(sens_dir[direction], "-valor-{}".format(direction))
            last_name = val_acum_prof.name
            val_acum_prof = val_acum_prof.to_frame()

        
        for k, height in enumerate(height_keys[1:]):
            val_prof = self.get_values_sensor_height_direction(sensor_name, height,
                                direction, offset=0.0 if offsets is None else offsets[k+1])
            if val_prof is not None:
                if direction[:3] == "ver":
                    val_prof.name = val_prof.name + "-valor-vertical"
                else:
                    val_prof.name = val_prof.name.replace(sens_dir[direction], "-valor-{}".format(direction))
                new_name = val_prof.name
                # if difference_depth:
                #     val_prof = val_prof - val_acum_prof[last_name]
                val_prof.name = new_name
                last_name = new_name
                if val_acum_prof is not None:
                    val_acum_prof = pd.concat([val_acum_prof, val_prof], axis=1)
                else:
                    val_acum_prof = val_prof.copy()
        return val_acum_prof


    def get_columns_sensor(self, sensor_id):
        columns_sensor = [column for column in self.columns_sensors \
                          if column[:len(sensor_id)] == sensor_id]
        return columns_sensor


    @staticmethod
    def get_fileName(node_id, dt_month=None, folder=None, custom=True, compacted=True):
        date_str = "current" if dt_month is None else "{}-{:02d}".format(dt_month.year, dt_month.month)
        fileName = "readings-{}-{}.dat".format(node_id, date_str)
        if custom:
            fileName = "custom-{}".format(fileName)
        if compacted:
            fileName = "compacted-{}".format(fileName)
        if folder is not None:
            if dt_month is None:
                fileName = "{}/current/{}".format(folder, fileName)
                #print("Current fileName", fileName)
            else:
                fileName = "{}/{}/{}".format(folder, dt_month.year, fileName)
        return fileName


    @staticmethod
    def exists_fileName(node_id, dt_month=None, folder=None):
        fileName = reader_source_clino.get_fileName(node_id, dt_month=dt_month, folder=folder)
        #print("To check", fileName)
        return exists(fileName)


class reader_sensors_clino:
    def __init__(self, fileName=None):
        self.sensors = pd.read_csv(fileName, delimiter=";", encoding='latin1')
        self.sensors.set_index("id", inplace=True)
        return

    def get_sensores(self):
        return self.sensors


    def get_ejes_in_node(self, node_id):
        sensors_node = self.get_sensors_node(node_id)
        ejes = list(set(sensors_node["Eje"]))
        ejes.sort()
        return ejes


    def get_instruments_in_node(self, node_id):
        sensors_node = self.get_sensors_node(node_id)
        instruments = list(set(sensors_node["id_instrumento"]))
        instruments.sort()
        return instruments


    def get_sensors_node(self, node_id):
        sensor_ids = self.sensors["node_id"] == node_id
        sensors_node = self.sensors[sensor_ids]
        return sensors_node


    def get_sensors_eje(self, eje):
        sensor_ids = self.sensors["Eje"] == eje
        sensors_eje = self.sensors[sensor_ids]
        return sensors_eje


    def get_sensors_node_eje(self, node_id, eje):
        sensor_ids   = self.sensors["node_id"] == node_id
        sensor_ids  &= self.sensors["Eje"] == eje
        sensors_out  = self.sensors[sensor_ids]
        return sensors_out


    def get_sensors_node_intrument(self, node_id, instrument_id):
        sensor_ids   = self.sensors["node_id"] == node_id
        sensor_ids  &= self.sensors["id_instrumento"] == instrument_id
        sensors_out  = self.sensors[sensor_ids]
        return sensors_out


    def get_eje_name_instrument(self, node_id, instrument_id):
        sensors = self.get_sensors_node_intrument(node_id, instrument_id)
        if len(sensors) == 0:
            return
        return sensors.iloc[0]["Eje"]


    def get_sensor_name_eje(self, node_id, eje):
        sensors = self.get_sensors_node_eje(node_id, eje)
        if len(sensors) == 0:
            return
        return sensors.iloc[0]["sensor"]


    def get_sensor_name_instrument(self, node_id, instrument_id):
        sensors = self.get_sensors_node_intrument(node_id, instrument_id)
        if len(sensors) == 0:
            return
        return sensors.iloc[0]["sensor"]


    def get_instrument(self, node_id, eje):
        sensors = self.get_sensors_node_eje(node_id, eje)
        if len(sensors) == 0:
            return
        return sensors.iloc[0]["id_instrumento"]


    def get_references_node_intrument(self, node_id, instrument_id):
        sensors    = self.get_sensors_node_intrument(node_id, instrument_id)
        references = {}
        for direction in ["vertical", "transversal", "longitudinal"]:
            ref_name   = "ref_{}".format(direction[:4])
            references[direction] = {sensor["profundidad"]:json.loads(sensor[ref_name]) for k, sensor in sensors.iterrows()}
        return references



class reader_umbrales_clino:
    def __init__(self, fileName=None):
        self.ejes = pd.read_csv(fileName, delimiter=";", encoding='latin1')
        self.ejes.set_index("id_eje", inplace=True)
        return

    def get_ejes(self):
        ejes = set(self.ejes.index)
        if np.nan in ejes:
            ejes.remove(np.nan)
        ejes = list(ejes)
        ejes.sort()
        return ejes
    
    def get_eje(self,eje):
        eje_bool = self.ejes.index == eje
        return self.ejes[eje_bool]

    def get_umbrales_sismo(self, eje, direccion):
        if direccion[:4] == "vert": direccion = "vert"
        eje_bool = (self.ejes.index == eje) & (self.ejes["direccion"] == direccion) & (self.ejes["etapa"] == "sismo")
        ejes = self.ejes[eje_bool]
        if len(ejes) == 0:
            return
        row = ejes.iloc[0].to_dict()
        for key in ['direccion', 'etapa', 'ventana']:
            row.pop(key)
        return row

    def get_umbrales_oper(self, eje, direccion, ventana):
        if direccion[:4] == "vert": direccion = "vert"
        eje_bool = (self.ejes.index == eje) & (self.ejes["etapa"] == "oper")
        eje_bool = eje_bool & (self.ejes["ventana"] == ventana) & (self.ejes["direccion"] == direccion)
        ejes = self.ejes[eje_bool]
        if len(ejes) == 0:
            return
        row = ejes.iloc[0].to_dict()
        for key in ['direccion', 'etapa', 'ventana']:
            row.pop(key)
        return row

    def get_umbrales_constr(self, eje, direccion, ventana):
        if direccion[:4] == "vert": direccion = "vert"
        eje_bool = (self.ejes.index == eje) & (self.ejes["etapa"] == "constr")
        eje_bool = eje_bool & (self.ejes["ventana"] == ventana) & (self.ejes["direccion"] == direccion)
        ejes = self.ejes[eje_bool]
        if len(ejes) == 0:
            return
        row = ejes.iloc[0].to_dict()
        for key in ['direccion', 'etapa', 'ventana']:
            row.pop(key)
        return row


class reader_values_umbrales_clino_tasa:

    def __init__(self, id_instrument, direction, ventana, folder,
                    sdate=None, edate=None, with_date_range=False):
        fileName = reader_values_umbrales_clino_tasa.get_fileName(id_instrument, direction, #ventana, 
                                        sdate=sdate, edate=edate, with_date_range=with_date_range)
        self.fileName = "{}/{}".format(folder, fileName)
        self.data        = pd.read_csv(self.fileName, parse_dates=["TIMESTAMP"], delimiter=",")
        #print("columns", self.data.columns)
        self.data.set_index("TIMESTAMP", inplace=True)
        self.ventana     = ventana
        self.direction   = direction
        self.sensor_name = id_instrument
        self.heights     = self.get_heights()
        return

    @property
    def columns(self):
        return self.data.columns
    
    @property
    def table(self):
        return self.data

    @staticmethod
    def get_fileName(id_instrument, direction, ventana=None, sdate=None, edate=None, with_date_range=False):
        fileOut = "{}-tasas-{}".format(id_instrument, direction)
        if ventana is not None:
            fileOut = "{}-{}".format(fileOut, ventana)
        if with_date_range:
            fileOut += "-{}-{}.csv".format( sdate.strftime("%Y%m%d"), edate.strftime("%Y%m%d")) #node_id, eje+
        else:
            fileOut += ".csv"
        return fileOut


    def get_heights(self):
        tasa_str    = "-valor-{}-tasa-{}".format(self.direction, self.ventana)
        heigths_all = [elem[len(self.sensor_name)+1:-len(tasa_str)] for elem in self.columns if elem[-len(tasa_str)-1:-len(tasa_str)]=="m" and elem[-len(tasa_str):] == tasa_str]
        heights = set(heigths_all)
        
        heights = [(round(np.float(elem.rstrip("m")),3),elem.rstrip("m")) for elem in heights]
        heights.sort(key=lambda tup: tup[0])
        return {elem[0]:elem[1] for elem in heights}


    def plot_values_umbral(self, id_instrument, direction, ventana, axis):
        from matplotlib import dates

        dateFmt = dates.DateFormatter("%Y/%m/%d")
        # fig, axes = plt.subplots(ncols=grid_width, nrows=grid_height, figsize=(15,15))
        # fig.tight_layout(pad=2.4, w_pad=0.5, h_pad=6.0)
        # axes = axes.reshape(grid_width*grid_height)
        thresholds = ["alert-{}-min", "caution-{}-min", "preventive-{}-min", "preventive-{}-max", "caution-{}-max", "alert-{}-max"]
        thresholds = [threshold.format(ventana) for threshold in thresholds]
        heights    = self.get_heights()
        #print("heights", heights)
        #print(self.data.columns)
        for k, height in enumerate(heights):
            curve = "{}_{}m-valor-{}-tasa-{}".format(id_instrument, heights[height], direction, ventana)
            print(curve)
            self.data[curve].plot(ax=axis, title="{} {}".format(id_instrument, heights[height]))
            axis.tick_params(axis='x', labelrotation=30)
            axis.xaxis.set_major_formatter(dateFmt)
        for threshold in thresholds:
            thresh_name = "{}-{}".format(id_instrument, threshold)
            if thresh_name in self.data.columns:
                self.data[thresh_name].plot(ax=axis, style="--")
        axis.legend(loc="best")
        axis.set_title("{} {} {}".format(id_instrument, direction, ventana))
        return


class reader_values_clino:
    def __init__(self, intrument_id, folder, sdate=None, edate=None, with_date_range=False): #direction, 
        fileName      = reader_values_clino.get_fileName(intrument_id, sdate=sdate, edate=edate, 
                                                with_date_range=with_date_range) #direction,
        self.fileName = "{}/{}/{}".format(folder, DIR_DIFERENCIAL, fileName)
        self.tableVal = pd.read_csv(self.fileName, delimiter=",")
        self.tableVal.set_index("TIMESTAMP", inplace=True)
        self.tableVal.index = np.array([datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') for dt in self.tableVal.index])
        return

    @property
    def columns(self):
        return self.tableVal.columns
    
    @property
    def sensor_name(self):
        name = self.columns[0]
        name = name[:name.rfind('_')]
        return name


    @staticmethod
    def get_fileName(intrument_id, direction=None, sdate=None, edate=None, with_date_range=False):
        fileOut  = "{}-valores".format(intrument_id)
        if direction is not None:
            fileOut += "-{}".format(direction)
        if with_date_range:
            fileOut += "-{}-{}.csv".format( sdate.strftime("%Y%m%d"), edate.strftime("%Y%m%d")) #node_id, eje+
        else:
            fileOut += ".csv"
        return fileOut


    def heights(self, direction):
        tasa_cols = [column[:-len(direction)-1] for column in self.columns if column[-(len(direction)):] == direction]
        heights_val =  [0] + [-float(row[row.rfind('_')+1:-1]) for row in tasa_cols] # list(range(0,-len(tasa_cols)-1,-1)) #
        return heights_val

    def heights_str(self, direction):
        tasa_cols = [column[:-len(direction)-1] for column in self.columns if column[-(len(direction)):] == direction]
        heights_str = [row[row.rfind('_')+1:] for row in tasa_cols]
        return heights_str


    def plot_values(self, axis, direction):
        from matplotlib import dates
        dateFmt = dates.DateFormatter("%Y/%m/%d")
        for column in self.tableVal.columns:
            if column.split('-')[-1] == direction:
                self.tableVal[column].plot(ax=axis, title="{}".format(self.sensor_name))
        axis.tick_params(axis='x', labelrotation=30)
        axis.xaxis.set_major_formatter(dateFmt)
        axis.legend(loc="best")
        return



class reader_diferencial:
    def __init__(self, fileName):
        self.fileName = fileName
        self.tableVal = pd.read_csv(fileName, delimiter=",")
        self.tableVal.set_index("TIMESTAMP", inplace=True)
        self.tableVal.index = np.array([datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') for dt in self.tableVal.index])
        return

    @property
    def columns(self):
        return self.tableVal.columns
    
    @property
    def sensor_name(self):
        name = self.columns[0]
        name = name[:name.rfind('_')]
        return name
    
    @property
    def heights(self):
        vert      = "diferencia-vertical"
        tasa_cols = [column[:-len(vert)-1] for column in self.columns if column[-(len(vert)):] == vert]
        heights_val = [0] + [-float(row[row.rfind('_')+1:-1]) for row in tasa_cols]
        return heights_val


    def heights_unitary(self, direction):
        vert      = "diferencia-{}".format(direction)
        tasa_cols = [column[:-len(vert)-1] for column in self.columns if column[-(len(vert)):] == vert]
        heights_val = list(range(-1,-len(tasa_cols)-1,-1))
        return heights_val


    def heights_str(self, direction="vertical"):
        vert      = "diferencia-{}".format(direction)

        tasa_cols = [column[:-len(vert)-1] for column in self.columns if column[-(len(vert)):] == vert]
        heights_str = [row[row.rfind('_')+1:] for row in tasa_cols]
        return heights_str


    def get_values_row(self, row_id, include_zero=True):
        row     = self.tableVal.iloc[row_id]
        values  = [0.0] if include_zero else []
        values += [row["{}_{}-vertical".format(self.sensor_name, height)] for height in self.heights_str]
        return values


    # def plot_parcial_total(self, axis, year, month):
    #     from matplotlib import dates
    #     dateFmt = dates.DateFormatter("%Y/%m/%d")
    #     #print('columns', self.tableVal.columns)

    #     for k, elem in enumerate(self.tableVal.iterrows()):
    #         dt, row = elem
    #         if (row.name >= date(year,month,1)) and (row.name < date(year,month+1,1)):
    #             values = self.get_values_row(k)
    #             axis.plot(values, self.heights_unitary, label=row.name.strftime("%Y/%m/%d"))
    #     axis.legend()
    #     axis.set_title("Asentamiento Parcial Total de {}, mes {:02d}/{}".format(self.sensor_name, month, year))
    #     return


    def plot_parcial_diferencial_puntos(self, axis, direction, vert_displace=True, sdate=None, edate=None):
        from matplotlib import dates
        dateFmt = dates.DateFormatter("%Y/%m/%d")

        days    = self.tableVal.index
        min_val = -(len(self.tableVal.columns)//3+1)
        max_val = 2
        n = 0
        for k, column in enumerate(self.tableVal.columns):
            if column.split('-')[-1] == direction:
                #print(column, self.tableVal[column])
                displace = n if vert_displace else 0
                values   = self.tableVal[column] - displace
                min_val  = min(np.nanmin(values), min_val)
                max_val  = max(np.nanmax(values), max_val)
                
                axis.plot(days, values, label="{}".format(column))
                #print("values", min(values), max(values))
                n += 1
        print("Rango valores {} -> {}".format(min_val, max_val))
        min_val = max(-(n+1), min_val)
        max_val = min(2, max_val)
        axis.tick_params(axis='x', labelrotation=30)
        axis.xaxis.set_major_formatter(dateFmt)
        if (sdate is not None) or (edate is not None):
            #print("dates", sdate, edate)
            valid_dates =  (days <= np.datetime64(edate)) & (days >= np.datetime64(sdate) )
            #max_val     = np.nanmax(values[valid_dates])
            #print("max_val", type(max_val))
            #if (max_val is not None) and (not np.isnan(max_val)) and (axis.get_ylim() is not None):
            axis.set_ylim([min_val, max_val])
            axis.set_xlim([sdate, edate])
        axis.legend(loc="best")
        axis.set_title("Asentamiento Diferencial de Puntos {} - {}".format(self.sensor_name, direction))
        return


    def plot_deformacion_dif_parcial(self, axis, year, month, direccion):
        SCALE = 1000
        UNIT  = "mm"
        from matplotlib import dates
        dateFmt = dates.DateFormatter("%Y/%m/%d")
        markers = ",o+Dxv^<>p"

        for k, dt_row in enumerate(self.tableVal.iterrows()):
            dt, row = dt_row
            next_year, next_month = next_year_month(year, month)
            if (row.name.date() >= date(year,month,1)) and (row.name.date() < date(next_year,next_month,1)):
                heights_str = self.heights_str(direccion)
                if len(heights_str) > 0:
                    if direccion == "vertical":
                        values = [0] + [row["{}_{}-diferencia-{}".format(self.sensor_name, height, direccion)]*SCALE for height in heights_str]
                        heights = [0] + self.heights_unitary(direccion)
                    else:
                        values = [row["{}_{}-diferencia-{}".format(self.sensor_name, height, direccion)]*SCALE for height in heights_str] +[0]
                        heights = self.heights_unitary(direccion) + [-len(heights_str)-1]
                    #print(heights, values)
                    mk_id = k % 10
                    axis.plot(values, heights, label=row.name.strftime("%Y/%m/%d"), marker=markers[mk_id])
                else:
                    print("no hay valores para direccion {} en archivo {}".format(direccion, self.fileName))
        axis.legend(loc="best")
        axis.set_title("Deformacion {} de {}, mes {:02d}/{}".format(direccion, self.sensor_name, month, year))
        axis.set_xlabel('Desplazamiento ({})'.format(UNIT))
        axis.axvline(x=0)
        return


def next_year_month(year, month):
    if month < 12:
        return year, month+1
    return year+1, 1


def make_sensor_umbral_tasa_vertical(ejes, eje_name, sdate, edate, estado, ventana, sensor_name):
    direc = "vertical"
    if estado == "oper":
        eje = ejes.get_umbrales_oper(eje_name, direc, ventana)
    elif estado == "constr":
        eje = ejes.get_umbrales_constr(eje_name, direc, ventana)
    elif estado == "sismo":
        eje = ejes.get_umbrales_sismo(eje_name, direc)
    else:
        raise Exception("estado no definido {}".format(estado))
    if eje is None:
        print("no se cargo eje", eje_name)

    delta  = edate - sdate
    tdates = [sdate + timedelta(days=i) for i in range(delta.days + 1)]
    dataf   = {"Date-and-time":tdates}
    levels = {"prev":"preventive", "caution":"caution", "alert":"alert"}
    for level in levels:
        for segment in ["min", "max"]:
            dst_name = "{}-{}-{}-{}".format(sensor_name, levels[level], ventana, segment)
            if eje is not None:
                src_name = "{}_{}".format(level, segment)
                if src_name in eje:
                    if eje[src_name] is not np.nan:
                        #print(src_name, dst_name)
                        dataf[dst_name] = np.ones(len(tdates)) * eje[src_name]
                else:
                    dataf[dst_name] = None
            else:
                dataf[dst_name] = None

    table = pd.DataFrame(dataf)
    table.set_index("Date-and-time", inplace=True)
    return table


def make_sensor_umbral_none(name_sensor, sdate, edate):
    delta   = edate - sdate
    tdates  = [sdate + timedelta(days=i) for i in range(delta.days + 1)]
    dataf   = {"Date-and-time":tdates}
    for level in ["preventive", "caution", "alert"]:
        for segment in ["min", "max"]:
            dataf["{}-{}-{}".format(name_sensor, level, segment)] = [None] * len(tdates)
    table = pd.DataFrame(dataf)
    table.set_index("Date-and-time", inplace=True)
    return table


def get_sensor_values_all(node_id, sensor_name, sdate, edate, folder, direction,
                          delete_zeros=True, nans_to_zeros=False, difference_depth=True):
    """ Para todos los sensores con nombre **sensor_name**
    :param node_id: de cada sensor
    :type node_id: str
    :param sensor_name: de cada sensor
    :type sensor_name: str
    :param sdate: _description_
    :type sdate: datetime
    :param edate: _description_
    :type edate: datetime
    :param folder: _description_
    :type folder: str
    :param direction: _description_
    :type direction: str
    :return: _description_
    :rtype: pd.DataFrame
    """
    # node_id = int(sensor["node_id"])
    # sensor_name = sensor["sensor"]
    # print("sensor_name", sensor_name)
    #print("checking", reader_source_clino.get_fileName(node_id, dt_month=dt_month, folder=folder_path))
    
    with_year_folder = False
    table = None
    for dt_month in rrule.rrule(rrule.MONTHLY, dtstart=sdate, until=edate):
        try:
            #print(dt_month)
            if with_year_folder:
                folder_path = "{}/{}".format(folder, dt_month.year)
            else:
                folder_path = folder
            stack = None
            if reader_source_clino.exists_fileName(node_id, dt_month=dt_month, folder=folder_path):
                reader = reader_source_clino(node_id=node_id, dt_month=dt_month, folder=folder_path)
                #print("csv", reader.columns)
                stack = reader.get_values_sensor_direction(sensor_name, direction, difference_depth=difference_depth)
            if stack is not None:
                if table is None:
                    table = stack.copy()
                else:
                    table =  pd.concat([table, stack], axis=0) # table.join(stack)}
        except:
            fileName = reader_source_clino.get_fileName(node_id, dt_month=dt_month, folder=folder_path)
            print("Could not load file {} with node {}, month {}".format(fileName, node_id, dt_month))
            traceback.print_exc(limit=2)

    if reader_source_clino.exists_fileName(node_id, folder=folder):
        reader = reader_source_clino(node_id=node_id, folder=folder)
        stack = reader.get_values_sensor_direction(sensor_name, direction, difference_depth=difference_depth)
        if stack is not None:
            if table is None:
                table = stack.copy()
            else:
                table = pd.concat([table, stack], axis=0)
    if table is None: # no data
        print("No data found for node {}, sensor {}".format(node_id, sensor_name))
        return
    table.index.name = "TIMESTAMP"
    if delete_zeros:
        for column in table.columns:
            table[column].replace(to_replace=0.0, value=np.nan, inplace=True)
    if nans_to_zeros:
        for column in table.columns:
            table[column].replace(to_replace=np.nan, value=0.0, inplace=True)
            table[column].replace(to_replace=None, value=0.0, inplace=True)
    return table


def get_sensor_first_value(node_id, instrument_id, sdate, edate, folder, direction):
    table = get_sensor_values_all(node_id, instrument_id, sdate, edate, folder, direction,
                                        delete_zeros=True, nans_to_zeros=False, difference_depth=False)
    last_str = "-valor-{}".format(direction)
    
    heigths_all = [elem[len(instrument_id)+1:-len(last_str)] for elem in table.columns \
                    if (elem[:len(instrument_id)] == instrument_id) and (elem[-len(last_str):] == last_str)]
    heights = set(heigths_all)
    #print(heights)
    heights = [(round(np.float(elem.rstrip("m")),3),elem.rstrip("m")) for elem in heights]
    heights.sort(key=lambda tup: tup[0])
    out_tup = {}
    for height, height_str in heights:
        value = None
        for k in range(len(table)):
            column = "{}_{}m-valor-{}".format(instrument_id, height_str, direction)
            if not pd.isnull(table.iloc[k][column]):
                value = table.iloc[k][column]
                break
        out_tup[height] = value
    return out_tup



def get_node_tasas(node_id, sensor_name, sdate, edate, folder, direction, ventana,
                    delete_zeros=True, nans_to_zeros=False, difference_depth=True):
    ndays  = 7 if ventana[:3]=="sem" else 30 
    # node_id = sensor["node_id"]
    # sensor_name = sensor["sensor"]
    #print(node_id, sensor_name, sdate, edate, folder, direction, ventana)
    table  = get_sensor_values_all(node_id, sensor_name, sdate, edate, folder, direction,
                                delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros, difference_depth=difference_depth)
    
    if table is None:
        return
    
    delta  = edate - sdate
    tdates = [sdate + timedelta(days=i) for i in range(delta.days + 1)]
    data   = None #pd.DataFrame() #columns = [column_out]
    for col_name in table.columns:
        # col_name = "{}_{}m".format(sensor["sensor"], sensor["profundidad"])
        # if direction[:4]=="vert":
        #     col_name += "_vertical"
        column_out = col_name + "-tasa-{}".format(ventana)
        values_out = []
        dates_out  = []
        for k, eday in enumerate(tdates):
            #print(eday)
            sday = eday + timedelta(days=-ndays)
            filter = (table.index > sday) & (table.index <= eday)
            values_filt = table[filter]
            # if k == 0: print(eday, values_filt.columns)
            dct = {}
            values = values_filt[col_name]
            values = values.dropna()
            if len(values) > 1:
                tstamps = values.index.values.astype(np.int64) // 1E9
                tstamps = tstamps / (30*86400)
                linear = linregress(tstamps, values.values)
                #dct[column_out] = linear.slope
                values_out.append(linear.slope)
            else:
                #dct[column_out] = np.nan
                values_out.append(np.nan)
            #data.loc[eday] = dct
            dates_out.append(eday)
        serie = pd.Series(data=values_out, index=dates_out)
        serie.name = column_out
        if data is None:
            data = serie.replace(np.nan, 0.0).to_frame()
        else:
            if difference_depth:
                serie = data[last_col] + serie.replace(np.nan, 0.0)
            data = pd.concat([data, serie], axis=1)
        last_col = serie.name
    return data


def get_table_sensor_tasas_umbral(node_id, instrument_id, sdate, edate, folder, direction, estado, ventana,
                                  rename_sensor=True, delete_zeros=True, nans_to_zeros=False):
    file_config = "{}/clinos_umbrales_tasa.csv".format(folder)
    file_info   = "{}/clinos_info.csv".format(folder)
    rd_sensors  = reader_sensors_clino(file_info)
    sensors     = rd_sensors.get_sensors_node_intrument(node_id, instrument_id)
    #print("sensors", sensors)
    if len(sensors) == 0:
        print("No sensors for node:{}, instrumento:{}".format(node_id, instrument_id))
        return

    sensor_name = rd_sensors.get_sensor_name_instrument(node_id, instrument_id)
    table       = get_node_tasas(node_id, sensor_name, sdate, edate, folder, direction, ventana,
                                delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros, difference_depth=False)
    #print("table tasas", table.columns)
    ejes        = reader_umbrales_clino(file_config)
    # print("ejes", ejes.ejes.columns)
    # eje_elem    = ejes.get_eje(eje_name)
    # print("eje_elem", eje_elem)
    eje_name    = rd_sensors.get_eje_name_instrument(node_id, instrument_id)
    thresh      = make_sensor_umbral_tasa_vertical(ejes, eje_name, sdate, edate, estado, ventana, sensor_name)
    if thresh is not None:
        table       = pd.concat([table, thresh], axis=1)
    if rename_sensor:
        #print("sensors.columns", sensors.columns)
        #print("renombre",sensor_name, "->", instrument_id)
        rename_cols = {column: column.replace(sensor_name, instrument_id) for column in table.columns}
        #print(rename_cols)
        table.rename(columns=rename_cols, inplace = True)
    return table


def procesa_nodo_clino_tasa_ventana(node_id, instrument_id, sdate, edate, folder, direction, estado, ventana,
                                    with_date_range=False, delete_zeros=True, nans_to_zeros=False):
    print(">>node_id", node_id)
    try:
        table  = get_table_sensor_tasas_umbral(node_id, instrument_id, sdate, edate, folder, direction, estado, ventana,
                                                delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros)
        if table is None:
            print("No se pudo cargar datos con nodo {} y instrument_id {}".format(node_id, instrument_id))
            return
        table.index.name = "TIMESTAMP"

        #Guardado de archivo
        folder_out = folder + "/tasas"
        file_info  = "{}/clinos_info.csv".format(folder)
        rd_sensors = reader_sensors_clino(file_info)
        #id_instrument = rd_sensors.get_instrument(node_id, eje_name)
        fileNameOut = reader_values_umbrales_clino_tasa.get_fileName(instrument_id, direction, ventana,
                                                sdate=sdate, edate=edate, with_date_range=with_date_range)
        fileOut = "{}/{}".format(folder_out, fileNameOut)
                      
        if table is not None:
            table.to_csv(fileOut, sep=',', doublequote=False, quoting=csv.QUOTE_NONNUMERIC)
            print(fileOut, "saved")
        else:
            print("No hay umbrales para nodo", node_id)
    except :
        print(">>No se pudo procesar nodo", node_id)
        traceback.print_exc(limit=2)
    return


def procesa_nodo_clino_tasa(node_id, instrument_id, sdate, edate, folder, direction, estado,
                            with_date_range=False, delete_zeros=True, nans_to_zeros=False):
    # Mes
    try:
        table_mes = get_table_sensor_tasas_umbral(node_id, instrument_id, sdate, edate, folder, direction, estado, "mes",
                                                delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros)
        if table_mes is None:
            print("No se pudo cargar datos con nodo {}, instrument_id {} y ventana mes".format(node_id, instrument_id))
        else:
            table_mes.index.name = "TIMESTAMP"
    except :
        print(">>No se pudo procesar nodo {} ventana mes".format(node_id))
        traceback.print_exc(limit=2)
        table_mes = None
    
    # Semana
    try:
        table_sem = get_table_sensor_tasas_umbral(node_id, instrument_id, sdate, edate, folder, direction, estado, "sem",
                                                delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros)
        if table_sem is None:
            print("No se pudo cargar datos con nodo {}, instrument_id {} y ventana semana".format(node_id, instrument_id))
        else:
            table_sem.index.name = "TIMESTAMP"
    except :
        print(">>No se pudo procesar nodo {} ventana semanal".format(node_id))
        traceback.print_exc(limit=2)
        table_sem = None
    if table_mes is not None:
        if table_sem is not None:
            table = pd.concat([table_mes, table_sem], axis=1)
        else:
            table = table_mes
    else:
        if table_sem is not None:
            table = table_sem
        else:
            print("No se pudo procesar mes ni semana")
            return

    #Guardado de archivo
    folder_out = folder + "/tasas"
    file_info  = "{}/clinos_info.csv".format(folder)
    rd_sensors = reader_sensors_clino(file_info)
    #id_instrument = rd_sensors.get_instrument(node_id, eje_name)
    fileNameOut = reader_values_umbrales_clino_tasa.get_fileName(instrument_id, direction,
                                            sdate=sdate, edate=edate, with_date_range=with_date_range)
    fileOut = "{}/{}".format(folder_out, fileNameOut)
    table.to_csv(fileOut, sep=',', doublequote=False, quoting=csv.QUOTE_NONNUMERIC)
    print(fileOut, "saved")
    return


def get_table_nodo_clino_valores(node_id, instrument_id, sdate, edate, folder, direction,
                                rename_sensor=True, delete_zeros=True, nans_to_zeros=False, difference_depth=False):
    file_info  = "{}/clinos_info.csv".format(folder)
    rd_sensors = reader_sensors_clino(file_info)

    sensors    = rd_sensors.get_sensors_node_intrument(node_id, instrument_id)
    if len(sensors) == 0:
        print("No sensors for node:{}, instrument:{}".format(node_id, instrument_id))
        return
    try:
        sensor_name = rd_sensors.get_sensor_name_instrument(node_id, instrument_id)
        table = get_sensor_values_all(node_id, sensor_name, sdate, edate, folder, direction,
                                        delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros, difference_depth=difference_depth)
        if table is None:
            return
        table = table.groupby(table.index.date).mean()
        table.index = [datetime(dt.year, dt.month, dt.day, 12) for dt in  table.index]
        table.index.name = "TIMESTAMP"
        if rename_sensor:
            #print("sensors.columns", sensors.columns)
            #print("renombre",sensor_name, "->", instrument_id)
            rename_cols = {column: column.replace(sensor_name, instrument_id) for column in table.columns}
            #print(rename_cols)
            table.rename(columns=rename_cols, inplace = True)
    except Exception as e:
        print("No se pudo procesar nodo", node_id, e)
        traceback.print_exc(limit=2)
        table = None
    return table


def get_table_nodo_clino_diferencial(node_id, instrument_id, sdate, edate, folder, direction, ref_values=None,
                                    rename_sensor=True, delete_zeros=True, nans_to_zeros=False, difference_depth=True):
    #
    file_info  = "{}/clinos_info.csv".format(folder)
    rd_sensors = reader_sensors_clino(file_info)
    sensors    = rd_sensors.get_sensors_node_intrument(node_id, instrument_id)
    if len(sensors) == 0:
        print("No sensors for node:{}, instrument:{}".format(node_id, instrument_id))
        return
    try:
        sensor_name = rd_sensors.get_sensor_name_instrument(node_id, instrument_id)
        tableVals = get_sensor_values_all(node_id, sensor_name, sdate, edate, folder, direction,
                                            delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros, difference_depth=False)
        #Diferencia Total: the location is the one of ref_date
        #print("tableVals", tableVals.columns)
        tableVals = tableVals.groupby(tableVals.index.date).mean()
        tableVals.index = [datetime(dt.year, dt.month, dt.day, 12) for dt in  tableVals.index]
        tableVals.index.name = "TIMESTAMP"

        valor_dir = "valor-{}".format(direction)
        tasa_cols = [column[:-len(valor_dir)-1] for column in tableVals.columns if column[-(len(valor_dir)):] == valor_dir]
        #print("tasa_cols", tasa_cols)
        heights   = [(row.split('_')[-1], float(row.split('_')[-1][:-1])) for row in tasa_cols]
        #print("heights", heights, "ref_values", ref_values)
        
        if ref_values is not None:
            for k in range(len(heights)):
                #if not np.isnan(ref_values[k]):
                col_name_0 = "{}_{}-{}".format(sensor_name, heights[k][0], valor_dir)
                #print(col_name_0, ref_values[k])
                ref_value_vec = np.zeros(len(tableVals))
                index_dt = np.array([tt.date() for tt in tableVals.index])
                for start_str, ref_value in ref_values[k]:
                    start_dt = datetime.strptime(start_str, "%Y-%m-%d").date()
                    valid = index_dt >= start_dt
                    ref_value_vec[valid] = ref_value
                #print("ref_value_vec", ref_value_vec)
                tableVals[col_name_0] -= ref_value_vec

        # print("Diferencia Total")
        if difference_depth:
            if direction == "vertical": #la diferencia es parcial
                for k in range(len(heights)-1):
                    col_name_1 = "{}_{}-{}".format(sensor_name, heights[k+1][0], valor_dir)
                    col_name_0 = "{}_{}-{}".format(sensor_name, heights[k][0], valor_dir)
                    #print(col_name_0, col_name_1)
                    tableVals[col_name_1] -= tableVals[col_name_0]
            else: #la diferencia es acumulada
                for k in range(len(heights)-1, 0, -1):
                    col_name_1 = "{}_{}-{}".format(sensor_name, heights[k][0], valor_dir)
                    col_name_0 = "{}_{}-{}".format(sensor_name, heights[k-1][0], valor_dir)
                    #print(col_name_0, col_name_1)
                    tableVals[col_name_0] += tableVals[col_name_1]
        #rename valor
        rename_cols = {column: column.replace("valor", "diferencia") for column in tableVals.columns}
        tableVals.rename(columns=rename_cols, inplace = True)



        if rename_sensor:
            #print("sensors.columns", sensors.columns)
            #print("renombre",sensor_name, "->", instrument_id)
            rename_cols = {column: column.replace(sensor_name, instrument_id) for column in tableVals.columns}
            #print(rename_cols)
            tableVals.rename(columns=rename_cols, inplace = True)

    except Exception as e:
        print("No se pudo procesar nodo", node_id, e)
        traceback.print_exc(limit=2)
        tableVals = None
    return tableVals


def procesa_nodo_clino_valores(node_id, instrument_id, sdate, edate, folder,
                                rename_sensor=True, delete_zeros=True, nans_to_zeros=False):
    direction = "vertical"
    table = get_table_nodo_clino_valores(node_id, instrument_id, sdate, edate, folder, direction,
                                rename_sensor=rename_sensor, delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros, difference_depth=False)
    for direction in ["transversal", "longitudinal"]:
        thoriz = get_table_nodo_clino_valores(node_id, instrument_id, sdate, edate, folder, direction,
                                rename_sensor=rename_sensor, delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros, difference_depth=False)
        table = pd.concat([table, thoriz], axis=1)
    folder_out = folder + "/valores"
    fileOut  = "{}/{}".format(folder_out, reader_values_clino.get_fileName(instrument_id))
    if table is not None:
        table.to_csv(fileOut, sep=',', doublequote=False, quoting=csv.QUOTE_NONNUMERIC)
        print(fileOut, "saved")
    else:
        print("No hay informacion para nodo {}, instrumento {}, direccion {}".format(node_id, instrument_id, direction))
    return


def procesa_nodo_clino_diferencial(node_id, instrument_id, sdate, edate, folder, ref_values=None,
                                rename_sensor=True, delete_zeros=True, nans_to_zeros=False, difference_depth=True):
    direction = "vertical"
    refs      = list(ref_values[direction].values())
    #print(direction, refs)
    tableVals = get_table_nodo_clino_diferencial(node_id, instrument_id, sdate, edate, folder, direction, refs,
                                rename_sensor=rename_sensor, delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros, difference_depth=difference_depth)
    for direction in ["transversal", "longitudinal"]:
        refs     = list(ref_values[direction].values())
        tableHor = get_table_nodo_clino_diferencial(node_id, instrument_id, sdate, edate, folder, direction, refs,
                                rename_sensor=rename_sensor, delete_zeros=delete_zeros, nans_to_zeros=nans_to_zeros, difference_depth=difference_depth)
        tableVals = pd.concat([tableVals, tableHor], axis=1)

    #Guardado de archivo
    folder_out = folder + "/{}".format(DIR_DIFERENCIAL)
    fileOut  = "{}/{}-diferencial.csv".format(folder_out, instrument_id) #sdate.strftime("%Y%m%d"), edate.strftime("%Y%m%d")
    if tableVals is not None:
        tableVals.to_csv(fileOut, sep=',', doublequote=False, quoting=csv.QUOTE_NONNUMERIC)
        print(fileOut, "saved")
    else:
        print("No hay umbrales para nodo", node_id)
    return


def generate_plot_tasa_instrument(id_instrument, direction, ventana, folder, folder_dst, sdate=None, edate=None, with_date_range=False):
    from matplotlib import pyplot as plt
    #ejes = ["A", "B", "C", "D", "E", "F", "G", "H"]
    #dir_tasa = "{}-tasa".format(direction)
    # file_info  = "{}/clinos_info.csv".format(folder)
    # rd_sensors = reader_sensors_clino(file_info)
    #for eje in ejes:
        #id_instrument = rd_sensors.get_instrument(node_id, eje)
    fileTasas = reader_values_umbrales_clino_tasa.get_fileName(id_instrument, direction, #ventana, =None
                                            sdate=sdate, edate=edate, with_date_range=with_date_range)
    folder_tasas = "{}/tasas".format(folder)
    fileTasas = "{}/{}".format(folder_tasas, fileTasas)
    if exists(fileTasas):
        table = reader_values_umbrales_clino_tasa(id_instrument, direction, ventana, folder_tasas,
                                            sdate=sdate, edate=edate, with_date_range=with_date_range)
        #for sensor_name in table.get_sensors(ventana):
        fig, ax = plt.subplots(figsize=(12,6), squeeze=True)
        table.plot_values_umbral(id_instrument, direction, ventana, ax)
        plt.tight_layout()
        fileOut = "{}/{}-tasas-{}-{}.png".format(folder_dst, id_instrument, direction, ventana)
        fig.savefig(fileOut, facecolor="white")
    else:
        print("No existe archivo", fileTasas)
    return


def generate_plot_value_instrument(id_instrument, direction, folder, folder_dst, sdate=None, edate=None, with_date_range=False):
    from matplotlib import pyplot as plt

    file_values = reader_values_clino.get_fileName(id_instrument, sdate=sdate, edate=edate, with_date_range=with_date_range) #direction, 
    file_values = "{}/valores/{}".format(folder, file_values)
    if exists(file_values):
        values_clino = reader_values_clino(id_instrument, "{}/valores".format(folder)) #direction, 
        #dateFmt = dates.DateFormatter("%Y/%m/%d")
        fig, axes = plt.subplots(ncols=1, nrows=1, figsize=(15,9))
        values_clino.plot_values(axes, direction)
        fig.tight_layout(pad=2.4, w_pad=0.5, h_pad=6.0)

        fileOut = "{}/{}-valores-{}.png".format(folder_dst, id_instrument, direction)
        fig.savefig(fileOut, facecolor="white")
    else:
        print("No existe archivo", file_values)
    return


def generate_plot_dif_parcial_puntos(id_instrument, folder, folder_dst, sdate=None, edate=None, direction="vertical",
                                     with_date_range=False):
    from matplotlib import pyplot as plt
    
    fileAsentDif  = "{}/{}/{}-diferencial.csv".format(folder, DIR_DIFERENCIAL, id_instrument) #, direction, sdate.strftime("%Y%m%d"), edate.strftime("%Y%m%d"))
    print(fileAsentDif)

    if exists(fileAsentDif):
        #print("File", fileAsentDif)
        tableAsentDif = reader_diferencial(fileAsentDif)
        #print('Columns', tableAsentDif.columns)
        fig, axes = plt.subplots(ncols=1, nrows=1, figsize=(15,9))
        tableAsentDif.plot_parcial_diferencial_puntos(axes, direction, vert_displace=True, sdate=sdate, edate=edate)
        fig.tight_layout(pad=2.4, w_pad=0.5, h_pad=6.0)

        fileOut = "{}/{}-dif-valores-{}.png".format(folder_dst, id_instrument, direction)
        fig.savefig(fileOut, facecolor="white")
    else:
        print("No existe archivo", fileAsentDif)
    return


def generate_plot_dif_parcial(id_instrument, folder, folder_dst, year, month, direction):
    from matplotlib import pyplot as plt

    fileAsentDif  = "{}/{}/{}-diferencial.csv".format(folder, DIR_DIFERENCIAL, id_instrument) #, direction,  sdate.strftime("%Y%m%d"), edate.strftime("%Y%m%d"))
    
    #direccion = "longitudinal"
    if exists(fileAsentDif):
        #print("File", fileAsentDif)
        fig, axis = plt.subplots(ncols=1, nrows=1, figsize=(15,9))
        tableAsentDif = reader_diferencial(fileAsentDif)

        #print('Columns', tableAsentDif.columns)
        tableAsentDif.plot_deformacion_dif_parcial(axis, year, month, direction)
        fig.tight_layout(pad=2.4, w_pad=0.5, h_pad=6.0)

        fileOut = "{}/{}-diferencia-{}-{}{:02d}.png".format(folder_dst, id_instrument, direction, year, month)
        fig.savefig(fileOut, facecolor="white")
        plt.close()
        print("saving", fileOut)
    else:
        print("No existe archivo", fileAsentDif)
    return
