import logging
import datetime
import os
#logging.basicConfig(filename='clinox_source.log', encoding='utf-8', level=logging.DEBUG)
logging.basicConfig(handlers=[logging.FileHandler(filename="./clinox_source.log", 
                                                 encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)
#logging.debug('This message should go to the log file')
#logging.info('So should this')
#logging.warning('And this, too')
#logging.error('And non-ASCII stuff, too, like Øresund and Malmö')
print("Examinando archivos de clinoextensometros")
clino_1='compacted-custom-readings-20665-current.dat'
clino_2='compacted-custom-readings-21367-current.dat'
dir_clino='D:\geoalert-data\mel\clinoextensometros\current\\'
try:
    with open(dir_clino+clino_1) as file:
        print(file.name)
        # file modification
        timestamp = os.path.getmtime(file.name)
 
        # convert timestamp into DateTime object
        datestamp = datetime.datetime.fromtimestamp(timestamp)
        print ('El archivo de clinoextensometro :',clino_1)
        print('Ha sido modifucado el :', datestamp)
        #cls
        # logging.info('Modified Date/Time:', datestamp)
   # No need to close the file
except FileNotFoundError:
    print('El archivo {} no fue descargado'.format(clino_1))
    exit()