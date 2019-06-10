import ssl
import sys
import json
import random
import time
import paho.mqtt.client
import paho.mqtt.publish
import numpy as np
import datetime
from datetime import timedelta
import psycopg2
import pandas as pd
import sqlite3
import itertools

conn = psycopg2.connect(host = 'localhost', user= 'postgres', password ='123456', dbname= 'ABD')
cursor = conn.cursor()


def on_connect(client, userdata, flags, rc):
    print('conectado publicador')

def main():
    client = paho.mqtt.client.Client("Unimet", False)
    client.qos = 0
    client.connect(host='localhost')
    segun = 50
    numeromesas = cursor.execute('select count(*) from mesa')
    numeromesas = cursor.fetchone()[0]
    while(True):

        tienetelf = random.choice([True, False])

        cursor.execute('select count(*) from estadisticas_acceso where ingreso = false')
        HanSalido = cursor.fetchone()[0]

        cursor.execute('select count(*) from estadisticas_acceso where ingreso = true')
        HanEntrado = cursor.fetchone()[0]
        
                
        
        if HanEntrado > HanSalido :
            
        

            suma = 200
            segun = segun + suma
            minut= datetime.timedelta(seconds=segun)
            hora = datetime.datetime.now() + minut
        
            mesa = random.randint(1,numeromesas)
            cursor.execute("select count(*) from estadisticas_mesa where vacia = true AND id_mesa = '%s'" % mesa )
            uso1 = cursor.fetchone()[0]
            cursor.execute("select count(*) from estadisticas_mesa where vacia = false AND id_mesa = '%s'" % mesa )
            uso2 = cursor.fetchone()[0]
        
        
        
            if uso1 > uso2 :
                boo= False
            else:
                boo= True

        
            if boo == False and tienetelf == True:
                cursor.execute("select count(*) from estadisticas_acceso where id_telf is not null and ingreso=true")
                si =cursor.fetchone()[0]
                cursor.execute("select count(*) from estadisticas_acceso where id_telf is not null and ingreso=false")
                no= cursor.fetchone()[0]
                
                if si > no:
            
                    while (True):
                        cursor.execute("select id_estadi_acceso from estadisticas_acceso where id_telf is not null")
                        rows = cursor.fetchall()
                        result_list = list(itertools.chain(*rows))
                      
                        cursor.execute("select count(*) from estadisticas_acceso where id_telf is not null")
                        resultado=cursor.fetchone()[0]
                        numero = random.randint(1,resultado)
                        id = result_list[numero-1]

                        cursor.execute("select id_telf from estadisticas_acceso where id_estadi_acceso = '%s'"% id)
                        telf = cursor.fetchone()[0]
                    
                        cursor.execute("select count(*) from estadisticas_acceso where ingreso = true AND id_telf = '%s'"% telf)
                        telfentraron = cursor.fetchone()[0]

                        cursor.execute("select count(*) from estadisticas_acceso where ingreso = false AND id_telf = '%s'"% telf)
                        telfsalieron = cursor.fetchone()[0]
                        print('a')
                        if telfentraron > telfsalieron:
                                payload = {
                
                                "ingreso":str(boo),
                                "ultimouso": str(hora),
                                "id_mesa": str(mesa),
                                "idtelf":str(telf),
                                "info":str('sesientacontelf'),
                                "query":str('mesas')
                
                                }
                                client.publish('unimet/admin/bd',json.dumps(payload),qos=0)             
                                print(payload)
                                time.sleep(0.5)
                                break
        
            
                

            if tienetelf==False:
                payload = {
                
                    "ingreso":str(boo),
                    "ultimouso": str(hora),
                    "id_mesa": str(mesa),
                    "info":str('sesientasintelf'),
                    "query":str('mesas')
                
                
                
                }
        
                client.publish('unimet/admin/bd',json.dumps(payload),qos=0)             
                print(payload)
                time.sleep(0.5)
if __name__ == '__main__':
        main()
        sys.exit(0)
