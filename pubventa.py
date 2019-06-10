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
import string
import names 

conn = psycopg2.connect(host = 'localhost', user= 'postgres', password ='123456', dbname= 'ABD')
cursor = conn.cursor()


def on_connect(client, userdata, flags, rc):
    print('conectado publicador')

def main():
    client = paho.mqtt.client.Client("Unimet", False)
    client.qos = 0
    client.connect(host='localhost')
   
    canttiendas = cursor.execute('select count(*) from tienda')
    canttiendas = cursor.fetchone()[0]
    cursor.execute('select count(*) from telf_inteligente')
    cantphones  = cursor.fetchone()[0]
    segundos = 50

   
    while(True):

        tienetelf = random.choice([True, False])

        iddetienda = random.randint(1,canttiendas)
        cursor.execute("select count(*) from estadisticas_tienda where ingreso = true AND id_camara ='%s' " % iddetienda)
        personaentraron = cursor.fetchone()[0]
        cursor.execute("select count(*) from estadisticas_tienda where ingreso = false AND id_camara ='%s' " % iddetienda)
        personasalieron = cursor.fetchone()[0]
        personasentienda = personaentraron - personasalieron
        idtelf=0
        
        


        if tienetelf == True:
            
            cursor.execute("SELECT id_telf FROM estadisticas_tienda where id_camara ='%s' ORDER BY fecha_hora DESC LIMIT 1 "% iddetienda )
            prueba=cursor.rowcount
            print(prueba)
            #hola=cursor.fetchone()[0]
            #print(hola)
            if prueba ==1:
                hola=cursor.fetchone()[0]
                if hola is not None:
                    idtelf = hola
                    print(idtelf)
                    cursor.execute("SELECT ingreso FROM estadisticas_tienda where id_camara ='%s' AND id_telf='%s' ORDER BY fecha_hora DESC LIMIT 1 " ,(iddetienda,idtelf) )
                    variablebool = cursor.fetchone()[0]

                    if variablebool == False:
                        cont = 0
                        while(True):
                            idtelf = random.randint(1,cantphones)
                            cursor.execute("SELECT ingreso FROM estadisticas_tienda where id_camara ='%s'  AND id_telf ='%s' ORDER BY fecha_hora DESC LIMIT 1 ",(iddetienda,idtelf))
                            chao=cursor.rowcount
                        
                            print('aaa')
                        #print(chao)
                            cont = cont + 1
                            if chao==1:
                                chao2 = cursor.fetchone()[0]
                                if chao2 is not None:
                                    estaentienda = chao
                                       
                                    if estaentienda == True:
                                        break
                                    elif cont >50:
                                        idtelf=0           
                                        break
            else:
                 idtelf=0
                        
                    
        
        if personasentienda>0:
        
            sumaventa = 250
            segundos = segundos + sumaventa
            minutventa= datetime.timedelta(seconds=segundos)
            horaventa = datetime.datetime.now() + minutventa
            nombre = names.get_full_name()
            cedula = random.randint(1,30000000)


            total = random.randint(1000,100000)
        
            payload = {
                
                "monto":str(total),
                "fecha_horaventa": str(horaventa),
                "id_tienda": str(iddetienda),
                "id_telf":str(idtelf),
                "cedula":str(cedula),
                "nombre":str(nombre),
                "query":str('venta')
                
                
                
            }
            client.publish('unimet/admin/bd',json.dumps(payload),qos=0)             
            print(payload)
            time.sleep(0.5)
if __name__ == '__main__':
        main()
        sys.exit(0)
