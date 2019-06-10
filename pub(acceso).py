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
   
    cantsensores = cursor.execute('select count(*) from camara_acc')
    cantsensores = cursor.fetchone()[0]
    cantpersonas = cursor.execute('select count(*) from persona')
    cantpersonas = cursor.fetchone()[0]
    horaBase= datetime.datetime.now()
    horaBase = horaBase
    segun = 50
    segundos = 100
    numerotiendas = cursor.execute('select count(*) from tienda')
    numerotiendas = cursor.fetchone()[0]
    camarastienda = cursor.execute('select count(*) from camara_tienda')
    camarastienda = cursor.fetchone()[0]
    while(True):

        #Personas que entran y salen del cc aun sin telf
        cursor.execute('select count(*) from estadisticas_acceso where ingreso = false')
        HanSalido = cursor.fetchone()[0]

        cursor.execute('select count(*) from estadisticas_acceso where ingreso = true')
        HanEntrado = cursor.fetchone()[0]
        
        
        
        if HanEntrado > HanSalido :
            boo =  random.choice([True, False])
        elif HanEntrado == HanSalido :
            boo = True

        suma = 200
        segun = segun + suma
        minut= datetime.timedelta(seconds=segun)
        hora = datetime.datetime.now() + minut
        
        camara = random.randint(1,cantsensores)
        
        mac = random.choice([True, False])
        if mac == True:
            jhony = "%02x:%02x:%02x:%02x:%02x:%02x" % (
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255)
            )
                
        if boo == True:
            persona = random.randint(1,cantpersonas)
            

            
        
        elif boo == False:
            if mac == False:
                while (True):
                    cantidaddeaccesos = cursor.execute('select count(*) from estadisticas_acceso')
                    cantidaddeaccesos = cursor.fetchone()[0]

                    aleatorio = random.randint(1,cantidaddeaccesos)
                    sql1 = '''select id_persona from estadisticas_acceso where id_estadi_acceso = %s;'''
                    cursor.execute(sql1, ([aleatorio]))
                    personaaleatoria = cursor.fetchone()[0]
                    cursor.execute("select count(*) from estadisticas_acceso where ingreso = true AND id_persona = '%s' " % personaaleatoria)
                    DeEsosEntraron = cursor.fetchone()[0]
                    cursor.execute("select count(*) from estadisticas_acceso where ingreso = false AND id_persona = '%s' " % personaaleatoria)
                    DeEsosSalieron =cursor.fetchone()[0]
                    persona = personaaleatoria
                    if DeEsosEntraron > DeEsosSalieron:
                        break
            else:
                cursor.execute("select count(*) from estadisticas_acceso where id_telf is not null and ingreso=true")
                si =cursor.fetchone()[0]
                cursor.execute("select count(*) from estadisticas_acceso where id_telf is not null and ingreso=false")
                no= cursor.fetchone()[0]
                
                if si > no:
                    while (True):
                        cursor.execute("select id_estadi_acceso from estadisticas_acceso where id_telf is not null")
                        rows = cursor.fetchall()
                        result_list = list(itertools.chain(*rows))
                        #result_list = [row for row in rows]
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


                        
                        

                        if telfentraron > telfsalieron:
                            payload = {
                
                            "ingreso":str(boo),
                            "fecha_hora": str(hora),
                            "id_camara": str(camara),
                            "idtelf":str(telf),
                            "info":str('salecontelf'),
                            "query":str('accesocc')
                
                            }
                            client.publish('unimet/admin/bd',json.dumps(payload),qos=0)             
                            print(payload)
                            time.sleep(0.5)
                            break
                else:
                    mac == False

                                    

                    

        

        
        if mac == True and boo== True:
            payload = {
                
                "ingreso":str(boo),
                "fecha_hora": str(hora),
                "id_camara": str(camara),
                "id_persona": str(persona),
                "mac":str(jhony),
                "info":str('entracontelf'),
                "query":str('accesocc')
                
                
                
            }
            client.publish('unimet/admin/bd',json.dumps(payload),qos=0)             
            print(payload)
            time.sleep(0.5)
        else:
            payload = {
                
                "ingreso":str(boo),
                "fecha_hora": str(hora),
                "id_camara": str(camara),
                "id_persona": str(persona),
                "info":str('sintelf'),
                "query":str('accesocc')
                
                
                
            }
            client.publish('unimet/admin/bd',json.dumps(payload),qos=0)             
            print(payload)
            time.sleep(0.5)
            
if __name__ == '__main__':
        main()
        sys.exit(0)
