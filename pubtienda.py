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

        cursor.execute('select count(*) from estadisticas_acceso where ingreso = false')
        HanSalidocc = cursor.fetchone()[0]

        cursor.execute('select count(*) from estadisticas_acceso where ingreso = true')
        HanEntradocc = cursor.fetchone()[0]

        if HanEntradocc > HanSalidocc:
        
            #personas que entran y salen de tiendas aun sin telf
            contelf=random.choice([True, False])
        
            idtienda = random.randint(1,camarastienda)
            HanEntradotienda = cursor.execute("select count(*) from estadisticas_tienda where ingreso = true AND id_camara = '%s'" % idtienda)
            HanEntradotienda = cursor.fetchone()[0]

            HanSalidotienda = cursor.execute("select count(*) from estadisticas_tienda where ingreso = false AND id_camara = '%s'" % idtienda)
            HanSalidotienda = cursor.fetchone()[0]

            if HanEntradotienda > HanSalidotienda:
                bootienda =  random.choice([True, False])
            elif HanEntradotienda == HanSalidotienda:
                bootienda = True

            sumatienda = 250
            segundos = segundos + sumatienda
            minutienda= datetime.timedelta(seconds=segundos)
            horatienda = datetime.datetime.now() + minutienda    
            telf = 0
        
                
            if bootienda == False and contelf == False:
               cont = 0
               while (True):
                    cantidaddeaccesos3 = cursor.execute('select count(*) from estadisticas_acceso')
                    cantidaddeaccesos3 = cursor.fetchone()[0]
                    idtienda = random.randint(1,camarastienda)
                    aleatorio3 = random.randint(1,cantidaddeaccesos3)
                    sql4 = '''select id_persona from estadisticas_acceso where id_estadi_acceso = %s;'''
                    cursor.execute(sql4, ([aleatorio3]))
                    personaaleatoria3 = cursor.fetchone()[0]
                    cursor.execute("select count(*) from estadisticas_acceso where ingreso = true AND id_persona = '%s' " % personaaleatoria3)
                    DeEsosEntraron3 = cursor.fetchone()[0]
                    cursor.execute("select count(*) from estadisticas_acceso where ingreso = false AND id_persona = '%s' " % personaaleatoria3)
                    DeEsosSalieron3 =cursor.fetchone()[0]
                    #persona3 = personaaleatoria3
                
                
                    cursor.execute("select count(*) from estadisticas_tienda where ingreso = true AND id_persona = '%s' AND id_camara = '%s'", (personaaleatoria3,idtienda))
                    DeEsosentraronAtienda = cursor.fetchone()[0]

                    cursor.execute("select count(*) from estadisticas_tienda where ingreso = false AND id_persona = '%s' AND id_camara = '%s' " ,(personaaleatoria3,idtienda))
                    DeEsossalierondetienda = cursor.fetchone()[0]
              
                    cont=cont + 1
                    #print(cont)
                    personatienda = personaaleatoria3
                    if (DeEsosEntraron3 > DeEsosSalieron3 and DeEsosentraronAtienda > DeEsossalierondetienda)  or cont ==500:
                        break
                        bootienda = True                
                
            elif bootienda == True and contelf == False:
                while (True):
                    cantidaddeaccesos2 = cursor.execute('select count(*) from estadisticas_acceso')
                    cantidaddeaccesos2 = cursor.fetchone()[0]

                    aleatorio2 = random.randint(1,cantidaddeaccesos2)
                    sql3 = '''select id_persona from estadisticas_acceso where id_estadi_acceso = %s;'''
                    cursor.execute(sql3, ([aleatorio2]))
                    personaaleatoria2 = cursor.fetchone()[0]
                    cursor.execute("select count(*) from estadisticas_acceso where ingreso = true AND id_persona = '%s' " % personaaleatoria2)
                    DeEsosEntraron2 = cursor.fetchone()[0]
                    cursor.execute("select count(*) from estadisticas_acceso where ingreso = false AND id_persona = '%s' " % personaaleatoria2)
                    DeEsosSalieron2 =cursor.fetchone()[0]
                    persona2 = personaaleatoria2
                    personatienda = persona2
                    if DeEsosEntraron2 > DeEsosSalieron2:
                        break

                    
            if bootienda == False and contelf == True:
                cursor.execute("select count(*) from estadisticas_acceso where id_telf is not null and ingreso=true")
                si =cursor.fetchone()[0]
                cursor.execute("select count(*) from estadisticas_acceso where id_telf is not null and ingreso=false")
                no= cursor.fetchone()[0]
                
                if si > no:
                    cont = 0
                    while (True):
                        #print(cont)
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
                        cont = cont + 1
                        if telfentraron > telfsalieron or cont >2000:
                            cursor.execute("select count(*) from estadisticas_tienda where id_telf = '%s' AND id_camara = '%s' and ingreso = true",(telf,idtienda))
                            contelfentienda = cursor.fetchone()[0]
                            cursor.execute("select count(*) from estadisticas_tienda where id_telf = '%s' AND id_camara = '%s' and ingreso = false",(telf,idtienda))
                            contelfentienda2 = cursor.fetchone()[0]
                            cont = cont + 1
                            if contelfentienda > contelfentienda2:
                                cursor.execute("select id_persona from telf_inteligente where id_telf = '%s'" % telf)
                                personatienda = cursor.fetchone()[0]
                                break
                            elif cont >2002:
                                telf=0
                                break

            elif bootienda == True and contelf == True:
                cursor.execute("select count(*) from estadisticas_acceso where id_telf is not null and ingreso=true")
                si =cursor.fetchone()[0]
                cursor.execute("select count(*) from estadisticas_acceso where id_telf is not null and ingreso=false")
                no= cursor.fetchone()[0]
                if si > no:
                    cont = 0
                    while (True):
                        #print('yeaaaa')
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
                        cont = cont + 1
                        #print(cont)
                        if telfentraron > telfsalieron or cont > 500:
                            cursor.execute("select count(*) from estadisticas_tienda where ingreso = true AND id_telf = '%s' AND id_camara = '%s'",(telf,idtienda))
                            contelfentienda = cursor.fetchone()[0]
                            cursor.execute("select count(*) from estadisticas_tienda where ingreso = false AND id_telf = '%s' AND id_camara = '%s'  ",(telf,idtienda))
                            contelfentienda2 = cursor.fetchone()[0]
                            cont = cont + 1
                            if contelfentienda == contelfentienda2  :
                                cursor.execute("select id_persona from telf_inteligente where id_telf = '%s'" % telf)
                                personatienda  = cursor.fetchone()[0]
                                break
                            if cont > 502:
                                telf=0
                                while (True):
                                    cantidaddeaccesos2 = cursor.execute('select count(*) from estadisticas_acceso')
                                    cantidaddeaccesos2 = cursor.fetchone()[0]

                                    aleatorio2 = random.randint(1,cantidaddeaccesos2)
                                    sql3 = '''select id_persona from estadisticas_acceso where id_estadi_acceso = %s;'''
                                    cursor.execute(sql3, ([aleatorio2]))
                                    personaaleatoria2 = cursor.fetchone()[0]
                                    cursor.execute("select count(*) from estadisticas_acceso where ingreso = true AND id_persona = '%s' " % personaaleatoria2)
                                    DeEsosEntraron2 = cursor.fetchone()[0]
                                    cursor.execute("select count(*) from estadisticas_acceso where ingreso = false AND id_persona = '%s' " % personaaleatoria2)
                                    DeEsosSalieron2 =cursor.fetchone()[0]
                                    persona2 = personaaleatoria2
                                    personatienda = persona2
                                    if DeEsosEntraron2 > DeEsosSalieron2:
                                        break
                                break       
                
                
           
        # si es true alguien ingreso, si es false alguien salio
            payload = {
                
                "ingresotienda":str(bootienda),
                "idcamaratienda":str(idtienda),
                "fecha_hora_tienda":str(horatienda),
                "idpersona_tienda":str(personatienda),
                "id_telf":str(telf),
                "query":str('tienda')
                
            }
            client.publish('unimet/admin/bd',json.dumps(payload),qos=0)             
            print(payload)
            time.sleep(0.5)
if __name__ == '__main__':
        main()
        sys.exit(0)
