import ssl
import sys
import psycopg2 #conectarte python con postresql
import paho.mqtt.client #pip install paho-mqtt
import json


conn = psycopg2.connect(host = 'localhost', user= 'postgres', password ='123456', dbname= 'ABD')


def doQuery(a):
    cursor = conn.cursor()

    if a["query"] == 'venta':
        if a["id_telf"] == '0':
            sql = '''INSERT INTO cliente (nombre,cedula) VALUES ( %s, %s);'''
            cursor.execute(sql, (a["nombre"],a["cedula"]))
            conn.commit()
            cursor.execute("SELECT id_cliente from cliente where cedula='%s'"%a["cedula"])
            id=cursor.fetchone()[0]

            sql1 = '''INSERT INTO venta (fecha_hora,monto,id_tienda,cliente) VALUES ( %s, %s,%s,%s);'''
            cursor.execute(sql1, (a["fecha_horaventa"],a["monto"],a["id_tienda"],id))
            conn.commit()
            
        else:
            cursor.execute("SELECT count(*) from cliente where telf='%s'"%a["id_telf"])
            cuenta =cursor.fetchone()[0]
            print(cuenta)
            if cuenta == 0:
                sql = '''INSERT INTO cliente (nombre,telf,cedula) VALUES ( %s, %s,%s);'''
                cursor.execute(sql, (a["nombre"],a["id_telf"],a["cedula"]))
                conn.commit()
                cursor.execute("SELECT id_cliente from cliente where cedula='%s'"%a["cedula"])
                id=cursor.fetchone()[0]

                sql1 = '''INSERT INTO venta (fecha_hora,monto,id_tienda,cliente) VALUES ( %s, %s,%s,%s);'''
                cursor.execute(sql1, (a["fecha_horaventa"],a["monto"],a["id_tienda"],id))
                conn.commit()
            else:
                cursor.execute("SELECT id_cliente from cliente where telf='%s'"%a["id_telf"])
                id=cursor.fetchone()[0]

                sql1 = '''INSERT INTO venta (fecha_hora,monto,id_tienda,cliente) VALUES ( %s, %s,%s,%s);'''
                cursor.execute(sql1, (a["fecha_horaventa"],a["monto"],a["id_tienda"],id))
                conn.commit()
                
                
        

            
        
    if a["query"] == 'tienda':
       if a["id_telf"] == '0':
           sql = '''INSERT INTO estadisticas_tienda (ingreso,id_camara,fecha_hora,id_persona) VALUES ( %s, %s,%s,%s);'''

           cursor.execute(sql, (a["ingresotienda"],a["idcamaratienda"],a["fecha_hora_tienda"],a["idpersona_tienda"]))
           conn.commit()
       else:
           sql = '''INSERT INTO estadisticas_tienda (ingreso,id_camara,fecha_hora,id_persona,id_telf) VALUES ( %s, %s,%s,%s,%s);'''

           cursor.execute(sql, (a["ingresotienda"],a["idcamaratienda"],a["fecha_hora_tienda"],a["idpersona_tienda"],a["id_telf"]))
           conn.commit()
        


    if a["query"] == 'mesas':
        if a["info"]=='sesientacontelf':

            sql = '''INSERT INTO estadisticas_mesa (vacia,ultimo_uso,id_mesa,id_telf) VALUES ( %s, %s,%s,%s);'''

            cursor.execute(sql, (a["ingreso"],a["ultimouso"],a["id_mesa"],a["idtelf"]))
            conn.commit()

        else:
            sql = '''INSERT INTO estadisticas_mesa (vacia,ultimo_uso,id_mesa) VALUES ( %s, %s,%s);'''

            cursor.execute(sql, (a["ingreso"],a["ultimouso"],a["id_mesa"]))
            conn.commit()
        
        
    if a["query"] == 'accesocc':
        if a["info"] == 'entracontelf':
                mac = a["mac"]
        
        
        
                sql = '''INSERT INTO telf_inteligente (mac,id_persona) VALUES ( %s, %s);'''
        
                cursor.execute(sql, (a["mac"],a["id_persona"]))

                conn.commit()

                cursor.execute("select id_telf from telf_inteligente where MAC = '%s'"% mac)
                idmac =  cursor.fetchone()[0]
        
                sql1 = '''INSERT INTO estadisticas_acceso (ingreso, fecha_hora,id_camara,id_persona,id_telf) VALUES ( %s, %s,%s,%s,%s);'''
        
                cursor.execute(sql1, (a["ingreso"],a["fecha_hora"],a["id_camara"],a["id_persona"],idmac))
                conn.commit()
        elif a["info"] == 'salecontelf':
        

                idmac =  a["idtelf"]

                cursor.execute("select id_persona from telf_inteligente where id_telf = '%s'"% idmac)
                persona = cursor.fetchone()[0]
        
                sql1 = '''INSERT INTO estadisticas_acceso (ingreso, fecha_hora,id_camara,id_persona,id_telf) VALUES ( %s, %s,%s,%s,%s);'''
                cursor.execute(sql1, (a["ingreso"],a["fecha_hora"],a["id_camara"],persona,idmac))
                conn.commit()

        elif a["info"] == 'sintelf':
                sql1 = '''INSERT INTO estadisticas_acceso (ingreso, fecha_hora,id_camara,id_persona) VALUES ( %s, %s,%s,%s);'''
        
                cursor.execute(sql1, (a["ingreso"],a["fecha_hora"],a["id_camara"],a["id_persona"]))
                conn.commit()

                
        

    
def on_connect(client, userdata, flags, rc):    
    print('conectado fino (%s)' % client._client_id)
    client.subscribe(topic='unimet/#', qos = 0)        

def on_message(client, userdata, message):   
    a = json.loads(message.payload)
    print(a) 
    #print(message.qos)   
    print('------------------------------')     
    doQuery(a)



def main():	
	client = paho.mqtt.client.Client()
	client.on_connect = on_connect
	client.message_callback_add('unimet/admin/bd', on_message)
	client.connect(host='localhost') 
	client.loop_forever()


if __name__ == '__main__':
	main()
	sys.exit(0)
