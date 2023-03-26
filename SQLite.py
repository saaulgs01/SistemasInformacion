import sqlite3
from sqlalchemy import create_engine

import pandas as pd


def practica1():
    con = sqlite3.connect('Practica1.db')
    cur = con.cursor()
    cur.execute('drop table if exists alertas')
    cur.execute('drop table if exists analisis')
    cur.execute('drop table if exists devices')
    cur.execute('drop table if exists puertos_abiertos')
    cur.execute('drop table if exists responsable')
    con.commit()
    cur.execute("CREATE TABLE if NOT exists alertas ('index' integer, timestamp datetime,sid INTEGER, msg TEXT,clasificacion TEXT,prioridad INTEGER,protocolo TEXT,origen TEXT,destino TEXT,puerto TEXT, PRIMARY KEY('index'),FOREIGN KEY(destino) REFERENCES devices(ip),FOREIGN KEY(origen) REFERENCES devices(ip))")
    cur.execute("CREATE TABLE IF NOT EXISTS analisis ('index' integer, servicios INTEGER, servicios_inseguros INTEGER, vulnerabilidades_detectadas INTEGER ,PRIMARY KEY('index'),FOREIGN KEY('index') REFERENCES devices('index'))")
    cur.execute("CREATE TABLE IF NOT EXISTS devices ('index' integer, id TEXT, ip TEXT, localizacion TEXT, PRIMARY KEY('index'))")
    cur.execute("CREATE TABLE IF NOT EXISTS puertos_abiertos ('index' integer, puerto TEXT, device integer, FOREIGN KEY(device) REFERENCES analisis('index'))")
    cur.execute("CREATE TABLE IF NOT EXISTS responsable ('index' integer, nombre TEXT, telefono TEXT, ROL TEXT, FOREIGN KEY(nombre) REFERENCES devices('index'))")
    con.commit()


    filename = 'alerts.csv'
    data = pd.read_csv(filename, header=0)

    pd.options.display.max_columns= 50
    pd.options.display.max_rows= 50

    filename2 = 'devices.json'
    data2 = pd.read_json(filename2)

    print(data2)
    df1 = data2.reindex(columns=['id','ip','localizacion'])

    df2 = data2.reindex(columns=['responsable'])
    columnas = ['nombre', 'telefono', 'rol']
    df2 = pd.DataFrame(data2['responsable'].tolist()).reindex(columns=columnas)
    print(data2)


    df3 = data2.reindex(columns=['analisis'])
    columnas2 = ['servicios', 'servicios_inseguros','vulnerabilidades_detectadas']
    df3 = pd.DataFrame(data2['analisis'].tolist()).reindex(columns=columnas2)

    data.to_sql(name='alertas', con=con,if_exists='append')
    df1.to_sql(name='devices',con=con, if_exists='append')
    df2.to_sql(name='responsable',con=con, if_exists='append')
    df3.to_sql(name='analisis',con=con, if_exists='append')
    cur.execute("INSERT into puertos_abiertos ('index', 'puerto','device') values (1,'80/TCP',0),(2,'443/TCP',0),(3,'3306/TCP',0),(4,'40000/UDP',0),(5,'None',1),(6,'1194/UDP',2),(7,'8080/TCP',2),(8,'8080/UDP',2),(9,'40000/UDP',2),(10,'443/UDP',3),(11,'80/TCP',3),(12,'80/TCP',4),(13,'67/UDP',4),(14,'68/UDP',4),(15,'8080/TCP',5),(16,'3306/TCP',5),(17,'3306/UDP',5),(18,'80/TCP',6),(19,'443/TCP',6),(20,'9200/TCP',6),(21,'9300/TCP',6),(22,'5601/TCP',6)")
    cur.execute("update devices set localizacion = NULL where localizacion = 'None'")
    cur.execute("update puertos_abiertos set puerto = NULL where puerto = 'None'")
    con.commit()


    #EJERCICIO2 (2 PUNTOS)
    #cuestion 1:
    preg1 = "SELECT COUNT(*) as devices FROM devices"
    result = pd.read_sql_query(preg1, con)
    print(result)


    con.close()


practica1()

