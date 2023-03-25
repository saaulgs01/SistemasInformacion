import sqlite3
from sqlalchemy import create_engine

import pandas as pd
conn = sqlite3.connect('ejemplo1.db')

filename = 'alerts.csv'
data = pd.read_csv(filename, header=0)
print(conn)
#print(data.shape)
#print(data.head(10))

pd.options.display.max_columns= 50
pd.options.display.max_rows= 50

filename2 = 'devices.json'
data2 = pd.read_json(filename2)
#print(data2.shape)
print(data2)
#print(data2.values)


df1 = data2.reindex(columns=['id','ip','localizacion'])
print(df1)
print("\n")

#df2 = data2.reindex(columns=['responsable'])
columnas = ['nombre', 'telefono', 'rol']
df2 = pd.DataFrame(data2['responsable'].tolist()).reindex(columns=columnas)
print(df2)
print("\n")

#df3 = data2.reindex(columns=['analisis'])
columnas2 = ['puertos_abiertos, servicios', 'servicios_inseguros','vulnerabilidades_detectadas']
df3 = pd.DataFrame(data2['analisis'].tolist()).reindex(columns=columnas2)
print(df3)
print("\n")


#df3['puertos_abiertos'] = df3['puertos_abiertos'].astype('string')
#data.to_sql(name='probin',con=conn, if_exists='append')
#df1.to_sql(name='devices',con=conn, if_exists='append')
#df2.to_sql(name='responsable',con=conn, if_exists='append')
#df3.to_sql(name='analisis',con=conn, if_exists='append')
#df4.to_sql(name='puertos_abiertos',con=conn, if_exists='append')





def sql_update(con):
    cursorObj = con.cursor()
    cursorObj.execute('UPDATE analisis SET puertos_abiertos = "None" where puertos_abiertos = "NULL"')
    con.commit()

def sql_fetch(con):
   cursorObj = con.cursor()
   cursorObj.execute('SELECT * FROM devices')
   #SELECT dni, nombre FROM usuarios WHERE altura > 1.0
   rows = cursorObj.fetchall()
   for row in rows:
      print(row)

def sql_delete(con):
    cursorObj = con.cursor()
    cursorObj.execute('DELETE FROM analisis where vulnerabilidades_detectadas = "15"')
    con.commit()

def sql_delete_table(con):
    cursorObj = con.cursor()
    cursorObj.execute('drop table if exists usuarios1')
    con.commit()

def sql_create_table(con):
    cursorObj = con.cursor()
    cursorObj.execute("CREATE TABLE IF NOT EXISTS devices (id text, ip text, localization text)")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS responsable (nombre text, telefono text, rol text)")
    cursorObj.execute("CREATE TABLE IF NOT EXISTS analisis (puertos_abiertos text, servicios integer, servicios_inseguros integer, vulnerabilidades_detectadas integer)")
  #  cursorObj.execute("INSERT INTO devices VALUES ('web', '172.18.0.0', 'None') ")
   # cursorObj.execute("INSERT INTO responsable VALUES ('admin', '656445552', 'Administrador de sistemas') ")
    cursorObj.execute("INSERT INTO analisis VALUES ('80/TCP, 443/TCP, 3306/TCP, 40000/UDP,', '3', '0', '15') ")


  #  con.commit()

#con = sqlite3.connect('ejemplo1.db')
#sql_create_table(con)
#sql_fetch(con)
#sql_update(con)
#sql_fetch(con)
#sql_delete(con)
#sql_fetch(con)
#sql_delete_table(con)

#EJERCICIO2 (2 PUNTOS)

#cuestion 1:
preg1 = "SELECT COUNT(*) as devices FROM devices"
result = pd.read_sql_query(preg1, conn)
print(result)

preg2 = "SELECT COUNT(*) as campos_None FROM devices where localizacion is 'None'"
result = pd.read_sql_query(preg2, conn)
print(result)

#Cuestrion 2:
preg = "select count(*) as alertas from alertas"
result = pd.read_sql_query(preg, conn)
print(result)
#Cuestion 3:

preg = "select count(*) as Media_puertos from puertos_abiertos"
preg = "SELECT AVG(device) AS media_puertos_abiertos FROM puertos_abiertos INNER JOIN analisis ON puertos_abiertos.device = analisis.indice"
result = pd.read_sql_query(preg, conn)
print(result)
#cuestion 4:

preg = "select avg(servicios_inseguros) as mediaServicios_inseguros from analisis"
result = pd.read_sql_query(preg, conn)
print(result)

preg = "SELECT AVG((puerto - sub.a) * (puerto - sub.a)) as var from puertos_abiertos, (SELECT AVG(puerto) AS a FROM puertos_abiertos) AS sub"
result = pd.read_sql_query(preg, conn)
print(result)

#cuestion 5:
preg = "select avg(vulnerabilidades_detectadas) as media_vulnerabilidades_detectadas from analisis"
result = pd.read_sql_query(preg, conn)
print(result)
#cuestion 6:

preg = "SELECT COUNT(device) as cantidad_maxima, device FROM puertos_abiertos GROUP BY device ORDER BY cantidad_maxima DESC LIMIT 1"
result = pd.read_sql_query(preg, conn)
print(result)

preg = "SELECT min(count_val) as cantidad_minima,device FROM (SELECT COUNT(case WHEN puerto IS 'None' then NULL else device END) as count_val, device FROM puertos_abiertos GROUP BY device order by count_val) as counts"
result = pd.read_sql_query(preg, conn)
print(result)
#Cuestion 7:

preg = "select min(vulnerabilidades_detectadas) as min_vul from analisis"
result = pd.read_sql_query(preg, conn)
print(result)

preg = "select max(vulnerabilidades_detectadas) as max_vul from analisis"
result = pd.read_sql_query(preg, conn)
print(result)

#EJERCICIO3 (2.5 PUNTOS)
#cuestion 1:
preg = "select count(vulnerabilidades_detectadas) as vulnerabilidades_detectadas, strftime('%m', fechas) as mes, prioridad  from analisis inner join devices on analisis.indice = devices.indice inner join alertas on devices.ip=alertas.origen or devices.ip=alertas.destino where  strftime('%m', fechas) IN ('07','08') group by prioridad,mes order by mes"
result = pd.read_sql_query(preg, conn)
print(result)
#cuestion 2:
preg = "select count(vulnerabilidades_detectadas) as campos_none, strftime('%m', fechas) as mes, prioridad  from analisis inner join devices on analisis.indice = devices.indice inner join alertas on devices.ip=alertas.origen or devices.ip=alertas.destino where  localizacion='None' and strftime('%m', fechas) IN ('07','08') group by prioridad,mes order by mes"
result = pd.read_sql_query(preg, conn)
print(result)

#cuestion 3:
preg = "select median(vulnerabilidades_detectadas) as mediana_vulnerabilidades_detectadas, strftime('%m', fechas) as mes, prioridad  from analisis inner join devices on analisis.indice = devices.indice inner join alertas on devices.ip=alertas.origen or devices.ip=alertas.destino where  strftime('%m', fechas) IN ('07','08') group by prioridad,mes order by mes"
#result = pd.read_sql_query(preg, conn)
print(result)
#cuestion 4:
preg = "select avg(vulnerabilidades_detectadas) as media_vulnerabilidades_detectadas, strftime('%m', fechas) as mes, prioridad  from analisis inner join devices on analisis.indice = devices.indice inner join alertas on devices.ip=alertas.origen or devices.ip=alertas.destino where  strftime('%m', fechas) IN ('07','08') group by prioridad,mes order by mes"
result = pd.read_sql_query(preg, conn)
print(result)
#cuestion 5:
preg = "select stdev(vulnerabilidades_detectadas) as varvulnerabilidades_detectadas, strftime('%m', fechas) as mes, prioridad  from analisis inner join devices on analisis.indice = devices.indice inner join alertas on devices.ip=alertas.origen or devices.ip=alertas.destino where  strftime('%m', fechas) IN ('07','08') group by prioridad,mes order by mes"
#result = pd.read_sql_query(preg, conn)
print(result)
#cuestion 6:
preg = "select max(vulnerabilidades_detectadas) as max_vulnerabilidades_detectadas, strftime('%m', fechas) as mes, prioridad  from analisis inner join devices on analisis.indice = devices.indice inner join alertas on devices.ip=alertas.origen or devices.ip=alertas.destino where  strftime('%m', fechas) IN ('07','08') group by prioridad,mes order by mes"
result = pd.read_sql_query(preg, conn)
print(result)
#cuestion 6:
preg = "select min(vulnerabilidades_detectadas) as min_vulnerabilidades_detectadas, strftime('%m', fechas) as mes, prioridad  from analisis inner join devices on analisis.indice = devices.indice inner join alertas on devices.ip=alertas.origen or devices.ip=alertas.destino where  strftime('%m', fechas) IN ('07','08') group by prioridad,mes order by mes"
result = pd.read_sql_query(preg, conn)
print(result)

























conn.close()