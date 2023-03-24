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
#print(data2)
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
columnas2 = ['puertos_abiertos', 'servicios', 'servicios_inseguros','vulnerabilidades_detectadas']
df3 = pd.DataFrame(data2['analisis'].tolist()).reindex(columns=columnas2)
print(df3)
print("\n")
#df3['puertos_abiertos'] = df3['puertos_abiertos'].astype('string')
#data.to_sql(name='probin',con=conn, if_exists='append')
#df1.to_sql(name='devices',con=conn, if_exists='append')
#df2.to_sql(name='responsable',con=conn, if_exists='append')
#df3.to_sql(name='analisis',con=conn, if_exists='append')




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

ej1 = "SELECT COUNT(*) FROM devices"
result = pd.read_sql_query(ej1, conn)
print(result)

ej1v2 = "select count(*) as nones from devices WHERE localizacion='None'"
result = pd.read_sql_query(ej1v2,conn)
print("-------\n")
print(result)


ej2 = "SELECT COUNT(*) FROM alertas"
result = pd.read_sql_query(ej2, conn)
print(result)

ej3 = "SELECT AVG(CAST(length(puertos_abiertos) - length(replace(puertos_abiertos, ',', '')) + 1 AS INTEGER)) AS media_puertos_abiertos FROM analisis WHERE puertos_abiertos IS NOT NULL"
result = pd.read_sql_query(ej3,conn)
print(result)


sql_update(conn)


ej3 = "SELECT AVG(CAST(length(COALESCE(puertos_abiertos, '0')) - length(REPLACE(COALESCE(puertos_abiertos, '0'), ',', '')) + 1 AS INTEGER)) AS media_puertos_abiertos FROM analisis"
result = pd.read_sql_query(ej3,conn)
print(result)

ej4 = "select avg(servicios_inseguros) as insecure from analisis"
result = pd.read_sql_query(ej4,conn)
print(result)

ej5 = "select avg(vulnerabilidades_detectadas) as insecure from analisis"
result = pd.read_sql_query(ej5,conn)
print(result)

ej6  = "SELECT min(CAST(length(COALESCE(puertos_abiertos, '0')) - length(REPLACE(COALESCE(puertos_abiertos, '0'), ',', '')) + 1 AS INTEGER)) as minPuertosAbiertos  FROM analisis"

result = pd.read_sql_query(ej6,conn)
print(result)
ej6v2 = "SELECT max(CAST(length(COALESCE(puertos_abiertos, '0')) - length(REPLACE(COALESCE(puertos_abiertos, '0'), ',', '')) + 1 AS INTEGER)) as minPuertosAbiertos  FROM analisis"
result = pd.read_sql_query(ej6v2,conn)
print(result)

ej7 = "select min(vulnerabilidades_detectadas) as minVul from analisis"
result = pd.read_sql_query(ej7,conn)
print(result)
ej7v2 = "select max(vulnerabilidades_detectadas) as maxVul from analisis"
result = pd.read_sql_query(ej7v2,conn)

print(result)


ejemplo = "SELECT min(CASE WHEN puertos_abiertos = 'None' THEN 0 ELSE (SELECT (CAST(length(COALESCE(puertos_abiertos, '0')) - length(REPLACE(COALESCE(puertos_abiertos, '0'), ',', '')) + 1 AS INTEGER))) END) AS minimo_numero_puertos_abiertos FROM analisis"
result = pd.read_sql_query(ejemplo,conn)
print(result)

conn.close()