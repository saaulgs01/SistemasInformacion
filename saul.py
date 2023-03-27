
import sqlite3

import numpy as np
import pandas as pd
import plotly.express as px

pd.options.plotting.backend = "plotly"
pd.options.display.max_columns = 50
pd.options.display.max_rows = 50

def setup():
    con = sqlite3.connect('ejemplo1.db')
    cur = con.cursor()
    cur.execute('drop table if exists alertas')
    cur.execute('drop table if exists analisis')
    cur.execute('drop table if exists devices')
    cur.execute('drop table if exists puertos_abiertos')
    cur.execute('drop table if exists responsable')
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS devices (id TEXT, ip TEXT, localizacion TEXT, PRIMARY KEY('id'))")
    cur.execute("CREATE TABLE if NOT exists alertas (timestamp DATETIME,sid INTEGER, msg TEXT,clasificacion TEXT,prioridad INTEGER,protocolo TEXT,origen TEXT,destino TEXT,puerto TEXT, FOREIGN KEY(destino) REFERENCES devices(ip),FOREIGN KEY(origen) REFERENCES devices(ip))")
    cur.execute("CREATE TABLE IF NOT EXISTS responsable (id TEXT, nombre TEXT, telefono TEXT, ROL TEXT, PRIMARY KEY('id'), FOREIGN KEY('id') REFERENCES devices('id'))")
    cur.execute( "CREATE TABLE IF NOT EXISTS analisis (id TEXT, servicios INTEGER, servicios_inseguros INTEGER, vulnerabilidades_detectadas INTEGER ,PRIMARY KEY('id'),FOREIGN KEY('id') REFERENCES devices('id'))")
    cur.execute("CREATE TABLE IF NOT EXISTS puertos_abiertos (id TEXT, puerto TEXT, FOREIGN KEY('id') REFERENCES analisis('id'))")
    con.commit()

    alerts = pd.read_csv('alerts.csv', header=0)
    alerts.replace("None", None, inplace=True)
    alerts.to_sql(name="alertas", con=con, if_exists="append", index=False)

    devices = pd.read_json("devices.json")
    devices_base = devices.reindex(columns=["id", "ip", "localizacion"])
    devices_base.replace("None", None, inplace=True)
    devices_base.to_sql(name="devices", con=con, if_exists="append", index=False)

    devices_responsable = devices.reindex(columns=["responsable"])
    devices_responsable = pd.DataFrame(devices_responsable["responsable"].tolist()).reindex(
        columns=['nombre', 'telefono', 'rol'])
    devices_responsable["id"] = devices_base["id"]
    devices_responsable.replace("None", None, inplace=True)
    devices_responsable.to_sql(name="responsable", con=con, if_exists="append", index=False)

    devices_analisis = devices.reindex(columns=["analisis"])
    devices_analisis = pd.DataFrame(devices_analisis["analisis"].tolist()).reindex(
        columns=['servicios', 'servicios_inseguros', 'vulnerabilidades_detectadas'])
    devices_analisis["id"] = devices_base["id"]
    devices_analisis.replace("None", None, inplace=True)
    devices_analisis.to_sql(name="analisis", con=con, if_exists="append", index=False)

    devices_analisis_ports = devices.reindex(columns=["analisis"])
    devices_analisis_ports = pd.DataFrame(devices_analisis_ports["analisis"].tolist()).reindex(
        columns=["puertos_abiertos"])
    devices_analisis_ports["id"] = devices_base["id"]
    devices_analisis_ports.replace("None", None, inplace=True)
    # print(type(devices_analisis_ports.loc[1, "puertos_abiertos"]))
    temp = pd.DataFrame(columns=["id", "puertos_abiertos"])
    for index, row in devices_analisis_ports.iterrows():
        if row["puertos_abiertos"] is not None:
            for r in row["puertos_abiertos"]:
                temp.loc[len(temp)] = [row["id"], r]
        else:
            temp.loc[len(temp)] = [row["id"], None]
    temp.rename(columns={"puertos_abiertos": "puerto"}, inplace=True)
    devices_analisis_ports = temp
    devices_analisis_ports.to_sql(name="puertos_abiertos", con=con, if_exists="append", index=False)
    # print(devices_analisis_ports)
    con.close()


def practica1():
    setup()

    con = sqlite3.connect('ejemplo1.db')
    cur = con.cursor()

    # EJERCICIO 2

    print("EJERCICIO 2")
    cur.execute("SELECT COUNT(*) FROM devices")
    print("Numero de dispositivos: " + str(cur.fetchone()[0]))
    cur.execute("SELECT COUNT(*) FROM alertas")
    print("Numero de alertas: " + str(cur.fetchone()[0]))

    df_puertos_abiertos = pd.DataFrame(cur.execute("SELECT * FROM puertos_abiertos").fetchall(),columns=["ID", "PUERTO"])
    df_aux = df_puertos_abiertos["ID"].value_counts()

    print(
        "Media y desviacion estandar del total de puertos abiertos: " + str(df_aux.mean()) + " || " + str(df_aux.std()))
    df_analisis = pd.DataFrame(cur.execute("SELECT * FROM analisis").fetchall(),
                               columns=["ID", "SERVICIOS", "SERVICIOS_INSEGUROS", "VULNERABILIDADES_DETECTADAS"])
    df_aux = df_analisis["SERVICIOS_INSEGUROS"]
    print("Media y desviacion estandar del numero de servicios inseguros detectados: " + str(
        df_aux.mean()) + " || " + str(df_aux.std()))
    df_aux = df_analisis["VULNERABILIDADES_DETECTADAS"]
    print("Media y desviacion estandar del numero de vulnerabilidades detectadas: " + str(df_aux.mean()) + " || " + str(
        df_aux.std()))
    df_aux = df_puertos_abiertos["ID"].value_counts()
    aux = None
    if None in df_puertos_abiertos[
        "PUERTO"].values:  # esta parte es necesaria porque cuenta None como un valor normal cuando no lo es
        aux = 0
    else:
        aux = df_aux.min()
    print("Valor minimo y valor maximo del total de puertos abiertos: " + str(aux) + " || " + str(df_aux.max()))
    df_aux = df_analisis["VULNERABILIDADES_DETECTADAS"]
    print("Valor minimo y valor maximo del numero de vulnerabilidades detectadas: " + str(df_aux.min()) + " || " + str(
        df_aux.max()))

    # EJERCICIO 3

    print("\nEJERCICIO 3")
    df_prueba_alertas = pd.DataFrame(
        cur.execute("SELECT * FROM alertas").fetchall(),
        columns=["TIME", "SID", "MSG", "CLASIFICACION", "PRIORIDAD", "PROTOCOLO", "ORIGEN", "DESTINO", "PUERTO"])
    df_prueba_devices = pd.DataFrame(
        cur.execute("SELECT * FROM devices").fetchall(),
        columns=["ID", "IP", "LOCALIZACION"])
    df_prueba_vulnerabilidades = pd.DataFrame(
        cur.execute("SELECT * FROM analisis").fetchall(),
        columns=["ID", "servicios", "servicios_inseguros", "vulnerabilidades_detectadas"])

    df_aux = pd.merge(df_prueba_alertas, pd.merge(df_prueba_devices, df_prueba_vulnerabilidades, on="ID"),
                      left_on="ORIGEN", right_on="IP", how="inner")
    df_aux2 = pd.merge(df_prueba_alertas, pd.merge(df_prueba_devices, df_prueba_vulnerabilidades, on="ID"),
                       left_on="DESTINO", right_on="IP", how="inner")
    df_aux = pd.concat([df_aux, df_aux2])
    df_aux.drop_duplicates(inplace=True)
    df_aux.reset_index(drop=True, inplace=True)


    # Crear columna con el mes
    df_aux["TIME"] = pd.to_datetime(df_aux["TIME"])
    df_aux["MES"] = df_aux["TIME"].dt.month
    df_aux = df_aux.query("MES == 7 or MES == 8")

    print("Vulnerabilidades agrupadas por prioridad y mes(JULIO Y AGOSTO):\n")

    print("Numero de observaciones encontradas:\n " + str(df_aux.groupby(["PRIORIDAD", "MES"])["vulnerabilidades_detectadas"].sum()))
    # VALORES AUSENTES????
    print("\nMediana:\n" + str(df_aux.groupby(["PRIORIDAD", "MES"])["vulnerabilidades_detectadas"].median()))
    print("\nMedia:\n" + str(str(df_aux.groupby(["PRIORIDAD", "MES"])["vulnerabilidades_detectadas"].mean())))
    print("\nVarianza:\n" + str(str(df_aux.groupby(["PRIORIDAD", "MES"])["vulnerabilidades_detectadas"].var())))
    print("\nValor minimo\n " + str(str(df_aux.groupby(["PRIORIDAD", "MES"])["vulnerabilidades_detectadas"].min()) + "\nValor maximo:\n " + str(str(df_aux.groupby(["PRIORIDAD", "MES"])["vulnerabilidades_detectadas"].max()))))


        #EJERCICIO4

    print("\nEJERCICIO 4")

    # mostrar las 10 ip de origen mas problematicas
    df_alertas = pd.DataFrame(cur.execute("SELECT * FROM alertas").fetchall(),
                              columns=["TIME", "SID", "MSG", "CLASIFICACION", "PRIORIDAD", "PROTOCOLO", "ORIGEN",
                                       "DESTINO", "PUERTO"])
    df_alertas_41 = df_alertas.reindex(columns=["PRIORIDAD", "ORIGEN"])
    df_alertas_41.drop(df_alertas[df_alertas["PRIORIDAD"] > 1].index, inplace=True)
    df_alertas_41.reset_index(drop=True, inplace=True)

    print("Mostrar las 10 IP de origen mas problematicas:\n" + str(df_alertas_41["ORIGEN"].value_counts().head(10)))
    grafico = px.bar(df_alertas_41["ORIGEN"].value_counts().head(10), y="ORIGEN")
    # grafico.show()

    # numero de alertas en el tiempo
    df_alertas_42 = df_alertas.reindex(columns=["TIME"])
    df_alertas_42.reset_index(drop=True, inplace=True)
    print("Numero de alertas en el tiempo:\n" + str(df_alertas_42))
    grafico = px.line(df_alertas_42, x="TIME")
    # grafico.show()

    # numero de alertas por categoria

    df_alertas_43 = df_alertas.reindex(columns=["CLASIFICACION"])
    # df_alertas_43.reset_index(inplace=True)
    print(df_alertas_43.value_counts())
    grafico = px.bar(df_alertas_43["CLASIFICACION"].value_counts(), log_y=True)
    # grafico.show()

    # dispositivos mas vulnerables
    df_analisis_44 = df_analisis.reindex(columns=["ID"])
    df_analisis_44["VULNERABLE"] = df_analisis["SERVICIOS_INSEGUROS"] + df_analisis["VULNERABILIDADES_DETECTADAS"]
    print(df_analisis_44)
    grafico = px.bar(df_analisis_44, x='ID', y="VULNERABLE")
    # grafico.show()

    # Media de puertos abiertos frente a servicios inseguros y frente al total de servicios detectados.
    # ??????????????



    con.close()


if __name__ == '__main__':
    practica1()
