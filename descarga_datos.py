
import os
import psycopg2
import json

ruta_archivo_credenciales = "credenciales.json"
objeto_archivo = open(ruta_archivo_credenciales, "r")
cred_datos = json.load(objeto_archivo)

coneccion = psycopg2.connect(**cred_datos)
cursor = coneccion.cursor()

sql = """
SELECT
    compaa,
    ramo_tcnicocdigo_sucursalplizanmero_siniestro,
    cdigo_ramo_tcnico,
    ramo_tcnico,
    cdigo_ramo_comercial,
    ramo_comercial,
    cdigo_sucursal
FROM acc.sise_appgenerali_siniestros_incurridos_fusion
LIMIT 10
;
"""

cursor.execute(sql)

header = "~".join([desc[0] for desc in cursor.description])
data = cursor.fetchall()

final_data = []
for row in data:
    final_data.append("~".join([str(col) for col in row]))

cursor.close()
coneccion.close()


nombre_archivo = os.path.join("datos", "siniestros_incurridos_fusion.csv")

archivo_salida = open(nombre_archivo, "w")

archivo_salida.write(header + "\n")
archivo_salida.write("\n".join(final_data))

print("Esta es la ruta donde se guardó el archivo: ", os.path.abspath(nombre_archivo))
