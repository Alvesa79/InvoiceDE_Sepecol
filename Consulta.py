import MySQLdb
import json
import os
import requests
from datetime import datetime
import pyodbc
import numero_letras
import genpdf
import sys


# Usar una configuración central para las credenciales
DB_HOST = os.getenv('DB_HOST', 'pronosneco.cog7zhynxhqk.us-east-1.rds.amazonaws.com')
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASS = os.getenv('DB_PASS', 'Sanchez8779')
DB_NAME = os.getenv('DB_NAME', 'Databoard')
CONFIG_FILE_PATH = 'C:/ConfigFE/configDS.json'


def Conectar():
    df_conexion = {}

    # Abriendo el archivo de configuración de manera más segura
    print("Abriendo archivo de configuración")
    try:
        with open(CONFIG_FILE_PATH, "r") as f:
            content = f.read()
            jsondecoded = json.loads(content)

        df_conexion['nit'] = jsondecoded.get("NIT")
    except FileNotFoundError:
        print(f"El archivo de configuración no se encuentra en {CONFIG_FILE_PATH}")
        return {}
    except json.JSONDecodeError:
        print("Error al leer el archivo de configuración, formato JSON inválido.")
        return {}

    # Conexión con la base de datos AWS
    print("Conectando con AWS")
    try:
        miConexion = MySQLdb.connect(
            host=DB_HOST,
            user=DB_USER,
            passwd=DB_PASS,
            db=DB_NAME
        )
        cur = miConexion.cursor()

        df_conexion['miConexion'] = miConexion
        df_conexion['cur'] = cur
    except MySQLdb.MySQLError as e:
        print(f"Ocurrió un error al conectar a la base de datos: {e}")
        return {}

    # Consultamos la tabla de clientes
    try:
        cur.execute("SELECT * FROM Databoard.Clientes WHERE nit = %s;", [df_conexion['nit']])
        df_config = cur.fetchone()

        if df_config:
            # Validamos si el cliente está activo
            print(f"Validando que el cliente esté activo: {df_config[1]}")
            if df_config[2] == 1:  # Suponiendo que la columna 2 es la de 'activo'
                print(f"Usuario {df_config[1]} Activo")

                # Configuración del cliente
                df_conexion.update({
                    'modo': df_config[3],
                    'ruta': df_config[4],
                    'servidor': df_config[5],
                    'usuario': df_config[6],
                    'clave': df_config[7],
                    'BD': df_config[8],
                    'empresa': df_config[9],
                    'user': df_config[10],
                    'password': df_config[11],
                    'mostrar': df_config[15],
                    'driver': df_config[25],
                    'Razon_Social': df_config[1]
                })
                
                df_conexion['ruta']="F:/OFIMATICA/InvoiceDE/"

                # Traemos las URLs
                cur.execute("SELECT * FROM Databoard.URL WHERE modo = %s;", [df_conexion['modo']])
                df_api = cur.fetchone()
                if df_api:
                    df_conexion['urlrangos'] = df_api[1]
                    df_conexion['urlApi'] = df_api[2]
                    
                    # Conectarmos al ERP
                    ConexERP(df_conexion)
                    
                else:
                    print("No se encontró la configuración de URLs para el modo solicitado.")
            else:
                print(f"Usuario {df_config[1]} Inactivo")
        else:
            print(f"El Cliente con NIT {df_conexion['nit']} no existe.")
    except MySQLdb.MySQLError as e:
        print(f"Ocurrió un error al ejecutar la consulta SQL: {e}")
    finally:
        # Cerrar la conexión de forma segura si ya no se necesita
        if 'miConexion' in df_conexion:
            df_conexion['miConexion'].close()

def ConexERP(ResultConexion):
    print("Conectando al ERP")
    server =ResultConexion['servidor']
    usuario =ResultConexion['usuario']
    database =ResultConexion['BD']
    pwd =ResultConexion['clave']
    compania=ResultConexion['empresa']

    try:
        # Cadena de Conexión
        con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                             f"Server={server};"
                             f"Database={database};"
                             f"uid={usuario};pwd={pwd}")

        # OK! conexión exitosa    
        print("Conexión SQL Exitosa, No Cerrar esta ventana")
        #Consultamos e Insertamos los nuevos documentos
        tascode(con,compania,ResultConexion)

    except Exception as e:
        # Atrapar error
        print("Ocurrió un error al conectar a SQL Server: ", e)
    return None

def tascode(con, compania,ResultConexion):
    factura = input('¿Ingrese el numero de la factura FA12345?')
    with con.cursor() as cursor:
        # Consultamos los nuevos documentos generados (TIPODCTO='NC')
        cursor.execute(f"SELECT DISTINCT TASCODE FROM {compania}.PROCESAR_DS WHERE TIPOFE!='DS' AND DOCUMENTO='"+factura+"'")
        df_nuevos = cursor.fetchall()

        if df_nuevos:
            tascode=df_nuevos[0][0]
            #Consultamos los datos de los documentos
            status(ResultConexion, tascode,factura)
        else:
            print("No existe el documento.")

def status(ResultConexion, tascode,factura):
    urlApi = "https://wrk.tas-la.com/facturacion.v30/invoice/"
    user = ResultConexion['user']
    password = ResultConexion['password']
    ResultConexion['ruta']="F:/OFIMATICA/InvoiceDE/"
    # Consultamos los rangos
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = {
        "verifyStatus": {
            "tascode": tascode
        }
    }

    try:
        # Realizamos la petición POST a la API
        response = requests.post(urlApi, data=json.dumps(data), headers=headers, auth=(user, password))
        response.raise_for_status()  # Lanza un error si el código de respuesta no es 2xx

        # Procesamos la respuesta JSON
        result = response.json()

        ruta_json = os.path.join(ResultConexion['ruta'], 'Getinfo', f"{factura}.json")
        with open(ruta_json, 'w') as fp:
            json.dump(result, fp, indent=2)
        
    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")
        return None, None
    except KeyError as e:
        print(f"Error en la estructura de la respuesta JSON: falta la clave {e}")
        return None, None

    

# Ejecución main
if __name__ == '__main__':
    Conectar()
