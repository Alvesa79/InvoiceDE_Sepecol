#PROGRAMA DE FACTURACION ELECTRONICA SEPECOL ULTIMA VERSION 2025-05
import MySQLdb
import json
import os
import requests
from datetime import datetime
import pyodbc
import numero_letras
import sys
import time
from email.mime.text import MIMEText
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Usar una configuración central para las credenciales
DB_HOST = os.getenv('DB_HOST', 'pronosneco.cog7zhynxhqk.us-east-1.rds.amazonaws.com')
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASS = os.getenv('DB_PASS', 'Sanchez8779')
DB_NAME = os.getenv('DB_NAME', 'Databoard')
CONFIG_FILE_PATH = 'C:/APIS/SEPECOL/ConfigFE/configDSsep.json'


def Conectar():
    df_conexion = {}

    # Abriendo el archivo de configuración de manera más segura
    print("Abriendo archivo de configuración")
    try:
        with open(CONFIG_FILE_PATH, "r", encoding="utf-8-sig") as f:
            content = f.read()
            #print("Contenido del archivo:")
            #print(content)
            jsondecoded = json.loads(content)

            df_conexion['nit'] = jsondecoded.get("NIT")
            df_conexion['recipient'] = jsondecoded.get("recipient")
            df_conexion['smtp_server'] = jsondecoded.get("smtp_server")
            df_conexion['smtp_user'] = jsondecoded.get("smtp_user")
            df_conexion['smtp_password'] = jsondecoded.get("smtp_password")
            df_conexion['smtp_port'] = jsondecoded.get("smtp_port")   
            df_conexion['subject'] = jsondecoded.get("subject") 
            df_conexion['body'] = jsondecoded.get("body")    

         
    except FileNotFoundError:
        print(f"El archivo de configuración no se encuentra en {CONFIG_FILE_PATH}")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error al leer el archivo de configuración, formato JSON inválido.")
        print(f"Detalles: {e}")
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
                
                #df_conexion['ruta']="F:/OFIMATICA/InvoiceDE/"

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
    driver = '{SQL Server}'

    try:
        
        connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={usuario};PWD={pwd}"
        con = pyodbc.connect(connection_string)
   
        # OK! conexión exitosa    
        print("Conexión SQL Exitosa, No Cerrar esta ventana")
        #Consultamos e Insertamos los nuevos documentos
        insert_new(con,compania,ResultConexion)

    except Exception as e:
        # Atrapar error
        print("Ocurrió un error al conectar a SQL Server: ", e)
        # Enviar correo de error
        enviar_error(ResultConexion, "Error al conectar a SQL Server", str(e))
    return None

def enviar_error(ResultConexion,subject, body):
       send_email_with_attachments(
            subject=subject,
            body=body,
            recipient=ResultConexion['recipient'],
            attachments=[],
            smtp_server=ResultConexion['smtp_server'],
            smtp_user=ResultConexion['smtp_user'],
            smtp_password=ResultConexion['smtp_password']
        )
       
def insert_new(con, compania,ResultConexion):
    with con.cursor() as cursor:
        # Consultamos los nuevos documentos generados (TIPODCTO='NC')
        cursor.execute(f"SELECT DISTINCT DOCUMENTO FROM {compania}.V_DATOS_PROCESAR_FE WHERE FECHA>='20241114' ")
        df_nuevos = cursor.fetchall()

        if df_nuevos:
            # Extraemos todos los DOCUMENTO para hacer una sola consulta posterior
            documentos_nuevos = [linea[0] for linea in df_nuevos]
            
            # Verificamos si los documentos ya están en PROCESAR_DS
            # Creamos un string con ? para pasar como parámetros
            query = f"SELECT DOCUMENTO FROM {compania}.PROCESAR_DS WHERE DOCUMENTO IN ({','.join(['?']*len(documentos_nuevos))})"
            cursor.execute(query, documentos_nuevos)
            documentos_existentes = {doc[0] for doc in cursor.fetchall()}
                        
            # Filtramos los documentos que aún no están en PROCESAR_DS
            documentos_a_insertar = [doc for doc in documentos_nuevos if doc not in documentos_existentes]
            
            if documentos_a_insertar:
                # Insertamos los nuevos documentos en PROCESAR_DS
                insert_query = f"""
                    INSERT INTO {compania}.PROCESAR_DS 
                    (FECHA, PROVEEDOR, DOCUMENTO, ENVIADO, APROBADO, CUNE, TASCODE, DOCUMENT, INTID, TIPODCTO, TIPOFE)
                    SELECT FECHA, NIT, DOCUMENTO, ENVIADO, APROBADO, CUNE, TASCODE, DOCUMENT, INTID, TIPODCTO, TIPOFE
                    FROM {compania}.V_DATOS_PROCESAR_FE
                    WHERE DOCUMENTO IN ({','.join(['?']*len(documentos_a_insertar))})
                """
              
                cursor.execute(insert_query, documentos_a_insertar)
                print(f"{len(documentos_a_insertar)} nuevos registros insertados.")
            else:
                print("Todos los documentos ya están en la tabla PROCESAR_DS.")
            
            #Consultamos los datos de los documentos
            procesar(con,compania,ResultConexion)
        else:
            print("No hay nuevos documentos.")

def procesar(con,compania,ResultConexion):
    with con.cursor() as cursor:
        cursor.execute("SELECT DISTINCT * FROM " + compania + ".Procesar_DS WHERE "+
                "APROBADO='0' AND TIPOFE!='DS' ORDER BY DOCUMENTO")
        df_procesar=cursor.fetchall()
 
        cursor.execute("SELECT  substring(NRORESOL,1,14) as NRORESOL,convert(date,FVENRESO),convert(date,FHAUTORIZ),PREFIJO,CONSECINI,CONSECFIN  "+
             " FROM CONSECUT WHERE ORIGEN='FAC' AND TIPODCTO='FA'")

        df_resdian=cursor.fetchall() 
        
        #Recorremos el listado de documentos que esten en la tabla Procesar
        for registro in df_procesar:
            Documento=registro[2][2:10]
            Prefijo=registro[5][0:2]
            Tipodoc=registro[9]                
            print("Procesando "+Tipodoc+Documento)
            #Consultamos los datos del empleado
            cursor.execute("SELECT * FROM " + compania + ".V_ENC_INVOICE WHERE "+
                " DOCUMENTO='"+Documento+"' AND TIPODCTO='"+Tipodoc+"'")
            df_documento=cursor.fetchall()
            
            #Consultamos los items de la factura
            cursor.execute("SELECT * FROM " + compania + ".V_DET_INVOICE WHERE "+
                " DOCUMENTO='"+Documento+"' AND TIPODCTO='"+Tipodoc+"'")
            df_detalle=cursor.fetchall()
            
            Encabezado(df_documento,df_detalle,df_resdian,ResultConexion,con)

def Encabezado(df_documento,df_detalle,df_resdian,ResultConexion,con):
  
    valor=numero_letras.numero_to_letras(df_documento[0][23])
    
    prefijo=df_documento[0][35]
    #prefijo='SETP'
    ResultRango=Rangos(ResultConexion,prefijo)
    
    if ResultRango[0] is None:
        enviar_error(ResultConexion, "Error al consultar rangos", f"No se encontró un rango activo para el prefijo {prefijo}.")
        return

    precons='SP'

    
    if df_documento[0][24]=='FA':
        invoice={
            "rangeKey":ResultRango[0],
            "intID":precons+str(df_documento[0][25]),
            "issueDate":str(df_documento[0][0]),
            "issueTime":"100521",
            "dueDate" : str(df_documento[0][2]),
            "paymentType" : str(df_documento[0][3]),
            "paymentCode"  : str(df_documento[0][4]),
            "note1" : valor,
            "note2" : str(df_documento[0][6]),
            "customer" : {
                "additionalAccountID":str(df_documento[0][7]),
                "name":str(df_documento[0][8]).replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('Ñ','N').replace('ñ','n').replace('&','y').replace('º',''),
                "city":str(df_documento[0][11]).replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('Ñ','N').replace('ñ','n').replace('&','y').replace('º',''),
                "countrySubentity":str(df_documento[0][13]),
                "addressLine":str(df_documento[0][14]).replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('Ñ','N').replace('ñ','n').replace('&','y').replace('º',''),
                "documentNumber":str(df_documento[0][15]),
                "documentType":str(df_documento[0][16]),
                "telephone":str(df_documento[0][17]),
                "email":str(df_documento[0][18])
            },
            "additional":{
                "documento_gn" : {
                "prefijo":str(df_documento[0][35]),
                "documento":str(df_documento[0][25])
                },
            },
            "amounts":{
                "totalAmount":f"{float(df_documento[0][19]):.2f}",
                "discountAmount":f"{float(df_documento[0][20]):.2f}",
                "taxAmount":f"{float(df_documento[0][22]):.2f}",
                "payAmount":f"{float(df_documento[0][23]):.2f}",
                "flexAmount":"true"
            }
        }


        
    else:
            invoice=dict(
            {
                "rangeKey":ResultRango[0],
                "intID":precons+str(df_documento[0][25]),
                "issueDate":str(df_documento[0][0]),
                "issueTime":"100521",
                "discrepancyCode" : "1",
                "note1" : valor,
                "note2" : str(df_documento[0][6]),
                "additional":{
                    "documento_gn" : {
                        "prefijo":str(df_documento[0][35]),
                        "documento":str(df_documento[0][25])
                    },
                },
                "amounts":{
                        "totalAmount":f"{float(df_documento[0][19]):.2f}",
                        "discountAmount":f"{float(df_documento[0][20]):.2f}",
                        "taxAmount":f"{float(df_documento[0][22]):.2f}",
                        "payAmount":f"{float(df_documento[0][23]):.2f}",
                        "flexAmount":"true"
                        }
                }
            )

            diafinal='30'
            if str(df_documento[0][0])[4:6]=='02':
                diafinal='28'

            #print(df_documento[0][27])
            #Validamos si se tiene referencia, de ser así la agregamos de lo contrario enviamos sin referencia
            if (df_documento[0][27])!=None:
                invoice['tascode']=str(df_documento[0][27])
            else:
                customerX={}
                customerX = {
                "additionalAccountID":str(df_documento[0][7]),
                "name":str(df_documento[0][8]).replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('Ñ','N').replace('ñ','n').replace('&','y').replace('º',''),
                "city":str(df_documento[0][11]).replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('Ñ','N').replace('ñ','n').replace('&','y').replace('º',''),
                "countrySubentity":str(df_documento[0][13]),
                "addressLine":str(df_documento[0][14]).replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('Ñ','N').replace('ñ','n').replace('&','y').replace('º',''),
                "documentNumber":str(df_documento[0][15]),
                "documentType":str(df_documento[0][16]),
                "telephone":"ND",
                "email":str(df_documento[0][18])
                }
                invoice['customerX']=customerX
                period={}
                period={
                        "startDate": str(df_documento[0][0])[0:6]+'01',
                        "startTime":"000000",
                        "endDate": str(df_documento[0][0])[0:6]+diafinal,
                        "endTime":"000000"
                }
                invoice['period']=period


    #Productos
    items=[]
    for k in range(len(df_detalle)):
        if df_detalle[k][2]!='0001':
            producto={
                "quantity":str(df_detalle[k][4]),
                    "unitPrice":f"{round(float(df_detalle[k][13]),2):.2f}",
                    "total":f"{round(float(df_detalle[k][13]),2):.2f}",
                    "description":str(df_detalle[k][3]).replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('Ñ','N').replace('ñ','n').replace('&','y').replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u'),
                    "brand":"LF",
                    "model":"Soporte",
                    "code": str(df_detalle[k][2])
            }
        
            #Si tiene IVA
            if(df_detalle[k][7]>0):
                taxes=[]
                iva={
                    "ID":"01",
                    "taxAmount":f"{float(df_detalle[k][7]):.2f}",
                    "percent":f"{float(df_detalle[k][6]):.2f}"
                    }
                taxes.append(iva)
                producto['taxes']=taxes
            #Agregamos el detalle de items
            items.append(producto)

        #Si el producto tiene AYS
        if (df_detalle[k][9]>0) :
            
            producto={
            "quantity":str(df_detalle[k][4]),
                "unitPrice":f"{round(float(df_detalle[k][12]),2):.2f}",
                "total":f"{round(float(df_detalle[k][12]),2):.2f}",
                "description":str(df_detalle[k][3]).replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('Ñ','N').replace('ñ','n').replace('&','y').replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u')+' AIU',
                "brand":"LF",
                "model":"Soporte",
                "code": str(df_detalle[k][2])
            }
    
            #Si tiene IVA
            taxes=[]
            iva={
                "ID":"01",
                "taxAmount":f"{round(float(df_detalle[k][8])*0.1*0.19,2):.2f}",
                "percent":'19.00'
                }
            taxes.append(iva)
            producto['taxes']=taxes
            #Agregamos el detalle de items
            items.append(producto)

            
    #Agregamos el arreglo de items   
    invoice['items']=items

    data={}

    if ResultRango[2]=='invoice':
        data['invoice']=invoice
    elif ResultRango[2]=='creditNote':
        data['creditNote']=invoice
    else:
        del(invoice['additional'])
        data['debitNote']=invoice
    archivo=ResultConexion['ruta']+"Json/"+df_documento[0][35]+df_documento[0][25]+".json"
    with open(archivo,'w') as fp:
            json.dump(data,fp,indent=2)
   
    if ResultConexion['mostrar']==1:
        archivo=ResultConexion['ruta']+"Json/"+df_documento[0][35]+df_documento[0][25]+".json"
        #print(archivo)
        with open(archivo,'w') as fp:
            json.dump(data,fp,indent=2)
    #Enviamos el documento al API
    enviarapi(data,ResultConexion,df_documento,df_detalle,df_resdian,con)
    

def Rangos(ResultConexion, Prefijo):
    urlApi = ResultConexion['urlrangos']
    user = ResultConexion['user']
    password = ResultConexion['password']
    rango = ''
    last = 0
    type=''

    # Consultamos los rangos
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    data = {
        "getRanges": {
            "mode": "active",
            "type": "all"
        }
    }

    try:
        # Realizamos la petición POST a la API
        response = requests.post(urlApi, data=json.dumps(data), headers=headers, auth=(user, password))
        response.raise_for_status()  # Lanza un error si el código de respuesta no es 2xx

        # Procesamos la respuesta JSON
        result = response.json()

        # Verificamos el código de estado
        status = result.get('generalResult', {}).get('status', {})
        if status.get('code') != 200:
            print(f"Error: {status.get('text')}")
            return None, None
        
        # Accedemos a los rangos y buscamos el que coincide con el prefijo
        ranges = result['generalResult'].get('ranges', [])

        for range_info in ranges:
            if range_info.get('prefix') == Prefijo.replace(' ',''):
                rango = range_info.get('rangeKey')
                last = range_info.get('last', 0)
                type= range_info.get('type', 0)
                break  # Terminamos el ciclo si encontramos la coincidencia
        return rango, last, type
    
    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud: {e}")
            # Enviar por correo
        enviar_error(ResultConexion, "Error al consultar rangos", str(e))

        return None, None
    except KeyError as e:
        print(f"Error en la estructura de la respuesta JSON: falta la clave {e}")
        enviar_error(ResultConexion, "Error en la respuesta de rangos", f"Falta la clave {e} en la respuesta JSON.")
        return None, None

    


def enviarapi(data,ResultConexion,df_documento,df_detalle,df_resdian,con):
    respuestas = {}
    respuestas['aprobado'] = 0
    respuestas['tascode'] = ''
    respuestas['document'] = ''
    respuestas['consecutivo'] = ''
    respuestas['CUFE']=''
    respuestas['text']=''

    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    
    # Determinamos la URL según el tipo de documento
    documento_tipo = df_documento[0][24]
    if documento_tipo in ['NC', 'N/C']:
        urlApi = ResultConexion['urlApi'].replace('invoice', 'creditNote')
    elif documento_tipo in ['ND', 'N/D']:
        urlApi = ResultConexion['urlApi'].replace('invoice', 'debitNote')
    else:
        urlApi = ResultConexion['urlApi']


    try:
        # Realizamos la solicitud POST
        response = requests.post(
            urlApi, 
            data=json.dumps(data), 
            headers=headers, 
            auth=(ResultConexion['user'], ResultConexion['password'])
        )

        # Verificamos el estado de la respuesta
        if response.status_code == 200:
            print(f"Json {df_documento[0][35]}{df_documento[0][25]} enviado al API")
            resp = response.json()

            # Procesamos la respuesta de la API
            invoice_result = resp.get('invoiceResult', {})
            status = invoice_result.get('status', {})
            code = status.get('code', None)
            text = status.get('text', 'Sin mensaje')
            respuestas['text']=text

            if code == 200:
                print(text)
                document = invoice_result.get('documento', {})
                respuestas['aprobado'] = 1
                respuestas['tascode'] = document.get('tascode', '')
                respuestas['document'] = document.get('document', '')
                respuestas['consecutivo'] = document.get('intID', '')

                # Dependiendo del tipo de documento, asignamos el CUFE o CUDE
                if documento_tipo == 'FA':
                    respuestas['CUFE'] = document.get('CUFE', '')
                else:
                    respuestas['CUFE'] = document.get('CUDE', '')

            else:
                # Si el código no es 200, guardamos la respuesta de rechazo
                print(text)
                
                guardar_rechazo(ResultConexion, df_documento, response.json(), data)
        else:
            # Si la respuesta no tiene código 200, registramos el error
            print(f"Error al enviar el documento. Código de estado: {response.status_code}")
            guardar_rechazo(ResultConexion, df_documento, response.json(), data)
        
        respuestas['enviado'] = 1
        #Actualizamos PROCESAR_DS
        update_procesar(con,df_documento,ResultConexion,respuestas,df_detalle,df_resdian)


    except requests.exceptions.RequestException as e:
        print(f"Error en la solicitud API: {e}")
        respuestas['aprobado'] = 0
        guardar_rechazo(ResultConexion, df_documento, {'error': str(e)}, data)
    

def guardar_rechazo(ResultConexion, df_documento, response_json, data):
    """
    Función auxiliar para guardar la respuesta de rechazo y el JSON enviado.
    """
    # ruta_rechazo = os.path.join(ResultConexion['ruta'], 'Rechazo', f"{df_documento[0][35]+df_documento[0][25]}.json")
    # with open(ruta_rechazo, 'w') as file:
    #     json.dump(response_json, file, indent=4)
    
    # # Guardamos el JSON original que se envió
    # ruta_json = os.path.join(ResultConexion['ruta'], 'Json', f"{df_documento[0][35]+df_documento[0][25]}.json")
    # with open(ruta_json, 'w') as fp:
    #     json.dump(data, fp, indent=2)

    # Enviar por correo
    send_email_with_attachments(
        subject=ResultConexion['subject']+ f" {df_documento[0][35]}{df_documento[0][25]}",
        body=ResultConexion['body'],
        recipient=ResultConexion['recipient'],
        attachments=[response_json, data],
        smtp_server=ResultConexion['smtp_server'],
        smtp_user=ResultConexion['smtp_user'],
        smtp_password=ResultConexion['smtp_password']
    )

def update_procesar(con,df_documento,ResultConexion,respuestas,df_detalle,df_resdian):
    compania=ResultConexion['empresa']

    with con.cursor() as cursor:
        cursor.execute("UPDATE "+compania+".PROCESAR_DS SET ENVIADO=1, APROBADO='"+str(respuestas['aprobado'])+"',"
            " tascode='"+respuestas['tascode']+"', INTID='"+str(respuestas['consecutivo'])+"',"
            " DOCUMENT='"+respuestas['document']+"', CUNE='"+respuestas['CUFE']+"', TEXT='"+respuestas['text']+"' "+
            " WHERE PROVEEDOR='"+df_documento[0][26]+"'"
            " AND DOCUMENTO='"+df_documento[0][24]+df_documento[0][25]+"' AND TIPOFE<>'DS'")
        con.commit()

        #sys.exit()

def send_email_with_attachments(subject, body, recipient, attachments, smtp_server, smtp_user, smtp_password):
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = recipient
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    for attachment in attachments:
        part = MIMEBase('application', 'octet-stream')
        with open(attachment, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment)}')
        msg.attach(part)

    with smtplib.SMTP(smtp_server, 587) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipient, msg.as_string())


# Ejecución main
if __name__ == '__main__':
    Conectar()
    time.sleep(5)