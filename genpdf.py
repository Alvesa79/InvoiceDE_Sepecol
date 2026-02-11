import textwrap
from numpy import DataSource
import qrcode
from reportlab.pdfgen import canvas #Importar camvas... servira para lacreacion de tu reporte
from reportlab.lib.units import mm #la unidad de mediada que uses yo uso milimetros
from reportlab.lib.pagesizes import A4#el tamanho de la hoja
import pandas as pd
from numero_letras import numero_to_letras
from lxml import etree
import pyodbc 
import zipfile
import os
import sys

def genpdf(df_documento,df_detalle,df_resdian,respuestas,ResultConexion):
    
    nit=ResultConexion["nit"][0:9]
    ruta=ResultConexion["ruta"]
    CUFE=respuestas['CUFE']

    documento=df_documento[0][25]

    #print(df_documento)
    #yo utilizo esta forma para el tamanho de la hokja no la anterior linea 'fv090129437600024000002'
    Path='F:/OFIMATICA/InvoiceDE/Pdf/'+df_documento[0][24]+documento+'.pdf'
    Width = 210
    Height = 297
    Font = 'Helvetica' #Tipo de letra
    FontSize = 8 #Tamaho de letra
    Line = 4 #variable que uso para Distancia entre lina y linea
    Line1 = 3 #variable que uso para Distancia entre lina y linea
    Line2 = 2 #variable que uso para Distancia entre lina y linea
    Y = 280 #Variable que uso para la corednada, posicion de columna
    Margin = 15 #variable que uso para marginar la hoja

    Center = (Width - (Margin * 2)) / 2 # variable que uso para enocntrar elcentro de una hoja

	
    #numproductos=len(df_detalle.index)

    #maximo de lineas sin totales 39, máximo de lineas con detalles 25		
    numarticulos=0
    maxlineas=0
    numproductos=0
	

    #DEFINIMOS EL NUMERO DE LINEAS
    for b in range(len(df_detalle)):
        if df_detalle[b][2]!='0001':
            if len(df_detalle[b][3].rstrip())<=60:
                numproductos=numproductos+1
            else:
                textodet=textwrap.wrap(df_detalle[b][3].rstrip(), width=60)
                for t in range(len(textodet)):
                    numproductos=numproductos+1


    #numproductos=numproductos+len(df_detalle)
    #print(numproductos)

    #ARREGLO PARA ALMACENAR LAS DESCRIPCIONES
    lst=[]
    for i in range(numproductos):
        lst.append([])

    z=0
    for b in range(len(df_detalle)):
        #print(len(df_detalle[b][3].rstrip()))
        if df_detalle[b][2]!='0001':
            if len(df_detalle[b][3].rstrip())<=60:
                lst[z].append(df_detalle[b][4])
                lst[z].append(df_detalle[b][3].rstrip())
                lst[z].append(df_detalle[b][5])
                lst[z].append(df_detalle[b][8])
                z=z+1
            else:
                textodet=textwrap.wrap(df_detalle[b][3].rstrip(), width=60)
                for t in range(len(textodet)):
                    if t==0:
                        lst[z].append(df_detalle[b][4])
                        lst[z].append(textodet[t])
                        lst[z].append(df_detalle[b][5])
                        lst[z].append(df_detalle[b][8])
                        z=z+1
                    else:
                        lst[z].append(textodet[t])
                        z=z+1
                    

    #print(numproductos)
    #print(lst)


    #definimos el numero de paginas
    if numproductos<=22:
        numpaginas=1
    else:
        adividir=numproductos-22
        numpaginas=adividir//60+1
        if numproductos % 60>0:
            numpaginas=numpaginas+1
        
    #print(numpaginas)	
    #print(documento)
    #print(df_documento[0][25])
    #creo mi reporte
    Report = canvas.Canvas(Path, pagesize=(Width*mm, Height*mm))
    #Metodo(posicionX, posicionY, cadena)
    logo='F:/OFIMATICA/IntegradorOFE/Integrador/Logo_Sepecol.jpg'
    Report.drawImage(logo, (Margin)*mm, 255*mm, width=210, height=60)

    maxlineas=47
    #print(maxlineas)
        

    for i in range(numpaginas):
        #print(numpaginas)
        #print(i)
        
        
        #print(maxlineas)

        Y = Y-Line
        #NOMBRE DE LA EMPRESA
        Report.setFont("Helvetica", 8)
        Report.drawString((Margin)+115*mm, Y*mm, ResultConexion["Razon_Social"],1)
        Report.setFont("Helvetica", 10)

        #Avanzo una linea 
        Y = Y-Line
        #NIT DE LA EMPRESA
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)+138*mm, Y*mm, 'NIT. '+nit+' - REGIMEN COMUN')

        #RESOLUCION
        Y = Y-Line
        Report.setFont("Helvetica-Bold", 6)
        Report.drawString((Margin)+130*mm, Y*mm, 'Actividad CIIU Código 8010 Tarifa 13.8 Por Mil')
        Y = Y-Line1
        Report.drawString((Margin)+115*mm, Y*mm, 'REGIMEN COMUN Resolución No. 012635 de 14 de Diciembre de 2018')
        Y = Y-Line1
        Report.drawString((Margin)+127*mm, Y*mm, 'Documento Oficial de Autorización de Numeración')
        Y = Y-Line1
        Report.drawString((Margin)+124*mm, Y*mm, 'de Facturas '+str(df_resdian[0][0])+' de '+str(df_resdian[0][1])+' al '+str(df_resdian[0][2]))
        Y = Y-Line1
        Report.drawString((Margin)+123*mm, Y*mm, 'Bloque Principal Bogotá: Prefijo SP del No. '+str(df_resdian[0][4])+' al No. '+str(df_resdian[0][5]))

        #ENCABEZADO
        Y = Y-2*Line
        #RECTANGULO ENCABEZADO
        Report.rect((Margin-2)*mm,252*mm,180*mm,-25*mm, fill=0)


        #LINEA VERTICAL
        Report.line(138*mm,252*mm,138*mm,227*mm)
        
        #LINEA HORIZONTAL
        Report.line(138*mm,243*mm,193*mm,243*mm)

        #NOMBRE CLIENTE
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)*mm, Y*mm, 'Señores:')
        Report.setFont("Helvetica", 8)

        #print(Y)
        Report.setFont("Helvetica-Bold", 8)
        if (df_documento[0][24]=='FA'):
            Report.drawString((Margin)+135*mm, Y*mm, 'FACTURA ELECTRÓNICA DE VENTA',1)
            
        elif (df_documento[0][24]=='NC'):
            Report.drawString((Margin)+135*mm, Y*mm, 'NOTA CRÉDITO ELECTRÓNICA',1)
            
        else:
            Report.drawString((Margin)+135*mm, Y*mm, 'NOTA DÉBITO ELECTRÓNICA',1)
            
        h=244
        Report.setFont("Helvetica-Bold", 10)
        Report.drawString((Margin)+152*mm, h*mm, 'No. '+str(df_documento[0][24]+ str(df_documento[0][25])))

        h=h-4
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)+135*mm, h*mm, 'Fecha Factura:')
        Report.setFont("Helvetica", 8)
        Report.drawString((Margin)+161*mm, h*mm, str(df_documento[0][0])+' / 10:00:00')

        h=h-3
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)+135*mm, h*mm, 'Fecha Expedición:')
        Report.setFont("Helvetica", 8)
        Report.drawString((Margin)+161*mm, h*mm, str(df_documento[0][2])+' / 10:00:00')

        h=h-3
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)+135*mm, h*mm, 'Fecha Vence:')
        Report.setFont("Helvetica", 8)
        Report.drawString((Margin)+161*mm, h*mm, str(df_documento[0][2]))

        h=h-3
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)+135*mm, h*mm, 'Método de Pago:')
        Report.setFont("Helvetica", 8)
        Report.drawString((Margin)+161*mm, h*mm, "Crédito")

        h=h-3
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)+135*mm, h*mm, 'Medio de Pago:')
        Report.setFont("Helvetica", 8)
        Report.drawString((Margin)+161*mm, h*mm, "Transf. Crédito")

        if len(df_documento[0][8])>60:
            textocliente=textwrap.wrap(df_documento[0][8], width=60)
            for t in range(len(textocliente)):			
                Report.drawString((Margin)+30*mm, Y*mm, str(textocliente[t]))
                Y=Y-Line
        else:
            textocliente=df_documento[0][8]                

        Y = Y-Line
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)*mm, Y*mm, 'NIT:')
        Report.setFont("Helvetica", 8)
        Report.drawString((Margin)+30*mm, Y*mm, str(df_documento[0][15]))


        Y = Y-Line
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)*mm, Y*mm, 'Dirección:')
        Report.setFont("Helvetica", 8)
        Report.drawString((Margin)+30*mm, Y*mm, str(df_documento[0][14]))

        
        Y = Y-Line
        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((Margin)*mm, Y*mm, 'Teléfono:')
        Report.setFont("Helvetica", 8)
        Report.drawString((Margin)+30*mm, Y*mm, str(df_documento[0][17]))

        Report.setFont("Helvetica-Bold", 8)
        Report.drawString(70*mm, Y*mm, 'Ciudad:')
        Report.setFont("Helvetica", 8)
        Report.drawString(82*mm, Y*mm, str(df_documento[0][11]))

        
        Y = 222
        Report.rect((Margin-2)*mm,225*mm,180*mm,-4*mm, fill=0)

        Report.line(138*mm,252*mm,138*mm,235*mm)


        Report.setFont("Helvetica-Bold", 8)
        Report.drawString((15)*mm, Y*mm, 'CANTIDAD')
        Report.drawString((50)*mm, Y*mm, 'DESCRIPCION SERVICIO / VENTA')
        
        Report.drawString((135)*mm, Y*mm, 'VALOR UNITARIO')
        Report.drawString((Margin)+165*mm, Y*mm, 'VALOR TOTAL')

        #print('Encabezado')		
        #DETALLE
        ays=0
        Y = Y-1-Line
        Report.setFont("Helvetica", 8)
        textoenc=textwrap.wrap(df_documento[0][6].rstrip(), width=105)
        
        
        for r in range(len(textoenc)):
            Report.drawString((Margin)*mm, Y*mm, textoenc[r])
            Y=Y-Line1

        if i==0:
            inicio=0
            if maxlineas<=numproductos:
                fin=maxlineas
            else:
                fin=numproductos
        else:
            inicio=i*(maxlineas)
            fin=inicio+(maxlineas)
            if fin>numproductos:
                fin=numproductos

        
        #print(fin)
        #fin=len(df_detalle)
        #print('Total Articulos'+ str(fin))
        #print('pagina'+str(i))
        z=0
        nlinea=0
        for x in range(inicio,fin):
            #print(len(lst[x]))
            if len(lst[x])>1:
                Y=Y-Line2
                #Cantidad
                Report.drawRightString((Margin)+15*mm, Y*mm, str('{:,}'.format(lst[x][0]).replace('.00','')))
                #Descripcion
                Report.drawString((Margin)+20*mm, Y*mm, str(lst[x][1]).replace("['",'').replace("']",''))
                #Unitario
                Report.drawRightString((Margin)+154*mm, Y*mm, str('{:,}'.format(lst[x][2])))
                #Total
                Report.drawRightString((Margin)+185*mm, Y*mm, str('{:,}'.format(lst[x][3])))				
                Y=Y-Line1
            else:
                #Descripcion
                Report.drawString((Margin)+20*mm, Y*mm, str(lst[x][0]).replace("['",'').replace("']",''))
                Y=Y-Line1
            
            

        if i==(numpaginas-1):	
            #print('Detalle')		

            #totales
            Y=110
            Report.setFont("Helvetica-Bold", 8)
            Report.drawString((Margin+2)*mm, Y*mm, 'Ley 1607 Artículo 462-1')
            Report.drawRightString((165)*mm, Y*mm, 'VALOR A Y S $')
            
            Report.drawRightString((190)*mm, Y*mm, str('{:,.2f}'.format(float(df_documento[0][34]))))

            Y = Y-Line
            Report.drawString((Margin+2)*mm, Y*mm, 'Hacer Retenciones sobre Base IVA')
            Report.drawRightString((165)*mm, Y*mm, 'SUBTOTAL VENTA $')

            
            Report.drawRightString((190)*mm, Y*mm, str('{:,.2f}'.format(float(df_documento[0][19]))))
            
            

            Y = Y-Line
            Report.drawString((Margin+2)*mm, Y*mm, '(AIU) 10%')
            Report.drawRightString((165)*mm, Y*mm, 'IMPUESTO DE IVA $')

            Report.drawRightString((190)*mm, Y*mm, str('{:,.2f}'.format(df_documento[0][22])))

            Y = Y-Line
            #Report.drawString((Margin+2)*mm, 99*mm, 'Base Impuesto IVA $   '+str('{:,.2f}'.format(df_documento[0][19])))
            Report.drawString((Margin+2)*mm, 99*mm, 'Base Impuesto IVA $   '+str('{:,.2f}'.format(float(df_documento[0][32]))))

            Report.drawRightString((165)*mm, Y*mm, 'Ley 1819 Art. 182 RETEFUENTE $')
            #Report.drawRightString((190)*mm, Y*mm, str('{:,.2f}'.format(df_documento[0][19])))


            #RECTANGULO
            Report.rect((Margin)*mm,(Y+15)*mm,52*mm,-15*mm, fill=0)

            #LINEA HORIZONTAL
            Report.line(Margin*mm,(Y-1)*mm,195*mm,(Y-1)*mm)
            
            #MONTO EN LETRAS
            Y = Y-Line-3
            numero=round(df_documento[0][23],2)
            Report.setFont("Helvetica", 8)
            texto=numero_to_letras(numero)
            #print(texto)
            if len(texto)>65:
                texto=textwrap.wrap(texto, width=65)
                Report.drawString((Margin)*mm, (Y+2)*mm, 'Son: '+texto[0])
            
                Report.setFont("Helvetica-Bold", 8)
                Report.drawRightString((165)*mm, (Y+2)*mm, 'TOTAL A PAGAR $')
                Report.drawRightString((190)*mm, (Y+2)*mm, str('{:,.2f}'.format(numero)))
            
                Y = Y-Line
                Report.setFont("Helvetica", 8)
                Report.drawString((Margin)*mm, (Y+2)*mm, texto[1])
            else:
                Report.drawString((Margin)*mm, (Y+2)*mm, 'Son: '+texto)
                Report.setFont("Helvetica-Bold", 8)
                Report.drawRightString((165)*mm, (Y+2)*mm, 'TOTAL A PAGAR $')
                Report.drawRightString((190)*mm, (Y+2)*mm, str('{:,.2f}'.format(numero)))
            
                Y = Y-Line
            #print('Totales')
            
            #LINEA HORIZONTAL
            Report.line(Margin*mm,(Y-1)*mm,195*mm,(Y-1)*mm)

            Y = Y-2*Line
            #RECTANGULO
            Report.rect(Margin*mm,Y*mm,180*mm,-5*mm, fill=0)

            Report.drawCentredString(105*mm, (Y-3)*mm, 'Esta asimila a la letra de cambio y surte todos sus efectos según el Art. 774 del C.D.C. Su pago es exigible y presta mérito ejecutivo.')
            
   
            cadena="https://catalogo-vpfe.dian.gov.co/document/searchqr?documentkey="+CUFE

            
            Y = Y-2*Line
            Report.drawString((Margin)*mm, Y*mm, 'En la fecha Acepto expresamente el contenido de la presente factura')
            Y = Y-Line1
            Report.drawString((Margin)*mm, Y*mm, 'y hago constar que he recibido los servicios prestados a conformidad.')

            
            Y = Y-25
            #GENERADOR CODIGO QR
            img = qrcode.make(cadena)
            #img = qrcode.make("HOLA")
            f = open('F:/OFIMATICA/IntegradorOFE/Integrador/output.png', "wb")
            img.save(f)
            f.close()
            Report.drawImage('F:/OFIMATICA/IntegradorOFE/Integrador/output.png', (100)*mm, (Y)*mm, width=70, height=70)

            #LINEA HORIZONTAL
            Report.line((Margin+10)*mm,(Y+3)*mm,90*mm,(Y+3)*mm)
            Report.drawCentredString((Margin+40)*mm, Y*mm, 'Firma y Sello de Recibido y/o Aceptación')

            #LINEA HORIZONTAL
            Report.line(140*mm,(Y+3)*mm,190*mm,(Y+3)*mm)
            Report.drawCentredString((Margin+150)*mm, Y*mm, 'Firma Autorizada')

            Y = Y-Line1
            Report.drawString((Margin+15)*mm, Y*mm, 'Nombre:')
            Y = Y-Line1
            Report.drawString((Margin+15)*mm, Y*mm, 'C.C.')
            Y = Y-Line1
            Report.drawString((Margin+15)*mm, Y*mm, 'Fecha de Recibido:')

            
            #CUFE
            rotulo="CUFE; "
            if df_documento[0][24]!='FA':
                rotulo="CUNE: "
            
            Y = Y-(2*Line1)
            Report.drawCentredString(110*mm, Y*mm, rotulo+CUFE)

            Y = Y-Line1
            #RECTANGULO
            Report.rect(Margin*mm,Y*mm,180*mm,-8*mm, fill=0)
            Report.drawCentredString(110*mm, (Y-3)*mm, 'PAGUESE A NOMBRE DE SEPECOL LTDA. CON SELLO RESTRINGIDO AL PRIMER BENEFICIARIO O BANCO CAJA SOCIAL')
            Y = Y-Line1
            Report.drawCentredString(110*mm, (Y-3)*mm, 'CUENTA DE AHORROS No. 26500837937')

        
        #PIE DE PAGINA
        Y = 16
        #print(str(Y))
        Report.drawCentredString(110*mm, (Y-3)*mm, 'Calle 83 Bis N 24 78 - Bogotá D.C.')
        Y = Y-Line1
        Report.drawCentredString(110*mm, (Y-3)*mm, 'PBX. 2 36 08 18 - VENTAS: 2 36 88 73 - FAX 2 36 08 23 / 6 91 93 99 Ext. 129')
        Y = Y-Line1
        Report.drawCentredString(110*mm, (Y-3)*mm, 'Email: contabilidad@sepecol.com')
        Y = Y-Line1
        Report.drawCentredString(110*mm, (Y-3)*mm, 'Programa de facturación propio')
        
        

        #Creo un nueva pagina
        Report.showPage()

        Y = 280 #Variable que uso para la corednada, posicion de columna
        
        #Grabo el reporte para finalizar
        #print("PDF "+str(df_documento[0][24])+ str(df_documento[0][25])+" Generado")
    Report.save()
