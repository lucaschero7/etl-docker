from datetime import datetime
from typing import Literal
from Conectores_BD import *
from Funciones_CotyData import *
import pandas as pd
import json
from tenacity import retry, stop_after_attempt, wait_fixed, wait_incrementing, wait_exponential
from API_IPN import *
from typing import Literal
from utils import *
import os 
import numpy as np

sql_db = sqlDb(**get_credencials('CotyData'))  
my_sql = sqlDb(**get_credencials('CotyApp'))
API_KEY = get_apikey()

# Logs
class apiLogs:
    def __init__(self, table:str, insertedRows:int = 0, totalRecords:int = 0,statusOk:bool = True, errorMsg:str=None, urlRequest:str = None) -> None:
        self.table = table
        self.insertedRows = insertedRows
        self.totalRecords = totalRecords
        self.statusOk = statusOk
        self.errorMsg = errorMsg
        self.TimeStamp = get_current_timestamp()
        self.urlRequest = urlRequest 

    def getLogDict(self):
        #Create a dict with the log data
        logDict = {
        "loadTable": self.table,
        "insertedRows": self.insertedRows,
        "totalRecords": self.totalRecords,
        "statusOk": self.statusOk,
        "errorMsg": self.errorMsg,
        "TimeStamp": self.TimeStamp,
        "urlRequest": self.urlRequest + "/airflow-by-docker"}
        return logDict
    
    def loadLog(self):
        engine = get_conector_engine('CotyApp')

        #CotyDataLogs = CD_MongoDB()
        #CotyDataLogs.insertOneDocument(salesDocumentsLog.getLogJson())

        #crear un DF con los datos del log
        log = pd.DataFrame([self.getLogDict()])
        try:
            with engine.connect() as conn:
                #Cargar el log en la base de datos
                print('Cargando log en la base de datos...')
                log.to_sql('CotyDataLogs', con = conn,  if_exists = 'append' ,index = False)
        except Exception as e:
            print('Error al cargar el log en la base de datos:', e)

    def saveLog(table, insertedRows, totalRecords,statusOk, errorMsg,urlRequest):
        print(f"Guardando log de la carga de {table}...")
        #Crear un objeto apiLogs para guardar el log de la carga de ventas
        log = apiLogs(table, insertedRows, totalRecords,statusOk, errorMsg,urlRequest)
        log.loadLog()

# Ventas
class salesDocumentsCall:
    def __init__(self, encabezados:pd.DataFrame, detalles:pd.DataFrame, metodosPago:pd.DataFrame) -> None:
        self.encabezados = encabezados
        self.detalles = detalles
        self.metodos_pago = metodosPago

    def creditNotesSalesIDs(self):
        df = self.encabezados
        #Tomo un listado de los SaleId de las ventas donde el tipo de comprobante es de nota de credito
        creditNoteIDs = [8,10,11,12,17,20,27,28,29,37,38,39,43,44,45,47]
        #Filtro el DF de encabezados por los SaleID que estan en la lista creditNoteIDs
        df = df[df['InvoiceType'].isin(creditNoteIDs)]
        #Tomo solo la columna SaleID
        df = df[['SaleID']]
        #Convierto la columna SaleID en un set
        Set = set(df['SaleID'])
        return Set
    
    # Funciones de transformación
    def transformSalesHeader(self) -> pd.DataFrame:
        df = self.encabezados

        # Lista de columnas requeridas, estas columnas aparecen en el df si o si, aun no la api no las devuelva (como en el caso de SalesOrderNumber)
        columnas_requeridas = [
            'SaleID', 'InvoiceNumberChr', 'InvoiceType', 'CompanyID', 'StoreID',
            'InvoiceDate', 'Neto', 'DiscountAmt', 'GeneralDiscountAmt', 'NetoFinal',
            'IVAAmt', 'RechargeAmt', 'InvoiceTotal', 'CustomerCode', 'InvoiceTimeChr',
            'SalesOrderNumber' 
        ]
        
        df = df.reindex(columns=columnas_requeridas)
        
        # Cambiar los valores vacíos de la columna CustomerCode por None
        df['CustomerCode'] = df['CustomerCode'].replace('', None)
        #Separo el numero de PV del numero de comprobante
        df[['CODIGO_PUNTO_VENTA', 'NUMERO_COMPROBANTE']] = df['InvoiceNumberChr'].str.split('-', expand=True)
        del df['InvoiceNumberChr']
        df = df.astype({"CODIGO_PUNTO_VENTA": int, "NUMERO_COMPROBANTE": int})
        #Transformacion del formato de fecha
        df["VENTA_FECHA"] = pd.to_datetime(df["InvoiceDate"], format="%Y-%m-%dT%H:%M:%S").dt.date
        df["VENTA_FECHA2"] = pd.to_datetime(df["InvoiceDate"], format="%Y-%m-%dT%H:%M:%S")
        del df['InvoiceDate']
        #Calculo Utilidad Fiscal
        df.loc[df['InvoiceType'] == 3, 'UTILIDAD_FISCAL'] = ((df['NetoFinal'] / 1.21) * 0.21)
        df.loc[df['InvoiceType'] == 8, 'UTILIDAD_FISCAL'] = ((df['NetoFinal'] / 1.21) * 0.21)

        #PASAR DESCUENTOS A NEGATIVO CUANDO ES ID_COMPROBANTE 1,2,3
        comprobantes_venta = [1,2,3]
        df.loc[df.InvoiceType.isin(comprobantes_venta), 'DiscountAmt'] *= -1
        df.loc[df.InvoiceType.isin(comprobantes_venta), 'GeneralDiscountAmt'] *= -1

        #RENOMBRAMIENTO
        df.rename(columns={'SaleID':'ID_VENTA','InvoiceNumberChr':'NUMERO_COMPROBANTE','InvoiceType':'ID_COMPROBANTE_TIPO','CompanyID':'ID_RAZON_SOCIAL','StoreID':'CODIGO_SUCURSAL','InvoiceNumberChr':'CODIGO_PUNTO_VENTA','InvoiceDate':'VENTA_FECHA','Neto':'VENTA_SUBTOTAL','DiscountAmt':'DESCUENTO1','GeneralDiscountAmt':'DESCUENTO2','NetoFinal':'NETO_GRAVADO_VENTA','IVAAmt':'IVA_VENTA','RechargeAmt':'RECARGO_VENTA','InvoiceTotal':'TOTAL_VENTA','CustomerCode':'ID_CLIENTE','InvoiceTimeChr':'VENTA_HORA','InvoiceDate':'VENTA_FECHA2','SalesOrderNumber':'NUMERO_NOTA_VENTA'}, inplace=True)

        return df 
    def transformSalesDetails(self) -> pd.DataFrame: 
        df = self.detalles
       
        #Tomo las siguientes columnas del DF que se necesitan para el pipeline
        df = df[['DetailID','SaleID','ItemID','UnitPrice','UnitQty','UnitDiscount','UnitSubTotal','UnitCost']]

        #RENOMBRAMIENTO
        df.rename(columns={'DetailID':'ID_VENTA_FILA','SaleID':'ID_VENTA','ItemID':'ITEM_ID','UnitPrice':'PRECIO_UNITARIO_NETO','UnitQty':'CANTIDAD_VENTA','UnitDiscount':'DESCUENTO_UNITARIO','UnitSubTotal':'SUBTOTAL_NETO'}, inplace=True)

        #Calculo costo_neto venta
        df['COSTO_NETO'] = df.apply(lambda x: x['UnitCost'] * x['CANTIDAD_VENTA'], axis=1)
        del df['UnitCost']

        #Calculo utilidad venta
        df['UTILIDAD_TOTAL_NETA'] = df.apply(lambda x: x['SUBTOTAL_NETO'] - x['COSTO_NETO'], axis=1)
        df['UTILIDAD_TOTAL_NETA'] = df['UTILIDAD_TOTAL_NETA'].apply(lambda x: round(x, 4))

        #PASAR A NEGATIVO CUANDO ES UNA NOTA DE CREDITO
        # obtengo los SalesID de las notas de credito con el metodo creditNotesSalesIDs()
        # y luego multiplico por -1 la columna CANTIDAD_VENTA Y COSTO_NETO cuando el ID_VENTA este en la lista de SalesID de las notas de credito
        df.loc[df.ID_VENTA.isin(self.creditNotesSalesIDs()), ['CANTIDAD_VENTA','COSTO_NETO']] *= -1

        return df
    def transformSalesPayments(self) -> pd.DataFrame:
        df = self.metodos_pago
        try:
            #Tomo las siguientes columnas del DF que se necesitan para el pipeline
            df = df[['PaymentID','PaymentMethodID','SaleID','PaymentAmt','PaymentsQty','RechargeAmt','CCAuthCode', 'MP_PaymentID','MP_ExternalReference']]
        except KeyError: 
            df = df[['PaymentID','PaymentMethodID','SaleID','PaymentAmt','PaymentsQty','RechargeAmt', 'MP_PaymentID','MP_ExternalReference']]
            df['CCAuthCode'] = ''

        #RENOMBRAMIENTO

        df.rename(columns={'PaymentID':'ID_VENTA_METODO_PAGO','PaymentMethodID':'ID_METODO_PAGO','SaleID':'ID_VENTA','PaymentAmt':'METODO_PAGO_MONTO','PaymentsQty':'METODO_PAGO_CUOTAS','RechargeAmt':'METODO_PAGO_RECARGO','CCAuthCode':'METODO_PAGO_CODIGO_AUTORIZACION'}, inplace=True)
        
        #PASAR A NEGATIVO CUANDO ES UNA NOTA DE CREDITO
        # obtengo los SalesID de las notas de credito con el metodo creditNotesSalesIDs()
        # y luego multiplico por -1 las columnas METODO_PAGO_MONTO y METODO_PAGO_RECARGO donde
        #  el ID_VENTA este en la lista de SalesID de las notas de credito

        df.loc[df.ID_VENTA.isin(self.creditNotesSalesIDs()), ['METODO_PAGO_MONTO','METODO_PAGO_RECARGO']] *= -1

        #analizar el df y revisar si hay valores en ID_VENTA_METODO_PAGO repetidos. 
        #Si hay, tomar el ID_VENTA y quitar del df los valores que tengan el ID_VENTA_METODO_PAGO repetido
        #Esto se hace para evitar que se dupliquen los registros en la tabla de metodos de pago

        df = df.drop_duplicates(subset=['ID_VENTA_METODO_PAGO'])

        return df
    
    # Funciones de carga
    def loadSalesHeader(self, df: pd.DataFrame) -> tuple:

        tabla = 'VENTAS'
        status = True
        errorMsg = ''
        chunksz = calculate_chunksize(df)

        @retry(
            stop=stop_after_attempt(5),
            wait=wait_incrementing(start=5, increment=5))
        def load_data():
            print('Intentando cargar datos...')
            return df.to_sql(tabla, con = sql_db.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        try:
            cant = load_data()
        except Exception as e:
            errorMsg = repr(e)
            status = False
            cant = 0
            df.to_excel('Encabezado-ventas.xlsx', index=False)
        return tabla, cant, status ,errorMsg
    def loadSalesDetails(self, df: pd.DataFrame) -> tuple:
        #Carga datos a SQL
        tabla = 'CARGA_VENTAS_DETALLE'
        status = True
        errorMsg = ''
        chunksz = calculate_chunksize(df)

        @retry(
            stop=stop_after_attempt(3),  
            wait=wait_incrementing(start=5, increment=5) # Intenta 5 vececes y espera 5 segundos mas entre cada intento
        )
        def cargar_datos():
            return df.to_sql(tabla, con = sql_db.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)

        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_VENTAS_DETALLE")) #Se pasa por tabla intermedia para que el trigger se ejecute una sola vez y no por chunk

            cant = cargar_datos()
            
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_VENTAS_DETALLE"))
                conn.execute(text("EXEC SINCRONIZACION_COMBO_NO_EDITABLE"))
        except Exception as e:
            errorMsg = repr(e)
            status = False
            cant = 0

        return tabla, cant, status ,errorMsg
    def loadSalesPayments(self, df: pd.DataFrame) -> tuple:
                #Carga datos a SQL
        tabla = 'CARGA_VENTAS_METODOS_PAGO'
        status = True
        errorMsg = ''
        chunksz = calculate_chunksize(df)
        
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_incrementing(start=5, increment=5)
        )
        def cargar_datos():
            return df.to_sql(tabla, con = sql_db.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        try:
            with sql_db.engine.begin() as conn:
                # Se pasa por tabla intermedia para que el trigger se ejecute una sola vez y no por chunk
                conn.execute(text("EXEC STAGING_TABLE_VENTAS_METODOS_PAGO")) 

            cant = cargar_datos()

            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_VENTAS_METODOS_PAGO"))
        except Exception as e:
            errorMsg = repr(e)
            status = False
            cant = 0
            df.to_excel('Metodos-de-pago.xlsx', index=False)
        return tabla, cant, status ,errorMsg
    
    #Funciones de lectura y preprocesado
    def readResults(results:list, Total_records:int) -> tuple:
        #Cantidad de resultados en la lista
        headers = []
        items = []
        payments = []

        # Verificación
        total = len(results)
        if total != Total_records:
            print(f'Warning: La cantidad de resultados obtenidos ({total}) no coincide con la cantidad total de registros ({Total_records})')
        
        for i in range(0, total):
            for j in range(0,len(results[i]['Items'])):
                items.append(results[i]['Items'][j])
            for j in range(0,len(results[i]['Payments'])):
                payments.append(results[i]['Payments'][j])

            #Elimino los items y payments de la lista de resultados
            results[i].pop('Items', None)
            results[i].pop('Payments', None)

            #Guardo el header en la lista
            headers.append(results[i])

        return headers, items, payments

    #Funcion principal (Extrae, transforma y carga los datos)
    def createSalesDocumentsLoad(dateFrom:date, dateTo:date, loadHeader:bool = True, loadDetail:bool = True,loadPayments:bool = True, razonSocial:list = [1,2]):

        salesDocuments = IPN_SalesDocuments(API_KEY)

        totalHeaders = 0
        totalDetails = 0
        totalPayments = 0

        for i in razonSocial:
            CompanyId = i
            resultsList, urlRequest, Total_records  = salesDocuments.getManySalesDocuments(company_id=CompanyId, date_from=dateFrom, date_to=dateTo)
            
            if Total_records == 0:
                apiLogs.saveLog('VENTAS', 0, Total_records,1, 'No Records', urlRequest)
                continue

            #Tomar los datos necesarios de la api y separar las cargas con la funcion readResults
            headers, items, payments = salesDocumentsCall.readResults(resultsList, Total_records)

            CargaVentas = salesDocumentsCall(pd.DataFrame(headers),pd.DataFrame(items),pd.DataFrame(payments))
            
            if loadHeader == True:
                df = CargaVentas.transformSalesHeader()
                table, insertedHeaders, statusOk, errorMsg = CargaVentas.loadSalesHeader(df)
                apiLogs.saveLog(table, insertedHeaders, Total_records,statusOk, errorMsg, urlRequest)     
                totalHeaders += insertedHeaders

            if loadDetail == True:
                df =  CargaVentas.transformSalesDetails()
                table, insertedDetails, statusOk, errorMsg = CargaVentas.loadSalesDetails(df)
                apiLogs.saveLog(table, insertedDetails, len(items),statusOk, errorMsg, urlRequest)
                totalDetails += insertedDetails

            if loadPayments == True:
                df =  CargaVentas.transformSalesPayments()
                table, insertedPayments, statusOk, errorMsg = CargaVentas.loadSalesPayments(df)
                apiLogs.saveLog(table, insertedPayments, len(payments),statusOk, errorMsg, urlRequest)
                totalPayments += insertedPayments
        return totalHeaders, totalDetails, totalPayments

class clientsCall:
    def __init__(self, dfClients:pd.DataFrame, razonSocial: int) -> None:
        self.dfClients = dfClients
        self.razonSocial = razonSocial 
    # Funcion de transformacion y carga
    def transformClients(self) -> pd.DataFrame:
        df = self.dfClients
        df = df[['Id','Code','BusinessName','Tax','PriceList','Addresses','CustomAttribute','Audit']]

        #agregar la columna ID_RAZON_SOCIAL con el numero de razon social del objeto
        df['ID_RAZON_SOCIAL'] = self.razonSocial

        #extraer el IdentificationNumber de la columna Tax
        df['CUIT_CLIENTE'] = df['Tax'].apply(lambda x: x['IdentificationNumber'] if x is not None else None)
        del df['Tax']
        
        #extraer el name del customAtribute
        df['TIPO_CLIENTE'] = df['CustomAttribute'].apply(lambda x: x['Name'] if x is not None else None)
        #asignar None a los valores vacios
        df['TIPO_CLIENTE'] = df['TIPO_CLIENTE'].apply(lambda x: None if x == '' else x)
        del df['CustomAttribute']

        #extraer el ZipCode y City de la columna Addresses del objeto que donde el atributo 'Type' = 'fiscal_address'. No usar x[0] porque puede haber mas de una direccion.
        
        #extraer el objeto con el atributo 'Type' = 'fiscal_address'
        df['FiscalAddress'] = df['Addresses'].apply(lambda x: next((item for item in x if item["Type"] == "fiscal_address"), None) if x is not None else None)
        del df['Addresses']

        #del objeto extraido, extraer el ZipCode y concatenar el State con City en una sola columna
        df['CODIGO_POSTAL'] = df['FiscalAddress'].apply(
            lambda x: x.get('ZipCode') if x is not None and isinstance(x, dict) else None
        )
        df['City'] = df['FiscalAddress'].apply(lambda x: x.get('City') if x is not None and isinstance(x, dict) else None)
        df['State'] = df['FiscalAddress'].apply(lambda x: x.get('State') if x is not None and isinstance(x, dict) else None)

        # Por si llegara a no tener State o City, se le asigna none.
        df['LOCALIDAD'] = df.apply(
            lambda x: f"{x['State']}; {x['City']}" 
            if pd.notnull(x['State']) and pd.notnull(x['City']) 
            else None, 
            axis=1
        )

        del df['FiscalAddress']
        del df['City'] 
        del df['State']

        #extrar el id de PriceList
        df['ID_PRECIOS_LISTA'] = df['PriceList'].apply(lambda x: x['Id'] if x is not None else None)
        del df['PriceList']

        #extraer el creation date del objeto Audit
        df['CLIENTE_FECHA_CARGA'] = df['Audit'].apply(lambda x: x['CreationDate'] if x is not None else None)
        del df['Audit']
        
        #RENOMBRAMIENTO
        df.rename(columns={'Code':'ID_CLIENTE','Id':'NUMERO_CLIENTE','BusinessName':'RAZON_SOCIAL_CLIENTE'}, inplace=True)

        return df
    
    def loadClients(self, df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_CLIENTES'
        status = True
        errorMsg = ''
        cant = 0
        chunksz = calculate_chunksize(df)

        @retry(stop=stop_after_attempt(5), wait=wait_incrementing(start=5, increment=5))
        def load_data():
            return df.to_sql(tabla, con = sql_db.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        try:
            with sql_db.cnxn.cursor() as cursor:
                cursor.execute('STAGING_TABLE_CLIENTES')
                sql_db.cnxn.commit()
                cant = load_data() 
                cursor.execute('SINCRONIZACION_CLIENTES')
                sql_db.cnxn.commit()
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel('Clientes.xlsx', index=False)
        return tabla, cant, status ,errorMsg

    # Funcion principal
    def createClientsLoad(dateFrom:date, dateTo:date, razonSocial:list = [1,2]):
        clients = IPN_Customers(API_KEY)

        createdClients = None
        modifiedClients = None

        totalRows = 0

        for i in razonSocial:
            #Obtener una venta
            CompanyId = i
            try:
                # Extract
                createdClients, urlRequest, Total_records = clients.getManyCustomers(company_id=CompanyId, since_creation_date=dateFrom, to_creation_date=dateTo,returnDataFrame=True)
                createdClientsLoad = clientsCall(createdClients, i)
                
                # Transform
                df = createdClientsLoad.transformClients()

                # Load
                table, insertedRows, statusOk, errorMsg = createdClientsLoad.loadClients(df)

                apiLogs.saveLog(table, insertedRows, Total_records,statusOk, errorMsg, urlRequest)           

                totalRows += insertedRows
            except NoRecordsException:
                pass
            except Exception as e:
                raise Exception (e)
            
            time.sleep(1)

            try:
                # Extract
                modifiedClients,urlRequest,Total_records = clients.getManyCustomers(company_id=CompanyId, since_modification_date=dateFrom, to_modification_date=dateTo,returnDataFrame=True)
                modifiedClientsLoad = clientsCall(modifiedClients, i)

                # Transform
                df = modifiedClientsLoad.transformClients()
                
                # Load
                table, insertedRows, statusOk, errorMsg = modifiedClientsLoad.loadClients(df)

                # Save log
                apiLogs.saveLog(table, insertedRows, Total_records,statusOk, errorMsg, urlRequest)           

                totalRows += insertedRows
            except NoRecordsException:
                pass
            except Exception as e:
                raise Exception (e)

            time.sleep(1)
        return totalRows

# Articulos
class Item:
    apiKey = get_apikey()
    ItemsAPI = IPN_Items(apiKey)

    def __init__(self, itemID,itemCode,itemGuid,familyID,categoryID,description,trademark,creationDate, modificationDate, isDeleted, 
                 isItemsGroup,doNotAllowToBuy,doNotAllowToSell,loadBarcodes, loadSuppliers, loadAttributes, loadGroupDetails):
        self.itemID = itemID
        self.itemCode = itemCode
        self.itemGuid = itemGuid
        self.familyID = familyID
        self.categoryID = categoryID
        self.description = description
        self.trademark = trademark
        self.creationDate = creationDate
        self.modificationDate = modificationDate
        self.isDeleted = isDeleted
        self.isItemsGroup = isItemsGroup
        self.doNotAllowToBuy = doNotAllowToBuy
        self.doNotAllowToSell = doNotAllowToSell

        if loadBarcodes:
            self.barcodes = self._getBarcodes()

        if loadSuppliers and not isItemsGroup:  # Si no es combo y hay que cargar los proveedores lo busco
            self.suppliers = self._getSuppliers()

        if loadAttributes:
            self.attributes = self._getAttributes()

        if loadGroupDetails and isItemsGroup:  # Si es combo y hay que cargar los detalles del combo lo busco
            self.groupDetails = self._getGroupDetails()
    
    def _getItems(self) -> list:
        return [
            {
                'SKU': self.itemCode,
                'ITEM_ID': self.itemID,
                'ITEM_GUID': self.itemGuid,
                'CODIGO_CATEGORIA': self.categoryID,
                'CODIGO_FAMILIA': self.familyID,
                'CODIGO_MARCA': self.trademark,
                'DESCRIPCION_ARTICULO': self.description,
                'FECHA_CREACION_ARTICULO': self.creationDate,
                'FECHA_MODIFICACION_ARTICULO': self.modificationDate,
                'ARTICULO_HABILITADO': not self.isDeleted, # Se utiliza operador not para invertir el valor booleano
                'ARTICULO_COMBO': self.isItemsGroup,
                'ARTICULO_NO_COMPRABLE': self.doNotAllowToBuy,
                'ARTICULO_NO_VENDIBLE': self.doNotAllowToSell
            }
        ]

    def _getBarcodes(self) -> list:
        try:
            barcodes = Item.ItemsAPI.getOneBarcode(self.itemID)
        except:
            return []
        return [
            {
                'ITEM_ID': self.itemID, # a posterior esto se debe cambiar por el ITEM_ID
                'CODIGO_BARRA': barcode['Barcode']
            }
            for barcode in barcodes
        ]

    def _getAttributes(self) -> list:
        try:
            attributes = Item.ItemsAPI.getAttributes(self.itemID)
        except:
            #Si el articulo no tiene atributos, devuelve vacio.
            return []
        result = []
        for category in attributes:
            for attribute in category['Attributes']:
                for value in attribute['Values']:
                    result.append({
                        'ITEM_ID': self.itemID,
                        'CODIGO_VALOR': value['Id']
                    })
        return result
    
    def _getSuppliers(self) -> list:
        suppliers = Item.ItemsAPI.getSuppliers(self.itemID)
        return [
            {
                'ITEM_ID': self.itemID,
                'CODIGO_PROVEEDOR': supplier['Supplier']['Id']
            }
            for supplier in suppliers
        ]
   
    def _getGroupDetails(self) -> list:
        combos = Item.ItemsAPI.getGroupDetails(self.itemID)
        return [
            {
                'ITEM_ID': self.itemID,
                'ITEM_ID_COMPOSICION': combo['Id'],
                'CANTIDAD_COMPOSICION': combo['UnitQty']}
            for combo in combos
        ]
class itemCall:
    ItemsAPI = IPN_Items(API_KEY)

    @staticmethod
    def createItemsLoad( 
                        dateFrom:date,
                        dateTo:date,
                        loadItems:bool = True,
                        loadBarcodes:bool = True,
                        loadAttributes:bool = True,
                        loadSuppliers:bool = True,
                        loadGroupDetails:bool = True):

        totalItems,totalBarcodes,totalAttributes,totalSuppliers,totalGroupDetails = itemCall.newAndModifiedItems(dateFrom = dateFrom,
                            dateTo = dateTo,
                            loadItems = loadItems,
                            loadBarcodes = loadBarcodes,
                            loadAttributes = loadAttributes,
                            loadSuppliers = loadSuppliers,
                            loadGroupDetails = loadGroupDetails)

        itemCall.deletedAndUndeletedItems(dateFrom=dateFrom,dateTo=dateTo)

        return totalItems + totalBarcodes + totalAttributes + totalSuppliers + totalGroupDetails

    def newAndModifiedItems( 
                        dateFrom:date,
                        dateTo:date,
                        loadItems:bool = True,
                        loadBarcodes:bool = True,
                        loadAttributes:bool = True,
                        loadSuppliers:bool = True,
                        loadGroupDetails:bool = True):
        totalItems = 0
        totalBarcodes = 0
        totalAttributes = 0
        totalSuppliers = 0
        totalGroupDetails = 0

        # Diccionario que mapea cada acción a sus parámetros de fecha
        action_date_params = {
            'creacion': {'since_creation_date': dateFrom, 'to_creation_date': dateTo},
            'actualizacion': {'since_modification_date': dateFrom, 'to_modification_date': dateTo},
            'eliminacion': {'since_deletion_date': dateFrom, 'to_deletion_date': dateTo}
            }

        for dates in action_date_params.values():
            results, urlRequest, Total_records = itemCall.getItemsByDate(dates)

            if len(results) == 0:
                continue
            
            itemsList = []
            # Creo los objetos item con los resultados obtenidos y busco la info extra en caso de que se necesite
            for item in results:
                itemObject = Item(itemID = item['Id'],
                                       itemCode = item['Code'],
                                       itemGuid = item['Guid'],
                                       familyID = item['FamilyId'],
                                       categoryID = item['CategoryId'],
                                       description = item['Description'],
                                       trademark = item['TradeMarkId'],
                                       creationDate = item['Audit']['CreationDatetime'],
                                       modificationDate = item['Audit']['ModificationDatetime'] if 'ModificationDatetime' in item['Audit'] else None,
                                       isDeleted = item['IsDeleted'],
                                       isItemsGroup = item['IsItemsGroup'],
                                       doNotAllowToBuy = item['DoNotAllowToBuy'],
                                       doNotAllowToSell = item['DoNotAllowToSell'],
                                       loadGroupDetails = loadGroupDetails,
                                       loadBarcodes = loadBarcodes,
                                       loadAttributes = loadAttributes,
                                       loadSuppliers = loadSuppliers
                                       )
                itemsList.append(itemObject)

            if loadItems:
                try:
                    totalRecords, loadedItems = itemCall.loadItems(itemsList)
                    apiLogs.saveLog('CARGA_ARTICULOS', loadedItems, totalRecords, 1, '', urlRequest)
                    totalItems += loadedItems if loadedItems is not None else 0
                except Exception as errorMsg:
                    apiLogs.saveLog('CARGA_ARTICULOS', 0, totalRecords, 0, errorMsg, urlRequest)

            if loadBarcodes:
                try:
                    totalRecords, loadedBarcodes = itemCall.loadBarcodes(itemsList)
                    apiLogs.saveLog('CARGA_ARTICULOS_CODIGOS_BARRA', loadedBarcodes, totalRecords, 1, '', urlRequest)
                    totalBarcodes += loadedBarcodes if loadedBarcodes is not None else 0
                except Exception as errorMsg:
                    apiLogs.saveLog('CARGA_ARTICULOS_CODIGOS_BARRA', 0, totalRecords, 0, errorMsg, urlRequest)

            if loadAttributes:
                try:
                    totalRecords, loadedAttributes = itemCall.loadAttributes(itemsList)
                    apiLogs.saveLog('CARGA_ARTICULOS_ATRIBUTOS', loadedAttributes, totalRecords, 1, '', urlRequest)
                    totalAttributes += loadedAttributes if loadedAttributes is not None else 0
                except Exception as errorMsg:
                    apiLogs.saveLog('CARGA_ARTICULOS_ATRIBUTOS', 0, totalRecords, 0, errorMsg, urlRequest)

            if loadSuppliers:
                try:
                    totalRecords, loadedSuppliers = itemCall.loadSuppliers(itemsList)
                    apiLogs.saveLog('CARGA_ARTICULOS_PROVEEDORES', loadedSuppliers, totalRecords, 1, '', urlRequest)
                    totalSuppliers += loadedSuppliers if loadedSuppliers is not None else 0
                except Exception as errorMsg:
                    apiLogs.saveLog('CARGA_ARTICULOS_PROVEEDORES', 0, totalRecords, 0, errorMsg, urlRequest)

            if loadGroupDetails:
                try:
                    totalRecords, loadedGroupDetails = itemCall.loadGroupDetails(itemsList)
                    apiLogs.saveLog('CARGA_COMBOS', loadedGroupDetails, totalRecords, 1, '', urlRequest)
                    totalGroupDetails += loadedGroupDetails if loadedGroupDetails is not None else 0

                except Exception as errorMsg:
                    apiLogs.saveLog('CARGA_COMBOS', 0, totalRecords, 0, errorMsg, urlRequest)

        return totalItems,totalBarcodes,totalAttributes,totalSuppliers,totalGroupDetails

    def deletedAndUndeletedItems(
                        dateFrom:date,
                        dateTo:date):

        action_date_params = {'deseliminacion': {'since_undeletion_date': dateFrom, 'to_undeletion_date': dateTo}}

        itemsList = []

        # llamar a la api
        for action, dates in action_date_params.items():
            results, urlRequest, Total_records = itemCall.getItemsByDate(dates)
            itemsList.extend(results)
        if len(itemsList) == 0:
            pass
        else:
            # 1ro Convierto a dataframe
            dfItems = pd.DataFrame(itemsList)
            dfItems = dfItems[['Code','Description','Audit']]

            # 2do extraer DeletionDatetime y UndeletionDatetime de adentro de Audit
            dfItems['DeletionDatetime'] = dfItems['Audit'].apply(lambda x: x['DeletionDatetime'] if 'DeletionDatetime' in x else None)
            dfItems['UnDeletionDatetime'] = dfItems['Audit'].apply(lambda x: x['UnDeletionDatetime'] if 'UnDeletionDatetime' in x else None)

            dfItems['DeletionDatetime'] = pd.to_datetime(dfItems['DeletionDatetime'], format='ISO8601')
            dfItems['UnDeletionDatetime'] = pd.to_datetime(dfItems['UnDeletionDatetime'], format='ISO8601')

            # 4. Agrupar por SKU y seleccionar la fecha mas grande entre DeletionDatetime y UnDeletionDatetime
            dfItems = dfItems.groupby('Code').agg({'DeletionDatetime': 'max', 'UnDeletionDatetime': 'max'}).reset_index()

            # Si la fecha de eliminacion es mayor a la fecha de deseliminacion, entonces el articulo esta eliminado
            dfItems['ARTICULO_HABILITADO'] = dfItems.apply(lambda x: 0 if x['DeletionDatetime'] > x['UnDeletionDatetime'] else 1, axis=1)

            # Si UnDeletionDatetime es None, entonces el articulo esta eliminado
            dfItems.loc[dfItems['UnDeletionDatetime'].isnull(), 'ARTICULO_HABILITADO'] = 0

            dfItems = dfItems[['Code','ARTICULO_HABILITADO']]

            # # Quiero recorrer el df y actualizar el articulo en la base de datos 
            with sql_db.engine.begin() as conn:
                for index, row in dfItems.iterrows():
                    conn.execute(text("UPDATE ARTICULOS SET ARTICULO_HABILITADO = {} WHERE SKU = '{}'".format(row['ARTICULO_HABILITADO'], row['Code'])))


            return len(dfItems)

    def getItemsByDate(dateParams):
        items, urlRequest, Total_records = itemCall.ItemsAPI.getManyItems(**dateParams)
        return items, urlRequest, Total_records
                
    def loadBarcodes(itemsList):
        # crear un df con los barcodes de los items
        barcodesList = []
        for item in itemsList:
            barcodesList.extend(item.barcodes)

        dfBarcodes = pd.DataFrame(barcodesList)

        tabla = 'CARGA_ARTICULOS_CODIGOS_BARRA'
        chunksz = calculate_chunksize(dfBarcodes)

        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_ARTICULOS_CODIGOS_BARRA"))
                insertedRows = dfBarcodes.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_ARTICULOS_CODIGOS_BARRA"))
        except Exception as e:
            raise e
        return len(barcodesList), insertedRows
    
    def loadItems(itemsList):
        ItemsList = []
        for item in itemsList:
            ItemsList.extend(item._getItems())
        dfItems = pd.DataFrame(ItemsList)

        tabla = 'CARGA_ARTICULOS'
        chunksz = calculate_chunksize(dfItems)
        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_ARTICULOS"))
                insertedRows = dfItems.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_ARTICULOS_CARGA"))
        except Exception as e:
            raise e
        return len(ItemsList), insertedRows
    
    def loadAttributes(itemsList):
        AttributesList = []
        for item in itemsList:
            AttributesList.extend(item.attributes)
        df_attributes = pd.DataFrame(AttributesList)

        tabla = 'CARGA_ARTICULOS_ATRIBUTOS_VALORES'
        chunksz = calculate_chunksize(df_attributes)

        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_ARTICULOS_ATRIBUTOS_VALORES"))
                insertedRows = df_attributes.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_ARTICULOS_ATRIBUTOS_VALORES"))
        except Exception as e:
            raise e
        return len(AttributesList), insertedRows
    
    def loadSuppliers(itemsList):
        suppliersList = []

        for item in itemsList:
            if item.isItemsGroup == False:
                suppliersList.extend(item.suppliers)
        if len(suppliersList) == 0:
            return 0, 0

        df_suppliers = pd.DataFrame(suppliersList)
        tabla = 'CARGA_ARTICULOS_PROVEEDORES'
        chunksz = calculate_chunksize(df_suppliers)

        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_ARTICULOS_PROVEEDORES"))
                insertedRows = df_suppliers.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_ARTICULOS_PROVEEDORES"))
                return len(suppliersList), insertedRows
        except Exception as e:
            raise e

    def loadGroupDetails(itemsList):
        itemsGroupList = []

        for item in itemsList:
            if item.isItemsGroup:  # Solo si es un combo
                itemsGroupList.extend(item.groupDetails)
        if len(itemsGroupList) == 0:
            return 0, 0

        dfItemsGroupDetails = pd.DataFrame(itemsGroupList)
        tabla = 'CARGA_COMBOS'
        chunksz = calculate_chunksize(dfItemsGroupDetails)

        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_COMBOS_COMPOSICION"))
                insertedRows = dfItemsGroupDetails.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_COMBOS_COMPOSICION"))
                return len(itemsGroupList), insertedRows
        except Exception as e:
            raise e

# CotyApp
class ItemCallCotyApp:
    @staticmethod
    def createItemsLoad(dateTo:date, dateFrom:date):
        action_date_params = {
            'creacion': {'since_creation_date': dateFrom, 'to_creation_date': dateTo},
            'actualizacion': {'since_modification_date': dateFrom, 'to_modification_date': dateTo}
        }

        totalRows = 0

        for action, params in action_date_params.items():
            items, urlRequest, Total_records = IPN_Items(API_KEY).getManyItems(**params)

            if len(items) == 0:
                continue

            lista_de_items = ItemCallCotyApp.transformItems(items)
            lista_de_barcodes_items = ItemCallCotyApp.getBarcodes(lista_de_items)

            tabla, cant, status, errorMsg = ItemCallCotyApp.loadItems(lista_de_barcodes_items)

            totalRows += cant

            apiLogs.saveLog(tabla, cant, Total_records, status, errorMsg, urlRequest)

        return totalRows

    def transformItems(itemsList:list) -> list:
        itemsList2 = []
        for item in itemsList:
            itemsList2.append({
                'ITEM_ID': item['Id'],
                'Sku': item['Code'],
                'Descripcion': item['Description'],
                'Fecha_Modificacion': item['Audit']['ModificationDatetime'] if 'ModificationDatetime' in item['Audit'] else None,
            })
        return itemsList2

    def getBarcodes(itemsList:list) -> pd.DataFrame:
        apikey = get_apikey()
        barcodesData = []
        for item in itemsList:
            try: 
                barcodes = IPN_Items(apikey).getOneBarcode(item['ITEM_ID']) 
            except Exception as e:
                continue
            if len(barcodes) == 0:
                continue
            for barcode in barcodes:
                barcodesData.append({
                    'Sku': item['Sku'],
                    'Descripcion': item['Descripcion'],
                    'Fecha_Modificacion': item['Fecha_Modificacion'],
                    'Codigo_Barra': barcode['Barcode']
                })
        
        dfBarcodes = pd.DataFrame(barcodesData)
        
        return dfBarcodes

    def loadItems(df: pd.DataFrame):
        tabla = 'CargaArticulos'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        cant = 0
        try:
            my_sql.insertQuery('TRUNCATE TABLE CargaArticulos')
            cant = df.to_sql(tabla, con = my_sql.engine, if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
            my_sql.insertQuery('TRUNCATE TABLE CargaArticulos')
        except Exception as e:
            status = False
            errorMsg = repr(e)
            df.to_excel('Articulos-CotyApp.xlsx', index=False)
        
        return tabla, cant, status, errorMsg

class SupplierCallCotyApp:

    @staticmethod
    def createSupplierLoad():
        suppliers, urlRequest, Total_records = IPN_Suppliers(API_KEY).getManySuppliers()

        lista_de_suppliers = SupplierCallCotyApp.transformSuppliers(suppliers)

        tabla, cant, status, errorMsg = SupplierCallCotyApp.loadSuppliers(lista_de_suppliers)

        apiLogs.saveLog(tabla, cant, Total_records, status, errorMsg, urlRequest)

        return cant

    def transformSuppliers(suppliersList:list) -> pd.DataFrame:
        suppliersList2 = []
        for supplier in suppliersList:
            suppliersList2.append({
                'codigo_proveedor': supplier['Id'],
                'razon_social_proveedor': supplier['SupplierFiscalName'],
                'nombre_fantasia': supplier['SupplierName'],
                'identificador_intercompany': supplier['SupplierCode'],
                'id_razon_social_coty': supplier['SupplierCompany']['Id']
            })
        df = pd.DataFrame(suppliersList2)
        return df

    def loadSuppliers(df: pd.DataFrame):
        tabla = 'CARGA_PROVEEDORES'
        chunksz = calculate_chunksize(df)
        cant = 0
        status = True
        errorMsg = ''
        
        try: 
            my_sql.insertQuery("DROP TABLE IF EXISTS CARGA_PROVEEDORES")

            query = 'CREATE TABLE CARGA_PROVEEDORES (codigo_proveedor smallint(6) PRIMARY KEY , id_razon_social_coty tinyint(4), razon_social_proveedor varchar(100), nombre_fantasia varchar(75), identificador_intercompany varchar(10))'

            my_sql.insertQuery(query)

            cant = df.to_sql(tabla, con = my_sql.engine, if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
            
            query2 = 'INSERT INTO PROVEEDORES (codigo_proveedor, id_razon_social_coty, razon_social_proveedor, nombre_fantasia, identificador_intercompany) SELECT codigo_proveedor, id_razon_social_coty, razon_social_proveedor, nombre_fantasia, identificador_intercompany FROM CARGA_PROVEEDORES ON DUPLICATE KEY UPDATE id_razon_social_coty = CARGA_PROVEEDORES.id_razon_social_coty, razon_social_proveedor = CARGA_PROVEEDORES.razon_social_proveedor, nombre_fantasia = CARGA_PROVEEDORES.nombre_fantasia, identificador_intercompany = CARGA_PROVEEDORES.identificador_intercompany'

            my_sql.insertQuery(query2)
            my_sql.insertQuery('DROP TABLE CARGA_PROVEEDORES')
        except Exception as e:
            status = False
            errorMsg = repr(e)
        return tabla, cant, status, errorMsg
# Familias, categorias y marcas
class itemFamiliesCall: 
    @staticmethod
    def transformFamilies(listFamilies: list) -> pd.DataFrame:
        df = pd.DataFrame(listFamilies)
        df= df[['Id','Name']]
        df.rename(columns = {'Id':'CODIGO_FAMILIA','Name':'DESCRIPCION_FAMILIA'}, inplace = True)
        return df
    def loadFamilies(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_ARTICULO_FAMILIA'
        status = True
        errorMsg = ''
        cant = 0
        chunksz = calculate_chunksize(df)

        @retry(stop=stop_after_attempt(5), wait=wait_incrementing(start=5, increment=5))
        def load_data():
            return df.to_sql(tabla, con = sql_db.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_ARTICULO_FAMILIA"))
            cant = load_data()
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_ARTICULO_FAMILIA"))
        except Exception as e:
            sql_db.cnxn.rollback()
            errorMsg = repr(e)
            status = False
        return tabla, cant, status ,errorMsg 
    def createFamiliesLoad():
        try:
            listFamilies, urlRequest, Total_records = IPN_Items(API_KEY).getManyFamilies()
            df = itemFamiliesCall.transformFamilies(listFamilies)
            table, insertedRows, statusOk, errorMsg = itemFamiliesCall.loadFamilies(df)
            apiLogs.saveLog(table, insertedRows, Total_records, statusOk, errorMsg, urlRequest)     
            return insertedRows
        except Exception as e:
            print(f"Error al insertar datos: {repr(e)}")
            raise Exception (e)
class itemCategoriesCall:

    def transformCategories(listCategories:list) -> pd.DataFrame:
        listCategories2 = []
        for category in listCategories:
            listCategories2.append({
                'Id': category['Id'],
                'Name': category['Name'],
                'FamilyId': category['ItemFamily']['Id']
            })
        df = pd.DataFrame(listCategories2)
        df.rename(columns = {'Id':'CODIGO_CATEGORIA', 'Name':'DESCRIPCION_CATEGORIA', 'FamilyId':'CODIGO_FAMILIA'}, inplace = True)
        return df
    
    def loadCategories(df: pd.DataFrame):
        tabla = 'CARGA_CATEGORIAS'
        status = True
        errorMsg = ''
        cant = 0
        chunksz = calculate_chunksize(df)

        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text('STAGING_TABLE_CATEGORIAS_ARTICULOS'))
                cant = df.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
                conn.execute(text('SINCRONIZACION_CATEGORIAS_ARTICULOS'))
        except Exception as e:
            errorMsg = repr(e)
            status = False
            cant = 0
        return tabla, cant, status ,errorMsg
    @staticmethod
    def createCategoriesLoad():
        try:
            listCategories, urlRequest, Total_records = IPN_Items(API_KEY).getManyCategories()
            df = itemCategoriesCall.transformCategories(listCategories)
            table, insertedRows, statusOk, errorMsg = itemCategoriesCall.loadCategories(df)
            apiLogs.saveLog(table, insertedRows, Total_records, statusOk, errorMsg, urlRequest)     
            return insertedRows
        except Exception as e:
            raise Exception (e)

class itemFamiliesCall:
    @staticmethod
    def transformFamilies(listFamilies: list) -> pd.DataFrame:
        df = pd.DataFrame(listFamilies)
        df= df[['Id','Name']]
        df.rename(columns = {'Id':'CODIGO_FAMILIA','Name':'DESCRIPCION_FAMILIA'}, inplace = True)

        return df
    def loadFamilies(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_ARTICULO_FAMILIA'
        status = True
        errorMsg = ''
        cant = 0
        chunksz = calculate_chunksize(df)
            
        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_ARTICULO_FAMILIA"))
                cant = df.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
                conn.execute(text("EXEC SINCRONIZACION_ARTICULO_FAMILIA"))
        except Exception as e:
            errorMsg = repr(e)
            status = False
            cant = 0
        return tabla, cant, status ,errorMsg 
    @staticmethod
    def createFamiliesLoad():
        try:
            listFamilies, urlRequest, Total_records = IPN_Items(API_KEY).getManyFamilies()
            df = itemFamiliesCall.transformFamilies(listFamilies)
            table, insertedRows, statusOk, errorMsg = itemFamiliesCall.loadFamilies(df)
            apiLogs.saveLog(table, insertedRows, Total_records, statusOk, errorMsg, urlRequest)     
            return insertedRows
        except Exception as e:
             raise Exception (e)

class itemMarksCall:
    def transformMarks(listMarks: list) -> pd.DataFrame:
        df = pd.DataFrame(listMarks)
        df = df[['Id','Name']]
        df.rename(columns = {'Id':'CODIGO_MARCA_CARGA','Name':'DESCRIPCION_MARCA_CARGA'}, inplace = True)
        return df
    def loadMarks(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_MARCAS'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_MARCAS"))
                cant = df.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
                conn.execute(text("EXEC SINCRONIZACION_MARCAS"))
        except Exception as e:
            errorMsg = repr(e)
            status = False
            cant = 0
        return tabla, cant, status, errorMsg
    @staticmethod
    def createMarksLoad():
        try:
            listMarks, urlRequest, Total_records = IPN_Items(API_KEY).getManyTrademarks()
            df = itemMarksCall.transformMarks(listMarks)
            table, insertedRows, statusOk, errorMsg = itemMarksCall.loadMarks(df)
            apiLogs.saveLog(table, insertedRows, Total_records, statusOk, errorMsg, urlRequest)     
            return insertedRows
        except Exception as e:
            raise Exception (e)

# Remitos V2
class itemDeliveryNotesCallV2:
    @staticmethod
    def transformLoadDeliveryNotes(listDeliveryNotes):
        # Crear un DataFrame con la lista de remitos
        df = pd.DataFrame(listDeliveryNotes)
        
        # Seleccionar las columnas necesarias
        df = df[['SaleReferID','SaleReferNumber','SalesOrderNumber','StoreID', 'DestinationStoreID','CategoryID','CreationDate','CreatedByUserName']]

        # Cambio formato de fecha sin horas, minutos y segundos
        df['CreationDate'] = df['CreationDate'].apply(lambda x: x.split('T')[0])

        # Cambiar nombre de columnas
        df.rename(columns = {'SaleReferID':'REFER_ID','SaleReferNumber':'NUMERO_REMITO','StoreID':'CODIGO_SUCURSAL_ORIGEN','DestinationStoreID':'CODIGO_SUCURSAL_DESTINO','CategoryID':'ID_CATEGORIA_REMITO_MOVIMIENTO','CreationDate':'FECHA_CREACION_REMITO_MOVIMIENTOS', 'SalesOrderNumber': 'NUMERO_PEDIDO_REPOSICION', 'CreatedByUserName':'USUARIO'}, inplace = True)

        # Creo columna con nombre REMITO_ANULADO y le asigno 0
        df['REMITO_ANULADO'] = 0

        df['NUMERO_PEDIDO_REPOSICION'] = df['NUMERO_PEDIDO_REPOSICION'].replace('', None)

        df['USUARIO'] = df['USUARIO'].replace('', None)

        # Pido datos a la base de datos para obtener la columna CODIGO_SUCURSAL y ID_RAZON_SOCIAL
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute('SELECT CODIGO_SUCURSAL, ID_RAZON_SOCIAL FROM SUCURSALES')
            query = cursor.fetchall()

        # Creo un dataframe con los datos de la consulta
        dfSucursales = pd.DataFrame([{'CODIGO_SUCURSAL': row[0], 'ID_RAZON_SOCIAL': row[1]} for row in query])

        # Agregar columna ID_RAZON_SOCIAL_ORIGEN
        df = pd.merge(
            df,
            dfSucursales,
            how='left',
            left_on='CODIGO_SUCURSAL_ORIGEN',
            right_on='CODIGO_SUCURSAL'
        )
        df.rename(columns={'ID_RAZON_SOCIAL': 'ID_RAZON_SOCIAL_ORIGEN'}, inplace=True)
        df.drop(columns=['CODIGO_SUCURSAL'], inplace=True)  # Eliminar columna duplicada

        # Agregar columna ID_RAZON_SOCIAL_DESTINO
        df = pd.merge(
            df,
            dfSucursales,
            how='left',
            left_on='CODIGO_SUCURSAL_DESTINO',
            right_on='CODIGO_SUCURSAL'
        )
        df.rename(columns={'ID_RAZON_SOCIAL': 'ID_RAZON_SOCIAL_DESTINO'}, inplace=True)
        df.drop(columns=['CODIGO_SUCURSAL'], inplace=True)  # Eliminar columna duplicada

        #Reemplazar con 7 los 0 de la columna ID_CATEGORIA_REMITO_MOVIMIENTO
        df['ID_CATEGORIA_REMITO_MOVIMIENTO'] = df['ID_CATEGORIA_REMITO_MOVIMIENTO'].replace(0, 7)

        # Carga datos SQL
        tabla = 'REMITOS_MOVIMIENTOS'
        status = True
        errorMsg = ''
        chunksz = calculate_chunksize(df)
        try:
            cant = df.to_sql(tabla, con = sql_db.engine, if_exists= 'append' , index = False, method='multi', chunksize=chunksz)
        except Exception as e:
            sql_db.cnxn.rollback()
            errorMsg = repr(e)
            status = False
            cant = 0
        
        return tabla, cant, status, errorMsg
    def updateDeliveryNotesCancel(listDeliveryNotesCancel):

        statusOk = True
        errorMsg = ''

        if not listDeliveryNotesCancel:
            return 'ID_REMITO_MOVIMIENTOS', 0, 1, 'No hubo anulación de remitos en las fechas seleccionadas.'
        
        try:
            totalRemitosAnulados = 0
            for deliveryNote in listDeliveryNotesCancel:
                try:
                    id_remito = deliveryNote.get('SaleReferID')
                    
                    # if not id_remito:
                    #     continue
                    # Crear un log de estos casos.

                    query = "UPDATE REMITOS_MOVIMIENTOS SET REMITO_ANULADO = 1 WHERE REFER_ID = ?"
                    
                    with sql_db.cnxn.cursor() as cursor:
                        cursor.execute(query, id_remito)
                        totalRemitosAnulados += 1
                        
                except Exception as e:
                    sql_db.cnxn.rollback()  # Revierte solo esta transacción fallida
                    continue
            
            sql_db.cnxn.commit()

            return 'ID_REMITO_MOVIMIENTOS', totalRemitosAnulados, statusOk, errorMsg
            
        except Exception as e:
            statusOk = False
            errorMsg = str(e)

            return {
                'tabla': 'REMITOS_MOVIMIENTOS',
                'cant': 0,
                'status': False,
                'errorMsg': str(e)
            }
        finally:
            if 'cnxn' in locals():
                sql_db.cnxn.close()
    def transformLoadDeliveryNotesDetails(listDeliveryNotesDetails):

        df = pd.DataFrame(listDeliveryNotesDetails)
        df = df[['SaleReferID','SaleReferNumber','ItemID','UnitQty']]

        # Cambiar nombre de columnas de la tabla REMITOS_MOVIMIENTO_DETALLE
        df.rename(columns = {
            'SaleReferID':'REFER_ID',
            'SaleReferNumber':'NUMERO_REMITO',
            'ItemID':'ITEM_ID',
            'UnitQty':'CANTIDAD_REMITIDA'
        }, inplace = True)

        #Sumarizar detalle
        df = df.groupby(['REFER_ID','ITEM_ID', 'NUMERO_REMITO']).sum('CANTIDAD_REMITIDA').reset_index()

        # Carga datos a SQL
        tabla = 'REMITOS_MOVIMIENTOS_DETALLE'
        status = True
        errorMsg = ''
        chunksz = calculate_chunksize(df)
        try:
            cant = df.to_sql(tabla, con = sql_db.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        except Exception as e:
            errorMsg = repr(e)
            status = False
            cant = 0
        
        return tabla, cant, status ,errorMsg

    # Funcion que obtiene los items de cada remito
    def readResults(results: list, Total_records: int) -> tuple:
        # Inicializar listas para encabezados e ítems
        headers = []
        items = []

        for i in range(0, Total_records):
            # Obtener el número de remito del encabezado actual
            sale_refer_number = results[i].get('SaleReferNumber', None)
            sale_refer_id = results[i].get('SaleReferID', None)

            # Agregar los ítems individuales a la lista de ítems
            for item in results[i].get('Items', []):
                # Añadir el número de remito a cada ítem
                item['SaleReferNumber'] = sale_refer_number 
                item['SaleReferID'] = sale_refer_id
                items.append(item)

            # Eliminar los ítems del resultado actual para dejar solo el encabezado
            results[i].pop('Items', None)

            # Agregar el encabezado a la lista de encabezados
            headers.append(results[i])

        return headers, items
    
    # Funcion principal (pedir datos a la API)
    def createDeliveryNotesLoad(
        company_id:int,
        date_from:date,
        date_to:date,
        loadHeader:bool = True,
        loadDetail:bool = True,
        loadCancelNotes:bool = True,
        ):

        deliveryNotes = IPN_DeliveryNotes(API_KEY)

        totalHeaders = 0
        totalDetails = 0
        totalCancelNotes = 0

        if loadHeader or loadDetail:
            # Llamo a la API para obtener los remitos
            resultsList, urlRequest, Total_records = deliveryNotes.getDeliveryNotes(company_id=company_id, date_from=date_from, date_to=date_to, search_type=1, refer_type_id=1)

            # Separar los encabezados e ítems de los resultados
            headers, items = itemDeliveryNotesCallV2.readResults(resultsList, Total_records)

            if loadHeader:

                table, insertedHeaders, statusOk, errorMsg = itemDeliveryNotesCallV2.transformLoadDeliveryNotes(headers)
                apiLogs.saveLog(table, insertedHeaders, Total_records,statusOk, errorMsg, urlRequest)

                totalHeaders += insertedHeaders

            if loadDetail:

                table, insertedDetails, statusOk, errorMsg = itemDeliveryNotesCallV2.transformLoadDeliveryNotesDetails(items)
                apiLogs.saveLog(table, insertedDetails, len(items),statusOk, errorMsg, urlRequest)

                totalDetails += insertedDetails

        if loadCancelNotes:
            # Llamo a la API para obtener remitos anulados
            resultsListCancel, urlRequestCancel, total_records_cancel = deliveryNotes.getDeliveryNotes(company_id=company_id, date_from=date_from, date_to=date_to, search_type=4,refer_type_id=1)

            table, insertedCancelNotes, statusOk, errorMsg = itemDeliveryNotesCallV2.updateDeliveryNotesCancel(resultsListCancel)
            apiLogs.saveLog(table, insertedCancelNotes, total_records_cancel,statusOk, errorMsg, urlRequestCancel)

            totalCancelNotes += insertedCancelNotes

        return totalHeaders, totalDetails, totalCancelNotes

# Remitos V3
class DeliveryNotesCall:

    def transformDeliveryNotes(listDeliveryNotes: list) -> pd.DataFrame:
        listDeliveryNotesFiltered = []
        for deliveryNote in listDeliveryNotes:
            listDeliveryNotesFiltered.append({
                'USUARIO': deliveryNote['Audit']['CreationUsername'],
                'REFER_ID': deliveryNote['Id'],
                'NUMERO_REMITO': deliveryNote['DeliveryNoteNumber'],
                'FECHA_CREACION_REMITO_MOVIMIENTOS': deliveryNote['Audit']['CreationDatetime'],
                'SUCURSAL_ORIGEN': deliveryNote['Origin']['Name'],
                'SUCURSAL_DESTINO': deliveryNote['Destination']['Name'],
                'ID_CATEGORIA_REMITO_MOVIMIENTO': deliveryNote['DeliveryNoteCategory']['Id'],
                'NUMERO_PEDIDO_REPOSICION': deliveryNote['SalesOrder']['SalesOrderNumberChr'] if 'SalesOrder' in deliveryNote else None,

            })
        df = pd.DataFrame(listDeliveryNotesFiltered)
        df = Funciones_CotyData.get_codigo_sucursal(df,'SUCURSAL_ORIGEN')
        df.rename(columns={'CODIGO_SUCURSAL':'CODIGO_SUCURSAL_ORIGEN', 'ID_RAZON_SOCIAL':'ID_RAZON_SOCIAL_ORIGEN'}, inplace=True)

        df = Funciones_CotyData.get_codigo_sucursal(df,'SUCURSAL_DESTINO')
        df.rename(columns={'CODIGO_SUCURSAL':'CODIGO_SUCURSAL_DESTINO', 'ID_RAZON_SOCIAL':'ID_RAZON_SOCIAL_DESTINO'}, inplace=True)

        df.drop(columns=['SUCURSAL_ORIGEN', 'SUCURSAL_DESTINO'], inplace=True)

        df['FECHA_CREACION_REMITO_MOVIMIENTOS'] = pd.to_datetime(df['FECHA_CREACION_REMITO_MOVIMIENTOS'], format='ISO8601')

        return df
    def transformDeliveryNotesDetails(listDeliveryNotesDetails: list) -> pd.DataFrame:
        listDeliveryNotesDetailsFiltered = []
        for deliveryNoteDetail in listDeliveryNotesDetails:
            for item in deliveryNoteDetail['Details']:
                listDeliveryNotesDetailsFiltered.append({
                    'NUMERO_REMITO': deliveryNoteDetail['DeliveryNoteNumber'],
                    'REFER_ID': deliveryNoteDetail['Id'],
                    'ITEM_ID': item['Item']['Id'],
                    'CANTIDAD_REMITIDA': item['UnitQty'],
                })
        df = pd.DataFrame(listDeliveryNotesDetailsFiltered)
        df = df.groupby(['ITEM_ID', 'NUMERO_REMITO', 'REFER_ID']).sum('CANTIDAD_REMITIDA').reset_index()

        return df
    
    def loadDeliveryNotes(df: pd.DataFrame) -> tuple:
        tabla = 'REMITOS_MOVIMIENTOS'
        status = True
        errorMsg = ''
        chunksz = calculate_chunksize(df)
        
        try:
            cant = df.to_sql(tabla, con = sql_db.engine, if_exists= 'append' , index = False, method='multi', chunksize=chunksz)
        except Exception as e:
            errorMsg = repr(e)
            status = False
            cant = 0
        return tabla, cant, status, errorMsg
    def loadDeliveryNotesDetails(df: pd.DataFrame) -> tuple:
        tabla = 'REMITOS_MOVIMIENTOS_DETALLE'
        status = True
        errorMsg = ''
        chunksz = calculate_chunksize(df)

        try:
            cant = df.to_sql(tabla, con = sql_db.engine, if_exists= 'append' , index = False, method='multi', chunksize=chunksz)
        except Exception as e:
            df.to_excel(f'{tabla}.xlsx', index=False)
            errorMsg = repr(e)
            status = False
            cant = 0
        return tabla, cant, status ,errorMsg
    
    def updateDeliveryNotesCancel(listDeliveryNotesCancel: list) -> tuple:

        statusOk = True
        errorMsg = ''

        if not listDeliveryNotesCancel:
            return 'ID_REMITO_MOVIMIENTOS', 0, 1, 'No hubo anulación de remitos en las fechas seleccionadas.'
        
        try:
            totalRemitosAnulados = 0
            for deliveryNote in listDeliveryNotesCancel:
                try:
                    id_remito = deliveryNote.get('Id')
                    fecha_anulacion = deliveryNote["Audit"]["DeletionDatetime"]
                                        
                    with sql_db.engine.begin() as conn:
                        conn.execute(
                            text("UPDATE REMITOS_MOVIMIENTOS SET FECHA_ANULACION = :fecha_anulacion WHERE REFER_ID = :id_remito"),
                            {"fecha_anulacion": fecha_anulacion, "id_remito": id_remito}
                        )
                    totalRemitosAnulados += 1
                        
                except Exception as e:
                    print(f"Error al actualizar remito {id_remito}: {str(e)}")
                    continue

            return 'REMITOS_MOVIMIENTOS_ANULADOS', totalRemitosAnulados, statusOk, errorMsg
            
        except Exception as e:
            statusOk = False
            errorMsg = str(e)

            return {
                'tabla': 'REMITOS_MOVIMIENTOS',
                'cant': 0,
                'status': False,
                'errorMsg': str(e)
            }
        finally:
            if 'cnxn' in locals():
                sql_db.cnxn.close()
    
    # Funcion principal
    def createDeliveryNotesLoad(
        date_from:date,
        date_to:date,
        loadHeader:bool = True,
        loadDetail:bool = True,
        loadCancelNotes:bool = True,
        ):

        deliveryNotes = IPN_V3_DeliveryNotes(API_KEY)

        totalHeaders = 0
        totalDetails = 0
        totalCancelNotes = 0

        if loadHeader or loadDetail:
            # Llamo a la API para obtener los remitos
            resultsList, urlRequest, Total_records = deliveryNotes.getDeliveryNotes(date_from=date_from, date_to=date_to)

            if loadHeader:

                df = DeliveryNotesCall.transformDeliveryNotes(resultsList)
                table, insertedHeaders, statusOk, errorMsg = DeliveryNotesCall.loadDeliveryNotes(df)
                apiLogs.saveLog(table, insertedHeaders, Total_records,statusOk, errorMsg, urlRequest)

                totalHeaders += insertedHeaders

            if loadDetail:

                df = DeliveryNotesCall.transformDeliveryNotesDetails(resultsList)
                table, insertedDetails, statusOk, errorMsg = DeliveryNotesCall.loadDeliveryNotesDetails(df)
                apiLogs.saveLog(table, insertedDetails, len(df),statusOk, errorMsg, urlRequest)

                totalDetails += insertedDetails

        if loadCancelNotes:
            resultsListCancel, urlRequestCancel, total_records_cancel = deliveryNotes.getDeliveryNotes(date_from=date_from, date_to=date_to, search_type='4')

            table, insertedCancelNotes, statusOk, errorMsg = DeliveryNotesCall.updateDeliveryNotesCancel(resultsListCancel)
            apiLogs.saveLog(table, insertedCancelNotes, total_records_cancel,statusOk, errorMsg, urlRequestCancel)

            totalCancelNotes += insertedCancelNotes

        return totalHeaders, totalDetails, totalCancelNotes

# Atributos
class attributesCall:
    def transformAttributes(listAttributes: list) -> pd.DataFrame:
        attributesList = []
        for attribute in listAttributes:
            attributesList.append({
                'CODIGO_ATRIBUTO' : attribute['Id'],
                'DESCRIPCION_ATRIBUTO' : attribute['Name'],
                'CODIGO_CATEGORIA_ATRIBUTO' : attribute['AttributeCategory']['Id'],
                'CODIGO_TIPO_ATRIBUTO' : attribute['AttributeType']['Id'],
                'REQUERIDO' : attribute['IsRequired'],
                'OBLIGATORIO' : attribute['IsMandatory']
            })
        df = pd.DataFrame(attributesList)
        return df

    def transformAttributesValues(listAttributes: list) -> pd.DataFrame:
        attributesValuesList = []
        for attributeValue in listAttributes:
            if 'Values' in attributeValue and attributeValue['Values']:
                for value in attributeValue['Values']:
                    attributesValuesList.append({
                        'CODIGO_VALOR': value['Id'],
                        'VALOR_DESCRIPCION': value['Value'],
                        'CODIGO_ATRIBUTO': attributeValue['Id'],
                    })
        df = pd.DataFrame(attributesValuesList)
        mask = (df['CODIGO_VALOR'] == 218) & (df['CODIGO_ATRIBUTO'] == 8) & (df['VALOR_DESCRIPCION'] == 'NO')
        df = df[~mask]

        mask = (df['CODIGO_VALOR'] == 320) & (df['CODIGO_ATRIBUTO'] == 9) & (df['VALOR_DESCRIPCION'] == 'NO')
        df = df[~mask]

        mask = (df['CODIGO_VALOR'] == 321) & (df['CODIGO_ATRIBUTO'] == 10) & (df['VALOR_DESCRIPCION'] == 'NO')
        df = df[~mask]
        return df

    def loadAttributes(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_ATRIBUTOS'
        status = False
        cant = 0
        errorMsg = ''
        chunksz = calculate_chunksize(df)
        try:
            with sql_db.engine.begin() as conn: 
                conn.execute(text("EXEC STAGING_TABLE_ATRIBUTOS"))

            cant = df.to_sql(tabla, con=sql_db.engine, if_exists='append', index=False, method='multi', chunksize=chunksz)
            
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_ATRIBUTOS"))
            status = True

        except Exception as e:
            errorMsg = repr(e)
            cant = 0
        
        return tabla, cant, status, errorMsg

    def loadValues(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_ATRIBUTOS_VALORES'
        status = False
        cant = 0
        errorMsg = ""
        chunksz = calculate_chunksize(df)
        try:
            with sql_db.engine.begin() as conn: 
                conn.execute(text("EXEC STAGING_TABLE_ATRIBUTOS_VALORES"))
            cant = df.to_sql(tabla, con=sql_db.engine, if_exists='append', index=False, method='multi', chunksize=chunksz)
            
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_ATRIBUTOS_VALORES"))
            status = True
        except Exception as e:
            sql_db.cnxn.rollback()
            errorMsg = repr(e)
        
        return tabla, cant, status, errorMsg
    
    @staticmethod
    def createAttributesLoad(
        loadAttributes:bool = True,
        loadAttributesValues:bool = True
    )-> tuple:
        
        attributes = IPN_Attributes(API_KEY)

        totalAttributes = 0
        totalAttributesValues = 0
        
        resultsList, urlRequest, Total_records = attributes.getManyAttributes()

        if loadAttributes: 
            df = attributesCall.transformAttributes(resultsList)
            table, insertedHeaders, statusOk, errorMsg = attributesCall.loadAttributes(df)
            apiLogs.saveLog(table, insertedHeaders, Total_records,statusOk, errorMsg, urlRequest)
            totalAttributes += insertedHeaders
        if loadAttributesValues: 
            df = attributesCall.transformAttributesValues(resultsList)
            table, insertedDetails, statusOk, errorMsg = attributesCall.loadValues(df)
            apiLogs.saveLog(table, insertedDetails, Total_records,statusOk, errorMsg, urlRequest)
            totalAttributesValues += insertedDetails

        insertRows = totalAttributes + totalAttributesValues
        return insertRows

# Categorias de Atributos
class categoriesAttributesCall:
    def transformCategoriesAttributes(list: list) -> pd.DataFrame:
        listCategoriesAttributes = []
        for category in list:
            listCategoriesAttributes.append({
                'CODIGO_CATEGORIA_ATRIBUTO' : category['Id'],
                'CATEGORIA_ATRIBUTO_DESCRIPCION' : category['Name']
            })
        df = pd.DataFrame(listCategoriesAttributes)
        return df
    
    def loadCategoriesAttributes(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_ATRIBUTOS_CATEGORIA'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        cant = 0
        try:
            with sql_db.engine.begin() as conn: 
                conn.execute(text("EXEC STAGING_TABLE_ATRIBUTOS_CATEGORIA"))

            cant = df.to_sql(tabla, con = sql_db.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)

            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC SINCRONIZACION_ATRIBUTOS_CATEGORIA"))
        except Exception as e:
            sql_db.cnxn.rollback()
            errorMsg = repr(e)
            status = False
            cant = 0
        return tabla, cant, status, errorMsg
    
    @staticmethod
    def createCategoriesAttributesLoad() -> int:
        try:
            ipn_categories_attributes = IPN_CategoriesAttributes(API_KEY)
            resultsList, Total_records, urlRequest = ipn_categories_attributes.getCategoriesAttributes()
            df = categoriesAttributesCall.transformCategoriesAttributes(resultsList)
            table, insertedRows, statusOk, errorMsg = categoriesAttributesCall.loadCategoriesAttributes(df)
            apiLogs.saveLog(table, insertedRows, Total_records,statusOk, errorMsg, urlRequest)
            return insertedRows
        
        except Exception as e:
            print(f"Error al insertar datos: {repr(e)}")   
            return 0

# Delivery Notes CotyApp
class DeliveryNotesCall_V3:
    def transform_delivery_notes(delivery_notes: list) -> pd.DataFrame:
        delivery_notes_list = []
        for delivery_note in delivery_notes:
            if delivery_note.get('DeliveryNoteCategory', {}).get('Name') != 'Devolución a proveedor':
                delivery_notes_list.append({
                    'NUMERO_REMITO' : delivery_note['DeliveryNoteNumber'],
                    'FECHA_CREACION' : delivery_note['DeliveryNoteDate'],
                    'CATEGORIA_REMITO' : delivery_note['DeliveryNoteCategory']['Name'],
                    'SUCURSAL_ORIGEN': delivery_note['Origin']['Name'],
                    'SUCURSAL_DESTINO': delivery_note['Destination']['Name']
                })
        df = pd.DataFrame(delivery_notes_list)
        # Cambio el formato de fecha yyyy-mm-dd
        df['FECHA_CREACION'] = pd.to_datetime(df['FECHA_CREACION']).dt.strftime('%Y-%m-%d')

        df = Funciones_CotyData.get_codigo_sucursal(df,'SUCURSAL_ORIGEN')
        df.rename(columns={'CODIGO_SUCURSAL':'CODIGO_SUCURSAL_ORIGEN', 'ID_RAZON_SOCIAL':'ID_RAZON_SOCIAL_ORIGEN'}, inplace=True)

        df = Funciones_CotyData.get_codigo_sucursal(df,'SUCURSAL_DESTINO')
        df.rename(columns={'CODIGO_SUCURSAL':'CODIGO_SUCURSAL_DESTINO', 'ID_RAZON_SOCIAL':'ID_RAZON_SOCIAL_DESTINO'}, inplace=True)

        df.drop(columns=['SUCURSAL_ORIGEN', 'SUCURSAL_DESTINO'], inplace=True)
        return df

    def load_delivery_notes(df: pd.DataFrame) -> tuple:
        tabla = 'REMITOS_REPOSICION'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''

        try:
            cant = df.to_sql(tabla, con = my_sql.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        except Exception as e:
            errorMsg = repr(e)
            status = False
            cant = 0
        return tabla, cant, status, errorMsg
    
    def transform_delivery_notes_details(delivery_notes: list) -> pd.DataFrame:
        listDeliveryNotesDetails = []
        for deliveryNote in delivery_notes:
            if deliveryNote.get('DeliveryNoteCategory', {}).get('Name') != 'Devolución a proveedor':
                for item in deliveryNote['Details']: 
                    listDeliveryNotesDetails.append({
                        'NUMERO_REMITO': deliveryNote['DeliveryNoteNumber'],
                        'SKU': item['Item']['Code'],
                        'CANTIDAD_REMITIDA': item['UnitQty']
                    })
        df = pd.DataFrame(listDeliveryNotesDetails)
        
        df = df.groupby(['NUMERO_REMITO','SKU']).sum('CANTIDAD_REMITIDA').reset_index()
        return df
    
    def load_delivery_notes_details(df: pd.DataFrame) -> tuple:
        tabla = 'REMITOS_REPOSICION_DETALLE'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''

        try:
            cant = df.to_sql(tabla, con = my_sql.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        except Exception as e:
            my_sql.cnxn.rollback()
            errorMsg = repr(e)
            status = False
            cant = 0
        return tabla, cant, status, errorMsg

    @staticmethod
    def create_delivery_notes(date_from: date, date_to: date, load_header: bool = True, load_detail: bool = True):
        try:
            totalHeaders = 0
            totalDetails = 0

            # Carga del encabezados
            results, url_request, total_records = IPN_V3_DeliveryNotes(API_KEY).getDeliveryNotes(date_from=date_from, date_to=date_to)
            
            if load_header:
                df = DeliveryNotesCall_V3.transform_delivery_notes(results)
                table, insertedRows, statusOk, errorMsg = DeliveryNotesCall_V3.load_delivery_notes(df)
                apiLogs.saveLog(table, insertedRows, total_records,statusOk, errorMsg, url_request)
                totalHeaders += insertedRows
            
            if load_detail:
                df = DeliveryNotesCall_V3.transform_delivery_notes_details(results)
                table, insertedRows, statusOk, errorMsg = DeliveryNotesCall_V3.load_delivery_notes_details(df)
                apiLogs.saveLog(table, insertedRows, df.shape[0],statusOk, errorMsg, url_request)
                totalDetails += insertedRows

            insertedRows = totalHeaders + totalDetails

            return insertedRows
            
        
        except Exception as e:
            raise e

# Listado de precios
class PriceListCall:
    def transformPriceList(listPriceList:list, price_list_id:list) -> pd.DataFrame:
        listPriceList2 = [] 
        for priceList in listPriceList:
            listPriceList2.append({
                'ITEM_ID': priceList['Item']['Id'], # Agregar despues
                'ID_PRECIOS_LISTA': price_list_id,
                'PRECIO_VIGENCIA_DESDE': priceList['Audit']['CreationDatetime'],
                'PRECIO_NETO': priceList['NetValueNew']
            })
        df = pd.DataFrame(listPriceList2)
        df['PRECIO_VIGENCIA_DESDE'] = pd.to_datetime(df['PRECIO_VIGENCIA_DESDE'], format='mixed')
        df['PRECIO_VIGENCIA_DESDE'] = df['PRECIO_VIGENCIA_DESDE'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str.slice(0, 23)
        df.drop_duplicates(subset=['ITEM_ID', 'ID_PRECIOS_LISTA'], keep='first', inplace=True)
        return df
    def loadPriceList(df: pd.DataFrame) -> tuple:
        tabla = 'PRECIOS_HISTORICO'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        cant = 0

        try:
            with sql_db.engine.begin() as conn:
                cant = df.to_sql(tabla, con=conn, if_exists='append', index=False, method='multi', chunksize=chunksz)
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel('Precios_historico.xlsx', index=False)
            raise e
        finally:
            return tabla, cant, status, errorMsg
    @staticmethod
    def createPriceListLoad(date_from: date, date_to: date):
        # pedir id de listas de precios a la db
        with sql_db.engine.connect() as conn:
            ids = conn.execute(text("SELECT ID_PRECIOS_LISTA FROM PRECIOS_LISTA")).fetchall()
            ids = [i[0] for i in ids]
        df_final = pd.DataFrame()
        for id in ids:
            resultsList, urlRequest, Total_records = IPN_ItemsPricelists(API_KEY).getManyPriceLogs(
                price_list_id=id,
                date_from=date_from,
                date_to=date_to
                )
            df_final = pd.concat([df_final, PriceListCall.transformPriceList(resultsList, id)], ignore_index=True)
        table, insertedRows, statusOk, errorMsg = PriceListCall.loadPriceList(df_final)
        apiLogs.saveLog(table, insertedRows, df_final.shape[0],statusOk, errorMsg, urlRequest)
        return insertedRows

# Listado de costos
class CostListCall:
    def transformCostList(costList) -> pd.DataFrame:
        costList2 = []
        for cost in costList:
            costList2.append({
                'ITEM_ID': cost['Item']['Id'],
                'COSTO_NETO': cost['PriceCostValueNew'],
                'COSTO_VIGENCIA_DESDE': cost['Audit']['ModificationDatetime']
            })
        df = pd.DataFrame(costList2)
        df.drop_duplicates(subset=['ITEM_ID'], keep='first', inplace=True)
        df['COSTO_VIGENCIA_DESDE'] = pd.to_datetime(df['COSTO_VIGENCIA_DESDE'], format='mixed')
        df['COSTO_VIGENCIA_DESDE'] = df['COSTO_VIGENCIA_DESDE'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str.slice(0, 23)
        return df
    
    def loadCostList(df: pd.DataFrame) -> tuple:
        tabla = 'COSTOS_HISTORICO'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        cant = 0

        try:
            with sql_db.engine.begin() as conn:
                cant = df.to_sql(tabla, con=conn, if_exists='append', index=False, method='multi', chunksize=chunksz)
                conn.execute(text("EXEC SINCRONIZACION_COSTOS_COMBOS_FACTOR_DISTRIBUCION"))
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel('Costos_historico.xlsx', index=False)
            raise e
        finally:
            return tabla, cant, status, errorMsg
    @staticmethod
    def createCostListLoad(date_from: date, date_to: date):
        resultsList, urlRequest, Total_records = IPN_ItemsCost(API_KEY).getManyItemsCost(date_from=date_from, date_to=date_to)
        df = CostListCall.transformCostList(resultsList)
        table, insertedRows, statusOk, errorMsg = CostListCall.loadCostList(df)
        apiLogs.saveLog(table, insertedRows, Total_records,statusOk, errorMsg, urlRequest)
        return insertedRows

class PurchaseOrderCall:
    def transformPurchaseOrderHeaders(purchaseOrder: list) -> pd.DataFrame:
        purchaseOrder2 = []
        for purchase in purchaseOrder:
            purchaseOrder2.append({
                'ID_ORDEN_COMPRA': purchase['Id'],
                'NUMERO_ORDEN_COMPRA': purchase['PurchaseOrderNumber'],
                'FECHA_ORDEN_COMPRA': datetime.strptime(purchase['Audit']['CreationDate'], '%d/%m/%Y'),
                'IMPORTE_NETO_ORDEN_COMPRA': purchase['TotalNetAmt'],
                'CODIGO_PROVEEDOR': purchase['Supplier']['Id'],
                'CODIGO_COMPRADOR': purchase['BuyerEmployee']['Id'],
                'PRESCRITA': (1 if purchase['PurchaseOrderStatus']['Id'] == 13 else 0),
                'CODIGO_CATEGORIA_OC': purchase.get('PurchaseOrderType', {}).get('Id', 7)
            })
        df = pd.DataFrame(purchaseOrder2)
        return df
    def transformPurchaseOrderDetails(purchaseOrder: list) -> pd.DataFrame:
        purchaseOrder3 = []
        for purchase in purchaseOrder:
            for item in purchase['Details']:
                purchaseOrder3.append({
                    'ID_ORDEN_COMPRA': purchase['Id'],
                    'NUMERO_ORDEN_COMPRA': purchase['PurchaseOrderNumber'],
                    'ITEM_ID': item['Item']['Id'],
                    'CANTIDAD_ORDEN_COMPRA': item['UnitQty'],
                    'PRECIO_COMPRA': item['UnitPrice'],
                    'ID_ORDEN_COMPRA_DETALLE' : item['Id']
                })
        df = pd.DataFrame(purchaseOrder3)
        return df
    
    def loadPurchaseOrderHeaders(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_ORDENES_COMPRA'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        cant = 0

        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_ORDENES_COMPRA"))
                cant = df.to_sql(tabla, con=conn, if_exists='append', index=False, method='multi', chunksize=chunksz)
                conn.execute(text("EXEC SINCRONIZACION_ORDENES_COMPRA"))
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel('Ordenes_compra.xlsx', index=False)
        finally:
            return tabla, cant, status, errorMsg
    def loadPurchaseOrderDetails(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_ORDENES_COMPRA_DETALLE'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        cant = 0

        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_ORDENES_COMPRA_DETALLE"))
                cant = df.to_sql(tabla, con=conn, if_exists='append', index=False, method='multi', chunksize=chunksz)
                conn.execute(text("EXEC SINCRONIZACION_ORDENES_COMPRA_DETALLE"))
        except Exception as e:
            sql_db.cnxn.rollback()
            errorMsg = repr(e)
            status = False
            df.to_excel('Ordenes_compra_detalle.xlsx', index=False)
        finally:
            return tabla, cant, status, errorMsg

    @staticmethod
    def createPurchaseOrderLoad(
        date_from: date,
        date_to: date,
        load_headers: bool = True,
        load_details: bool = True,
        ):

        action_date_params = {
            'creacion': {'since_creation_date': date_from, 'to_creation_date': date_to},
            'modificacion': {'since_modification_date': date_from, 'to_modification_date': date_to}
        }

        for action, params in action_date_params.items():
            resultsList, urlRequest, Total_records = IPN_PurchaseOrders(API_KEY).getPurchaseOrders(**params)

            if Total_records == 0:
                continue  # Si no hay registros, continuar con la siguiente acción
            
            if load_headers:
                total_headers = 0
                df = PurchaseOrderCall.transformPurchaseOrderHeaders(resultsList)
                if action == 'creacion':
                    table, insertedRows, statusOk, errorMsg = PurchaseOrderCall.loadPurchaseOrderHeaders(df)
                    total_headers += insertedRows
                else:
                    table, insertedRows, statusOk, errorMsg = PurchaseOrderCall.loadPurchaseOrderHeaders(df)
                    total_headers += insertedRows

                apiLogs.saveLog(table, insertedRows, Total_records, statusOk, errorMsg, urlRequest)
                
            if load_details:
                total_details = 0
                df_details = PurchaseOrderCall.transformPurchaseOrderDetails(resultsList)
                if action == 'creacion':
                    table, insertedRows, statusOk, errorMsg = PurchaseOrderCall.loadPurchaseOrderDetails(df_details)
                    total_details += insertedRows
                else:
                    table, insertedRows, statusOk, errorMsg = PurchaseOrderCall.loadPurchaseOrderDetails(df_details)
                    total_details += insertedRows

                apiLogs.saveLog(table, insertedRows, df_details.shape[0], statusOk, errorMsg, urlRequest)

class SupplierCall:
    @staticmethod
    def createSupplierLoad():
        listSuppliers, urlRequest, Total_records = IPN_Suppliers(API_KEY).getManySuppliers()
        
        df = SupplierCall.transformSuppliers(listSuppliers)
        
        table, insertedRows, statusOk, errorMsg = SupplierCall.loadSuppliers(df)
        
        apiLogs.saveLog(table, insertedRows, Total_records,statusOk, errorMsg, urlRequest)
        
        return insertedRows
    
    def transformSuppliers(listSuppliers: list) -> pd.DataFrame:
        suppliers = []
        for supplier in listSuppliers:
            suppliers.append({
                'CODIGO_PROVEEDOR': supplier['Id'],
                'RAZON_SOCIAL_PROVEEDOR': supplier['SupplierFiscalName'],
                'NOMBRE_FANTASIA_PROVEEDOR': supplier['SupplierName'],
                'CUIT': supplier['Tax']['IdentificationNumber'],
                'ID_RAZON_SOCIAL': supplier['SupplierCompany']['Id'],
                'IDENTIFICADOR_INTERCOMPANY': supplier['SupplierCode'],
                'ID_TIPO_PROVEEDOR': supplier['SupplierType']['Id'],
                'ID_SUB_TIPO_PROVEEDOR': supplier.get('SupplierSubType', {}).get('Id'),
                'ID_TIPO_CONDICION_FISCAL': supplier['Tax']['TaxCondition']['Id'],
            })
        df = pd.DataFrame(suppliers)
        return df
    
    def loadSuppliers(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_PROVEEDORES'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        cant = 0

        try:
            with sql_db.engine.begin() as conn:
                conn.execute(text("EXEC STAGING_TABLE_PROVEEDORES"))
                cant = df.to_sql(tabla, con=conn, if_exists='append', index=False, method='multi', chunksize=chunksz)
                conn.execute(text("EXEC SINCRONIZACION_PROVEEDORES"))
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel('Proveedores-prueba-datos.xlsx', index=False) # Para el debug
        finally:
            return tabla, cant, status, errorMsg

class GoodsReceiptCall:
    
    @staticmethod
    def createGoodsReceipt(date_from: date, date_to: date, load_headers: bool = True, load_details: bool = True, load_anulados: bool = True):
        stores = select_query_df(DataBase='CotyData', query='SELECT CODIGO_SUCURSAL FROM SUCURSALES')
        stores = stores['CODIGO_SUCURSAL'].tolist()
        results, urlRequest, Total_records = IPN_GoodsReceipts(API_KEY).getManyGoodsReceipts(since_creation_date=date_from, to_creation_date=date_to, stores=stores)

        if Total_records == 0:
            return 0

        total_headers = 0
        total_details = 0
        total_anulados = 0

        if load_headers:
            df = GoodsReceiptCall.transformGoodsReceipt(results)
            table, insertedRows, statusOk, errorMsg = GoodsReceiptCall.loadGoodsReceipt(df)
            total_headers += insertedRows
            apiLogs.saveLog(table, insertedRows, Total_records, statusOk, errorMsg, urlRequest)

        if load_details:
            df_details = GoodsReceiptCall.transformGoodsReceiptDetails(results)
            table, insertedRows, statusOk, errorMsg = GoodsReceiptCall.loadGoodsReceiptDetails(df_details)
            total_details += insertedRows
            apiLogs.saveLog(table, insertedRows, df_details.shape[0], statusOk, errorMsg, urlRequest)
        
        if load_anulados:
            results_anulados, urlRequest, Total_records = IPN_GoodsReceipts(API_KEY).getManyGoodsReceipts(since_deletion_date=date_from, to_deletion_date=date_to, stores=stores)
            table, insertedRows, statusOk, errorMsg = GoodsReceiptCall.updateGoodsReceiptCancel(results_anulados)
            total_anulados += insertedRows
            apiLogs.saveLog(table, insertedRows, Total_records, statusOk, errorMsg, urlRequest)
        
        return total_headers + total_details + total_anulados

    def transformGoodsReceipt(results: list) -> pd.DataFrame:
        goodsReceipt = []
        for goods in results:
            goodsReceipt.append({
                'ID_REMITO_COMPRA': goods['Id'],
                'NUMERO_REMITO_COMPRA': goods['ReceiptNumber'],
                'FECHA_REMITO': goods['Audit']['CreationDatetime'].split('T')[0],
                'FECHA_REMITO2': goods['Audit']['CreationDatetime'],
                'CODIGO_SUCURSAL': goods['Store']['Id'],
                'CODIGO_PROVEEDOR': goods['Supplier']['Id'],
                'REMITO_ANULADO': goods['IsDeleted']
            })
        df = pd.DataFrame(goodsReceipt)
        return df
    
    def transformGoodsReceiptDetails(results: list) -> pd.DataFrame:
        goodReceiptDetails = []
        for goods in results:
            for item in goods['Details']:
                goodReceiptDetails.append({
                    'ID_REMITO_COMPRA': goods['Id'],
                    'NUMERO_REMITO_COMPRA': goods['ReceiptNumber'],
                    # 'ID_ORDEN_COMPRA': item['PurchaseOrder']['Id'],
                    'NUMERO_ORDEN_COMPRA': item['PurchaseOrder']['PurchaseOrderNumber'],
                    'ITEM_ID': item['Item']['Id'],
                    # 'SKU': item['Item']['Code'],
                    'CANTIDAD_DETALLE_REMITO_COMPRA': item['UnitQty'],
                    'CODIGO_PROVEEDOR': goods['Supplier']['Id'],
                })
        df = pd.DataFrame(goodReceiptDetails)
        return df

    def loadGoodsReceipt(df: pd.DataFrame) -> tuple:
        tabla = 'REMITOS_COMPRA'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        cant = 0

        try:
            with sql_db.engine.begin() as conn:
                cant = df.to_sql(tabla, con=conn, if_exists='append', index=False, method='multi', chunksize=chunksz)
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel(f'{tabla}.xlsx', index=False)
        finally:
            return tabla, cant, status, errorMsg

    def loadGoodsReceiptDetails(df: pd.DataFrame) -> tuple:
        tabla = 'REMITOS_COMPRA_DETALLE'
        chunksz = calculate_chunksize(df)
        status = True
        errorMsg = ''
        cant = 0

        try:
            with sql_db.engine.begin() as conn:
                cant = df.to_sql(tabla, con=conn, if_exists='append', index=False, method='multi', chunksize=chunksz)
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel(f'{tabla}.xlsx', index=False)
        finally:
            return tabla, cant, status, errorMsg
        
    def updateGoodsReceiptCancel(listGoodsReceiptCancel: list) -> tuple:
        statusOk = True
        errorMsg = ''
        totalRemitosCompraAnulados = 0
        
        # Validación inicial
        if not listGoodsReceiptCancel:
            return 'ID_REMITO_COMPRA', 0, True, 'No se encontraron remitos para cancelar'
        
        try:
            # Procesar cada remito
            for goodReceipt in listGoodsReceiptCancel:
                try:
                    id_remito_compra = goodReceipt['ReceiptNumber']
                    query = "UPDATE REMITOS_COMPRA SET REMITO_ANULADO = 1 WHERE NUMERO_REMITO_COMPRA = ?"
                    
                    with sql_db.cnxn.cursor() as cursor:
                        cursor.execute(query, id_remito_compra)
                        # Verificar si se actualizó algún registro
                        if cursor.rowcount > 0:
                            totalRemitosCompraAnulados += 1
                        
                except KeyError as e:
                    errorMsg = f"Clave faltante en goodReceipt: {e}"
                    statusOk = False
                    break
                except Exception as e:
                    errorMsg = f"Error procesando remito {id_remito_compra}: {repr(e)}"
                    statusOk = False
                    break
            
            # Commit solo si todo fue exitoso
            if statusOk:
                sql_db.cnxn.commit()
            else:
                sql_db.cnxn.rollback()
                
        except Exception as e:
            statusOk = False
            errorMsg = f"Error general: {repr(e)}"
            sql_db.cnxn.rollback()
        
        return 'REMITOS_COMPRA', totalRemitosCompraAnulados, statusOk, errorMsg  

def Cargar_Orden_Compra_categoria():
    #Busco las categorias en la BD
    query = "SELECT codigo_cat_oc AS CODIGO_CATEGORIA_OC, Descripcion_Cat_OC AS DESCRIPCION_CATEGORIA FROM Categorias_OC"
    df = select_query_df('CotyApp',query)

    try:
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute('SELECT CODIGO_CATEGORIA_OC, DESCRIPCION_CATEGORIA FROM ORDEN_COMPRA_CATEGORIA')
            QWRY = cursor.fetchall()
    except Exception as e:
        print(e)
        sql_db.cnxn.close()
        raise e
    claves = []
    for values in QWRY:
        claves.append(values[0])

    #Borro repetidas
    for x in claves:
        df = df.loc[df["CODIGO_CATEGORIA_OC"] != x]

    #Carga datos a SQL
    tabla = 'ORDEN_COMPRA_CATEGORIA'
    chunksz = calculate_chunksize(df)

    try: 
        insertedRows = df.to_sql(tabla, con = sql_db.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        apiLogs.saveLog(tabla, insertedRows, df.shape[0], True, '', tabla)
    except Exception as e:
        apiLogs.saveLog(tabla, 0, df.shape[0], False, repr(e), tabla)
    return insertedRows

def Cargar_Categorizacion_Orden_Compra():

    query = 'SELECT numero_OC AS NUMERO_ORDEN_COMPRA, Categoria_OC AS CODIGO_CATEGORIA_OC, Prescrita AS PRESCRITA, Fecha_carga AS FECHA_CARGA_CATEGORIA_OC, Fecha_Actualizacion AS FECHA_ACTUALIZACION_CATEGORIA_OC FROM Ordenes_compra WHERE Fecha_carga BETWEEN DATE_ADD(date(now()), INTERVAL -5 DAY) AND DATE_ADD(date(now()), INTERVAL -1 DAY)'
    df = select_query_df('CotyApp', query)

    if df.empty:
        return 0

    tabla = 'CARGA_CATEGORIZACION_ORDEN_COMPRA'
    chunksz = calculate_chunksize(df)
    try:
        with sql_db.engine.begin() as conn:
            conn.execute(text('EXEC STAGING_TABLE_CATEGORIZACION_ORDEN_COMPRA'))
            insertedRows = df.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
            conn.execute(text('EXEC SINCRONIZACION_CATEGORIZACION_OC'))
    except Exception as e:
        raise
    return insertedRows

def Cargar_Recepciones_Mercaderia():
    query = 'SELECT fecha_recepcion as FECHA_RECEPCION, estado as ESTADO, numero_remito as NUMERO_REMITO_COMPRA, codigo_proveedor as CODIGO_PROVEEDOR, Fecha_control as FECHA_CONTROL FROM INGRESOS_MERCADERIA WHERE CAST(fecha_recepcion AS DATE) BETWEEN DATE_ADD(date(now()), INTERVAL -5 DAY) AND DATE_ADD(date(now()), INTERVAL -1 DAY)'

    df = select_query_df('CotyApp', query)
    
    #Carga datos a SQL
    tabla = 'CARGA_RECEPCIONES_MERCADERIA'
    chunksz = calculate_chunksize(df)

    try:
        with sql_db.engine.begin() as conn:
            conn.execute(text('STAGING_TABLE_RECEPCIONES_MERCADERIA'))
            insertedRows = df.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
            conn.execute(text('SINCRONIZACION_RECEPCIONES_MERCADERIA'))
    except Exception as e:
        raise 
    return insertedRows

class CustomersCall:
    def transformCustomers(listClients: list, rz: int) -> pd.DataFrame:
        clients = []
        for client in listClients:
            clients.append({
                'ID_CLIENTE': client['CustomerCode'], # Este es el numero de cliente pero en la db se guardo como ID_CLIENTE, en un futuro se debe corregir!
                'TIPO_CLIENTE': (client.get('CustomAttribute') or {}).get('Name'),
                'RAZON_SOCIAL_CLIENTE': client['CustomerFiscalName'],
                'CUIT_CLIENTE': client['Tax']['IdentificationNumber'],
                'CODIGO_POSTAL': client['Addresses'][1]['ZipCode'],
                # Por si llegara a no tener State o City, se le asigna none.
                'LOCALIDAD': (lambda a: (f"{a.get('State')}; {a.get('City')}" if a.get('City') else a.get('State')) if a and a.get('State') else None)(
                    next((x for x in (client.get('Addresses') or []) if x.get('Type') == 'fiscal_address'), None)
                ),
                'NUMERO_CLIENTE': client['Id'], # A corregir (El numero y id estan invertidos)
                'ID_PRECIOS_LISTA': client['PriceList']['Id'],
                'ID_RAZON_SOCIAL': rz,
                'CLIENTE_FECHA_CARGA': client['Audit']['CreationDate'].split('T')[0]
            })
        df = pd.DataFrame(clients)
        return df

    @staticmethod
    def createCustomers(rz_list: list, date_from: date, date_to: date) -> tuple:
        action_date_params = {
            'creacion': {'since_creation_date': date_from, 'to_creation_date': date_to},
            'modificacion': {'since_modification_date': date_from, 'to_modification_date': date_to}
        }
        total_records = 0

        for action, params in action_date_params.items():
            for rz in rz_list:
                customers = IPN_Customers_V3(API_KEY)
                listClients, urlRequest, Total_records = customers.getManyCustomers(company_id=rz, **params)
                total_records += Total_records
                
                if Total_records == 0:
                    continue
                else: 
                    df = CustomersCall.transformCustomers(listClients=listClients, rz=rz)
                    table, insertedRows, statusOk, errorMsg = CustomersCall.loadCustomers(df)
                    apiLogs.saveLog(table, insertedRows, Total_records,statusOk, errorMsg, urlRequest)
        return total_records

    def loadCustomers(df: pd.DataFrame) -> tuple:
        tabla = 'CARGA_CLIENTES'
        status = True
        errorMsg = ''
        cant = 0
        chunksz = calculate_chunksize(df)
        
        try:
            with sql_db.engine.begin() as conn:  
                conn.execute(text('EXEC STAGING_TABLE_CLIENTES'))
                df.to_sql(tabla, con=conn, if_exists='append', index=False, 
                        method='multi', chunksize=chunksz)
                cant = len(df)
                conn.execute(text('EXEC SINCRONIZACION_CLIENTES'))
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel(f'{tabla}.xlsx', index=False)
            
        return tabla, cant, status, errorMsg
    

class SalesOrder:
    @staticmethod
    def createSalesOrder(
        date_from: date,
        date_to: date,
        load_head: bool = True, 
        load_details: bool = True
        ) -> int:

        sales_orders = IPN_Sales_Orders(API_KEY)

        listSalesOrdersComplete = []
        stores = get_list_stores()

        for store in stores:
    
            listSalesOrders, urlRequest, Total_records = sales_orders.getManySalesOrders(date_from=date_from, date_to=date_to, include_details=load_details, include_payments=load_details, store_id=store)

            if Total_records == 0:
                continue

            listSalesOrdersComplete.extend(listSalesOrders)


        Total_records = len(listSalesOrdersComplete)

        total_headers = 0
        total_details = 0

        if load_head:
            df = SalesOrder.transformSalesOrder(listSalesOrdersComplete)
            table, insertedRows, statusOk, errorMsg = SalesOrder.loadSalesOrder(df)
            total_headers = insertedRows
            apiLogs.saveLog(table, insertedRows, Total_records,statusOk, errorMsg, urlRequest)
            

        if load_details:
            df = SalesOrder.transformSalesOrderDetails(listSalesOrdersComplete)
            table, insertedRows, statusOk, errorMsg = SalesOrder.loadSalesOrderDetails(df)
            total_details = insertedRows
            apiLogs.saveLog(table, insertedRows, len(df),statusOk, errorMsg, urlRequest)

        return total_headers + total_details

    def transformSalesOrder(listSalesOrders: list) -> pd.DataFrame:
        sales_orders = []
        for sales_order in listSalesOrders:
            sales_orders.append({
              'NUMERO_NOTA_VENTA': sales_order['SalesOrderNumberChr'],
              'FECHA_NOTA_VENTA': sales_order['SalesOrderDate'].split('T')[0],
              'CODIGO_SUCURSAL': sales_order['StoreId'],
              'ID_RAZON_SOCIAL': sales_order['CompanyId'],
              'USUARIO': sales_order['EmployeeName'],
              'SUBTOTAL': sales_order['SalesOrderTotalAmt'],
              'GUID': sales_order['SalesOrderGuid']
            })
        df = pd.DataFrame(sales_orders)
        return df
    
    def transformSalesOrderDetails(listSalesOrderDetails: list) -> pd.DataFrame:
        sales_order_details = []
        for sales_order_detail in listSalesOrderDetails:
            for item in sales_order_detail['Items']:
                sales_order_details.append({
                    'NUMERO_NOTA_VENTA': sales_order_detail['SalesOrderNumberChr'],
                    'CANTIDAD_NOTA_VENTA': item['UnitQty'],
                    'SUBTOTAL_DETALLE': item['ItemSubTotal'],
                    'ITEM_ID': item['ItemId'],
                })
        
        df = pd.DataFrame(sales_order_details)
        return df

    def loadSalesOrder(df: pd.DataFrame) -> tuple:
        tabla = 'NOTAS_VENTA'
        status = True
        errorMsg = ''
        cant = 0
        chunksz = calculate_chunksize(df)
        try:
            with sql_db.engine.connect() as conn:
                cant = df.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel(f'{tabla}_{datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx', index=False)
        return tabla, cant, status ,errorMsg

    def loadSalesOrderDetails(df: pd.DataFrame) -> tuple:
        tabla = 'NOTAS_VENTA_DETALLE'
        status = True
        errorMsg = ''
        cant = 0
        chunksz = calculate_chunksize(df)
        try:
            with sql_db.engine.connect() as conn:
                cant = df.to_sql(tabla, con = conn,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
        except Exception as e:
            errorMsg = repr(e)
            status = False
            df.to_excel(f'{tabla}_{datetime.now().strftime("%Y%m%d%H%M%S")}.xlsx', index=False)
        return tabla, cant, status ,errorMsg


if __name__ == "__main__":
    pass
