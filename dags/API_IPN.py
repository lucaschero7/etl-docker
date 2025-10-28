from datetime import date
from numpy import double
import requests
import pandas as pd
import time
from urllib.parse import urlencode
from typing import Literal
import json

#Exceptions
class AuthenticationException(Exception):
    def __init__(self, message):
        self.message = message
class validationException(Exception):
    def __init__(self, message):
        self.message = message
class NoRecordsException(Exception):
    def __init__(self, message):
        self.message = message

class API_IPN(): 
    def __init__(self, api_key) -> None:
        self.api_key = api_key
        self.baseUrl = 'https://api.ipn.com.ar/'
    def buildUrl(baseUrl: str, url_params: dict) -> str:
        url = baseUrl + '?'
        for key, value in url_params.items():
            if value != None and key != 'self':
                url = url + str(key) + '=' + str(value) + '&'
        url = url[:-1]
        return url
    def checkResponse(response):
        if response.status_code == 200:
            if len(response.json()['Response']['Results']) == 0:
                raise NoRecordsException("No se encontraron registros para los parametros solicitados")
            return
        elif response.status_code == 400:
            raise validationException("")
        elif response.status_code == 401:
            raise AuthenticationException("La autenticacion con el API Key no fue exitosa")
        elif response.status_code == 500:
            raise Exception("Error en el servidor - Error 500")
        elif response.status_code == 502:
            raise Exception("Error en el servidor")

    def apiCall(self, urlRequest:str, params:dict=None) -> dict:
        # Transformar la lista de sucursales a str
        if params is None:
            params = {}
            return requests.get(urlRequest, headers={'Authorization': 'Bearer ' + self.api_key}, params=params)
        
        if 'stores' in params:
            params['stores'] = ','.join(map(str, params['stores']))

        #Transformar date -> string. Formato que solicita la api. dd/mm/aaaa
        for k, v in params.items():
            if isinstance(v, date) and v is not None:
                params[k] = v.strftime("%d/%m/%Y")

        return requests.get(urlRequest, headers={'Authorization': 'Bearer ' + self.api_key}, params=params)
    def postCall(self, urlRequest:str, payload:dict=None) ->dict:
        response = requests.post(urlRequest, data=payload, headers={'Authorization': 'Bearer ' + self.api_key, 'Content-Type': 'application/json-patch+json'})
        return response
    def putCall(self, urlRequest:str, payload:dict=None) -> dict:
        response = requests.put(urlRequest, data=payload, headers={'Authorization': 'Bearer ' + self.api_key, 'Content-Type': 'application/json-patch+json'})
        return response
    def patchCall(self, urlRequest:str) -> dict:
        response = requests.patch(urlRequest, headers={'Authorization': 'Bearer ' + self.api_key})
        return response

    def readResults(results:list) -> list:
        #Cantidad de resultados en la lista
        values = []
        for i in results:
            #Guardo el header en la lista
            values.append(i)
        return values   
    def paginatedApiCall(self, urlBase, params):
        """
        Fetches data from a paginated API.

        Args:
            base_url (str): The base URL of the API.
            params (dict, optional): Parameters to be sent with the API request. Defaults to {}.

        Returns:
            list: A list containing all results retrieved from the API.

        Raises:
            Exception: If there's an error while querying the API.

        Example:
            >>> base_url = 'https://example.com/api'
            >>> parameters = {'param1': 'value1', 'param2': 'value2'}
            >>> results = fetch_paginated_api_data(base_url, parameters)
            >>> print(results)
        """
        
        #Transformar date -> string. Formato que solicita la api. dd/mm/aaaa
        for k, v in params.items():
            if isinstance(v, date) and v is not None:
                params[k] = v.strftime("%d/%m/%Y")

        # Transformar la lista de sucursales a str
        if 'stores' in params:
            params['stores'] = ','.join(map(str, params['stores']))

        all_results = []
        current_page = 0
        Total_records = 0
        while True:
            params['offset'] = current_page
            response = requests.get(urlBase, params=params, headers={'Authorization': 'Bearer ' + self.api_key})
            
            #Analiza el response para ver si hubo algun error
            try:
                API_IPN.checkResponse(response)
            except NoRecordsException:
                break
            
            if current_page == 0:
                Total_records = response.json()['Response']['Total_records']

            data = response.json()['Response']
            page_results = data.get('Results', [])
            all_results.extend(page_results)
            current_page += 1

        return all_results, response.url, Total_records

# Ventas V2
class IPN_SalesDocuments(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto a traves del cual se pueden hacer consultas al endpoint de sales-documents
        --------------------------------------------
        Se requiere un API_KEY
        """
        super().__init__(api_key)
        self.endpoint = 'v2/erp/sales-documents/'
    def getOneSalesDocument(self, sale_id: int, company_id: int, returnDataFrame:bool = False) -> tuple:
        """ Con el sale_id y el company_id, devuelve todos los datos asociados a esa venta.
         
        Devuelve una tupla con:
            3 list:
                1- Encabezado de la venta
                2- Detalle de la venta
                3- Metodos de pago de la venta
            1 str: url a la que se hizo el request
            1 int: cantidad de resultados que devolvio la api.
        --------------------------------------------
        
        Si se indica returnDataframe=True en vez de devolver listas, devuleve DataFrames de Pandas.
            """
        
        # construir la URL a consultar
        urlBase = self.baseUrl + self.endpoint + str(sale_id) + '/'
        urlRequest = API_IPN.buildUrl(urlBase, {'company_id': company_id})

        # hacer un request a la url construida con el api_key en el header
        response = API_IPN.apiCall(self, urlRequest)

        # Analiza el response para ver si hubo algun error
        API_IPN.checkResponse(response)

        #Extraigo los resultados importantes del response
        results = response.json()['Response']['Results']
        Total_records = response.json()['Response']['Total_resultset']

        return results, urlRequest, Total_records
    def getManySalesDocuments(self, company_id: int, date_from:date, date_to:date, stores: list = None,salesman_id:int = None, customer_type:int = None, customer_id:int = None, salespoint_id:int=None, sale_condition_id:int=None, invoice_number:str=None, page_count:int=None):
        """ Devuelve todos los datos asociados a las ventas bajo los parametros indicados.
        OBLIGATORIO: company_id, date_from, date_to. 

        --------------------------------------------

        Devuelve una tupla con:
            list: lista de diccionarios con las ventas.
            str: url a la que se hizo el request.
            int: cantidad de resultados que devolvio la api.
        """

        # Diccionario de parámetros
        params = {k: v for k, v in vars().items() if v is not None and k != 'self'}
                
        #API call
        resultsList, urlRequest, Total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        return resultsList, urlRequest, Total_records

# Clientes V2 --> Pasar a V3
class IPN_Customers(API_IPN):
    def __init__(self,api_key) -> None:
        super().__init__(api_key)
        self.endpoint = 'v2/erp/customers'
    def getOneCustomer(self, company_id: int,customer_id:str, returnDataFrame:bool = False) -> tuple:
        """ 
        Con el id de la venta y el id del cliente, devuelve todos sus datos
        """
        
        # construir la URL a consultar
        urlBase = self.baseUrl + self.endpoint + '/' + str(customer_id) + '/'
        urlRequest = API_IPN.buildUrl(urlBase, {'company_id': company_id})

        # hacer un request a la url construida con el api_key en el header 
        response = API_IPN.apiCall(self, urlRequest)

        # Analiza el response para ver si hubo algun error
        API_IPN.checkResponse(response)
        
        #Creo las listas vacias para guardar los resultados
        customer = []

        #Extraigo los resultados importantes del response
        results = response.json()['Response']['Results']
        Total_resultset = response.json()['Response']['Total_resultset']

        #Guardo los primeros resultados
        customer = IPN_Customers.readResults(results, Total_resultset,customer)

        # si se quiere pide devolver los resultados en dataframe, crea los dataframes y los devuelve en una lista
        #  sino, devulve los diccionarios
        if returnDataFrame == True:
            df_customer = pd.DataFrame(customer)
            return df_customer
        return customer
    def getManyCustomers(self, company_id: int, since_creation_date: date = None, to_creation_date: date = None, since_modification_date: date = None,  to_modification_date: date = None, since_deletion_date: date = None, to_deletion_date: date = None, customer_type_id: int = None, salesman_id: int = None, relation_type_id: int = None, search_term: str = None, last_customer_id: int = None,  customer_id: int = None, returnDataFrame:bool = False):
        
        #Diccionario de parametros
        params = vars()
        params.pop('self')
        params.pop('returnDataFrame')

        #Transformar date -> string. Formato que solicita la api. dd/mm/aaaa
        date_params = [i for i in params if isinstance(params[i], date) and params[i] != None]
        for i in date_params:
           params[i] = params[i].strftime("%d/%m/%Y")

        #Contruir el URL a consultar
        urlBase = self.baseUrl + self.endpoint
        urlRequest = API_IPN.buildUrl(urlBase, params)

        #hacer un request a la url construida con el api_key en el header 
        response = API_IPN.apiCall(self, urlRequest)

        # Analiza el response para ver si hubo algun error
        API_IPN.checkResponse(response)

        #Creo las listas vacias para guardar los resultados
        customers = []

        #Extraigo los resultados importantes del response
        results = response.json()['Response']['Results']
        Total_resultset = response.json()['Response']['Total_resultset']
        Total_pages = response.json()['Response']['Total_pages']
        Total_records = response.json()['Response']['Total_records']

        #Guardo los primeros resultados
        customers = IPN_Customers.readResults(results, Total_resultset, customers)

        if Total_pages > 1:
            for page in range(1,Total_pages):
                params['offset'] = page
                urlRequest = API_IPN.buildUrl(urlBase, params)

                #hacer un request a la url construida con el api_key en el header
                time.sleep(1)
                response = API_IPN.apiCall(self, urlRequest)
                
                # Analiza el response para ver si hubo algun error
                API_IPN.checkResponse(response)

                #Extraigo los resultados importantes del response
                results = response.json()['Response']['Results']
                Total_resultset = response.json()['Response']['Total_resultset']

                #Guardo los resultados
                customers = IPN_Customers.readResults(results, Total_resultset, customers)

        if returnDataFrame == True:
            df_customers = pd.DataFrame(customers)

            return df_customers, urlRequest,Total_records
        return customers,urlRequest,Total_records
    def readResults(results:list, Total_resultset:int, customers:list) -> list:
        #Cantidad de resultados en la lista

        for i in range(0,Total_resultset):
            #Guardo el header en la lista
            customers.append(results[i])

        return customers

# Clientes V3
class IPN_Customers_V3(API_IPN):
    def __init__(self, api_key) -> None:
        super().__init__(api_key)
        self.endpoint = 'v3/erp/customers'
    def getOneCustomer(
        self,
        company_id: int,
        customer_id: str
        ) -> tuple:

        urlBase = self.baseUrl + self.endpoint + '/' + str(customer_id) + '/'
        urlRequest = API_IPN.buildUrl(urlBase, {'company_id': company_id})

        response = API_IPN.apiCall(self, urlRequest)

        API_IPN.checkResponse(response)

        results = response.json()['Response']['Results']


        return results
    
    def getManyCustomers(
        self,
        company_id: int,
        since_creation_date: date = None,
        to_creation_date: date = None,
        since_modification_date: date = None,
        to_modification_date: date = None,
        since_deletion_date: date = None,
        to_deletion_date: date = None,
        customer_type_id: int = None,
        salesman_id: int = None,
        relation_type_id: int = None,
        last_customer_id: int = None,
        search_term: str = None,
        customer_id: int = None,
        acct_executive_id: int = None,
        customer_city: str = None,
        fiscal_state_id: int = None,
        customer_status: int = None,
        condition_type: int = None,
        attr_id_to_filter_str: str = None,
        attr_value_to_filter_str: str = None,
        page_count: int = 250,
        offset: int = None
    ) -> tuple:

        params = {k: v for k, v in vars().items() if v is not None and k != 'self'}

        urlBase = self.baseUrl + self.endpoint

        results, urlRequest, total_records = API_IPN.paginatedApiCall(self, urlBase=urlBase, params=params)

        return results, urlRequest, total_records

# Remitos V2
class IPN_DeliveryNotes(API_IPN):

    def __init__(self,api_key) -> None:
        super().__init__(api_key)
        self.endpoint = 'v2/erp/sale-refer'
    def getDeliveryNotes(    
        self,
        company_id:int,
        date_from:date,
        date_to:date,
        sale_refer_id:int=None,
        source_stores:str=None,
        destination_stores:str=None,
        status_id:int=None,
        salerefer_number:str=None,
        sales_order_number:str=None,
        invoice_number:str=None,
        search_type: Literal[1,2,3,4] = 1,
        salerefer_category_id:int=None,
        customer_name:str=None,
        refer_type_id: Literal[-1,0,1,2] = -1
        ) -> tuple:
        
        # Diccionario de parametros
        params = {k : v for k,v in vars().items() if v is not None and k != 'self' and k != 'returnDataFrame'}

        # Api Call
        resultsList, urlRequest, Total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        # Retornar los resultados
        return resultsList, urlRequest, Total_records
#V3
class IPN_V3_DeliveryNotes(API_IPN):
    def __init__(self,api_key) -> None:
        super().__init__(api_key)
        self.endpoint = 'v3/erp/delivery-notes'
    def getDeliveryNotes(
        self,
        date_from: date = None,
        date_to: date = None,
        source_stores: list = None,
        destination_stores = None,
        status_id: int = None,
        delivery_note_number: str = None,
        sales_order_number: str = None,
        invoice_number: str = None,
        search_type: int = None,
        delivery_note_category_id: int = None,
        customer_name: str = None,
        since_creation_date: date = None,
        to_creation_date: date = None,
        since_modification_date: date = None,
        to_modification_date: date = None,
        since_deletion_date:str=None,
        to_deletion_date:str=None,        
        delivery_note_type_id: int = None
        ) -> tuple:
        """
        Lista el listado de remitos.
        """
        # Diccionario de parámetros
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}
        
        # Realizar llamada paginada
        delivery_notes_list, total_records, url_request = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        return delivery_notes_list, total_records, url_request  
    def getDeliveryNoteTypes(self):
        """
        Lista los tipos de remitos.
        """
        #Solicito los datos a la API
        response = API_IPN.apiCall(self, f"{self.baseUrl}{self.endpoint}/types")
        API_IPN.checkResponse(response)
        results = response.json()['Response']['Results']
        return results
    def postDeliveryNote(self, 
                         sourceStoreId:int,
                         salesPointId:int,
                         destinationStoreId:int,
                         itemsQty: double,
                         itemsAmt: double,
                         items: list,
                         transportId: int,
                         deliveryNoteCategoryId:int = None,
                         itemsGrossWeight: double = None,
                         printComments: bool = None,
                         comments: str = None,
                         ) -> tuple:
        """
        Crea un nuevo remito.
        All items must have valid Id, Description, UnitQty and UnitPrice
        """

        # Diccionario de parámetros
        payload = vars()
        payload.pop('self')
        # Eliminar los parametros que son None
        payload = {k: v for k, v in payload.items() if v is not None}
        payload = json.dumps(payload)

        # Construir la URL a consultar
        urlRequest = self.baseUrl + self.endpoint + "/stock-tracking"
        # Hacer un request a la URL construida con el api_key en el header
        response = API_IPN.postCall(self, urlRequest, payload=payload)
        # Analiza el response para ver si hubo algun error
        API_IPN.checkResponse(response)

        # leer el campo CreatedAt del body
        CreatedAt = response.json()['CreatedAt']

        # consultar la url hasta que el estado del remito sea 1 (Confirmado)
        while True:
            time.sleep(1)
            response = API_IPN.apiCall(self, CreatedAt).json()
            CreatedId = response['CreatedId']
            
            if response['ReturnCode'] == 1:
                break
        
        return CreatedId, urlRequest
    def cancelDeliveryNote(self, delivery_note_id:int) -> tuple:
        """
        Cancela un remito existente.
        """

        # Construir la URL a consultar
        urlRequest = self.baseUrl + self.endpoint + f'/{str(delivery_note_id)}/cancel'
        
        # Hacer un request a la URL construida con el api_key en el header
        response = API_IPN.patchCall(self, urlRequest)
        
        # Analiza el response para ver si hubo algun error
        if response.status_code == 200:
            pass
        elif response.status_code != 200:
            raise Exception("Error al cancelar el remito")
                
        return urlRequest

# Items V3
class IPN_Items(API_IPN):
    """
    Un "artículo" en iPN es la entidad que nuclea los bienes y servicios que una empresa fabrica, distribuye y/o comercializa, 
    siendo bienes tangibles, materias primas, combos o servicios.
    """

    def __init__(self, api_key) -> None:
        """
        Inicializa el objeto IPN_Items para consultar items, familias y categorías
        """
        super().__init__(api_key)
        # Endpoints base
        self.endpoint = 'v3/erp/items'
        
    # ------- MÉTODOS PARA ITEMS -------
    
    def getManyItems(self,
                    item_id:int =None,
                    item_code: str = None, #SKU 
                    family_id: int = None,
                    category_id: int = None,
                    pricelist_id: str = None,
                    provider_id: int = None,
                    trademark_id: int = None,
                    since_creation_date:date = None,
                    to_creation_date:date = None,
                    since_modification_date:date = None,
                    to_modification_date:date = None,
                    since_deletion_date:date = None,
                    to_deletion_date:date = None,
                    since_undeletion_date:date = None,
                    to_undeletion_date:date = None,
                    sort_field:Literal['item_code', 'unit_qty', 'description']=None,
                    sort_dir:Literal['ASC','DESC']=None
                    ):
        """
        Obtiene múltiples items según los criterios de filtrado
        """
        # Diccionario de parámetros
        params = vars()
        params.pop('self')
                
        # Realizar llamada paginada
        items, url_request, total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        return items, url_request, total_records  

    # ------- MÉTODOS PARA COMBOS -------

    def getGroupDetails(self, item_id: int):
        """
        Obtiene detalles de combo para un item específico
        """
        url = f"{self.baseUrl}{self.endpoint}/{str(item_id)}/group-details"
        
        response = API_IPN.apiCall(self, url)
        API_IPN.checkResponse(response)
        
        results = response.json()['Response']['Results']
                
        return results

    # ------- MÉTODOS PARA ATRIBUTOS -------
    def getAttributes(self, item_id: str):
        """
        Lista los atributos registrados en un artículo en cuestión.
        """
        #Solicito los datos a la API
        response = API_IPN.apiCall(self, f"{self.baseUrl}{self.endpoint}/{str(item_id)}/attributes")
        API_IPN.checkResponse(response)
        
        results = response.json()['Response']['Results']

        return results

    # ------- MÉTODOS PARA FAMILIAS -------
    
    def getManyFamilies(self, family_desc: str = None, item_family_id: int = None, page_count: int = None):
        """
        Obtiene múltiples familias según los criterios de filtrado
        """
        params = {
            'family_desc': family_desc,
            'item_family_id': item_family_id,
            'page_count': page_count
        }
        
        # Filtrar parámetros None
        params = {k: v for k, v in params.items() if v is not None}
        
        # Limitar page_count a 250
        if params.get('page_count', 0) > 250:
            params['page_count'] = 250
            
        # Realizar llamada paginada
        families, url_request, total_records = self.paginatedApiCall(
            self.baseUrl + self.endpoint + '/families',
            params
        )
        
        return families, url_request, total_records
    def getOneFamily(self, item_family_id: int):
        """
        Obtiene una familia específica por su ID
        """
        params = {'item_family_id': item_family_id}
        
        # Realizar llamada a la API
        url_base = self.baseUrl + self.endpoint + '/families'
        url_request = API_IPN.buildUrl(url_base, params)
        response = API_IPN.apiCall(self, url_request)
        API_IPN.checkResponse(response)
        
        #Extraer resultados
        results = response.json()['Response']['Results']
        total_resultset = response.json()['Response']['Total_resultset']
        total_records = response.json()['Response']['Total_records']
        
        families = []
        families = self.readResults(results, total_resultset, families)
        
        return families, url_request, total_records
    
    # ------- MÉTODOS PARA CATEGORÍAS -------
    
    def getManyCategories(self, category_desc: str = None, item_category_id: int = None, item_family_id: int = None):
        """
        Obtiene múltiples categorías según los criterios de filtrado
        """
        params = {
            'category_desc': category_desc,
            'item_category_id': item_category_id,
            'item_family_id': item_family_id
        }
        
        # Filtrar parámetros None
        params = {k: v for k, v in params.items() if v is not None}
        
        # Realizar llamada paginada
        categories, url_request, total_records = self.paginatedApiCall(self.baseUrl + self.endpoint + '/categories',params)
        
        return categories, url_request, total_records
    def getOneCategory(self, item_category_id: int):
        """
        Obtiene una categoría específica por su ID
        """
        params = {'item_category_id': item_category_id}
        
        # Realizar llamada a la API
        url_base = self.baseUrl + self.endpoint + '/categories'
        url_request = API_IPN.buildUrl(url_base, params)
        response = API_IPN.apiCall(self, url_request)
        API_IPN.checkResponse(response)
        
        results = response.json()['Response']['Results']
        total_resultset = response.json()['Response']['Total_resultset']
        total_records = response.json()['Response']['Total_records']
        
        categories = []
        categories = self.readResults(results, total_resultset, categories)
        
        return categories, url_request, total_records

    # ------- MÉTODOS PARA MARCAS -------
    
    def getManyTrademarks(self, trademark_name: str = None):
        """
        Lista el listado de marcas.
        """

        # Diccionario de parámetros
        params = vars()
        params.pop('self')
                
        # Realizar llamada paginada
        trademarks, url_request, total_records = self.paginatedApiCall(self.baseUrl + self.endpoint + '/trademarks', params)

        return trademarks, url_request, total_records  
    def getOneTrademark(self, trademark_id:int):
        """
        Obtiene una marca específica por su ID
        """
        #Solicito los datos a la API
        response = API_IPN.apiCall(self, f"{self.baseUrl}{self.endpoint}/trademarks/{str(trademark_id)}")
        API_IPN.checkResponse(response)
        results = response.json()['Response']['Results']
        return results
        
    # ------- MÉTODOS PARA BARCODES -------

    def getOneBarcode(self, item_id: int):
        """
        Lista los códigos de barras registrados en un artículo en cuestión.
        """
        #Solicito los datos a la API
        response = API_IPN.apiCall(self, f"{self.baseUrl}{self.endpoint}/{str(item_id)}/barcodes")
        API_IPN.checkResponse(response)
        
        results = response.json()['Response']['Results']

        return results

    # ------- MÉTODOS PARA PROVEEDORES DEL ARTICULO -------

    def getSuppliers(self, item_id: int, company_id:int=None):
        """
        Lista los proveedores registrados en un artículo en cuestión.
        """
        #Solicito los datos a la API
        response = API_IPN.apiCall(self, f"{self.baseUrl}{self.endpoint}/{str(item_id)}/suppliers", {'company_id':company_id})
        API_IPN.checkResponse(response)
        
        results = response.json()['Response']['Results']

        return results

    # ------- MÉTODOS PARA INVENTARIO DEL ARTICULO -------
    def getInventory(self, 
                     item_code:str,
                     store_id_str: str = None,
                     min_stock: float = None,
                     max_stock: float = None):
        """
        Lista el inventario de todos los artículos.
        """
        # Diccionario de parámetros
        params = vars()
        params.pop('self')

        #Solicito los datos a la API
        items, url_request, total_records = self.paginatedApiCall(self.baseUrl + self.endpoint + "/inventory", params)
        
        return items, url_request, total_records
    def getStoreInventory(self,
                          store_id:int,
                          item_code:str):
        """
        Lista el inventario de un artículo en una sucursal específica.
        """
        # Diccionario de parámetros
        params = vars()
        params.pop('self')
        params.pop('store_id')

        # Solicito los datos a la API
        items, url_request, total_records = self.paginatedApiCall(self.baseUrl + self.endpoint + f"/inventory/{str(store_id)}", params)
        return items, url_request, total_records

    def putStockInStore(self,
                     store_id:int,
                     item_code:str,
                     unit_qty:float,
                     comments:str=None,
                     ):
        """
        Actualiza el stock de un artículo en una sucursal específica.
        /v3/erp/items/inventory/{store_id}

        Recibe una lista de diccionarios en la variable stockList con los siguientes campos:
        - item_code (str): Código del artículo.
        - unit_qty (float): Cantidad a actualizar.
        - comments (str, optional): Comentarios adicionales.
        """
        # Diccionario de parámetros
        payload = vars()
        payload.pop('self')
        payload.pop('store_id')
        payload = json.dumps(payload)

        # Construir la URL a consultar
        urlRequest = self.baseUrl + self.endpoint + f'/inventory/{str(store_id)}'
        
        # Hacer un request a la URL construida con el api_key en el header
        response = API_IPN.putCall(self, urlRequest, payload=payload)
        
        # Analiza el response para ver si hubo algun error
        API_IPN.checkResponse(response)
        
        # Extraigo los resultados importantes del response
        results = response.json()['Response']['Results']
        Total_records = response.json()['Response']['Total_records']
        
        return results, urlRequest, Total_records
    def putItemStockInStore(self,
                            item_id: int,
                            store_id: int,
                            unit_qty: float,
                            item_code: str = None,
                            comments: str = None
                            ):
        """
        Ajuste de existencias de inventario de un artículo en una ubicación en cuestión.
        /v3/erp/items/inventory/{store_id}/{item_id}
        """
        # Diccionario de parámetros
        payload = vars()
        payload.pop('self')
        payload.pop('item_id')
        payload.pop('store_id')
        payload = json.dumps(payload)

        # Construir la URL a consultar
        urlRequest = self.baseUrl + self.endpoint + '/' + str(item_id) + '/inventory/' + str(store_id)

        # Hacer un request a la URL construida con el api_key en el header
        response = API_IPN.putCall(self, urlRequest, payload=payload)

        # Analiza el response para ver si hubo algun error
        API_IPN.checkResponse(response)

        # Extraigo los resultados importantes del response
        results = response.json()['Response']['Results']
        Total_records = response.json()['Response']['Total_records']

        return results, urlRequest, Total_records

# B2C V2
class IPN_B2C_orders(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto IPN_B2C_orders para poder consultar las operaciones de B2C de la API de IPN.
        """
        super().__init__(api_key)
        self.endpoint = 'v2/b2c/orders/'
        self.urlBase = self.baseUrl + self.endpoint        
    def readResults(results:list, Total_resultset:int, orders:list) -> list:
        #Cantidad de resultados en la lista

        for i in range(0,Total_resultset):
            #Guardo el header en la lista
            orders.append(results[i])

        return orders     
    def getB2cOrders(self, 
                     store_id:int,
                     since_creation_date:date=None,
                     to_creation_date:date=None,
                     since_modification_date:date=None,
                     to_modification_date:date=None,
                     since_paid_date:date=None,
                     to_paid_date:date=None,
                     since_shipping_date:date=None,
                     to_shipping_date:date=None,
                     status_id:int=None,
                     payment_status_id:int=None,
                     shipping_status_id:int=None,
                     marketplace_id:int=None,
                     order_id:int=None):
        #Diccionario de parametros
        params = vars()
        params.pop('self')
           
        #API call
        resultsList, Total_records = self.paginatedApiCall(self.urlBase, params)

        return resultsList,Total_records

# Gastos V3
class IPN_Expenses(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto IPN_Expenses para poder consultar los gastos desde la API de IPN.
        """
        super().__init__(api_key)
        self.endpoint = 'v3/erp/expenses'
    def getManyExpenses(    
        self,
        company_id:int=None,
        date_from:str=None,
        date_to:str=None,
        stores:list=None,
        search_type:int=None,
        is_deleted:bool=None,
        payment_status:int=None,
        not_profit:bool=None,
        expense_concept:int=None,
        sub_expense_concept:int=None,
        company_name:str=None,
        invoice_types:str=None,
        since_creation_date: str = None,
        to_creation_date: str = None,
        since_modification_date:str=None,
        to_modification_date:str=None,
        since_deletion_date:str=None,
        to_deletion_date:str=None
        ) -> tuple:
        
        # Diccionario de parametros
        local_vars = locals().copy()
        params = {k: v for k, v in local_vars.items() 
                if k != 'self' and v is not None}

        # Api Call
        resultsList, urlRequest, Total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        # Retornar los resultados
        return resultsList, urlRequest, Total_records
    def getOneExpense(self, 
        expense_id:int
        ) -> tuple:
        """ 
        Con el id del gasto, devuelve todos sus datos
        """
        
        # construir la URL a consultar
        urlRequest = self.baseUrl + self.endpoint + '/' + str(expense_id)

        # hacer un request a la url construida con el api_key en el header 
        response = API_IPN.apiCall(self, urlRequest)

        # # Analiza el response para ver si hubo algun error
        # API_IPN.checkResponse(response)
        
        #Extraigo los resultados importantes del response
        results = response.json()['Response']['Result']
        return results

class IPN_ItemsPricelists(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto IPN_ItemsPricelists para poder consultar los cambios de precios desde la API de IPN.
        """
        super().__init__(api_key)
        self.endpoint = 'v3/erp/item-pricelists/{}/price-logs'

    def getManyPriceLogs(
        self,
        date_from:date,
        date_to:date,
        price_list_id:int = None,
        item_family_id:int=None,
        item_category_id:int=None,
        trademark_id:int=None,
        provider_id:int=None,
        show_distinct_item:bool=None,
        ) -> tuple:
        
        # Diccionario de parametros
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}

        # quitar el price_list_id del diccionario de parametros (Path Param)
        price_list_id = params.pop('price_list_id')

        # Api Call
        resultsList, urlRequest, Total_records = self.paginatedApiCall(self.baseUrl + self.endpoint.format(price_list_id), params)

        # Retornar los resultados
        return resultsList, urlRequest, Total_records

class IPN_ItemsCost(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto IPN_ItemsCost para poder consultar los costos de los items desde la API de IPN.
        """
        super().__init__(api_key)
        self.endpoint = 'v3/erp/items/cost-logs'
    def getManyItemsCost(
        self,
        date_from:date,
        date_to:date,
        item_family_id: int = None,
        item_category_id: int = None,
        trademark_id: int = None,
        provider_id: int = None,
        ) -> tuple:
        
        # Diccionario de parametros
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}

        # Api Call
        resultsList, urlRequest, Total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        # Retornar los resultados
        return resultsList, urlRequest, Total_records

class IPN_GoodsReceipts(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto IPN_GoodsReceipts para poder consultar los remitos de ingreso.
        """
        super().__init__(api_key)
        self.endpoint = 'v3/erp/goods-receipts'
    def getManyGoodsReceipts(    
        self,
        stores:list,
        date_from:date= None,
        date_to:date= None,
        date_type_to_filter:int= None,
        sort_order: int = None,
        provider_id:int=None,
        pending_invoice:bool=None,
        since_creation_date: date = None,
        to_creation_date: date = None,
        since_modification_date: date = None,
        to_modification_date: date = None,
        since_deletion_date: date = None,
        to_deletion_date: date = None
        ) -> tuple:
        
        # Diccionario de parametros
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}

        # Api Call
        resultsList, urlRequest, Total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        # Retornar los resultados
        return resultsList, urlRequest, Total_records
    def getOneGoodsReceipts(self,goods_receipt_id:int):
        # construir la URL a consultar
        urlRequest = self.baseUrl + self.endpoint + '/' + str(goods_receipt_id)

        # hacer un request a la url construida con el api_key en el header 
        response = API_IPN.apiCall(self, urlRequest)

        # # Analiza el response para ver si hubo algun error
        # API_IPN.checkResponse(response)
        
        #Extraigo los resultados importantes del response
        results = response.json()['Response']['Result']
        return results

class IPN_Suppliers(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto IPN_Suppliers para poder consultar los proveedores desde la API de IPN.
        """
        super().__init__(api_key)
        self.endpoint = 'v3/erp/suppliers'
    def getManySuppliers(
        self,
        supplier_name:str=None,
        company_name:str=None,
        tax_number:str=None,
        supplier_code:str=None,
        supplier_type_id:int=None,
        supplier_sub_type_id:int=None,
        company_id: int = None,
        is_deleted:bool=None,
        ):
        """
        Lista el listado de proveedores.
        """
        # Diccionario de parámetros
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}
        # Realizar llamada paginada
        suppliers, url_request, total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        return suppliers, url_request, total_records
    def getOneSupplier(self, supplier_id:int):
        """
        Obtiene un proveedor por su ID
        """
        # construir la URL a consultar
        urlRequest = self.baseUrl + self.endpoint + '/' + str(supplier_id)

        # hacer un request a la url construida con el api_key en el header 
        response = API_IPN.apiCall(self, urlRequest)

        API_IPN.checkResponse(response)
        
        #Extraigo los resultados importantes del response
        results = response.json()['response']['result']

        return results

# se puede agregar a items
class IPN_Attributes(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto IPN_V3_Attributes para poder consultar los atributos desde la API de IPN.
        """
        super().__init__(api_key)
        self.endpoint = 'v3/erp/items/attributes'
    def getManyAttributes(self, attribute_category_id: int=None):
        """
        Lista el listado de atributos.
        """
        # Diccionario de parámetros
        params = vars()
        params.pop('self')
        # Realizar llamada paginada
        attributes, url_request, total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        return attributes, url_request, total_records
class IPN_CategoriesAttributes(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto IPN_V3_CategoriesAttributes para poder consultar los atributos de las categorias desde la API de IPN.
        """
        super().__init__(api_key)
        self.endpoint = 'v3/erp/items/attributes/categories'
    def getCategoriesAttributes(self):
        """
        Lista el listado de atributos de las categorias.
        """
        # Realizar llamada paginada
        result = self.apiCall(self.baseUrl + self.endpoint)
        result = result.json()
        categories_attributes_list = result['Response']['Results']
        total_records = result['Response']['Total_records']
        url_request = self.baseUrl + self.endpoint

        return categories_attributes_list, total_records, url_request
class IPN_TypesCategoriesAttributes(API_IPN):
    def __init__(self,api_key) -> None:
        """
        Crea el objeto IPN_V3_TypesCategoriesAttributes para poder consultar los tipos de atributos de las categorias desde la API de IPN.
        """
        super().__init__(api_key)
        self.endpoint = 'v3/erp/items/attributes/types'
    def getTypesAttributes(self):
        """
        Lista el listado de tipos de atributos.
        """
        # Realizar llamada paginada
        result = self.apiCall(self.baseUrl + self.endpoint)
        result = result.json()
        types_attributes_list = result['Response']['Results']
        total_records = result['Response']['Total_records']
        url_request = self.baseUrl + self.endpoint

        return types_attributes_list, total_records, url_request

class IPN_PurchaseOrders(API_IPN):
    def __init__(self,api_key) -> None:
        super().__init__(api_key)
        self.endpoint = 'v3/erp/purchase-orders'
    def getPurchaseOrders(
        self,
        company_id: int = None,
        purchase_order_number: str = None,
        date_from: date = None,
        date_to: date = None,
        since_creation_date: date = None,
        to_creation_date: date = None,
        since_modification_date: date = None,
        to_modification_date: date = None,
        deliver_date_from: date = None,
        deliver_date_to: date = None,
        is_deleted: bool = None,
        store_id: int = None,
        deliver_store_id: int = None,
        supplier_id: int = None,
        supplier_type_id: int = None,
        sub_supplier_type_id: int = None,
        buyer_employee_id: int = None,
        item_code: str = None,
        purchase_order_type_id: int = None,
        status_id: int = None, # 13 = Prescrita
        approval_status: int = None,
    ) -> tuple:
        """
        Lista el listado de ordenes de compra.
        """
        
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}
        resultsList, urlRequest, Total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)

        # Retornar los resultados
        return resultsList, urlRequest, Total_records

# se pueden agregar a suppliers
class IPN_Suppliers_Sub_Types(API_IPN):
    def __init__(self,api_key) -> None:
        super().__init__(api_key)
        self.endpoint = 'v3/erp/suppliers/types'
    def getManySuppliersSubTypes(
        self,
        supplier_type_id:int,
        supplier_sub_type_name: str = None,
        ):
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}
        result = self.apiCall(F"{self.baseUrl}{self.endpoint}/{supplier_type_id}/sub-types")
        return result
class IPN_Suppliers_Types(API_IPN):
    def __init__(self,api_key) -> None:
        super().__init__(api_key)
        self.endpoint = 'v3/erp/suppliers/types'
    def getManySuppliersTypes(self):
        result = self.apiCall(self.baseUrl + self.endpoint)
        return result
    
class IPN_Sales_Orders(API_IPN):
    def __init__(self,api_key) -> None:
        super().__init__(api_key)
        self.endpoint = 'v3/erp/sales-orders'
    def getOneSalesOrder(
        self, 
        sales_order_id: int, 
        store_id: int
        ):
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}
        result = self.apiCall(self.baseUrl + self.endpoint + '/' + str(sales_order_id), params)
        return result
    def getManySalesOrders(self, 
        store_id: int,
        sales_order_id: int = None,
        sales_order_guid: str = None,
        customer_id: int = None,
        customer_name: str = None,
        salesorder_number: str = None,
        status_id: int = None,
        date_from: date = None,
        date_to: date = None,
        type_id: int = 5, # -> Por defecto 5: Nota de venta a cliente
        salesman_id: int = None,
        approval_status: int = None,
        fulfillment_store_id: int = None,
        last_order_id: int = None,
        include_details: bool = False,
        include_payments: bool = False,
        show_deleted: bool = False,
        search_type: int = None,
        invoice_number_chr: str = None,
        page_count: int = None,
        offset: int = None,
        ):
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}
        result, url_request, total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)
        return result, url_request, total_records

class IPN_Employees(API_IPN):
    def __init__(self,api_key) -> None:
        super().__init__(api_key)
        self.endpoint = 'v3/erp/employees'
    def getManyEmployees(self, stores: list = None):
        params = {k : v for k,v in vars().items() if v is not None and k != 'self'}
        result, url_request, total_records = self.paginatedApiCall(self.baseUrl + self.endpoint, params)
        return result, url_request, total_records


if __name__ == "__main__":
    pass