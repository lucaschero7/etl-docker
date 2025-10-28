#IMPORTS
from typing import Literal
from sqlalchemy import create_engine, text
import pyodbc

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import ReadPreference
import certifi

from urllib.parse import quote_plus
import urllib

import math
import pandas as pd

class connectors:
    def __init__(self, server:str, database:str, userID:str, password:str, trustedConnection:str = Literal['yes','no'] ,dbType:str = Literal['MySQL','SQLServer'], hostType:str = Literal['Local','Cloud']) -> None:
        self.server = server
        self.database = database
        self.userID = userID
        self.password = password
        self.dbType = dbType
        self.hostType = hostType
        self.trustedConnection = trustedConnection
    def engine(self):
        if self.trustedConnection == "yes":
            params = urllib.parse.quote_plus(
                'DRIVER={ODBC Driver 18 for SQL Server};'
                'SERVER='+self.server + ';'
                'DATABASE='+self.database + ';'
                'Trusted_Connection='+ self.trustedConnection +';'
                'TrustServerCertificate=yes;'
                'ENCRYPT=yes;'
                'ConnectRetryCount=3;'
                'ConnectRetryInterval=10;'
                'Connection Timeout=60;'
                )
        elif self.userID != "":
            params = urllib.parse.quote_plus(
                'DRIVER={ODBC Driver 18 for SQL Server};'
                'SERVER='+self.server + ';'
                'DATABASE='+self.database + ';'
                'UID='+self.userID + ';'
                'PWD='+ self.password + ';'
                'TrustServerCertificate=yes;'
                'ENCRYPT=yes;'
                'ConnectRetryCount=3;'
                'ConnectRetryInterval=10;'
                'Connection Timeout=60;'
                )

        if self.dbType == 'MySQL':
            # Usar urllib.parse.quote_plus para manejar caracteres especiales en la contraseña
            password_encoded = urllib.parse.quote_plus(self.password)
            engine = create_engine(f'mysql+pymysql://{self.userID}:{password_encoded}@{self.server}/{self.database}') 
            return engine

        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, pool_pre_ping=True,pool_recycle=1800)
        return engine
    def cnxn(self):
        if self.dbType == 'MySQL':
            import pymysql
            password_encoded = urllib.parse.quote_plus(self.password)
            connection = pymysql.connect(
                host=self.server,
                user=self.userID,
                password=self.password,
                database=self.database
            )
            return connection
        elif self.trustedConnection == "no":
            cnxn = pyodbc.connect(
                'DRIVER={ODBC Driver 18 for SQL Server};'
                'SERVER='+self.server+';'
                'DATABASE='+self.database+';'
                'UID='+ self.userID +';'
                'PWD=' + self.password +';'
                'Trusted_Connection=no;'
                'ENCRYPT=yes;'
                'ConnectRetryCount=3;'
                'ConnectRetryInterval=10;'
                'Connection Timeout=60;'
                )
        elif self.trustedConnection == "yes":
            cnxn = pyodbc.connect(
                'DRIVER={ODBC Driver 18 for SQL Server};'
                'SERVER='+self.server + ';'
                'DATABASE='+self.database + ';'
                'Trusted_Connection=yes;'
                'TrustServerCertificate=yes;'
                'ENCRYPT=yes;'
                'ConnectRetryCount=3;'
                'ConnectRetryInterval=10;'
                'Connection Timeout=60;'
                )

        return cnxn  
class sqlDb(connectors):
    def __init__(self, server:str, database:str, userID:str, password:str, trustedConnection:str = Literal['yes','no'] ,dbType:str = Literal['MySQL','SQLServer'], hostType:str = Literal['Local','Cloud']) -> None:
        super().__init__(server=server, database=database, userID=userID, password=password, trustedConnection=trustedConnection, dbType=dbType, hostType=hostType)
        self.engine = self.engine()
        # Inicializar cnxn para todos los tipos de base de datos
        self.cnxn = self.cnxn()
    def InsertDataFrame(self,df:pd.DataFrame, table:str, chunksz:int=None, method:str='multi'):
        #Carga datos a SQL
        if chunksz == None:
            chunksz = math.floor((2100 / len(df.columns)))
            if (2100 % chunksz) == 0:
                chunksz = chunksz -1

        rows_affected = df.to_sql(table, con = self.engine,  if_exists = 'append' ,index = False, method=method, chunksize = chunksz)
        return rows_affected
    def executeProcedure(self, Procedure_name:str):
        if self.dbType == 'MySQL':
            return 'Procedure not supported in MySQL'
        else: 
            try:
                with self.cnxn.cursor() as cursor:
                    cursor.execute(Procedure_name)
                    self.cnxn.commit()                
            finally:
                pass
    def selectQuery(self, query):
        connection  = self.engine.connect()
        try:
            result = connection.execute(text(query))
            result = result.fetchall()
        finally:
            pass
        return result
    def insertQuery(self, query, data = None):
        try:
            with self.cnxn.cursor() as cursor:
                if data == None:
                    cursor.execute(query)
                else:
                    cursor.execute(query, data)
                result = cursor.fetchall()                
        except pyodbc.ProgrammingError:
            result = cursor.rowcount
        finally:
            self.cnxn.commit()
        return result
        
#crear una excepcion personalizada de deleteProtecionException
class deleteProtecionException(Exception):
    """Exception raised when delete query is DELETE ALL."""
    def __init__(self):
        self.message = 'Query can not be empty.'
        super().__init__(self.message) 
class mongoDB():
    """
    Clase para conectarse a una base de datos de MongoDB.
    Con la URI de conexion y el usuario y contraseña mas la base de datos y la coleccion se puede conectar a la base de datos.
    
    # Operaciones soportadas:
        - Insertar un documento.
        - Insertar varios documentos.
        - Actualizar un documento.
        - Actualizar varios documentos.
        - Eliminar documentos segun un filtro.
        - Consultar documentos segun un filtro.
        - Testear y cerrar conexion.
    """
    
    def __init__(self, password, url, database, collection):
        
        password = quote_plus(password)
        uri = url.format(password)
        
        # Create a new client and connect to the server
        ca = certifi.where()
        self.client = MongoClient(uri, server_api=ServerApi('1'), read_preference = ReadPreference.SECONDARY_PREFERRED, tlsCAFile=ca)
        self.db = self.client[database]
        self.collection = self.db[collection]

    #insert
    def insertOneDocument(self, data):
        object_id = self.collection.insert_one(data).inserted_id
        return object_id
    def insertManyDocuments(self, data):

        object_ids = self.collection.insert_many(data).inserted_ids
        return object_ids

    #delete
    def deleteManyDocuments(self, query:dict, deleteProtection:bool = True):
        
        if deleteProtection == True:
            #si el query no tiene filtro, no se ejecuta
            if query == {}:
                raise deleteProtecionException()

        deleted_count = self.collection.delete_many(query).deleted_count
        return deleted_count

    #update
    def update_many(self, query, data): 
        newvalues = {"$set": data}
        modified_count = self.collection.update_many(query, newvalues).modified_count
        return modified_count
    def update_one(self, query, data):
        newvalues = {"$set": data}
        modified_count = self.collection.update_one(query, newvalues).modified_count
        return modified_count
    
    #search
    def findDoc(self, query:dict={}):
        result = self.collection.find(query)
        return result

    #General Purpose Functions
    def closeConnection(self):
        self.client.close()   
    def test_conection(self):
        try:
            # Send a ping to confirm a successful connection
            self.client.test
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
            
if __name__ == "__main__":
    pass
