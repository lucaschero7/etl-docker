import math
from Conectores_BD import *
import os
import pandas as pd
from sqlalchemy import text
from typing import Literal
import cryptography
from cryptography.fernet import Fernet
import json
import base64
import hashlib

# Password Encryp
passw = 'CotyData2025'

# Funciones Reutilizables Generales

def get_apikey():

    password = passw
    key = get_key_from_password(password)
    cipher = Fernet(key)
    
    # Obtener el directorio donde está este archivo .py
    current_dir = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(current_dir, 'credentials-encrypted.json')
    
    with open(credentials_path, 'rb') as f:
        encrypt_data = f.read()
        try:
            decrypted_data = cipher.decrypt(encrypt_data)
            decrypted_data2 = decrypted_data.decode('utf-8')
            decrypted_data = json.loads(decrypted_data2)
            apiKey = decrypted_data['apikey']
        except Exception as e:
            print(f"Error:{e}")
            return None
    return apiKey

# Funcion que calcula el chunksize
def calculate_chunksize(df):
    chunksz = math.floor((2100 / len(df.columns)))
    if (2100 % chunksz) == 0:
        chunksz = chunksz - 1
    if chunksz > 1000:
        chunksz = 1000
    return chunksz

# Obtener conector engine
def get_conector_engine(DataBase:Literal['CotyData','CotyApp','Local', 'Cams']):
    # poner print para cada paso
    print(f"Obteniendo credenciales para la base de datos: {DataBase}")
    credenciales = get_credencials(DataBase)
    engine = connectors(**credenciales)
    engine = engine.engine()
    print(f"Conector engine creado para la base de datos: {engine}")
    return engine

# Obtener conector cnxn
def get_conector_cnxn(DataBase:Literal['CotyData','CotyApp','Local', 'Cams']):
    credenciales = get_credencials(DataBase)
    cnxn = connectors(**credenciales)
    cnxn = cnxn.cnxn()
    return cnxn

# Funcion que obtiene ambos conectores
def get_conectors(DataBase:Literal['CotyData','CotyApp','Local', 'Cams']):
    engine = get_conector_engine(DataBase)
    cnxn = get_conector_cnxn(DataBase)
    return engine, cnxn

def select_query_df(DataBase:Literal['CotyData','CotyApp','Local', 'Cams'], query:str = ""):
    credencials = get_credencials(DataBase)
    sql = sqlDb(**credencials)
    response = sql.selectQuery(query)
    df = pd.DataFrame(response)
    return df

def get_key_from_password(password):
    """Genera una clave de encriptación a partir de una contraseña sin sal"""
    # Usamos SHA-256 para generar un hash de 32 bytes a partir de la contraseña
    hash_object = hashlib.sha256(password.encode())
    key = base64.urlsafe_b64encode(hash_object.digest())
    return key

def get_credencials(namedb:Literal['CotyData','CotyApp','Camaras','Local']):
    password = passw
    key = get_key_from_password(password)
    cipher = Fernet(key)
    
    # Obtener ruta dinámica del archivo
    current_dir = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(current_dir, 'credentials-encrypted.json')
    
    # Leer el archivo encriptado
    with open(credentials_path, 'rb') as f:
        encrypted_data = f.read()
    
    # Desencriptar el contenido
    try:
        decrypted_data = cipher.decrypt(encrypted_data)
        # se convierte el 'ARRAY BYTE'a str y despues a json o dict
        decrypted_data2 = decrypted_data.decode('utf-8')
        decrypted_data = json.loads(decrypted_data2)
        namedb = namedb.lower()
        credenciales = {
            'server': decrypted_data[f'database-{namedb}']['host'],
            'database': decrypted_data[f'database-{namedb}']['name'],
            'userID': decrypted_data[f'database-{namedb}']['user'],
            'password': decrypted_data[f'database-{namedb}']['password'],
            'trustedConnection': decrypted_data[f'database-{namedb}']['trust'],
            'dbType': decrypted_data[f'database-{namedb}']['type'],
            'hostType': decrypted_data[f'database-{namedb}']['hostType'],
            }
        return credenciales
    except Exception as e:
        print(f"Error al obtener las credenciales: {e}")
        return None

def encrypt_env_file(input_file, output_file, password):
    """Encripta un archivo .env usando una contraseña simple"""
    # Generar clave a partir de la contraseña
    key = get_key_from_password(password)
    cipher = Fernet(key)
    
    # Leer el contenido del archivo .env
    with open(input_file, 'rb') as f:
        data = f.read()
    
    # Encriptar el contenido
    encrypted_data = cipher.encrypt(data)
    
    # Guardar el contenido encriptado
    with open(output_file, 'wb') as f:
        f.write(encrypted_data)
    
    print(f"Archivo {input_file} encriptado exitosamente como {output_file}")

def decrypt_env_file(input_file, output_file, password):
    """Desencripta un archivo .env encriptado usando la misma contraseña"""
    # Generar clave a partir de la contraseña
    key = get_key_from_password(password)
    cipher = Fernet(key)
    
    # Leer el archivo encriptado
    with open(input_file, 'rb') as f:
        encrypted_data = f.read()
    
    # Desencriptar el contenido
    try:
        decrypted_data = cipher.decrypt(encrypted_data)
        
        # Guardar el contenido desencriptado
        with open(output_file, 'wb') as f:
            f.write(decrypted_data)
        
        print(f"Archivo {input_file} desencriptado exitosamente como {output_file}")
        return True
    except Exception as e:
        print(f"Error al desencriptar: {e}")
        return False
    
def get_list_stores():
    df = select_query_df(DataBase='CotyData', query="SELECT CODIGO_SUCURSAL FROM SUCURSALES WHERE VENTA_SUCURSAL = 1")
    return df['CODIGO_SUCURSAL'].astype(int).tolist()

def get_date_range():
    from datetime import date, timedelta
    today = date.today()
    today_weekday = today.weekday()
    
    if today_weekday == 0:
        date_from = today - timedelta(days=3)
        date_to = today - timedelta(days=1)
    else:
        date_from = today - timedelta(days=1)
        date_to = date_from
    
    return date_from, date_to

if __name__ == "__main__":
    # test = encrypt_env_file("credenciales.json", "credentials-encrypted.json", passw)
    # print(test)
    
    # test2 = decrypt_env_file("credentials-encrypted.json", "credenciales.env", passw)
    # print(test2)
    pass