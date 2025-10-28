import pandas as pd
from Conectores_BD import *
from utils import *
import math

sql_db = sqlDb(**get_credencials('CotyData'))
my_sql = sqlDb(**get_credencials('CotyApp'))

class Funciones_CotyData:
        # Funciones para limpiado de datos
    def get_id_precios_lista(df, nombre_campo):
        #Cambio string lista precio por su codigo
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT ID_PRECIOS_LISTA, PRECIOS_LISTA_DESCRIPCION FROM PRECIOS_LISTA")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos = {}
        for values in QWRY:
            codigos.update({values[1]:values[0]}) 
            
        #replace values from a pandas dataframe with keys from a dictionary
        df['ID_PRECIOS_LISTA'] = df[nombre_campo].map(codigos)
        del df[nombre_campo]

        return df
    def get_codigo_sucursal(df, nombre_campo):
        #Cambio string sucursal por su codigo de sucursal y razon social
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT DESCRIPCION_SUCURSAL, CODIGO_SUCURSAL, ID_RAZON_SOCIAL FROM SUCURSALES")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos_sucu = {}
        codigos_rz = {}
        for values in QWRY:
            codigos_sucu.update({values[0]:values[1]}) 
            codigos_rz.update({values[1]:values[2]}) 
            
        #replace values from a pandas dataframe with keys from a dictionary
        df['CODIGO_SUCURSAL'] = df[nombre_campo].map(codigos_sucu)
        df['ID_RAZON_SOCIAL'] = df['CODIGO_SUCURSAL'].map(codigos_rz)

        return df
    def mysql_get_codigo_sucursal(df):
        #Creo el cursor
        cnxn = my_sql.cnxn
         
        #Cambio string sucursal por su codigo de sucursal y razon social
        
        #Busco codigos
        with cnxn.cursor() as cursor:
            cursor.execute("SELECT SUCURSAL_DESCRIPCION AS DESCRIPCION_SUCURSAL, ID_SUCURSAL AS CODIGO_SUCURSAL, ID_RAZON_SOCIAL FROM SUCURSALES")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos_sucu = {}
        codigos_rz = {}

        for values in QWRY:
            codigos_sucu.update({values[0]:values[1]})
            codigos_rz.update({values[1]:values[2]})
        
        #Asigno valores segun descripcion en el dataframe
        for x, y in codigos_sucu.items():
            df.loc[df['Sucursal'] == x, 'CODIGO_SUCURSAL'] = y
        df = df.astype({"CODIGO_SUCURSAL": int})
        for x, y in codigos_rz.items():
            df.loc[df['CODIGO_SUCURSAL'] == x, 'ID_RAZON_SOCIAL'] = y
        df = df.astype({"ID_RAZON_SOCIAL": int})

        return df
    def get_codigo_razon_social_CC_codigo_sucursal(df):
        #Agrego el codigo de razon social desde el codigo de sucursal
        
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT CODIGO_SUCURSAL, ID_RAZON_SOCIAL FROM SUCURSALES")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos_rz = {}

        for values in QWRY:
            codigos_rz.update({values[0]:values[1]})
        
        for x, y in codigos_rz.items():
            df.loc[df['CODIGO_SUCURSAL'] == x, 'ID_RAZON_SOCIAL'] = y
        df = df.astype({"ID_RAZON_SOCIAL": int})

        return df
    def get_id_comprobante_tipo(df, nombre_campo):
        #Cambio string lista precio por su codigo
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT ID_COMPROBANTE_TIPO, DESCRIPCION_COMPROBANTE_TIPO FROM COMPROBANTE_TIPO")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos = {}
        for values in QWRY:
            codigos.update({values[1]:values[0]}) 

        #replace values from a pandas dataframe with keys from a dictionary
        df['ID_COMPROBANTE_TIPO'] = df[nombre_campo].map(codigos)
        
        del df[nombre_campo]
        
        return df
    def get_codigo_sucursal_desde_PV(df):
        #Cambio string sucursal por su codigo de sucursal y razon social
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT CODIGO_PUNTO_VENTA, CODIGO_SUCURSAL, ID_RAZON_SOCIAL FROM PUNTOS_VENTA")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos_sucu = {}
        codigos_rz = {}

        for values in QWRY:
            codigos_sucu.update({values[0]:values[1]})
            codigos_rz.update({values[1]:values[2]})
        
        #Asigno valores segun descripcion en el dataframe
        for x, y in codigos_sucu.items():
            df.loc[df['CODIGO_PUNTO_VENTA'] == x, 'CODIGO_SUCURSAL'] = y
        df = df.astype({"CODIGO_SUCURSAL": int})
        for x, y in codigos_rz.items():
            df.loc[df['CODIGO_SUCURSAL'] == x, 'ID_RAZON_SOCIAL'] = y
        df = df.astype({"ID_RAZON_SOCIAL": int})

        return df
    def get_codigo_comprador(df):
        #Cambio string lista precio por su codigo
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT CODIGO_COMPRADOR, DESCRIPCION_COMPRADOR FROM COMPRADORES")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos = {}
        for values in QWRY:
            codigos.update({values[0]:values[1]}) 
        
        #Asigno valores segun descripcion en el dataframe
        for x, y in codigos.items():
            df.loc[df['Comprador'] == y, 'CODIGO_COMPRADOR'] = x
        df = df.astype({"CODIGO_COMPRADOR": int})

        return df
    def correct_client_code(df,col_name): #IPN saca en el repo de ventas los codigos de cliente sin los 0's al inicio (tres 0's)
        #Indices de filas que NO contienen la palabra 'CLI'
        m = ~df[col_name].str.contains('CLI')
        df.loc[m, col_name] = ('000' + df.loc[m, col_name])
        return df
    def get_codigo_razon_social(df, nombre_campo):
        #Cambio string sucursal por su codigo de sucursal y razon social
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT ID_RAZON_SOCIAL, RAZON_SOCIAL_DESCRIPCION FROM RAZON_SOCIAL")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos = {}
        for values in QWRY:
            codigos.update({values[1]:values[0]}) 
            
        #replace values from a pandas dataframe with keys from a dictionary
        df['ID_RAZON_SOCIAL'] = df[nombre_campo].map(codigos)
        del df[nombre_campo]
        
        return df
    def get_codigo_razon_social_str(razonSocialStr):
        #Cambio string sucursal por su codigo de sucursal y razon social
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT ID_RAZON_SOCIAL, RAZON_SOCIAL_DESCRIPCION FROM RAZON_SOCIAL")
            QWRY = cursor.fetchall()

        #busco cual es el ID_RAZON_SOCIAL que corresponde a la razon social que me pasan
        for values in QWRY:
            if values[1] == razonSocialStr:
                return values[0]

        return ''   
    def get_codigo_proveedor(df):

        #Busco codigos
        sql = ("SELECT CODIGO_PROVEEDOR, RAZON_SOCIAL_PROVEEDOR, ID_RAZON_SOCIAL FROM PROVEEDORES")
        df_proveedores = pd.DataFrame(sql_db.engine.connect().execute(text(sql)))
        

        #Merge de ambos dataframes
        df_merged = pd.merge(df, df_proveedores,  how='inner', left_on=['Proveedor','ID_RAZON_SOCIAL'], right_on = ['RAZON_SOCIAL_PROVEEDOR','ID_RAZON_SOCIAL'])

        return df_merged
    def get_codigo_proveedor_from_nombre_fantasia(df, ColProv, ColRZ):

        #Busco codigos
        sql = ("SELECT CODIGO_PROVEEDOR, NOMBRE_FANTASIA_PROVEEDOR, ID_RAZON_SOCIAL FROM PROVEEDORES")
        df_proveedores = pd.DataFrame(sql_db.engine.connect().execute(text(sql)))
        #Merge de ambos dataframes
        df_merged = pd.merge(df, df_proveedores,  how='inner', left_on=[ColProv,ColRZ], right_on = ['NOMBRE_FANTASIA_PROVEEDOR','ID_RAZON_SOCIAL'])

        return df_merged
    def get_rz_from_oc_prefix(df, colOC):
        df['ID_RAZON_SOCIAL'] = df[colOC].str[:4]
        df['ID_RAZON_SOCIAL'] = df['ID_RAZON_SOCIAL'].replace(['0001'], 1)
        df['ID_RAZON_SOCIAL'] = df['ID_RAZON_SOCIAL'].replace(['0014'], 2)

        return df
    def get_id_metodo_pago(df, ColName):
        #Busco codigos
        sql = ("SELECT ID_METODO_PAGO, METODO_PAGO_DESCRIPCION FROM METODOS_PAGO")
        df_metodos_pago = pd.DataFrame(sql_db.engine.connect().execute(text(sql)))

        #Merge de ambos dataframes
        df_merged = pd.merge(df, df_metodos_pago,  how='inner', left_on=[ColName], right_on = ['METODO_PAGO_DESCRIPCION'])

        return df_merged
    def get_codigo_proveedor_por_descripcion(df, rs):
        #Busco codigos
        sql = ("SELECT CODIGO_PROVEEDOR, RAZON_SOCIAL_PROVEEDOR FROM PROVEEDORES WHERE ID_RAZON_SOCIAL = " + str(rs))
        df_proveedores = pd.DataFrame(sql_db.engine.connect().execute(text(sql)))

        #Merge de ambos dataframes
        df_merged = pd.merge(df, df_proveedores,  how='inner', left_on=['Proveedor'], right_on = ['RAZON_SOCIAL_PROVEEDOR'])

        return df_merged
    def get_codigo_tipo_y_sub_tipo_proveedor(df):
        #Busco codigos
       
        sql = ("SELECT TIPO_PROVEEDOR_DESCRIPCION, SUB_TIPO_PROVEEDOR_DESCRIPCION, PROVEEDOR_SUB_TIPO.ID_TIPO_PROVEEDOR, ID_SUB_TIPO_PROVEEDOR FROM PROVEEDOR_TIPO INNER JOIN PROVEEDOR_SUB_TIPO ON PROVEEDOR_TIPO.ID_TIPO_PROVEEDOR = PROVEEDOR_SUB_TIPO.ID_TIPO_PROVEEDOR")
        
        df_proveedores = pd.DataFrame(sql_db.engine.connect().execute(text(sql)))

        #transformar la columna 'Sub-tipo de proveedor' en pd object
        df['Sub-tipo de proveedor'] = df['Sub-tipo de proveedor'].astype(str)

        #Merge de ambos dataframes
        df_merged = pd.merge(df, df_proveedores,  how='left', left_on=['Tipo de proveedor','Sub-tipo de proveedor'], right_on = ['TIPO_PROVEEDOR_DESCRIPCION','SUB_TIPO_PROVEEDOR_DESCRIPCION'])

        return df_merged
    def get_codigo_tipo_gasto(df, nombre_campo):
        #Cambio string tipo de gasto por su codigo
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT TIPO_GASTO_DESCRIPCION, ID_TIPO_GASTO FROM GASTO_TIPOS")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos = {}
        for values in QWRY:
            codigos.update({values[0]:values[1]}) 
            
        #replace values from a pandas dataframe with keys from a dictionary
        df['ID_TIPO_GASTO'] = df[nombre_campo].map(codigos)

        return df  
    def get_codigo_sub_tipo_gasto(df, nombre_campo_id_tipo,nombre_campo_sub_tipo):
        #Cambio string sub tipo de gasto por su codigo
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT CONCAT(SUB_TIPO_GASTO_DESCRIPCION,'-',ID_TIPO_GASTO) AS SUB_TIPO_GASTO, ID_SUB_TIPO_GASTO FROM GASTO_SUB_TIPOS")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos = {}
        for values in QWRY:
            codigos.update({values[0]:values[1]}) 
        
            
        #Concat values from 2 columns from a data frame with a separator
        df['ID_SUB_TIPO_GASTO'] = df[nombre_campo_sub_tipo] + '-' + df[nombre_campo_id_tipo].astype(str)

        #replace values from a pandas dataframe with keys from a dictionary
        df['ID_SUB_TIPO_GASTO'] = df['ID_SUB_TIPO_GASTO'].map(codigos)

        return df
    def get_codigo_familia_and_categoria(df,campo_familia, campo_categoria):

        #Busco codigos
        sql = ("SELECT DESCRIPCION_FAMILIA,DESCRIPCION_CATEGORIA,ARTICULO_FAMILIA.CODIGO_FAMILIA,CODIGO_CATEGORIA FROM ARTICULO_FAMILIA INNER JOIN ARTICULO_CATEGORIA ON ARTICULO_FAMILIA.CODIGO_FAMILIA = ARTICULO_CATEGORIA.CODIGO_FAMILIA")
        df_fam_cat = pd.DataFrame(sql_db.engine.connect().execute(text(sql)))
        #Merge de ambos dataframes
        df_merged = pd.merge(df, df_fam_cat,  how='inner', left_on=[campo_familia,campo_categoria], right_on = ['DESCRIPCION_FAMILIA','DESCRIPCION_CATEGORIA'])

        del df_merged['DESCRIPCION_FAMILIA']
        del df_merged['DESCRIPCION_CATEGORIA']
        del df_merged[campo_familia]
        del df_merged[campo_categoria]

        return df_merged
    def get_codigo_marca(df,campo_marca):
        #Busco codigos
        sql = ("SELECT CODIGO_MARCA, DESCRIPCION_MARCA FROM MARCAS")
        df_marcas = pd.DataFrame(sql_db.engine.connect().execute(text(sql)))
        #Merge de ambos dataframes
        df_merged = pd.merge(df, df_marcas,  how='inner', left_on=[campo_marca], right_on = ['DESCRIPCION_MARCA'])
        del df_merged['DESCRIPCION_MARCA']
        del df_merged[campo_marca]
        return df_merged
    def update_remitos_compra_detalle_cod_proveedor(df):
        #crear un df con la columna 'NUMERO_REMITO_COMPRA' donde el ID_RAZON_SOCIAL es = 2 y la 'Sucursal Stock' = '01-DF CENTRAL'
        df2 = pd.DataFrame()
        df2[['NUMERO_REMITO_COMPRA','CODIGO_PROVEEDOR']] = df.loc[(df['ID_RAZON_SOCIAL'] == 2) & (df['Sucursal stock'] == '01-DF CENTRAL'),['# Remito','CODIGO_PROVEEDOR']]
        df2.drop_duplicates(inplace=True)
        #Creo el cursor
        cnxn = sql_db.engine.raw_connection()

        for index, row in df2.iterrows():
            sql = ("""  UPDATE REMITOS_COMPRA
                        SET CODIGO_PROVEEDOR +=1
                        WHERE NUMERO_REMITO_COMPRA = '{}' AND CODIGO_PROVEEDOR = {}""").format(row[0],(row[1]-1))
            with cnxn.cursor() as cursor:
                cursor.execute(sql)
                cnxn.commit()
    def get_codigo_categoria_remito_movimiento(df, nombre_campo):
        #Cambio string sucursal por su codigo de sucursal y razon social
        
        #Busco codigos
        with sql_db.cnxn.cursor() as cursor:
            cursor.execute("SELECT ID_CATEGORIA_REMITO_MOVIMIENTO,DESCRIPCION_CATEGORIA_REMITO_MOVIMIENTO FROM REMITOS_MOVIMIENTOS_CATEGORIAS")
            QWRY = cursor.fetchall()

        #Diccionario con los codigos
        codigos= {}
        for values in QWRY:
            codigos.update({values[1]:values[0]}) 
        
        #Merge de ambos dataframes
        df = df.replace(to_replace=codigos)
        return df         
class Funciones_CotyData_Reportes:
    def Gastos_Sin_Asignar_Crear_Reporte():

        sql = ("SELECT * FROM GASTOS_SIN_ASIGNAR")
        df1 = pd.DataFrame(sql_db.engine.connect().execute(text(sql)))

        sql2 = ("SELECT DESCRIPCION_SUCURSAL FROM SUCURSALES")
        df2 = pd.DataFrame(sql_db.engine.connect().execute(text(sql2)))
        
        Funciones_Excel.Crear_Excel_2DFs(df1,df2)
    def Gastos_Sin_Asignar_Procesar_Asignacion(file_name):
        #Read columns CODIGO_GASTO and Sucursal from excel and create dataframe
        df = pd.read_excel(file_name, sheet_name='Gastos', usecols=['CODIGO_GASTO','Sucursal'])
        df = Funciones_CotyData.get_codigo_sucursal(df,'Sucursal')

        #Remove column 'Sucursal' from dataframe
        del df['Sucursal']
        
        #Delete rows with na on column "CODIGO_SUCURSAL"
        df = df.dropna(subset=['CODIGO_SUCURSAL'])

        #Carga datos a SQL
        tabla = 'ACTUALIZACION_GASTOS_OPERATIVOS'
        chunksz = math.floor((2100 / len(df.columns)))
        if (2100 % chunksz) == 0:
            chunksz = chunksz -1

        try:
            with sql_db.cnxn.cursor() as cursor:
                cursor.execute('STAGING_TABLE_GASTOS_SIN_ASIGNAR')
                sql_db.cnxn.commit()                
                df.to_sql(tabla, con = sql_db.engine,  if_exists = 'append' ,index = False, method='multi', chunksize = chunksz)
                cursor.execute('SINCRONIZACION_GASTOS_SIN_ASIGNAR')
                sql_db.cnxn.commit()
        finally:
            sql_db.cnxn.close()