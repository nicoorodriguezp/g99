#import  libraries necesarios

#-- librearias conexion DB
from sqlalchemy import create_engine 
import pyodbc 
import csv
import psycopg2
import pathlib
import time

import pandas as pd
# this is imported from config folder

import os
from os import remove

#obtener password de variables de ambiente
pwd = "postgres"
uid = "postgres"
server = "localhost"
db = "postgres"
port = "5438"

#---
#os.path.abspath(os.getcwd())
dir = r'/Users/napo/Documents/proyecto/Big Data/BigData'
dir_payments = r'/Users/napo/Documents/proyecto/Big Data/BigData/data/payments'
dir_charges = r'/Users/napo/Documents/proyecto/Big Data/BigData/data/chargebacks'
#---
a_eliminar = []
flag_dbStaging = 0

def listDir():
    path = pathlib.Path(dir)
    carpeta = [carpeta.name for carpeta in path.iterdir() if carpeta.is_dir()]
    print(carpeta)

#extraer datos desde archivos
def extract(path):
    try:
        # starting directory
        directory = path
        headCharge = ['chargebacks_id','payment_date','notification_date','debit_date','currency_code','token_customer','amount','is_fraud']
        headPayment = ['payment_id','currency_code','gateway_code','payment_method','payment_date','token_customer','is_credit','is_debit','amount']
        
        if directory == dir_payments:
             tbl = "payments"
             headname = headPayment
        else:
            if directory == dir_charges:
             tbl = "chargebacks"
             headname = headCharge
            else: print("No existe las carpetas de datos ")
                    
        # itera con todo los archivos del directorio
        for filename in os.listdir(directory):
            #get filename without ext
            file_wo_ext = os.path.splitext(filename)[0]
            # only process csv files
            if filename.endswith(".csv"):
                f = os.path.join(directory, filename)
                # checking if it is a file
                if os.path.isfile(f):

                    #se tiene que agregar el seperador porque no es la coma
                    df = pd.read_csv(f,sep = ';', names = headname)
                    print (df)
            
                    print("#Cargando archvo: " + filename)
                    
                    # call to load
                    #load(df, file_wo_ext) //carga datos en la tabla con nombre del archivo
                    load(df, tbl)
                    
                    os.unlink(f)
                    print("Se elimino el archivo: " + filename + " despues de procesar.")
    except Exception as e:
        print("Error mientras extracción de datos: " + str(e))

#cargar datos a  postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
        #engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5438/postgres')
        print(f'importar fila {rows_imported} a {rows_imported + len(df)}... ')
        
        # save df to postgres
        #--se crea nueva tabla con prefijo stg_ >> staging
        df.to_sql(f"stg_{tbl}", engine, if_exists='append', index=False) 
        #df.to_sql(tbl, engine, if_exists='replace', index=False)  >> resultado no esperado sobre escribe tipo de datos
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data importada exitosamente!")
        global flag_dbStaging 
        flag_dbStaging = 1
    except Exception as e:
        print("Carga de Datos con error: " + str(e))

def remove(path):
    try:
        directory = path 
        for filename in os.listdir(directory):
           if filename.endswith(".csv"):
                f = os.path.join(directory, filename)
                # checking if it is a file
                if os.path.isfile(f):
                    os.unlink(f)
                    a_eliminar.append(filename) 
        print("los siguiente archivos se eliminaron exitosamente luego de procesar: ")  
        print(a_eliminar)  
    except OSError as e:
        print("Error mientras se eliminó los archivos: " + str(e.strerror))

def transform_toCube():
    cur = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
    conn = cur.connect()
    print("[1/7] - tranforma los valores 0 que tiene la tabla en un valor default")
    cur.execute("update stg_chargebacks set debit_date = 19000101 where debit_date = 0;")

    # CREAR DIMENSION DE TIEMPO
    print("[2/7] - CREAR DIMENSION DE TIEMPO")
    cur.execute("CREATE TABLE IF NOT EXISTS tiempo_dim (id_tiempo SERIAL PRIMARY KEY, fecha date,anio int8,mes int8,dia int8);")
    
    
    #CREAR DIMENSION DE DIVISA CON LOS TIPOS DE MONEDAS
    print("[3/7] - CREAR DIMENSION DE DIVISA CON LOS TIPOS DE MONEDAS")
    cur.execute("CREATE TABLE IF NOT EXISTS divisa_dim (id_currency SERIAL PRIMARY KEY,currency_code varchar(3));")
    
    #CREAR DIMENSION CON LOS GATEWAY
    print("[4/7] - CREAR DIMENSION CON LOS GATEWAY")
    cur.execute("CREATE TABLE IF NOT EXISTS gateway_dim (id_gateway SERIAL PRIMARY KEY,gateway_code varchar(20));")
   
    #CREAR DIMENSION TIPO DE PAGO
    print("[5/7] - CREAR DIMENSION TIPO DE PAGO")
    cur.execute("CREATE TABLE IF NOT EXISTS metodoPago_dim (id_paymentMethod SERIAL PRIMARY KEY, payment_method varchar(20));")
    
    #CREAR FACT TABLE DE PAGOS }
    print("[6/7] - CREAR FACT TABLE DE PAGOS")
    cur.execute("CREATE TABLE IF NOT EXISTS Fact_payments (payment_id SERIAL PRIMARY KEY, currency_code int8, gateway_code int8,payment_method int8,payment_date int8,token_customer VARCHAR(50),is_credit VARCHAR(1) NOT NULL,is_debit VARCHAR(1) NOT NULL,amount float8,FOREIGN KEY(payment_date) REFERENCES tiempo_dim (id_tiempo),FOREIGN KEY(currency_code) REFERENCES divisa_dim (id_currency),FOREIGN KEY (gateway_code) REFERENCES gateway_dim (id_gateway),FOREIGN KEY (payment_method) REFERENCES metodoPago_dim (id_paymentMethod));")
    
    #CREAR FACT TABLE DE CHARGEBACKS
    print("[7/7] - CREAR FACT TABLE DE CHARGEBACKS")
    cur.execute("CREATE TABLE IF NOT EXISTS Fact_chargebacks (chargebacks_id SERIAL PRIMARY KEY, payment_date int8,notification_date int8, debit_date int8,currency_code int8,token_customer VARCHAR(50),amount float8,is_fraud VARCHAR(1) NOT null,FOREIGN KEY(payment_date) REFERENCES tiempo_dim (id_tiempo),FOREIGN KEY(notification_date) REFERENCES tiempo_dim (id_tiempo),FOREIGN KEY(debit_date) REFERENCES tiempo_dim (id_tiempo),FOREIGN KEY(currency_code) REFERENCES divisa_dim (id_currency));")
    
    conn.close()
    cur.dispose()
    
def load_Cube():
    cur = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
    conn = cur.connect()
    
     # Cargar DIMENSION DE TIEMPO
    print("[1/6] - CARGA DATOS EN DIMENSION DE TIEMPO")
    cur.execute("insert into tiempo_dim (fecha) select distinct to_date(c.payment_date::text, 'YYYYMMDD') from stg_chargebacks c union select distinct to_date(c.notification_date::text, 'YYYYMMDD') from stg_chargebacks c union select distinct to_date(c.debit_date::text, 'YYYYMMDD') from stg_chargebacks c where  c.debit_date <> 0 -- la tabla puede tener valores en 0 union select distinct to_date(p.payment_date :: text, 'YYYYMMDD')  from stg_payments p;")
    cur.execute("update tiempo_dim set anio = date_part('year',fecha);")
    cur.execute("update tiempo_dim set mes = date_part('month',fecha);")
    cur.execute("update tiempo_dim set dia = date_part('day',fecha);")

     #Cargar DIMENSION DE DIVISA CON LOS TIPOS DE MONEDAS
    print("[2/6] - CARGA DATOS EN DIMENSION DE DIVISA CON LOS TIPOS DE MONEDAS")
    cur.execute("insert into divisa_dim (currency_code) select distinct c.currency_code from stg_chargebacks c union select distinct p.currency_code from stg_payments p where p.gateway_code is not null;")

    #Cargar DIMENSION CON LOS GATEWAY
    print("[3/6] - CARGA DATOS EN DIMENSION CON LOS GATEWAY")
    cur.execute("insert into gateway_dim (gateway_code) select distinct p.gateway_code from public.stg_payments p where p.gateway_code is not null; ")

    #Cargar DIMENSION TIPO DE PAGO
    print("[4/6] - CARGA DATOS EN DIMENSION TIPO DE PAGO")
    cur.execute("insert into metodoPago_dim (payment_method) select distinct p.payment_method from public.stg_payments p where p.payment_method is not null; ")
    
    #Cargar FACT TABLE DE PAGOS }
    print("[5/6] - CARGA DATOS EN FACT TABLE DE PAGOS")
    cur.execute("insert into fact_payments (payment_date, token_customer, is_credit, is_debit, amount, currency_code ,gateway_code,payment_method)select td.id_tiempo, p.token_customer, p.is_credit, p.is_debit , p.amount,case when p.currency_code = 'BRL' then 01 when p.currency_code = 'UYP' then 02 when p.currency_code = 'ARS' then 03 end, case when p.gateway_code = 'SLOPE_PROVIDER' then 1 when p.gateway_code = 'SPREEDLY_PAYU' then 2 when p.gateway_code = 'SPREEDLY_ADYEN' then 3 when p.gateway_code = 'WOMPI' then 4 when p.gateway_code = 'ADYEN' then 5 when p.gateway_code = 'KALA' then 6  when p.gateway_code = 'GOD-PAY' then 7 when p.gateway_code = 'WALLET' then 8  when p.gateway_code = 'PAYMENT' then 9 end, case  when p.payment_method = 'ADJUSTMENT' then 1 when p.payment_method = 'TRANSFER' then 2 when p.payment_method = 'TRANSFER_LINK' then 3 when p.payment_method = 'CREDIT' then 4 when p.payment_method = 'BONO' then 5 when p.payment_method = 'CASH' then 6 when p.payment_method = 'REFUND' then 7 when p.payment_method = 'SMART_LINK' then 8 when p.payment_method = 'CREDIT_CARD' then 9 when p.payment_method = 'BOLETO_BANCARIO' then 10 when p.payment_method = 'LENDING' then 11 when p.payment_method = 'ANNULMENT' then 12 end from stg_payments p inner join tiempo_dim td on (td.fecha = to_date(p.payment_date::text, 'YYYYMMDD'));")
    
    #Cargar FACT TABLE DE CHARGEBACKS
    print("[6/6] - CARGA DATOS EN FACT TABLE DE CHARGEBACKS")
    cur.execute("insert into fact_chargebacks (payment_date ,token_customer,amount,is_fraud,currency_code)select td.id_tiempo,c.token_customer, c.amount, c.is_fraud,case when c.currency_code = 'BRL' then 01 when c.currency_code = 'UYP' then 02 when c.currency_code = 'ARS' then 03 end from stg_chargebacks c inner join tiempo_dim td on (td.fecha = to_date(c.payment_date::text, 'YYYYMMDD'));")
    cur.execute("update fact_chargebacks set debit_date  = td.id_tiempo from stg_chargebacks sc inner join tiempo_dim td on (td.fecha = to_date(sc.debit_date ::text, 'YYYYMMDD'));")

    conn.close()
    cur.dispose()

#main
try: 
    t_start = time.time()
    #call extract function
    print("Grupo 99 ....>> runing ETL")
    print("(E)XTRACT DATA")
    print("  begin Extract >> PAYMENTS")
    df = extract(dir_payments)
    print("  >>end Extract PAYMENTS..")
    print("")
    print("  begin Extract >> CHARGEBACKS")
    df = extract(dir_charges)
    print("  >>end Extract CHARGEBACKS..")
    print(">>END - EXTRACT-DATA")
    print("")

    #remove(dir_charges)
    #df = listDir()}
    if flag_dbStaging == 1:
     print("(T)RANSFORM DATA")
     transform_toCube()
     print(">>END - TRANSFORM DATA")
     print("")
     print ("(L)OAD DATA ")
     load_Cube()
     print (">>END - LOAD DATA ")
     t_end = time.time()
     print("")
     print("El tiempo total de ejecucion del ETL es: ", end=" ")
     print("  ",t_end - t_start, "Segundos")

except Exception as e:
    print("Error mientras extracción de datos: " + str(e)) 