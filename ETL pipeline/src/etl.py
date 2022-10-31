#import needed libraries

#-- librearias conexion DB
from sqlalchemy import create_engine 
import pyodbc 
import csv
import psycopg2
import pathlib

import pandas as pd
# this is imported from config folder

import os
from os import remove

#get password from environmnet var
pwd = "postgres"
uid = "postgres"
server = "localhost"
db = "postgres"
port = "5438"
#---
#os.path.abspath(os.getcwd())
dir = r'/Users/napo/opt/data'
dir_payments = r'/Users/napo/opt/data/payments'
dir_charges = r'/Users/napo/opt/data/chargebacks'
#---
a_eliminar = []

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
                    
        # iterate over files in the directory
        for filename in os.listdir(directory):
            #get filename without ext
            file_wo_ext = os.path.splitext(filename)[0]
            # only process excel files
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
                    """""
                    os.unlink(f)
                    print("Se elimino el archivo: " + filename + " despues de procesar.")"""
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
        df.to_sql(f"stg_{tbl}", engine, if_exists='append', index=False) #creado nueva tabla con prefijo stg >>staging
        #df.to_sql(tbl, engine, if_exists='replace', index=False)  #cargar en tabla >> resultado no esperado sobre escribe tipo de datos
        rows_imported += len(df)

        # add elapsed time to final print out
        print("Data importada exitosamente!")
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


#main
try:
    #call extract function
    print("#---------1. PAYMENTS ----------#")
    df = extract(dir_payments)
    print("#--------END - PAYMENTS --------#")
    print("")
    print("")
    print("#---------2. CHARGEBACKS ----------#")
    df = extract(dir_charges)
    print("#-------END - CHARGEBACKS ---------#")
   
    #remove(dir_charges)
    #df = listDir()
except Exception as e:
    print("Error mientras extracción de datos: " + str(e)) 