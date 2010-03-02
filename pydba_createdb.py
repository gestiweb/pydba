#!/usr/bin/python
#   encoding: UTF8

# Fichero de creación de base de datos para PyDBa
import pg       # depends - python-pygresql
import _mysql   # depends - python-mysqldb

from pydba_utils import *

import os, os.path, traceback

        
def open_createSQL():
    dirname = os.path.dirname(__file__)
    filename = "newdatabase_1.sql"
    
    fullfilename = os.path.join(dirname,filename)
    
    f1 = open(fullfilename)
    filetext = f1.read()
    f1.close()
    return filetext

    
def create_db(options):
    print "Creando Base de datos %s ..." % options.ddb
    ddb=options.ddb
    options.ddb="postgres"
    db=dbconnect(options)
    options.ddb=ddb
    try:
        db.query("CREATE DATABASE %s WITH TEMPLATE = template0 ENCODING = 'UTF8';" % options.ddb)
    except:
        print "Fallo al crear la base de datos %s. Se asume que ya está creada y se continúa." % ddb
    db.close
    db=dbconnect(options)
    createsql = open_createSQL()
    createsql_s  = createsql.split("\n\n--\n")
    errores = 0
    for sql in createsql_s: 
        try:
            db.query(sql)
        except ValueError, x:
            if str(x) != 'empty query.': raise
        except:
            print traceback.format_exc(0)
            errores +=1
            
    if errores:
        print "Hubieron %d errores creando la base de datos, probablemente ya estuviera previamente creada." % errores
        
        
    return db
    
    
