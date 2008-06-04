#!/usr/bin/python
#     encoding: UTF8

# Fichero de reparación de base de datos para PyDBa
import pg             # depends - python-pygresql
import _mysql     # depends - python-mysqldb
        
from pydba_utils import *
from pydba_mtdparser import load_mtd    
from exmlparser import XMLParser

#    *************************** REPAIR DATABASE *****
#    
    
    
def repair_db(options,db=None,mode=0):
    if (not db):
        if (not options.ddb):
            print "RepairDB requiere una base de datos y no proporcionó ninguna."
            return 0
        db=dbconnect(options)
        if (not db): 
            return 0
    
    if (options.verbose):
        print "Inicializando reparación de la base de datos '%s'..." % options.ddb
        print " * Calcular firmas SHA1 de files y metadata"
    
    
    if (options.full):
        qry_modulos=db.query("SELECT idmodulo, nombre, contenido, sha "
                        "FROM flfiles WHERE sha!='' AND nombre NOT LIKE '%%alteredtable%%.mtd'"
                        " ORDER BY idmodulo, nombre");
    else:
        qry_modulos=db.query("SELECT idmodulo, nombre, sha "
                        "FROM flfiles WHERE sha!='' AND nombre NOT LIKE '%%alteredtable%%.mtd'"
                        " ORDER BY idmodulo, nombre");
                                
    modulos=qry_modulos.dictresult() 
    sql=""
    resha1="";
    xmlfiles=("xml","ui","qry","kut","mtd","ts")
    for modulo in modulos:
        xml=None
        if modulo.has_key('contenido'):
            sha1=SHA1(modulo['contenido'])
        else:            
            sha1=modulo['sha']
        
        if (sha1==None):
            print "ERROR: Carácteres no ISO en %s.%s (se omite SHA1)" % (modulo['idmodulo'],modulo['nombre'])
            sha1=modulo['sha']
        
        if f_ext(modulo['nombre']) in xmlfiles and options.full:
            xml=XMLParser()
            xml.parseText(modulo['contenido'])
            if xml.root==None:
                print "ERROR: Failed to parse xml %s.%s" %  (modulo['idmodulo'],modulo['nombre'])
                xml=None
            
        
        resha1=SHA1(resha1+sha1)
        if (modulo['sha']!=sha1):
            print "Updating " + modulo['nombre'] + " => " + sha1 + " ..."
            sql+="UPDATE flfiles SET sha='%s' WHERE nombre='%s';\n" %    (sha1,modulo['nombre'])
        elif (options.debug):
            print modulo['nombre'] + " is ok."
        
        if (f_ext(modulo['nombre'])=="mtd"):
            tabla=modulo['nombre'][:-4]
            qry_modulos=db.query("SELECT xml FROM flmetadata WHERE tabla='%s'" % tabla);
            tablas=qry_modulos.dictresult() 
            TablaCargada=False
            for txml in tablas:
                TablaCargada=True
                if txml['xml']!=sha1:
                    print "Actualizada la Tabla: " + tabla
                    sql+="UPDATE flmetadata SET xml='%s' WHERE tabla='%s';\n" % (sha1,tabla)
            if not TablaCargada:
                    print "Cargando tabla nueva %s ..." % tabla
                    sql+=("INSERT INTO flmetadata (tabla,bloqueo,seq,xml)"
                        " VALUES('%s','f','0','%s');\n" % (tabla,sha1))
            if xml:
                load_mtd(options,db,tabla,xml)
                
        if (len(sql)>1024):
            db.query(sql)
            sql=""
    
    if (len(sql)>0):    
        db.query(sql)
        sql=""
    
    qry_serial=db.query("SELECT sha FROM flserial");
    serials=qry_serial.dictresult() 
    for serial in serials:
        if (serial['sha']==resha1):
            resha1=False
                
    if (resha1):
        if len(serials)>0:
            db.query("UPDATE flserial SET sha='%s';" % (resha1))
            print "Updated flserial => %s." % (resha1)     
        else:        
            db.query("INSERT INTO flserial (serie,sha) VALUES(1,'%s')" % (resha1))
            print "Created flserial => %s." % (resha1)     
    
    
    # **************** COSAS QUE FALTAN POR REPARAR ********************
    
    # Reparar Secuencias de PostgreSQL, comprobar último número
    # Reparar Secuencias de AbanQ Contabilidad
    # Reparar Secuencias de AbanQ Facturacion
    


