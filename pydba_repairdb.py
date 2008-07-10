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
    
    
def repair_db(options,ddb=None,mode=0,odb=None):
    if (not ddb):
        if (not options.ddb):
            print "RepairDB requiere una base de datos de destino y no proporcionó ninguna."
            return 0
        ddb=dbconnect(options)
        if (not ddb): 
            return 0
    
    if (not odb):
        if (not options.odb):
            print "RepairDB requiere una base de datos de destino y no proporcionó ninguna."
            return 0
        odb=odbconnect(options)
        if (not odb): 
            return 0
        
    where=""
    if not options.full :
        if len(options.files_loaded)>0:
            where+=" AND nombre IN ('" + "','".join(options.files_loaded) + "')"
          
        if len(options.modules)>0:
            where+=" AND ( 0=1 "
            for modname,module in options.modules.iteritems():
                noloadtable=[]
                loadtable=[]
                for tablename, value in module.iteritems():
                    if value==False:
                        noloadtable.append(tablename + ".mtd")
                    else:
                        loadtable.append(tablename + ".mtd")
                
                try:
                    default=module['_default_']
                except:
                    default=False
                
                if default==True:
                    where+=" OR ( idmodulo = '%s' AND nombre NOT IN ('" % modname + "','".join(noloadtable) + "'))" 
                else:
                    where+=" OR ( idmodulo = '%s' AND nombre IN ('" % modname + "','".join(loadtable) + "'))" 
            where+=")"
                       
        if options.odb!=options.ddb:
            where+=" AND nombre LIKE '%.mtd'"
        
    if (options.verbose):
        print "Inicializando reparación de la base de datos '%s'..." % options.ddb
        print " * Calcular firmas SHA1 de files y metadata"
    
    
    qry_omodulos=odb.query("SELECT sha " +
            "FROM flfiles WHERE sha!='' AND nombre NOT LIKE '%%alteredtable%%.mtd' ORDER BY sha");
    ofiles=[]
    for row in qry_omodulos.dictresult():
        ofiles.append(row['sha'])
    
    qry_dmodulos=ddb.query("SELECT sha " +
            "FROM flfiles WHERE sha!='' AND nombre NOT LIKE '%%alteredtable%%.mtd' ORDER BY sha");
    dfiles=[]
    for row in qry_dmodulos.dictresult():
        if row['sha'] in ofiles:
            ofiles.remove(row['sha'])
        else:
            dfiles.append(row['sha'])
    
    # Eliminar los ficheros sobrantes.
    qry_dmodulos=ddb.query("DELETE FROM flfiles WHERE sha IN ('" + "','".join(dfiles) + "')")
    
    # Obtener los ficheros nuevos
    qry_omodulos=odb.query("SELECT * FROM flfiles WHERE sha IN ('" + "','".join(ofiles) + "')")
    
    # Insertarlos en la nueva DB.
    for row in qry_omodulos.dictresult():
        fields=row.keys()
        values=[]
        for field in fields:
            campo=row[field]
            if (campo is not None):#Si el valor es nulo
                values.append("'" + pg.escape_string(str(campo)) + "'")
            else:
                values.append("NULL")
        try:
            qry_dmodulos=ddb.query("DELETE FROM flfiles WHERE nombre ='" + row['nombre'] + "'")
            
            sql="INSERT INTO flfiles (" + ",".join(fields) + ") VALUES(" + ",".join(values) + ")"
            ddb.query(sql)
        except:
            print sql
            raise
        
        
    
    
    qry_modulos=ddb.query("SELECT idmodulo, nombre, contenido, sha " +
                    "FROM flfiles WHERE sha!='' AND nombre NOT LIKE '%%alteredtable%%.mtd' "
                    + where + " ORDER BY idmodulo, nombre");
                                
    modulos=qry_modulos.dictresult() 
    sql=""
    resha1="";
    xmlfiles=("xml","ui","qry","kut","mtd","ts")
    ficheros_actualizados=0
    for modulo in modulos:
        xml=None
        if options.full and modulo.has_key('contenido'):
            sha1=SHA1(modulo['contenido'])
        else:            
            sha1=modulo['sha']
        
        if (sha1==None):
            print "ERROR: Carácteres no ISO en %s.%s (se omite SHA1)" % (modulo['idmodulo'],modulo['nombre'])
            sha1=modulo['sha']
        
        if f_ext(modulo['nombre']) in xmlfiles:
            xml=XMLParser()
            xml.parseText(modulo['contenido'])
            if xml.root==None:
                print "ERROR: Failed to parse xml %s.%s" %  (modulo['idmodulo'],modulo['nombre'])
                xml=None
            
        
        resha1=SHA1(resha1+sha1)
        if (modulo['sha']!=sha1):
            ficheros_actualizados+=1
            print "Updating " + modulo['nombre'] + " => " + sha1 + " ..."
            sql+="UPDATE flfiles SET sha='%s' WHERE nombre='%s';\n" %    (sha1,modulo['nombre'])
        elif (options.debug):
            print modulo['nombre'] + " is ok."
        
        if (f_ext(modulo['nombre'])=="mtd"):
            tabla=modulo['nombre'][:-4]
            qry_modulos=ddb.query("SELECT xml FROM flmetadata WHERE tabla='%s'" % tabla);
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
                load_mtd(options,odb,ddb,tabla,xml)
                
        if (len(sql)>1024):
            ddb.query(sql)
            sql=""
    
    if (len(sql)>0):    
        ddb.query(sql)
        sql=""
    
    qry_serial=ddb.query("SELECT sha FROM flserial");
    serials=qry_serial.dictresult() 
    for serial in serials:
        if (serial['sha']==resha1):
            resha1=False
                
    if (resha1):
        if len(serials)>0 and ficheros_actualizados>0:
            ddb.query("UPDATE flserial SET sha='%s';" % (resha1))
            print "Updated flserial => %s." % (resha1)     
        else:        
            ddb.query("INSERT INTO flserial (serie,sha) VALUES(1,'%s')" % (resha1))
            print "Created flserial => %s." % (resha1)     
    
