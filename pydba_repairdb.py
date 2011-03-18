#!/usr/bin/python
#     encoding: UTF8

# Fichero de reparación de base de datos para PyDBa
import pg             # depends - python-pygresql
import _mysql     # depends - python-mysqldb
import sys
        
from pydba_utils import *
from pydba_mtdparser import load_mtd    
from exmlparser import XMLParser
import traceback
import pydba_loadpgsql
#    *************************** REPAIR DATABASE *****
#    
    
    
def repair_db(options,ddb=None,mode=0,odb=None):
    if (options.verbose):
        print "-> RepairDB"
    if (not ddb):
        if (not options.ddb):
            print "RepairDB requiere una base de datos de destino y no proporcionó ninguna."
            return 0
        ddb=dbconnect(options)
        if (not ddb): 
            return 0
    
    if (not odb):
        if (not options.odb):
            print "RepairDB requiere una base de datos de origen y no proporcionó ninguna."
            return 0
        odb=odbconnect(options)
        if (not odb): 
            return 0
        
    where=""
    if not options.full :
        #if len(options.files_loaded)>0:
        where+=" AND (nombre IN ('" + "','".join(options.files_loaded) + "' ) OR nombre LIKE '%.mtd')"
          
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
            where+=" OR nombre LIKE '%.mtd' )"
                       
        if options.odb!=options.ddb:
            where+=" AND nombre LIKE '%.mtd'"
        
    if (options.verbose):
        print "Inicializando reparación de la base de datos '%s'..." % options.ddb
        print " * Calcular firmas SHA1 de files y metadata"
    
    if options.transactions: 
        odb.query("BEGIN;");
        try:
            lltables = "flfiles,flserial,flmetadata".split(",")
            for ltable in lltables:
                sql = "LOCK %s NOWAIT;" % ltable
                if (options.verbose): print sql
                odb.query(sql);
                if (options.verbose): print "done."
        except:
            print "Error al bloquear la tabla %s , ¡algun otro usuario está conectado!" % ltable
            odb.query("ROLLBACK;");
            raise
            
       
    pydba_loadpgsql.process_drop(options,ddb)
    

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
                values.append("(E'" + pg.escape_string(str(campo)) + "')")
            else:
                values.append("NULL")
        try:
            qry_dmodulos=ddb.query("DELETE FROM flfiles WHERE nombre ='" + row['nombre'] + "'")
            
            sql="INSERT INTO flfiles (" + ",".join(fields) + ") VALUES(" + ",".join(values) + ")"
            ddb.query(sql)
        except:
            print sql
            raise
        
        
    
    sqlModulos = ("SELECT idmodulo, nombre, contenido, sha " +
                    "FROM flfiles WHERE sha!='' AND nombre NOT LIKE '%%alteredtable%%.mtd' "
                    + where + " ORDER BY idmodulo, nombre")
    # print sqlModulos
    qry_modulos=ddb.query(sqlModulos);
                                
    modulos=qry_modulos.dictresult() 
    # print "%d resultados." % len(modulos)
    sql=""
    resha1="";
    xmlfiles=("xml","ui","qry","kut","mtd","ts")
    ficheros_actualizados=0
    for modulo in modulos:
        if options.loadbaselec:
            if modulo['nombre'] != 'baselec.mtd': 
                continue
        xml=None
        if options.full and modulo.has_key('contenido'):
            sha1=SHA1(modulo['contenido'])
        else:            
            sha1=modulo['sha']
        
        if (sha1==None):
            print "ERROR: Carácteres no ISO en %s.%s (se omite SHA1)" % (modulo['idmodulo'],modulo['nombre'])
            sha1=modulo['sha']
        
        if f_ext(modulo['nombre']) in xmlfiles:
            xml=XMLParser("%s.%s" % (modulo['idmodulo'],modulo['nombre']))
            xml.parseText(modulo['contenido'])
            if xml.root==None:
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
            sql_update_metadata = ""
            for txml in tablas:
                TablaCargada=True
                if txml['xml']!=sha1 or options.full:
                    sql_update_metadata="UPDATE flmetadata SET xml='%s' WHERE tabla='%s';\n" % (sha1,tabla)
            if not TablaCargada:
                    print "Cargando tabla nueva %s ..." % tabla
                    sql_update_metadata=("INSERT INTO flmetadata (tabla,bloqueo,seq,xml)"
                        " VALUES('%s','f','0','%s');\n" % (tabla,sha1) )
            if xml:
                if options.loadbaselec and not sql_update_metadata:
                    sql_update_metadata = "--"
                    
                if sql_update_metadata and (load_mtd(options,odb,ddb,tabla,xml) or not TablaCargada):
                    if options.verbose:
                        print "Actualizando metadata para %s" % tabla
                    if sql_update_metadata != '--':
                        ddb.query(sql_update_metadata)
                
        if (len(sql)>1024):
            ddb.query(sql)
            sql=""
    
    if (len(sql)>0):    
        ddb.query(sql)
        sql=""
    
    qry_d_pkeys=ddb.query("SELECT table_name, column_name,constraint_name FROM information_schema.constraint_column_usage WHERE constraint_name LIKE '%_pkey_%';")
    for row in qry_d_pkeys.dictresult():
        qry_d_pkeys2=ddb.query("""
        SELECT table_name, column_name,constraint_name 
        FROM information_schema.constraint_column_usage 
        WHERE constraint_name = '%(table_name)s_pkey';
        """ % row)
        for row2 in qry_d_pkeys2.dictresult():
            sql = """
            ALTER TABLE %(table_name)s DROP CONSTRAINT %(constraint_name)s;
            """ % row2
            try: 
                ddb.query(sql)
                print "Borrado pkey de la tabla %(table_name)s" % row2
            except:
                print "Error en query corrigiendo pkey:", row2
                print traceback.format_exc()
                print "SQL:"
                print sql
        
        sql = """
        ALTER TABLE %(table_name)s DROP CONSTRAINT %(constraint_name)s;
        ALTER TABLE %(table_name)s ADD PRIMARY KEY (%(column_name)s);
        """ % row
        try: 
            ddb.query(sql)
            print "PK Regenerado: %(constraint_name)s" % row
            try:
                ddb.query("ALTER INDEX %(table_name)s_pkey SET (fillfactor = 80);" % row)
            except:
                pass
        except:
            print "Error en query corrigiendo pkey:", row
            print traceback.format_exc()
            print "SQL:"
            print sql
    
    qry_serial=ddb.query("SELECT sha FROM flserial");
    serials=qry_serial.dictresult() 
    for serial in serials:
        if (serial['sha']==resha1):
            resha1=False
                
    if resha1 and ficheros_actualizados>0:
        if len(serials)>0 :
            ddb.query("UPDATE flserial SET sha='%s';" % (resha1))
            print "Updated flserial => %s." % (resha1)     
        else:        
            ddb.query("INSERT INTO flserial (serie,sha) VALUES(1,'%s')" % (resha1))
            print "Created flserial => %s." % (resha1)     
    
    pydba_loadpgsql.process_create(options,ddb)
    if options.transactions: odb.query("COMMIT;");
