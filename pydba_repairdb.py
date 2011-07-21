#!/usr/bin/python
#     encoding: UTF8

# Fichero de reparación de base de datos para PyDBa
import pg             # depends - python-pygresql
import _mysql     # depends - python-mysqldb
import sys, pprint
        
from pydba_utils import *
from pydba_mtdparser import load_mtd    
from exmlparser import XMLParser
import traceback
import pydba_loadpgsql
#    *************************** REPAIR DATABASE *****
#    
import os, os.path
# Modo "windows"
ENCODING="cp1252"
ENCODING_ERRORS="xmlcharrefreplace"
# Modo "infosial/compatible"
ENCODING="iso-8859-15"
ENCODING_ERRORS="replace"

def utf8decode(txt):
    global ENCODING, ENCODING_ERRORS
    utxt = unicode(txt,"UTF8")
    txt= utxt.encode(ENCODING, ENCODING_ERRORS)
    return txt


def dump_db(options,odb=None):
    if (options.verbose):
        print "-> DumpDB"
    if (not odb):
        if (not options.odb):
            print "RepairDB requiere una base de datos de origen y no proporcionó ninguna."
            return 0
        odb=odbconnect(options)
        if (not odb): 
            return 0
    
    #Areas: ('descripcion', 'bloqueo', 'idarea')
    #Modules: ('version', 'icono', 'descripcion', 'idmodulo', 'bloqueo', 'idarea')
    #Files: ('sha', 'idmodulo', 'contenido', 'bloqueo', 'nombre')
    
    # Obtener areas
    qry_oareas=odb.query("SELECT * FROM flareas")
    areas = {}
    for area in qry_oareas.dictresult():
        key = area["idarea"]
        areas[key] = area
    
    
    print "Areas:", qry_oareas.listfields()
    # Obtener areas
    qry_omodulos=odb.query("SELECT * FROM flmodules")
    modules = {}
    for module in qry_omodulos.dictresult():
        
        key = module["idmodulo"]
        modules[key] = module
    print "Modules:", qry_omodulos.listfields()
    # Obtener los ficheros nuevos
    qry_ofiles =odb.query("SELECT * FROM flfiles")
    print "Files:", qry_ofiles.listfields()
    
    files = {}
    for file1 in qry_ofiles.dictresult():
        key = file1["nombre"]
        files[key] = file1
    
    folder_ext = {
        '.ar': 'reports',
        '.csv': 'docs',
        '.doc': 'docs',
        '.jasper': 'ireports',
        '.jrxml': 'ireports',
        '.kut': 'reports',
        '.mtd': 'tables',
        '.odt': 'docs',
        '.pgsql': 'pgsql',
        '.qry': 'queries',
        '.qs': 'scripts',
        '.sql': 'pgsql',
        '.ts': 'translations',
        '.txt': 'docs',
        '.ui': 'forms',
        '.xml': '.'}

    folder_area = {
        'C': 'contabilidad',
        'CRM': 'CRM',
        'D': 'direccion',
        'F': 'facturacion',
        'L': 'colaboracion',
        'R': 'rhumanos',
        'sys': 'sistema'}

    folder_module = {
        'fl_a3_nomi': 'nominas',
        'flar2kut': 'ar2kut',
        'flcolagedo': 'gesdoc',
        'flcolaproc': 'procesos',
        'flcolaproy': 'proyectos',
        'flcontacce': 'controlacceso',
        'flcontinfo': 'informes',
        'flcontmode': 'modelos',
        'flcontppal': 'principal',
        'flcrm_info': 'informes',
        'flcrm_mark': 'marketing',
        'flcrm_ppal': 'principal',
        'fldatosppal': 'datos',
        'fldireinne': 'analisis',
        'flfactalma': 'almacen',
        'flfactinfo': 'informes',
        'flfactppal': 'principal',
        'flfactteso': 'tesoreria',
        'flfacturac': 'facturacion',
        'flgraficos': 'graficos',
        'flrrhhppal': 'principal',
        'sys': 'administracion'}


    utf8_ext = [".ui", ".ts"]
    modified_vars = {}
    for areakey,area in areas.iteritems():
        foldername = folder_area.get(areakey,None)
        if foldername is None:
            foldername = raw_input("Area %s unknown,\n  which directory do you want? [%s]: " % (area['descripcion'],areakey.lower()))
            if not foldername: foldername = areakey
            folder_area[areakey] = foldername
            modified_vars["folder_area"] = folder_area
        foldername = foldername.lower()
        print "Area %s - %s (%s)... " % (areakey, area['descripcion'],foldername)
        create_folder(foldername)
        for modulekey, module in modules.iteritems():
            if module['idarea'] != areakey: continue
            module_foldername = folder_module.get(modulekey,None)
            if module_foldername is None:
                module_foldername = raw_input("Module %s/%s unknown,\n  which directory do you want? [%s]: " % (areakey,module['descripcion'],modulekey.lower()))
                if not module_foldername: module_foldername = modulekey
                folder_module[modulekey] = module_foldername
                modified_vars["folder_module"] = folder_module
            module_foldername = module_foldername.lower()
            module_folder = os.path.join(foldername, module_foldername)
            print "Module %s - %s (%s) ..." % (modulekey, module['descripcion'],module_folder)
            create_folder(module_folder)
            file_icono = open(os.path.join(module_folder,"%s.xpm" % modulekey),"w")
            file_icono.write(module['icono'])
            file_icono.close()
            
            compuesto = {}
            compuesto.update(area)
            compuesto.update(module)
            compuesto["area_descripcion"] = area["descripcion"]
            compuesto["module_descripcion"] = module["descripcion"]
            
            file_mod = open(os.path.join(module_folder,"%s.mod" % modulekey),"w")
            txt = """<!DOCTYPE MODULE>
<MODULE>
        <name>%(idmodulo)s</name>
        <alias>QT_TRANSLATE_NOOP("FLWidgetApplication","%(module_descripcion)s")</alias>
        <area>%(idarea)s</area>
        <areaname>QT_TRANSLATE_NOOP("FLWidgetApplication","%(area_descripcion)s")</areaname>
        <version>%(version)s</version>
        <icon>%(idmodulo)s.xpm</icon>
        <flversion>%(version)s</flversion>
        <dependencies />
        <description>%(module_descripcion)s</description>
</MODULE>
                """ % compuesto
            file_mod.write( utf8decode(txt) )
            file_mod.close()
            files_by_ext = {}
            for filekey, file1 in files.iteritems():
                if file1['idmodulo'] != modulekey: continue
                root, ext = os.path.splitext(filekey)
                if ext not in files_by_ext:
                    files_by_ext[ext] = []
                files_by_ext[ext].append(file1)
            
            for ext, file_list in sorted(files_by_ext.iteritems()):
                if len(ext) > 8: 
                    print "Ignoring extension %s (more than 8 chars)" % ext
                    continue
                ext_foldername = folder_ext.get(ext,None) # default -> others
                if ext_foldername is None:
                    ext_foldername = raw_input("Extension %s unknown,\n  which directory do you want? [others]: " % (ext))
                    if not ext_foldername: ext_foldername = "others"
                    folder_ext[ext] = ext_foldername                
                    modified_vars["folder_ext"] = folder_ext
                sys.stdout.write("writting %s files: " % ext)
                sys.stdout.flush()
                ext_foldername = ext_foldername.lower()                
                ext_folder = os.path.join(module_folder,ext_foldername)
                #print "Extension %s (%s)" % (ext,ext_folder)
                create_folder(ext_folder)
                
                for fileobj in file_list:
                    file_1 = open(os.path.join(ext_folder,fileobj['nombre']),"w")
                    txt = fileobj['contenido']
                        
                    file_1.write( utf8decode(txt) )
                    file_1.close()
                    sys.stdout.write(".")
                    sys.stdout.flush()
                sys.stdout.write("\n")
                sys.stdout.flush()
                    
                    
    print
    export = options.debug or options.verbose
    if modified_vars and not export:
        doexport = raw_input("Some config vars (%s) were modified.\n  Do yo want to export them? ([Y]/n): " % (", ".join(modified_vars.keys())))
        if doexport.lower() in ("y","s",""): export = True
    if export:
        if options.verbose:
            modified_vars["folder_ext"] = folder_ext
            modified_vars["folder_module"] = folder_module
            modified_vars["folder_area"] = folder_area
        print "Exporting variables:"
        for varname, content in modified_vars.iteritems():
            print "    %s = %s" % (varname, pprint.pformat(content, indent=8).strip().replace("{","{\n "))
            print
                
                
                
            
            
            
        
        
        
def create_folder(foldername):
    if not os.path.exists(foldername):
        print "Creando carpeta '%s' . . . " % foldername
        os.mkdir(foldername)
    
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
    
    if options.debug:
        print "* Revisando si existe una tabla de servidores réplica ..."
    tabla = None
    qry1 =ddb.query("SELECT relname FROM pg_class WHERE relname ='servidoresreplica'");
    for row in qry1.dictresult():
        tabla = row['relname']
    
    if tabla:
        if options.debug:
            print "Encontrada una tabla %s." % tabla
        qry1 =ddb.query("SELECT codservidor, activo, numero  FROM %s" % tabla);
        servsize = len(qry1.dictresult())
        servnumber = -1
        servname = None
        for row in qry1.dictresult():
            if row['activo'] == 't':
                try:
                    assert(servnumber == -1)
                except AssertionError:
                    print "PANIC: Hay mas de un servidor activo! El valor anterior era %s el nuevo %s. (activo vale %s)" % (
                        repr(servnumber),
                        repr(row['numero']),
                        repr(row['activo'])
                        )
                    raise
                servnumber = row['numero']
                servname = row['codservidor']
        activar_servrep = True
        if servsize < 1 or servsize > 10:
            activar_servrep = False
            print "WARN: No se activa la sincronización de secuencia para replicación master-master por error en la cantidad de servidores (%d)" % servsize
            
        if servnumber < 0 or servnumber >= servsize:
            activar_servrep = False
            print "WARN: No se activa la sincronización de secuencia para replicación master-master por error el numero de este servidor (%d/%d)" % (servnumber,servsize)
        
        if activar_servrep:
            print "INFO: Activando sincro. de secuencias para %s: %d/%d" % (repr(servname), servnumber,servsize)
            options.seqsync = (servnumber,servsize)
        
    else:
        if options.debug:
            print "No se encontró tabla de servidores de replica"
            
        
    
    
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
            
    tables_notrebuilt = []
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
                if sql_update_metadata:    
                    if load_mtd(options,odb,ddb,tabla,xml) or not TablaCargada:
                        if options.verbose:
                            print "Actualizando metadata para %s" % tabla
                        if sql_update_metadata != '--':
                            ddb.query(sql_update_metadata)
                    else:
                        tables_notrebuilt.append(modulo['nombre'])
                
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
    if tables_notrebuilt:
        print "Tables pending rebuild:", ", ".join(tables_notrebuilt)
        
