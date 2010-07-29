#!/usr/bin/python
#     encoding: UTF8

# Fichero de carga de módulos para PyDBa
import pg               # depends - python-pygresql
import _mysql           # depends - python-mysqldb
import os               # permite la función "walk"
from stat import *      # definiciones para Stat
import time             # saber la hora actual
import shelve           # persistencia de datos; especial para recordar SHA1

from exmlparser import XMLParser
from pydba_utils import *

import pydba_loadpgsql 

#    *************************** LOAD MODULE *****
#    
    
def load_module(options,db=None, preparse=False):
    if (not db):
        if (not options.ddb):
            print "LoadModule requiere una base de datos y no proporcionó ninguna."
            return 0
        db=dbconnect(options)
        if (not db): 
            return 0
    if options.verbose:
        print " -> LoadModule"
    if options.transactions: 
        sql="BEGIN;"
        db.query(sql)
        try:
            sql="LOCK flfiles, flmodules, flareas NOWAIT;"
            db.query(sql)
        except:
            print "Hay alguien usando las tablas de fl* de AbanQ! no se pueden cargar los módulos!"
            sql="ROLLBACK;"
            db.query(sql)
            raise
            
    
    modules=[]
    dirs2={}
    mod_filenames = []
    for root, dirs, files in os.walk(options.loaddir):
        FoundModule=False
        dirs2[root]=root
        for name in files:
            if (options.debug):
                print "Searching '%s' ..." % name
            if f_ext(name)=="mod":
                FoundModule=True
                if options.verbose:
                    print "Found module '%s' at '%s'" % (name, root)
                try:
                    assert (name not in mod_filenames)
                except:
                    print "ERROR FATAL: Encontré dos veces el mismo módulo. ¡revise que está cargando la carpeta adecuada!"
                    raise
                mod_filenames += [name]
                modules+=[root]
                
        delindex=[]                
        for index, directory in enumerate(dirs):
            Search=not FoundModule
            if directory[0]==".":
                Search=False
            if not Search:
                delindex+=[index]
        delindex.reverse()
        for delete in delindex:
            del dirs[delete]
    
    if options.debug:            
        dirs2=list(dirs2);
        dirs2.sort()
        lendir=len(options.loaddir)
        for directory in dirs2:
            print "Searched into '%s' directory." % directory[lendir:]
            
    if (len(modules)==0):
        print("LoadModule requiere uno o más módulos "
                    "y no se encontró ninguno en la ruta '%s'." % options.loaddir )                 
        return 0
    else:
        if (not options.quiet):
            print "%d modules found." % len(modules)

    options.sha_allowed_files = set([])
    options.filenames_allowed_files = set([])
    options.modules_loaded={}
    for module in modules:
        load_module_loadone(options,module,db,preparse)
    
    sql="DELETE FROM flfiles WHERE sha NOT IN ('%s');\n" % "', '".join(options.sha_allowed_files)
    db.query(sql)
    
    sql="DELETE FROM flfiles WHERE nombre NOT IN ('%s');\n" % "', '".join(options.filenames_allowed_files)
    db.query(sql)
    
    if (not options.quiet):
        print "* done"
    
    if options.flscriptparser == True:
        flscriptparser(launch=True)
    if options.transactions:     
        sql="COMMIT;"
        db.query(sql)
        
    pydba_loadpgsql.process_dependencies()
    
    return db
    
    
def touch(file):
    time_r=time.time()
    f1=open(file,"w")
    f1.write(str(time_r))
    f1.close()    
    
    
def load_module_loadone(options,modpath,db, preparse=False):
    module=""
    tables=[]
    mtd_files={}
    filetypes=["xml","ui","qry","kut","qs","mtd","ts", "pgsql"]
    unicode_filetypes=["ui","ts"]
    
    files=[]
    pd = shelve.open("/tmp/pydba") # open -- file may get suffix added by low-level lib
                          
    
    for root, dirs, walk_files in os.walk(modpath):
            for name in walk_files:
                fname = os.path.join(root, name)
                mtime = os.stat(fname)[ST_MTIME]
                loadFile = True
                if pd.has_key(fname):
                    if pd[fname]["mtime"]==mtime:
                        loadFile=False
                
                if f_ext(name)=="mod":
                    module=name[:-4]
                    file_module=loadfile_inutf8(root, name)
                    module_parse=XMLParser(name)
                    module_parse.parseText(file_module)
                    d_module={
                        'name' :        str(module_parse.root.module.name),
                        'alias' :     str(module_parse.root.module.alias),
                        'area' :        str(module_parse.root.module.area),
                        'areaname' :        str(module_parse.root.module.areaname),
                        'version' :        str(module_parse.root.module.version),
                        'icon' :        str(module_parse.root.module.icon),
                        }
                    if loadFile:
                        d_module['icon_data']=loadfile_inutf8(root, d_module['icon'])
                
                contents=""
                contents_1=""
                if f_ext(name) in filetypes:
                    contents_1=loadfile_inutf8(root,name)
                    contents=pg.escape_string(contents_1)
                    if pd[fname]["sha"] is None: loadFile = True # Some bug in database can cause sha is None.
                    if loadFile:                    
                        sha=SHA1(contents_1)
                        pd[fname]={"mtime":mtime, "sha":sha, 'root' : root,'name' : name}
                    options.sha_allowed_files |= set([pd[fname]["sha"]])    
                    options.filenames_allowed_files |= set([pd[fname]["name"]])    
                    
                if f_ext(name)=="qs" and loadFile==True and options.flscriptparser==True:
                    flscriptparser(root,name)              
                    
                
                if f_ext(name)=="pgsql":
                    array_name=name.split(".")
                    # name.ext1.pgsql
                    # array_name = [ "nombre" , "view" , "pgsql" ]
                    if len(array_name)==2:
                        pydba_loadpgsql.loadpgsqlfile(options = options, database = db, pgname = array_name[0], pgtype = "sql1", pgtext = contents_1)
                    elif len(array_name)!=3:
                        print "ERROR: Al cargar un .pgsql Se esperaban 2 o 3 elementos al estilo nombre.view.pgsql o nombre.pgsql y se encontró %s " % name
                        continue
                    else:
                        pydba_loadpgsql.loadpgsqlfile(options = options, database = db, pgname = array_name[0], pgtype = array_name[1], pgtext = contents_1)
                        
                        
                        
                    
                
                    
                
                if f_ext(name)=="mtd":
                    table=name[:-4]
                    # print "### Table: " + table
                    tables+=[table]
                    mtd_files[table]=contents_1
                    if preparse:
                        xml=XMLParser(name)
                        xml.parseText(contents_1)
                        if xml.root==None:
                            #print "ERROR: Failed to parse xml %s" %  (name)
                            xml=None
                        else:
                            import pydba_mtdparser
                            
                            mtd=xml.root.tmd
                            
                            mparser=pydba_mtdparser.MTDParser()
                            mparser.parse_mtd(mtd)
                            pydba_mtdparser.Tables[table]=mparser
                            
                                

                        
                    
                
                if contents and f_ext(name) in filetypes:
                    file={}
                    for key,val in pd[fname].iteritems():
                        file[key]=val
                    file["contents"]=contents
                    if (options.modules_loaded.has_key(name)):
                        print "ERROR: %s file was already loaded." % name
                        print "--> this file was found at %s" % root
                        print "--> previous file found at %s" % options.modules_loaded[name]['root']
                        print "* skipping file"
                    else:
                        options.modules_loaded[name]=file
                        files+=[file]
    
    pd.close()
    try:
        os.chmod("/tmp/pydba", S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)
    except:
        pass

    qry_areas=db.query("SELECT descripcion, bloqueo, idarea"
                                                " FROM flareas WHERE idarea='%s'" % d_module['area'])
    tareas=qry_areas.dictresult()                        
    if len(tareas)==0:
        print "Creando Area %s - %s " %(d_module['area'],d_module['areaname'])
        db.query("INSERT INTO flareas (descripcion, bloqueo, idarea)"
                "VALUES('%s','t','%s')" % (d_module['areaname'],d_module['area']))
                    
                                                    
    
    habilitar_carga=False
    qry_modulo=db.query("SELECT idmodulo, version, descripcion, bloqueo, idarea"
                            " FROM flmodules WHERE idmodulo='%s'" % module);
    tmodulo=qry_modulo.dictresult() 
    cargado=False
    for pmodulo in tmodulo:
        cargado=True
        if pmodulo['bloqueo']=='t':    # TRUE Es que NO está bloqueado. Está al revés.s
            habilitar_carga=True
    
    if not cargado:
        print "Se procede a crear el módulo nuevo %s" % module
        
        idmodulo        = pg.escape_string(d_module['name']) 
        idarea            = pg.escape_string(d_module['area'])
        version         = pg.escape_string(d_module['version'])
        bloqueo         = "t"
        descripcion = pg.escape_string(d_module['alias'])
        icono             = pg.escape_string(d_module['icon_data'])
        
        sql=("INSERT INTO flmodules (idmodulo, idarea, version, bloqueo, descripcion,icono) "    
                    "VALUES('%s','%s','%s','%s','%s','%s')" % 
                            (idmodulo, idarea, version, bloqueo, descripcion,icono))
        db.query(sql)     
        habilitar_carga=True     

    if not habilitar_carga:
        print "Error when trying to update the module '%s': non-loaded or locked module" % module
        return 0
    
    qry_modulos=db.query("SELECT nombre,sha FROM flfiles WHERE idmodulo='%s' " % (module));
    tuplas_modulos=qry_modulos.dictresult() 
    dmodulos={}
    for modulo in tuplas_modulos:
        dmodulos[modulo['nombre']]=modulo;
        
    loaded=[]
    
    for file in files:
        
        update=True        
        if (dmodulos.has_key(file['name'])):
            dm=dmodulos[file['name']]
            if (dm['sha']==file['sha']):
                update=False
        
        if options.loadbaselec:
            update=(file['name']=='baselec.mtd')
              
        if (update):
            loaded+=[file['name']]
            if (options.verbose):
                print "* Loading file '%s' => '%s'..." % (file['name'],file['sha'])
            sql="DELETE FROM flfiles WHERE nombre='%s';\n" % file['name']
            db.query(sql)
        
            file['module']=module
            
            sql=("INSERT INTO flfiles (contenido, bloqueo, sha, idmodulo, nombre) "    
                    "VALUES(E'%(contents)s', 't', '%(sha)s','%(module)s', '%(name)s')" % file)
            db.query(sql)
    
    
    
    if (not options.quiet and len(loaded)>0): 
        print "Module %s: loaded %d of %d files. (%s...)" % (module, len(loaded), len(files),",".join(loaded[:3]))
        import sys
        sys.stdout.flush()
            

    options.files_loaded+=loaded

