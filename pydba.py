#!/usr/bin/python
#   encoding: UTF8

#Importar módulo mysql para MySQL sobre Python 
import _mysql
# Importar módulo PyGreSQL para Postgres sobre Python
import pg
# Importar módulo para leer ficheros CFG y INI
import ConfigParser
# Importar el módulo para leer la línea de comandos
from optparse import OptionParser
from optparse import OptionGroup
import sys
import sha
import os
import re


# import subprocess

import xml.parsers.expat

    
def f_ext(filename):
  name_s=filename.split(".")
  numsplits=len(name_s)
  return name_s[numsplits-1]

def loadfile_inutf8(root, name):
  f1=open(os.path.join(root, name),"r")
  contents=f1.read()
  f1.close()
  #if (f_ext(name) in ['ui','ts']):
  #  contents=contents.decode('iso-8859-15')
  #else:
  #  contents=contents.decode('iso-8859-15')
  
  contents=contents.decode('iso-8859-15')
  contents=contents.encode('utf8')
  return contents
      
      


# Funcion de compatibilidad con QSA y AbanQ.
def QT_TRANSLATE_NOOP(From,String):
  return String

class XMLParser_data:
  _name=""
  _data=""
  _attrs={}
  def __str__(self):
    return self._data.encode('utf8')
    
  pass
  
class XMLParser:
  root=XMLParser_data()
  root._name="root"
  stack=[]
  xmlusing=root
  num_element=1
  
  
  def start_element(self,name, attrs):
    method_name=name.lower()
    if (not method_name.isalpha()):
      num_element+=1
      method_name="element%d" % num_element
    
    new=XMLParser_data()
    setattr(self.xmlusing,method_name,new)

    self.stack.append(self.xmlusing)
    self.xmlusing=new
    self.xmlusing._name=name
    self.xmlusing._attrs=attrs
    
  def end_element(self,name):
    if (name!=self.xmlusing._name):
      print "WARNING: <%s> ... </%s>" % (xmlusing._name,name)
    self.xmlusing=self.stack.pop()
  def char_data(self,data):
    
    m = re.search('QT_TRANSLATE_NOOP\("([^"]*)","([^"]*)"\)', data)
   
    if (m):
      data=QT_TRANSLATE_NOOP(m.group(1),m.group(2))
    
    self.xmlusing._data=data    
  
  def parseText(self,text):
  
    p = xml.parsers.expat.ParserCreate()
    
    p.StartElementHandler = self.start_element
    p.EndElementHandler = self.end_element
    p.CharacterDataHandler = self.char_data
    
    p.Parse(text, 1)



def get_popen(command):
     sp = subprocess
     return subprocess.Popen(command.split(), stdout=sp.PIPE, stderr=sp.STDOUT)


def SHA1(text):
  return sha.new(text).hexdigest();

def main():
  
  parser = OptionParser()
  parser.set_defaults(
                      action="none", 
                      dhost="localhost", 
                      dport="5432", 
                      ddriver="pgsql", 
                      duser="postgres", 
                      dpasswd="",
                      loaddir=".",
                      quiet=False,
                      verbose=False,
                      debug=False
                      )
  parser.add_option("--debug",dest="debug", action="store_true", help="Tons of debug output")
  parser.add_option("-v",dest="verbose", action="store_true", help="Be more verbose")
  parser.add_option("-q",dest="quiet", action="store_true", help="Be less verbose")
  
  g_action = OptionGroup(parser, "Actions","You MUST provide one of these:")
  
  # ******************* ACTIONS
  g_action.add_option("-l","--lmod", action="store_const", const="load_module"
    ,dest="action", help="load modules")
    
  g_action.add_option("-r","--reload-mtd", action="store_const", const="reload_mtd"
    ,dest="action", help="Parses MTD files on DB and complete tables")
		    
  g_action.add_option("-R","--repairdb", action="store_const", const="repair_db"
    ,dest="action", help="Execute tests to repair DB")
  
  g_action.add_option("-C","--createdb", action="store_const", const="create_db"
    ,dest="action", help="Create a new Database with basic fl* tables")
  
  g_action.add_option("-M","--mysql2pgsql", action="store_const", const="mysql_convert"
    ,dest="action", help="Convert MySQL Database to PostgreSQL")
		    
  g_action.add_option("-i","--ini", action="store_const", const="exec_ini"
    ,dest="action", help="load and execute INI file")
		    
  parser.add_option_group(g_action)  
  # ******************* CONFIG
  
  g_options = OptionGroup(parser, "Options","Optional database and host selection. Some actions use them")
  g_options.add_option("--dhost",dest="dhost", help="Set the destination host")
  g_options.add_option("--dport",dest="dport", help="Set the destination port")
  g_options.add_option("--ddriver",dest="ddriver", help="Set the driver for dest DB (mysql; pgsql)")
  g_options.add_option("--ddb",dest="ddb", help="Set DDB as destination Database")
  g_options.add_option("--duser",dest="duser", help="Provide user for DB connection")
  g_options.add_option("--dpasswd",dest="dpasswd", help="Provide password for DB connection")
  g_options.add_option("--loaddir",dest="loaddir", help="Select Working Directory for Modules")
  
  parser.add_option_group(g_options)  
  
  
		    
  #parser.add_option("-f", "--file", dest="filename",
  #                  help="write report to FILE", metavar="FILE")
  #parser.add_option("-q", "--quiet",
  #                  action="store_false", dest="verbose", default=True,
  #                  help="don't print status messages to stdout")
  
  (options, args) = parser.parse_args()
  
  if (options.action=="none"):
    print "You must provide at least one action"
  elif (options.action=="load_module"):
    load_module(options);
  elif (options.action=="repair_db"):
    repair_db(options);
  else:
    print "Unknown action: " + options.action;
  
  # sys.exit(0)

def dbconnect(options):
  if (not options.dpasswd):
    options.dpasswd = raw_input("Password: ")
  try:
    if (options.ddriver=='mysql'):
      cn = _mysql.connect(
                db=options.ddb, 
                port=int(options.dport),
                host=options.dhost, 
                user=options.duser, 
                passwd=options.dpasswd )
      cn.set_character_set("UTF8") # Hacemos la codificación a UTF8.
    # Si la conexión es a Postgres , de la siguiente manera            
    else:
      cn = pg.connect(
                dbname=options.ddb, 
                port=int(options.dport),
                host=options.dhost, 
                user=options.duser, 
                passwd=options.dpasswd )  
  except:
    print("Error trying to connect to database '" + options.ddb + "' in host " + 
      options.dhost + ":" + options.dport + " using user '" + options.duser + "'")
    return 0
         
  if options.debug: 
    print("* Succesfully connected to database '" + options.ddb + "' in host " + 
    options.dhost + ":" + options.dport + " using user '" + options.duser + "'")
  return cn
  
  
    
#  *************************** LOAD MODULE *****
#  
  
def load_module(options,db=None):
  if (not db):
    if (not options.ddb):
      print "LoadModule requiere una base de datos y no proporcionó ninguna."
      return 0
    db=dbconnect(options)
    if (not db): 
      return 0
  
  modules=[]
  dirs2={}
  for root, dirs, files in os.walk(options.loaddir):
    FoundModule=False
    dirs2[root]=root
    for name in files:
      if (options.debug):
        print "Searching '%s' ..." % name
      if name[-4:]==".mod":
        FoundModule=True
        if options.verbose:
          print "Found module '%s' at '%s'" % (name, root)
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
    
  options.modules_loaded={}
  for module in modules:
    load_module_loadone(options,module,db)
  
  repair_db(options,db)


  
def load_module_loadone(options,modpath,db):
  module=""
  tables=[]
  filetypes=["xml","ui","qry","kut","qs","mtd","ts"]
  unicode_filetypes=["ui","ts"]
  
  files=[]
  for root, dirs, walk_files in os.walk(modpath):
      for name in walk_files:
        if f_ext(name)=="mod":
          module=name[:-4]
          file_module=loadfile_inutf8(root, name)
          module_parse=XMLParser()
          module_parse.parseText(file_module)
          d_module={
            'name' :    str(module_parse.root.module.name),
            'alias' :   str(module_parse.root.module.alias),
            'area' :    str(module_parse.root.module.area),
            'areaname' :    str(module_parse.root.module.areaname),
            'version' :    str(module_parse.root.module.version),
            'icon' :    str(module_parse.root.module.icon),
            }
          d_module['icon_data']=loadfile_inutf8(root, d_module['icon'])
          print d_module['areaname']
        if f_ext(name)=="mtd":
          table=name[:-4]
          # print "Table: " + table
          tables+=[table]
        
        if f_ext(name) in filetypes:
          
          contents=loadfile_inutf8(root,name)
          sha=SHA1(contents)
          contents=pg.escape_string(contents)
          
          
          file={'name' : name,'sha': sha, 'contents' : contents, 'root' : root}
          if (options.modules_loaded.has_key(name)):
            print "ERROR: %s file was already loaded." % name
            print "--> this file was found at %s" % root
            print "--> previous file found at %s" % options.modules_loaded[name]['root']
            print "* skipping file"
          else:
            options.modules_loaded[name]=file
            files+=[file]
  
  habilitar_carga=False
  qry_modulo=db.query("SELECT idmodulo, version, descripcion, bloqueo, idarea"
                        " FROM flmodules WHERE idmodulo='%s'" % module);
  tmodulo=qry_modulo.dictresult() 
  cargado=False
  for pmodulo in tmodulo:
    cargado=True
    if pmodulo['bloqueo']=='t':  # TRUE Es que NO está bloqueado. Está al revés.s
      habilitar_carga=True
  
  if not cargado:
    print "Se procede a crear el módulo nuevo %s" % module
    
    idmodulo    = pg.escape_string(d_module['name']) 
    idarea      = pg.escape_string(d_module['area'])
    version     = pg.escape_string(d_module['version'])
    bloqueo     = "t"
    descripcion = pg.escape_string(d_module['alias'])
    icono       = pg.escape_string(d_module['icon_data'])
    
    sql=("INSERT INTO flmodules (idmodulo, idarea, version, bloqueo, descripcion,icono) "  
          "VALUES('%s','%s','%s','%s','%s','%s')" % 
              (idmodulo, idarea, version, bloqueo, descripcion,icono))
    db.query(sql)   
    habilitar_carga=True   

  if not habilitar_carga:
    print "Error when trying to update the module '%s': non-loaded or locked module" % module
    return 0
  
  qry_modulos=db.query("SELECT * FROM flfiles WHERE idmodulo='%s'" % module);
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
    
    if (update):
      loaded+=[file['name']]
      if (options.verbose):
        print "* Loading file '%s' => '%s'..." % (file['name'],file['sha'])
      sql="DELETE FROM flfiles WHERE nombre='%s';\n" % file['name']
      db.query(sql)
    
      file['module']=module
      
      sql=("INSERT INTO flfiles (contenido, bloqueo, sha, idmodulo, nombre) "  
            "VALUES('%(contents)s', 't', '%(sha)s','%(module)s', '%(name)s')" % file)
      db.query(sql)
  
  
  
  
  if (not options.quiet and len(loaded)>0): 
    print "Module %s: loaded %d of %d files. (%s)" % (module, len(loaded), len(files),",".join(loaded))
      
  
#  *************************** REPAIR DATABASE *****
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
  
  qry_modulos=db.query("SELECT idmodulo, nombre, contenido, sha FROM flfiles ORDER BY idmodulo, nombre");
  modulos=qry_modulos.dictresult() # El resultado de la consulta anterior lo volcamos en una variable de lista
  sql=""
  resha1="";
  for modulo in modulos:
    sha1=SHA1(modulo['contenido'])
    resha1=SHA1(resha1+sha1)
    if (modulo['sha']!=sha1):
      print "Updating " + modulo['nombre'] + " => " + sha1 + " ..."
      sql+="UPDATE flfiles SET sha='%s' WHERE nombre='%s';\n" %  (sha1,modulo['nombre'])
    elif (options.debug):
      print modulo['nombre'] + " is ok."
    
    if (modulo['nombre'][-4:]==".mtd"):
      tabla=modulo['nombre'][:-4]
      qry_modulos=db.query("SELECT xml FROM flmetadata WHERE tabla='%s'" % tabla);
      tablas=qry_modulos.dictresult() 
      for txml in tablas:
        if txml['xml']!=sha1:
          print "Actualizada la Tabla: " + tabla
          sql+="UPDATE flmetadata SET xml='%s' WHERE tabla='%s';\n" %  (sha1,tabla)
      
    if (len(sql)>1024):
      db.query(sql)
      sql=""
  
  if (len(sql)>0):  
    db.query(sql)
    sql=""
  
  qry_serial=db.query("SELECT sha FROM flserial");
  serials=qry_serial.dictresult() # El resultado de la consulta anterior lo volcamos en una variable de lista
  for serial in serials:
    if (serial['sha']==resha1):
      resha1=False
        
          
  if (resha1):
    if len(serials)>0:
      db.query("UPDATE flserial SET sha='%s';" %  (resha1))
      print "Updated flserial => %s." %  (resha1)   
    else:    
      db.query("INSERT INTO flserial (serie,sha) VALUES(1,'%s')" %  (resha1))
      print "Created flserial => %s." %  (resha1)   
  





#def exec_ini():
  ## Abrimos el archivo y leemos el fichero de configuración "replicas.ini"
  #ini = ConfigParser.ConfigParser()
  #ini.readfp(open('replicas.ini'))
  
  ## Inicializamos lista de tablas
  #tablas=[]
  
  ## Añadimos a la variable secciones los diferentes modulos del archivo replicas.ini
  #secciones=ini.sections()
  
  ## Recorremos los módulos (secciones) del archivo
  #for seccion in secciones:
    #items_seccion=ini.items(seccion)
    #tsec=seccion.split(".") # Divide la cadena por el caracter que le indicas
    #tipoSeccion=tsec[0] # Primera parte de los nombres de cada sección del archivo
    #nombreSeccion=tsec[1] # Segunda parte de los nombres de cada sección del archivo
    ## Si la sección empieza por mod.
    #if tipoSeccion=="mod":
      ## Recorremos los campos de cada tabla
      ## Dividimos de cada modulo los campos en los que aparece un punto
      #for tabla in items_seccion:    
        #t_tabla=tabla[0].split(".")
        #if (len(t_tabla)>0):
          #tabla_n1=t_tabla[0]
        #if (len(t_tabla)>1):
          #tabla_n2=t_tabla[1]
        ## Cogemos los campos que no empiezan por "__" ni terminan con "__"
        #if (tabla[0:3]=='__'):
          #continue
          
        #if len(t_tabla)==1 and tabla[1]=="Yes":
          #tablas.append(tabla[0])
          
    ## Si la sección empieza por db.
    #if tipoSeccion=="db":
      #configdb={}
      ## Recorremos cada item de cada sección
      #for item in items_seccion:
        #configdb[item[0]]=item[1]
      
      #if nombreSeccion=="origen":
        #configdborigen = configdb
        
      #if nombreSeccion=="destino":
        #configdbdestino = configdb
        
  
  ## Nos conectamos a la base de datos origen
  ## Si la conexión es a MySQL pasaremos por este if.  
  #if (configdborigen['driver']=='mysql'):
    #conectbd = _mysql.connect(
              #db=configdborigen['dbname'], 
              #port=int(configdborigen['port']),
              #host=configdborigen['host'], 
              #user=configdborigen['user'], 
              #passwd=configdborigen['passwd'])
    #conectbd.set_character_set("UTF8") # Hacemos la codificación a UTF8.
  ## Si la conexión es a Postgres , de la siguiente manera            
  #else:
    #conectbd = pg.connect(
              #dbname=configdborigen['dbname'], 
              #port=int(configdborigen['port']),
              #host=configdborigen['host'], 
              #user=configdborigen['user'], 
              #passwd=configdborigen['passwd'])
  
  ## Nos conectamos a la base de datos destino
  #psql = pg.connect(
              #dbname=configdbdestino['dbname'], 
              #port=int(configdbdestino['port']),
              #host=configdbdestino['host'], 
              #user=configdbdestino['user'], 
              #passwd=configdbdestino['passwd']
              #)
              
  ## Hacemos una consulta para sacar las tablas que concuerdan con la restriccion LIKE 'fl%'
  ##qry_tablas = psql.query(
  ##  "select table_name from information_schema.tables"   
  ##  " where table_schema='public' and table_type='BASE TABLE' and table_name LIKE 'fl%%'")
  
  #qry_tablas = psql.query(
    #"select table_name from information_schema.tables"   
    #" where table_schema='public' and table_type='BASE TABLE'")
    
  ## Guardamos el resultado de la consulta anterior en la variable
  #tupla_tablas=qry_tablas.getresult()
  
  
  ## Añadimos valores de tupla_tablas a tablas
  #for tupla in tupla_tablas:
    #tablas.append(tupla[0])
  
    
  #for tabla in tablas:
    #print tabla
      ##tablas.append(tabla[0])
    #qry_deltables= psql.query("delete from %s" % tabla)
    #if (configdborigen['driver']=='mysql'):
      #conectbd.query("select * from %s" % tabla) # Hacemos una select del contenido de la tabla
      #r=conectbd.store_result()
      #filas=r.fetch_row(maxrows=0,how=1) # Nos saca el resultado de la fila
      #if (len(filas)==0):
        #continue
      #campos=filas[0].keys()
    #else:  
      #qry_seltables= conectbd.query("select * from %s" % tabla) # Hacemos una select del contenido de la tabla
      #filas=qry_seltables.getresult() # El resultado de la consulta anterior lo volcamos en una variable de lista
      #campos=qry_seltables.listfields() # Cargamos la lista de nombres de campos a la variable campos
    
    #sqlvars={}
    #sqlvars['tabla']=tabla
    #separador=", "
    #sqlvars['fields']=separador.join(campos)
    ## *** Inicio proceso de insert into en la tabla
    ## insert into table (field1,field2) VALUES (val1,val2),(val1,val2),(val1,val2)
    #f=0
    #bytes=0
    #porcentaje=0
    #for fila in filas:
      #n=0
      #valores=[]
      #for campo in fila:
        #if (configdborigen['driver']=='mysql'):
          #campo=fila[campo]
        
        #if (campo is not None):#Si el valor es nulo
          #valores.append("'" + pg.escape_string(str(campo)) + "'")
        #else:
          #valores.append("NULL")
        #n+=1
      #text="(" + separador.join(valores) + ")"
      #bytes+=len(text)
      #f+=1
      ## En postgres no funcionan los insert multilínea
      #sqlvars['rows']=text
      #sql_text="INSERT INTO %(tabla)s (%(fields)s) VALUES %(rows)s;" % sqlvars
      #qry_instables=psql.query(sql_text)
      #bytes=0
      #if (porcentaje+5<=f*100/len(filas)):
        #porcentaje=f*100/len(filas)
        #print tabla + "(" + str(porcentaje) + "%)"
    
if __name__ == "__main__":
  main()