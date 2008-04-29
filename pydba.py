#!/usr/bin/python
#   encoding: UTF8

#Importar módulo mysql para MySQL sobre Python 
import _mysql
# db=_mysql.connect()
# Importar módulo PyGreSQL para Postgres sobre Python
import pg
# Importar módulo para leer ficheros CFG y INI
import ConfigParser


# Abrimos el archivo y leemos el fichero de configuración "replicas.ini"
ini = ConfigParser.ConfigParser()
ini.readfp(open('replicas.ini'))

# Inicializamos lista de tablas
tablas=[]

# Añadimos a la variable secciones los diferentes modulos del archivo replicas.ini
secciones=ini.sections()

# Recorremos los módulos (secciones) del archivo
for seccion in secciones:
  items_seccion=ini.items(seccion)
  tsec=seccion.split(".") # Divide la cadena por el caracter que le indicas
  tipoSeccion=tsec[0] # Primera parte de los nombres de cada sección del archivo
  nombreSeccion=tsec[1] # Segunda parte de los nombres de cada sección del archivo
  # Si la sección empieza por mod.
  if tipoSeccion=="mod":
    # Recorremos los campos de cada tabla
    # Dividimos de cada modulo los campos en los que aparece un punto
    for tabla in items_seccion:    
      t_tabla=tabla[0].split(".")
      if (len(t_tabla)>0):
         tabla_n1=t_tabla[0]
      if (len(t_tabla)>1):
         tabla_n2=t_tabla[1]
      # Cogemos los campos que no empiezan por "__" ni terminan con "__"
      if (tabla_n1[0:3]=='__' and tabla_n1[-2:]=='__'):
        continue
        
      if len(t_tabla)==1 and tabla[1]=="Yes":
        tablas.append(tabla[0])
        
  # Si la sección empieza por db.
  if tipoSeccion=="db":
    configdb={}
    # Recorremos cada item de cada sección
    for item in items_seccion:
      configdb[item[0]]=item[1]
    
    if nombreSeccion=="origen":
       configdborigen = configdb
       
    if nombreSeccion=="destino":
       configdbdestino = configdb
       

# Nos conectamos a la base de datos origen
# Si la conexión es a MySQL pasaremos por este if.  
if (configdborigen['driver']=='mysql'):
  conectbd = _mysql.connect(
            db=configdborigen['dbname'], 
            port=int(configdborigen['port']),
            host=configdborigen['host'], 
            user=configdborigen['user'], 
            passwd=configdborigen['passwd'])
  conectbd.set_character_set("UTF8") # Hacemos la codificación a UTF8.
# Si la conexión es a Postgres , de la siguiente manera            
else:
  conectbd = pg.connect(
            dbname=configdborigen['dbname'], 
            port=int(configdborigen['port']),
            host=configdborigen['host'], 
            user=configdborigen['user'], 
            passwd=configdborigen['passwd'])

# Nos conectamos a la base de datos destino
psql = pg.connect(
            dbname=configdbdestino['dbname'], 
            port=int(configdbdestino['port']),
            host=configdbdestino['host'], 
            user=configdbdestino['user'], 
            passwd=configdbdestino['passwd']
            )
            
# Hacemos una consulta para sacar las tablas que concuerdan con la restriccion LIKE 'fl%'
#qry_tablas = psql.query(
#  "select table_name from information_schema.tables"   
#  " where table_schema='public' and table_type='BASE TABLE' and table_name LIKE 'fl%%'")

qry_tablas = psql.query(
  "select table_name from information_schema.tables"   
  " where table_schema='public' and table_type='BASE TABLE'")
  
# Guardamos el resultado de la consulta anterior en la variable
tupla_tablas=qry_tablas.getresult()


# Añadimos valores de tupla_tablas a tablas
for tupla in tupla_tablas:
  tablas.append(tupla[0])

  
for tabla in tablas:
  print tabla
    #tablas.append(tabla[0])
  qry_deltables= psql.query("delete from %s" % tabla)
  if (configdborigen['driver']=='mysql'):
    conectbd.query("select * from %s" % tabla) # Hacemos una select del contenido de la tabla
    r=conectbd.store_result()
    filas=r.fetch_row(maxrows=0,how=1) # Nos saca el resultado de la fila
    if (len(filas)==0):
      continue
    campos=filas[0].keys()
  else:  
    qry_seltables= conectbd.query("select * from %s" % tabla) # Hacemos una select del contenido de la tabla
    filas=qry_seltables.getresult() # El resultado de la consulta anterior lo volcamos en una variable de lista
    campos=qry_seltables.listfields() # Cargamos la lista de nombres de campos a la variable campos
  
  sqlvars={}
  sqlvars['tabla']=tabla
  separador=", "
  sqlvars['fields']=separador.join(campos)
  # *** Inicio proceso de insert into en la tabla
  # insert into table (field1,field2) VALUES (val1,val2),(val1,val2),(val1,val2)
  f=0
  bytes=0
  porcentaje=0
  for fila in filas:
    n=0
    valores=[]
    for campo in fila:
      if (configdborigen['driver']=='mysql'):
        campo=fila[campo]
      
      if (campo is not None):#Si el valor es nulo
        valores.append("'" + pg.escape_string(str(campo)) + "'")
      else:
        valores.append("NULL")
      n+=1
    text="(" + separador.join(valores) + ")"
    bytes+=len(text)
    f+=1
    # En postgres no funcionan los insert multilínea
    sqlvars['rows']=text
    sql_text="INSERT INTO %(tabla)s (%(fields)s) VALUES %(rows)s;" % sqlvars
    qry_instables=psql.query(sql_text)
    bytes=0
    if (porcentaje+5<=f*100/len(filas)):
      porcentaje=f*100/len(filas)
      print tabla + "(" + str(porcentaje) + "%)"
  