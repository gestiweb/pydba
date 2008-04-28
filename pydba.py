#!/usr/bin/python
#   encoding: UTF8

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
    for tabla in items_seccion:
      if tabla[1]=="Yes":
        tablas.append(tabla[0])
        
  #Si la sección empieza por db.
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
qry_tablas = psql.query(
  "select table_name from information_schema.tables"   
  " where table_schema='public' and table_type='BASE TABLE' and table_name LIKE 'fl%%'")
  
# Guardamos el resultado de la consulta anterior en la variable
tupla_tablas=qry_tablas.getresult()


# Añadimos valores de tupla_tablas a tablas
for tupla in tupla_tablas:
  tablas.append(tupla[0])

  
for tabla in tablas:
  print tabla
  qry_deltables= psql.query("delete from %s" % tabla)
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
    
  
