#!/usr/bin/python
#   encoding: UTF8

# import ConfigParser
import pg
import ConfigParser

ini = ConfigParser.ConfigParser()
ini.readfp(open('replicas.ini'))

secciones=ini.sections()

for seccion in secciones:
  #print "* " + seccion
  items_seccion=ini.items(seccion)
  tsec=seccion.split(".")
  tipoSeccion=tsec[0]
  nombreSeccion=tsec[1]
  if tipoSeccion=="mod":
     
    for tabla in items_seccion:
      if tabla[1]=="Yes":
        print tabla[0] + " => " + tabla[1]
  
  if tipoSeccion=="db":
    configdb={}
    for item in items_seccion:
      configdb[item[0]]=item[1]
    
    if nombreSeccion=="origen":
       configdborigen = configdb
       
    if nombreSeccion=="destino":
       configdbdestino = configdb
       



psql = pg.connect(
            dbname=configdbdestino['dbname'], 
            port=int(configdbdestino['port']),
            host=configdbdestino['host'], 
            user=configdbdestino['user'], 
            passwd=configdbdestino['passwd']
            )
conectbd = pg.connect(
            dbname=configdborigen['dbname'], 
            port=int(configdborigen['port']),
            host=configdborigen['host'], 
            user=configdborigen['user'], 
            passwd=configdborigen['passwd'])

qry_tablas = psql.query(
  "select table_name from information_schema.tables"   
  " where table_schema='public' and table_type='BASE TABLE' and table_name LIKE 'fl%%'")

tupla_tablas=qry_tablas.getresult()
tablas=[]

for tupla in tupla_tablas:
  tablas.append(tupla[0])

#qry_deltables= psql.query("delete from %s" % tabla)
  
for tabla in tablas:
  print tabla
  #qry_deltables= psql.query("delete from %s" % tabla)
  qry_seltables= conectbd.query("select * from %s" % tabla)
  filas=qry_seltables.getresult()
  if(tabla == 'flfiles' or tabla == 'flmodules'):
    for fila in filas:
      valores={}
      n=0
      for campo in fila:
        if (campo is not None):
          valores[str(n)]="'" + pg.escape_string(campo) + "'"
        else:
          valores[str(n)]="NULL"
        n+=1
        
      valores["tabla"]=tabla
      if tabla == 'flfiles':  
        sql_text="insert into %(tabla)s (nombre, bloqueo , idmodulo , sha , contenido) values(%(0)s,%(1)s,%(2)s,%(3)s,%(4)s);" % valores
      
      if tabla == 'flmodules':  
        sql_text="insert into %(tabla)s (bloqueo , idmodulo , idarea , descripcion , version, icono ) values(%(0)s,%(1)s,%(2)s,%(3)s,%(4)s,%(5)s);" % valores
      # print sql_text
      #qry_instables=psql.query(sql_text)
      
  else:
    print tabla
    #psql.inserttable(tabla,filas)


