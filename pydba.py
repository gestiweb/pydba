#!/usr/bin/python
#   encoding: UTF8

# import ConfigParser
import pg

psql = pg.connect(dbname='x_trasluz2', host='192.168.3.13', user='gestiweb', passwd='')
conectbd = pg.connect(dbname='x_trasluz', host='192.168.3.13', user='gestiweb', passwd='')
#dbsql = pg.DB(dbname='x_trasluz3', host='192.168.3.13', user='gestiweb', passwd='')

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
  qry_deltables= psql.query("delete from %s" % tabla)
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
      qry_instables=psql.query(sql_text)
      
  else:
    psql.inserttable(tabla,filas)


