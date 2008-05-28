#!/usr/bin/python
#     encoding: UTF8

# Fichero de parser de MTD y carga de tablas para PyDBa
import pg             # depends - python-pygresql
import _mysql     # depends - python-mysqldb
 
from exmlparser import XMLParser
from pydba_utils import *

# Crea una tabla según las especificaciones del MTD
def create_table(db,table,mtd,existent_fields=[]):
    existent_fields=set(existent_fields)
    txtfields=[]
    typetr={
        'string' : 'character varying',
        'double' : 'double precision',
        'number' : 'integer',
        'int' : 'integer',
        'uint' : 'integer',
        'unit' : 'smallint',
        'stringlist' : 'text',
        'pixmap' : 'text',
        'unlock' : 'boolean',
    }
    constraints=[]
    indexes=[]
    if len(existent_fields)>0:
        mode="alter"
    else:
        mode="create"
        
    for field in mtd.field:
        if not str(field.name) in existent_fields:
            row={}
            row['name']=str(field.name)
            if hasattr(field,"type"):
                row['type']=str(getattr(field,"type"))
            else:
                print("ERROR: No se encontró tipo de datos para "
                        "%s.%s - se asume int." % (table,str(field.name)))
                row['type']="int"
                
            if typetr.has_key(row['type']):
                row['type']=typetr[row['type']]
                
            if hasattr(field,"length") and row['type']=='character varying':
                row['type']+="(%d)" % int(str(field.length))
            row['options']=""
            
            if hasattr(field,"null"):
                if str(field.null)=='false':        
                    row['options']+=" NOT NULL"
                    
                if str(field.null)=='true':        
                    if row['type']=='serial':
                        print "ERROR: Se encontró columna %s serial con NULL. Se omite." % str(field.name)
                    else:
                        row['options']+=" NULL"
                
            if hasattr(field,"pk"):
                if str(field.pk)=='true':
                    if mode=="create":
                        constraints+=["CONSTRAINT %s_pkey PRIMARY KEY (%s)" % (table,row['name'])]
                    else:
                        print("ERROR: Cannot alter table to add '%s' as a primary key!."
                                    " Delete table '%s' and try again." % (row['name'],table))
            
            if hasattr(field,"relation"):
                for relation in field.relation:
                    if hasattr(relation,"card"):
                        if relation.card=='M1':
                            if row['type']=="character varying":
                                indexes+=["CREATE INDEX %s_%s_m1_idx" 
                                        " ON %s USING btree (%s text_pattern_ops);" 
                                        % (table,row['name'],table,row['name'])]
                                indexes+=["CREATE INDEX %s_%sup_m1_idx" 
                                        " ON %s USING btree (upper(%s::text) text_pattern_ops);" 
                                            % (table,row['name'],table,row['name'])]
                            else:
                                indexes+=["CREATE INDEX %s_%s_m1_idx" 
                                          " ON %s USING btree (%s);" 
                                          % (table,row['name'],table,row['name'])]
                    else:
                        print("WARNING: %s.%s has one relation without "
                                "'card' tag" % (table,row['name']))
                
            txtfields+=["\"%(name)s\" %(type)s %(options)s" % row]
    
    if mode=="create":
        txtfields+=constraints
        txtcreate="CREATE TABLE %s (%s) WITHOUT OIDS; %s" % (table, ",\n".join(txtfields), "\n".join(indexes))
        try:
            db.query(txtcreate)
        except:
            print txtcreate
            raise            
    
        
        
def load_mtd(options,db,table,file_mtd):
    mtd_parse=XMLParser()
    mtd_parse.parseText(file_mtd)
    mtd=mtd_parse.root.tmd
    if str(mtd.name)!=table:
        print "WARNING: Expected '%s' in MTD name but found '%s' ***" % (table,str(mtd.name))
    
    qry_columns=db.query("select * from information_schema.columns WHERE table_schema = 'public' AND table_name='%s'" % table)
    #aux_columns
    tablefields={}
    aux_columns=qry_columns.dictresult()
    for column in aux_columns:
        tablefields[column['column_name']]=column
        
    if len(tablefields)==0:
        if (options.debug):
            print "Creando tabla '%s' ..." % table
        create_table(db,table,mtd)
    else:
        
        for field in mtd.field:
            name=str(field.name)
            if not tablefields.has_key(name):
                print "ERROR: La columna '%s' no existe en la tabla '%s'" % (name,table)
            #print "*****"
            #for atr in dir(field):
            #    if (atr[0]!='_'):
            #        print "%s : '%s'" % (atr,getattr(field,atr))
            

