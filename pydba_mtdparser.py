#!/usr/bin/python
# -*- coding: utf-8 -*-
#     encoding: UTF8

# Fichero de parser de MTD y carga de tablas para PyDBa
import pg             # depends - python-pygresql
import _mysql     # depends - python-mysqldb
import traceback
import os
import re
import sys

from pydba_utils import *



class MTDParser_data:
    name=""
    alias=""
    null=False
    dtype="integer"
    length=0
    pk=False
    has_relations_1m=False
    has_relations_m1=False
    

class MTDParser:
    typetr={
        'string'    : 'character varying',
        'double'    : 'double precision',
        'number'    : 'integer',
        'int'       : 'integer',
        'uint'      : 'integer',
        'unit'      : 'smallint',
        'stringlist': 'text',
        'pixmap'    : 'text',
        'unlock'    : 'boolean',
        'serial'    : 'serial',
        'bool'      : 'bool',
        'date'      : 'date',
        'time'      : 'time',
    }
    nonbasic_types= [ 'pixmap' ]
    
    def __init__(self):
        self.field={}
        self.basic_fields=[]
        self.nonbasic_fields=[]
        self.primary_key=[]
        self.child_tables=[]
        
    def check_field_attrs(self,field,table):
        tfield=MTDParser_data()
        name=getattr(field,"name","noname")
        global Tables
        if not hasattr(field,"name"):
            field.name='no_name'
            print "ERROR: Field name ommitted in MTD %s.%s" % (table,name)
            
        if not hasattr(field,"alias"):
            field.alias=str(field.name)
            print "ERROR: Field alias ommitted in MTD %s.%s" % (table,name)
            
        if not hasattr(field,"type_"):
            field.type_='uint'
            print "ERROR: Field type ommitted in MTD %s.%s" % (table,name)
            
        if not hasattr(field,"length"):
            field.length='0'
            if str(field.type_)=='string':
                field.length='32'
                if hasattr(field,"optionslist"):
                    options=str(field.optionslist).split(";")
                    maxlen=1
                    for option in options:
                        if len(option)>maxlen:
                            maxlen=len(option)
                    
                    field.length=str(maxlen+8)
                else:
                    print "ERROR: Field length ommitted and %s.%s is string." % (table,name)
                
        if not hasattr(field,"pk"):
            field.pk='false'
            print "ERROR: Field pk ommitted in MTD %s.%s" % (table,name)
        
        if not hasattr(field,"null"):
            field.null='true'  # AbanQ permite omitir NULL.
        
        
        if hasattr(field,"relation"):
            for relation in field.relation:
                relation_is_ok=True
                if not hasattr(relation,"table"):
                    print "ERROR: Relation table ommited in Field %s.%s." % (table,name)
                    relation_is_ok=False
                if not hasattr(relation,"field"):
                    print "ERROR: Relation field ommited in Field %s.%s." % (table,name)
                    relation_is_ok=False
                if not hasattr(relation,"card"):
                    print "ERROR: Relation card ommited in Field %s.%s." % (table,name)
                    relation_is_ok=False
                if relation_is_ok:
                    if str(relation.card)=="1M":
                        tfield.has_relations_1m=True
                    elif str(relation.card)=="M1":
                        tfield.has_relations_m1=True
                        # print "Relation field %s.%s -> %s.%s" % (table, name, relation.table,relation.field)
                        self.child_tables.append({"ntable" : str(table), "nfield" : str(name), "table" : str(relation.table), "field" : str(relation.field) })
                    else:
                        print "ERROR: Relation card unknown '%s' in Field %s.%s." % (str(relation.card),table,name)
                                        
        tfield.name=str(field.name)
        tfield.alias=str(field.alias)
        
        if not self.typetr.has_key(str(field.type_)):
            print "ERROR: Unknown field type '%s' in Field %s.%s." % (str(field.type_),table,name)
        else:
            tfield.dtype=self.typetr[str(field.type_)]
        
        
        
        if str(field.null)=='false':
            tfield.null=False
        elif str(field.null)=='true':
            tfield.null=True
        else:
            tfield.null=True
            print "ERROR: Unknown null '%s' in Field %s.%s." % (str(field.null),table,name)
            
        if str(field.pk)=='false':
            tfield.pk=False
        elif str(field.pk)=='true':
            tfield.pk=True
            self.primary_key+=[tfield.name]
        else:
            tfield.pk=False
            print "ERROR: Unknown pk '%s' in Field %s.%s." % (str(field.pk),table,name)
        
        if tfield.pk and tfield.null:
            print "WARN: Field %s.%s is primary key and can't be null." % (table,name)
            tfield.null = False
            
        
        
        if tfield.dtype=='character varying':
            tfield.length=int(str(field.length))
            if (tfield.length<=0):
                print "ERROR: Invalid length '%s' in Field %s.%s." % (str(field.length),table,name)
                
                
            
            
        if not tfield.name.islower():
            tfield.name+="_mtd_uppercased"
        
        if str(field.type_) in self.nonbasic_types:
            self.nonbasic_fields.append(tfield.name)
        else:
            self.basic_fields.append(tfield.name)
            
        return tfield
        
    
    def parse_mtd(self,mtd):
        self.field={}
        self.primary_key=[]        
        self.child_tables=[]        
        self.name = mtd.name
        for field in mtd.field:
            tfield=self.check_field_attrs(field,mtd.name)
            self.field[tfield.name]=tfield
            
        

# Crea una tabla según las especificaciones del MTD
def create_table(db,table,mtd,existent_fields=[],oldtable=None):
    existent_fields=set(existent_fields)
    txtfields=[]
    typetr={
        'string'    : 'character varying',
        'double'    : 'double precision',
        'number'    : 'integer',
        'int'       : 'integer',
        'uint'      : 'integer',
        'unit'      : 'smallint',
        'stringlist': 'text',
        'pixmap'    : 'text',
        'unlock'    : 'boolean',
    }
    constraints=[]
    indexes=[]
    drops=[]
    if len(existent_fields)>0:
        mode="alter"
    else:
        mode="create"
        
    for field in mtd.field:
        if not str(field.name) in existent_fields:
            row={}
            row['name']=str(field.name)
            if hasattr(field,"type_"):
                row['type']=str(getattr(field,"type_"))
            else:
                print("ERROR: No se encontró tipo de datos para "
                        "%s.%s - se asume int." % (table,str(field.name)))
                row['type']="int"
                
            if typetr.has_key(row['type']):
                row['type']=typetr[row['type']]
                
            if hasattr(field,"length") and row['type']=='character varying':
                length=int(str(field.length))
                if length==0:
                    print "ERROR: Se encontró una longitud 0 para una columna string %s.%s" % (table, str(field.name))
                    length=32
                row['type']+="(%d)" % length
            row['options']=""
            
            if hasattr(field,"null"):
                if str(field.null)=='false' and not hasattr(field,"calculated"):        
                    row['options']+=" NOT NULL"
                    
                if str(field.null)=='true':        
                    if row['type']=='serial':
                        print "ERROR: Se encontró columna %s serial con NULL. Se omite." % str(field.name)
                    else:
                        row['options']+=" NULL"
            else:
                if row['type']=='serial':
                    print "ERROR: Se encontró columna %s serial con NULL. Se omite." % str(field.name)
                else:
                    row['options']+=" NULL"
                            
            if row['type']=='character varying':
                index_adds="text_pattern_ops"
            else:                    
                index_adds=""
            
            if hasattr(field,"pk"):
                if str(field.pk)=='true':
                    if mode=="create":
                        #drops.append("DROP INDEX %s_%s_m1_idx CASCADE;" % (table,row['name']))
                        #drops.append("DROP INDEX %s_%sup_m1_idx CASCADE;" % (table,row['name']))
                        # if oldtable: drops.append("ALTER TABLE %s DROP CONSTRAINT %s_pkey CASCADE;" % (oldtable,table))
                        indexes+=["CREATE INDEX %s_%s_m1_idx" 
                                " ON %s USING btree (%s %s);" 
                                % (table,row['name'],table,row['name'],index_adds)]
                        indexes+=["CREATE INDEX %s_%sup_m1_idx" 
                                " ON %s USING btree (upper(%s::text) %s);" 
                                    % (table,row['name'],table,row['name'],index_adds)]
                        import random
                        rn = random.randint(100,999)
                        constraints+=["CONSTRAINT %s_pkey_%d PRIMARY KEY (%s)" % (table,rn,row['name'])]
                    else:
                        print("ERROR: Cannot alter table to add '%s' as a primary key!."
                                    " Delete table '%s' and try again." % (row['name'],table))
            index_loaded=False
            if hasattr(field,"relation"):
                for relation in field.relation:
                    if hasattr(relation,"card"):
                        if str(relation.card)=='M1' and index_loaded==False:
                            index_loaded=True
                            #drops.append("DROP INDEX %s_%s_m1_idx CASCADE;" % (table,row['name']))
                            #drops.append("DROP INDEX %s_%sup_m1_idx CASCADE;" % (table,row['name']))
                            indexes+=["CREATE INDEX %s_%s_m1_idx" 
                                    " ON %s USING btree (%s %s);" 
                                    % (table,row['name'],table,row['name'],index_adds)]
                            indexes+=["CREATE INDEX %s_%sup_m1_idx" 
                                    " ON %s USING btree (upper(%s::text) %s);" 
                                        % (table,row['name'],table,row['name'],index_adds)]
                    else:
                        print("WARNING: %s.%s has one relation without "
                                "'card' tag" % (table,row['name']))
                
            txtfields+=["\"%(name)s\" %(type)s %(options)s" % row]
    
    if mode=="create":
        for drop in drops:
            try:
                db.query(drop)
            except:
                pass
                #print "ERROR:", drop , " .. execution failed:"
                #import sys
                #traceback.print_exc(file=sys.stdout)
                #print "-------"
                
        txtfields+=constraints
        txtcreate="CREATE TABLE %s (%s) WITHOUT OIDS;" % (table, ",\n".join(txtfields))
        
        
        try:
            db.query(txtcreate)
        except:
            print txtcreate
            raise            
        
        for index in indexes:
            try:
                split_index=index.split(" ")
                index_name=split_index[2]
                # Borrar primero el índice si existe
                qry_indexes = db.query("""
        SELECT pc2.relname as indice
        FROM pg_class pc2 
        WHERE pc2.relname = '%s'
                """ % index_name)
                dt_indexes=qry_indexes.dictresult() 
                for fila in dt_indexes:
                    db.query("DROP INDEX %s;" % fila['indice'])
                
                db.query(index)
            except:
                print index
                import sys
                traceback.print_exc(file=sys.stdout)
    
                
        
Tables={}        
        
        
def load_mtd(options,odb,ddb,table,mtd_parse):
    if options.verbose:
        print "Analizando tabla %s . . ." % table
    fail = False
    mtd=mtd_parse.root.tmd
    if table!=table.lower():
        print "ERROR: Table name '%s' has uppercaps" % table
        table=table.lower()
    if str(mtd.name)!=table:
        print "WARNING: Expected '%s' in MTD name but '%s' found ***" % (table,str(mtd.name))
    
    qry_columns=ddb.query("SELECT * from information_schema.columns"
                " WHERE table_schema = 'public' AND table_name='%s'" % table)
    #aux_columns
    tablefields={}
    aux_columns=qry_columns.dictresult()
    for column in aux_columns:
        if not column['column_name'].islower():
            column['column_name']+="_ddb_uppercased"
        
        tablefields[column['column_name']]=column
    
    qry_columns2=odb.query("select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE' and table_name='%s'" % table)
    origin_tablefields=[]
    
    origin_fielddata={}
    
    if len(qry_columns2.dictresult())==1:
        qry_columns2=odb.query("SELECT * from information_schema.columns"
                    " WHERE table_schema = 'public' AND table_name='%s'" % table)
        #aux_columns
        aux_columns2=qry_columns2.dictresult()
        for column in aux_columns2:
            if not column['column_name'].islower():
                column['column_name']+="_odb_uppercased"
            column['pk']=False
            origin_tablefields.append(column['column_name'])
            origin_fielddata[column['column_name']]=column
            
    
    global Tables
    mparser=MTDParser()
    mparser.parse_mtd(mtd)
    Tables[table]=mparser
    if len(tablefields)==0:
        sql = """SELECT * from information_schema.tables 
            WHERE table_schema = 'public' AND table_name ~ '^%s_[0-9]{12}[0-9a-f]{2}$'""" % table
        qry_columns2=odb.query(sql)
        aux_columns2=qry_columns2.dictresult()
        if len(aux_columns2):
            print "ERROR GRAVE: Ya existe un backup de la tabla %s pero la tabla no existe! Revise esto manualmente." % table
            return False
            
        if (options.debug):
            print "Creando tabla '%s' ..." % table
        create_table(ddb,table,mtd)
        return True

    # La tabla existe, hay que ver cómo la modificamos . . . 

    qry_otable_count = odb.query("SELECT COUNT(*) as n from %s" % table)
    dict_otable_count = qry_otable_count.dictresult()
    old_rows = int(dict_otable_count[0]["n"])
    old_pkey = None
    new_pkey = None
    qry_columns3=odb.query("SELECT column_name from information_schema.key_column_usage"
                " WHERE table_schema = 'public' AND table_name='%s' AND ordinal_position = 1" % table)

    aux_columns3=qry_columns3.dictresult()
    for column in aux_columns3:
        old_pkey = column["column_name"]
        

    Regenerar=options.rebuildtables
    old_fields = []
    new_fields = []
    
    for field in mtd.field:
        name=str(field.name)
        mfield = mparser.field[name]
        if mfield.pk: new_pkey = name
        new_fields.append(name)
        
        if not tablefields.has_key(name):
            if mfield.pk:
                if options.verbose:
                    print "Regenerar: La tabla '%s' ha cambiado de primary key de %s a %s! (con %d filas)" % (table,repr(old_pkey),repr(name),old_rows)
                Regenerar=True
                
                #print "Abortando, no se puede regenerar esta tabla."
                #return False
                old_fields.append(old_pkey)
            else:
                if options.verbose:
                    print "Regenerar: La columna '%s' no existe (aun) en la tabla '%s'" % (name,table)
                Regenerar=True
                default_value = "NULL"
                
                if not mfield.null:
                    if mfield.dtype in ["serial","integer","smallint","double precision"]:
                        default_value = "0"
                    elif mfield.dtype in ["boolean","bool"]:
                        default_value = "false"
                    elif mfield.dtype in ["character varying","text"]:
                        default_value = "''"
                    elif mfield.dtype in ["date"]:
                        default_value = "'2001-01-01'"
                    elif mfield.dtype in ["time"]:
                        default_value = "'00:00:01'"
                    else:
                        default_value = "'0'"
                    
                    
                
                old_fields.append(default_value)
                
        else:
            old_fields.append(name)

            null=origin_fielddata[name]["is_nullable"]
            if null=="YES": null=True
            if null=="NO": null=False

            dtype=origin_fielddata[name]["data_type"]
            mfielddtype = mfield.dtype
            length=origin_fielddata[name]["character_maximum_length"]
            
            if length == None: length = 0
            if mfielddtype == "serial": mfielddtype = "integer"
            if mfielddtype == "bool": mfielddtype = "boolean"
            if dtype == "serial": dtype = "integer"
            if dtype[:4] == "time": dtype = "time" 

            #print null, dtype, length

            if null != mfield.null and null == False: 
                if options.verbose:
                    print "Regenerar: La columna '%s' en la tabla '%s' ha establecido null de %s a %s" % (name,table,null,mfield.null)
                Regenerar=True

            if dtype != mfielddtype: 
                if options.verbose:
                    print "Regenerar: La columna '%s' en la tabla '%s' ha cambiado el tipo de datos de %s a %s" % (name,table,dtype,mfielddtype)
                Regenerar=True

            if length < mfield.length: 
                if options.verbose:
                    print "Regenerar: La columna '%s' en la tabla '%s' ha cambiado el tamaño de %s a %s" % (name,table,length,mfield.length)
                Regenerar=True

            if length > mfield.length: 
                mfield.length = length
          
          
          
          #print origin_fielddata[name]
        
    if len(origin_tablefields)>0:
        for field in reversed(mparser.basic_fields):
            name=field
            if not name in origin_tablefields:
                #print "ERROR: La base de datos de origien no tiene la columna '%s' en la tabla '%s'" % (name,table)
                try:
                    mparser.basic_fields.remove(name)
                except:
                    pass
    else:
        #print "ERROR: La BD origien no tiene la tabla '%s'" % (table)
        mparser.basic_fields=[]
        #print "*****"
        #for atr in dir(field):
        #    if (atr[0]!='_'):
        #        prin create_table(db,table,mtd)t "%s : '%s'" % (atr,getattr(field,atr))
        
    
    if len(mparser.basic_fields)==0:
        # Si no hay campos que pasar, generamos la tabla de cero.
        Regenerar=True
    
    
        
    if Regenerar:
        primarykey = None
        for pkey in mparser.primary_key:
            tfield=mparser.field[pkey]
            primarykey = pkey

        if not primarykey or not old_pkey:
            print "*** ERROR: La tabla %s no tiene primarykey, no puede ser regenerada." % (table)
            Regenerar = False

    if options.loadbaselec and table == "baselec" and os.path.isfile(options.loadbaselec):
        print "Iniciando volcado de Baselec ****"
        import sys
        #sys.stdout.flush()
        print "Vaciando tabla . . . "
        sys.stdout.flush()
        try:
          ddb.query("DELETE FROM %s" % (table))
        except:
          print "No se pudo vaciar la tabla."
          return
        #print "done"
        Regenerar = True

     
    if options.getdiskcopy and len(mparser.basic_fields)>0 and len(mparser.primary_key)>0:
        Regenerar = True
        
        
        
    if Regenerar:
        try:       
            print "Regenerando tabla %s (%d filas)" % (table,old_rows)
            data = None
            # Borrar primero los índices (todos) que tiene la tabla:
            if False: # Anulado, porque es mejor hacer un DROP CASCADE
                qry_indexes = ddb.query("""
        SELECT pc.relname as tabla , pc2.relname as indice,pi.indkey as vector_campos
        FROM pg_class pc 
        INNER JOIN pg_index pi ON pc.oid=pi.indrelid 
        INNER JOIN pg_class pc2 ON pi.indexrelid=pc2.oid
        WHERE NOT pi.indisprimary AND NOT pi.indisunique
        AND pc.relname = '%s'
                """ % table)
                dt_indexes=qry_indexes.dictresult() 
                for fila in dt_indexes:
                    ddb.query("DROP INDEX %s;" % fila['indice'])
        
            import datetime, random
            now = datetime.datetime.now()
            nseed = random.randint(0,255)
        
            newnametable = "%s_%04d%02d%02d%02d%02d%02x" % (table,now.year,now.month,now.day,now.hour,now.minute, nseed)
            fail = False
            try:
              ddb.query("ALTER TABLE %s RENAME TO %s" % (table,newnametable))
            except:
              print "No se pudo renombrar la tabla."
              return
            primarykey = None
            for pkey in mparser.primary_key:
                tfield=mparser.field[pkey]
                primarykey = pkey
                if tfield.dtype=='serial':
                    desired_serial = "%s_%s_seq" % (table, tfield.name)
                    try:
                        # ALTER SEQUENCE - RENAME TO - solo esta disponible  a partir de psql 8.3
                        # ALTER TABLE - RENAME TO - es compatible y funciona desde 8.0 o antes
                        qry_serial=ddb.query("ALTER TABLE %s RENAME TO %s" % (desired_serial, desired_serial+str(random.randint(100,100000))))
                    except:
                        pass
            try:
              create_table(ddb,table,mtd,oldtable=newnametable)
            except:
              fail = True
              print "ERROR: Se encontraron errores graves al crear la tabla %s" % table
              why = traceback.format_exc()
              print "**** Motivo:" , why
            if options.loadbaselec and table == "baselec" and os.path.isfile(options.loadbaselec):
                print "Tabla Baselec encontrada."
            elif options.getdiskcopy and len(mparser.basic_fields)>0 and len(mparser.primary_key)>0:
                # Generar comandos copy si se especifico
                print "Cargando desde .dat"
                primarykey = mparser.primary_key[0]
                fields = ', '.join(mparser.basic_fields)
                try:
                    sql = "COPY %s (%s) FROM '/tmp/psqldiskcopy/%s.dat'" % (table, fields, table)
                    qry = ddb.query(sql)
                except:
                    print "Error al cargar datos."
                    print traceback.format_exc()
                    print "--------------"
                
        
        
            else:
                sqlDict = {
                    "new_table" : table,
                    "old_table" : newnametable,
                    "new_fields" : ",".join(new_fields),
                    "old_fields" : ",".join(old_fields),
                }
            
                sql = """INSERT INTO %(new_table)s (%(new_fields)s) 
                    SELECT %(old_fields)s FROM %(old_table)s ;
                """ % sqlDict
                fail1 = False
                try:
                    ddb.query(sql)
                except:
                    fail1 = True
                    print "Error al intentar regenerar por SQL la tabla %s:" % table
                    print traceback.format_exc()
                    print "Se intentará hacer manualmente."
            
                if not fail1:
                    try:
                        rows1 = old_rows
                        qry_otable_count = odb.query("SELECT COUNT(*) as n from %s" % table)
                        dict_otable_count = qry_otable_count.dictresult()
                        new_rows = int(dict_otable_count[0]["n"])
                        rows2 = new_rows
                    except:
                        print "Error al calcular el tamaño de las tablas."
                        print traceback.format_exc()
                        fail1 = True
            
                    if rows1 != rows2:
                        print "WARN: Las filas en la nueva tabla (%d) no coinciden con la original (%d). Se intentará manualmente." % (rows2,rows1)
                        fail1 = True
            
                
                if fail1:
            
                    try:
                        data=export_table(options,ddb,newnametable,"*",old_pkey,new_pkey)
                    except:
                        fail = True
        
                    if old_rows>200 or options.debug:
                        print "Regenerando tabla %s (%d filas)  ... " % (table, old_rows)
                    try:
                        log = auto_import_table(options,ddb,table,data,mparser.field, pkey = primarykey)
                    except:
                        fail = True
                        print traceback.format_exc()
            
                    del data # borrar datos para no acumularlos en memoria!

                    if options.debug:
                        print "finalizo la insercion de %d filas en %s" % (old_rows, table)
            
                    if len(log):
                        fail = True
                        print "ERROR: No se pudieron crear %d filas en la tabla %s" % (len(log), table)
                        print " **** Exite un backup de los datos originales en la tabla %s " % newnametable
                        filelog = open("/tmp/%s.log.sql" % newnametable,"w")
                        for line in log:
                            try:
                                why = line["*why*"]
                                del line["*why*"]
                                fields=[]
                                values=[]
                                for field,value in line.iteritems():
                                    if field[0]!='#' and field in mparser.field:
                                        fields.append(field)
                                        ivalue=sql_formatstring(value,mparser.field[field])
                                        values.append(ivalue)
                
                                sql="INSERT INTO %s (%s) VALUES(%s);" % (table, ", ".join(fields), ", ".join(values))
                                print >> filelog , sql
                            except:
                                why = traceback.format_exc()
                                print "**** Error al generar el sql de backup:" , why
                    
                            #print >> filelog , "/* motivo:"
                            #print >> filelog , why
                            #print >> filelog , "*/"
                    
                        filelog.close()
                        print " **** Se ha guardado un registro en formato SQL con las filas que faltan por migrar en /tmp/%s.log.sql" % newnametable
                    
                    try:
                        rows1 = old_rows
                        qry_otable_count = odb.query("SELECT COUNT(*) as n from %s" % table)
                        dict_otable_count = qry_otable_count.dictresult()
                        new_rows = int(dict_otable_count[0]["n"])
                        rows2 = new_rows
                    except:
                        print "Error al calcular el tamaño de las tablas."
                        print traceback.format_exc()
                        fail = True
            
                    if rows1 != rows2:
                        print "ERROR: Las filas en la nueva tabla (%d) no coinciden con la original (%d). Se deshace el cambio." % (rows2,rows1)
                        fail = True
        except:
            print "Error INESPERADO regenerando tabla: %s" % table
            print traceback.format_exc()
            fail = True
        
            
        if fail:
            try:
              ddb.query("ALTER TABLE %s RENAME TO %s_new;" % (table,newnametable))
            except:
              print "No se pudo renombrar la tabla nueva."
              pass  

            try:
              ddb.query("ALTER TABLE %s RENAME TO %s;" % (newnametable,table))
            except:
              print "No se pudo renombrar la tabla."
              pass  
            
            return           
        else:
            try:
              ddb.query("DROP TABLE %s CASCADE;" % (newnametable))
              ddb.query("VACUUM ANALYZE %s;" % (table))
            except:
              print "No se pudo borrar la tabla de backup."
              pass  
               
    if options.diskcopy and len(mparser.basic_fields)>0 and len(mparser.primary_key)>0:
        # Generar comandos copy si se especifico
        primarykey = mparser.primary_key[0]
        fields = ', '.join(mparser.basic_fields)
        f1 = open("/tmp/psqldiskcopy/%s.restore.sql" % table, "w")
        f1.write("-- primary key: %s\n\n" % primarykey)
        f1.write("COPY %s (%s) FROM '/tmp/psqldiskcopy/%s.dat'" % (table, fields, table))
        f1.close()
        
        sql = "COPY (SELECT %s FROM %s ORDER BY %s) TO '/tmp/psqldiskcopy/%s.dat'" % (fields, table, primarykey, table)
        print "Copiando a disco %s . . . " % table
        qry = ddb.query(sql)
        
    # ************************************* BASELEC *****************************************
            
    if options.loadbaselec and table == "baselec" and os.path.isfile(options.loadbaselec):
        import datetime
        
        csv=open(options.loadbaselec,"r")
        print "Contando lineas . . . "
        sys.stdout.flush()
        lineas = -1 # -1 debido a que la primera fila es la cabecera.
        for csvline in csv:
            lineas +=1
        print "%d registros en el fichero. " % lineas
        sys.stdout.flush()
        csv.close()
        
        csv=open(options.loadbaselec,"r")

        csvfields=csv.readline().split("\t")
        for n,field in enumerate(csvfields):
            field = field.replace(" ","")
            field = field.lower()
            field = field.replace("+","mas")
            field = re.sub(r'[^a-z0-9]','',field)
            if field in mparser.field:
              csvfields[n]=field
            else:
              csvfields[n]="* " + field
            
            
        data = []
        to1 = 0

        for csvline in csv:
            csvreg = csvline.split("\t")
            line = {}
            for n, val in enumerate(csvreg):
                fieldname = csvfields[n]
                if fieldname[0]=="*": continue
                try:
                    val=val.decode("cp1252")
                    val=val.encode("utf8")
                    
                except:
                    val=None
                if mparser.field[fieldname].dtype == 'double precision':
                    try:
                        val = float(re.sub(r"[^0-9\.]",'',val))
                    except:
                        val = 0
                elif mparser.field[fieldname].dtype == 'date':
                    try:
                        dt = datetime.strptime(val, "%d/%m/%Y %H:%M:%S")
                        val = dt.isoformat()
                    except:
                        val = None
                
                if val == "": val = None
                line[fieldname]=val
            data.append(line)
            to1 += 1
            if len(data)>=1000:
                from1 = to1 - len(data)
                pfrom1 = from1 * 100.0 / lineas
                pto1 = to1 * 100.0 / lineas
                
                print "@ %.2f %% Copiando registros %d - %d . . ." % (pfrom1, from1+1, to1+1)
                
                #sys.stdout.write('.')
                sys.stdout.flush()
                # print "Copiando registros %.1f%% - %.1f%% . . ." % (pfrom1, pto1)
                # auto_import_table(options,ddb,table,data,mparser.field,mparser.primary_key[0])    
                import_table(options,ddb,table,data,mparser.field)
                data = []                              

        from1 = to1 - len(data)
        pfrom1 = from1 * 100.0 / lineas
        pto1 = to1 * 100.0 / lineas

        print "@ %.2f %% Copiando registros %d - %d . . ." % (pfrom1, from1+1, to1+1)
        import sys
        sys.stdout.flush()
        #auto_import_table(options,ddb,table,data,mparser.field,mparser.primary_key[0])    
        import_table(options,ddb,table,data,mparser.field)
        data = []                              

        csv.close()
                        
          
          
    # SINCRONIZACION MAESTRO>ESCLAVO      
    if options.odb != options.ddb : # TODO: Aquí falta comparar también los puertos y IP's.
        import sha
        mparser.basic_fields.sort()
        try:
            origin_data=export_table(options,odb,table,mparser.basic_fields)
        except:
            print mparser.basic_fields
            raise
        origin_rows={}
        dest_rows={}
        update_rows={}
        origin_fields=mparser.basic_fields
        if len(origin_data)>0: origin_fields=origin_data[0].keys()
        
        if len(mparser.primary_key)==1:
            pkey=mparser.primary_key[0]
        else:
            print "ERROR: La tabla %s no tiene PK o tiene más de uno! " % table
        origin_fields.sort()
        for row in origin_data:
            row_hash=sha.new(repr(row)).hexdigest()
            row['#hash']=row_hash
            origin_rows[row[pkey]]=row
        
        dest_data=export_table(options,ddb,table,origin_fields)
        for row in dest_data:
            row_hash=sha.new(repr(row)).hexdigest()
            row['#hash']=row_hash
            if origin_rows.has_key(row[pkey]):
                if origin_rows[row[pkey]]['#hash']!=row_hash:
                    update_rows[row[pkey]]={}
                    for field,value in origin_rows[row[pkey]].iteritems():
                        # value=origin_rows[row[pkey]][field]
                        if value!=row[field]:
                            update_rows[row[pkey]][field]=value
            
                del origin_rows[row[pkey]]
            else:
                dest_rows[row[pkey]]=row
        
        if len(origin_rows)>0 or len(dest_rows)>0 or len(update_rows)>0:
            # Aplicar filtros aquí: ------
            
            # -----------------------------
            
            # Origin Row -> Insertar nueva fila;
            for pk,row in origin_rows.iteritems():
                fields=[]
                values=[]
                try:
                    for field,value in row.iteritems():
                        if field[0]!='#':
                            fields.append(field)
                            ivalue=sql_formatstring(value,mparser.field[field])
                            values.append(ivalue)
                
                    sql="INSERT INTO %s (%s) VALUES(%s)" % (table, ", ".join(fields), ", ".join(values))
                    try:
                        ddb.query(sql)
                    except:
                        print "Error al ejecutar la SQL: " + sql
                        raise
                except:
                    print "Cannot insert: ", repr(row)
                    raise
            
            # Dest Row -> Borrar fila;
            for pk,row in dest_rows.iteritems():
                try:
                    sql="DELETE FROM %s  WHERE %s = %s" % (table, pkey, sql_formatstring(pk,mparser.field[pkey]))
                    try:
                        ddb.query(sql)
                    except:
                        print "Error al ejecutar la SQL: " + sql
                except:
                    print "Cannot delete: ",repr(row)
                    raise
            
            
            # Update Rows -> Actualizar fila;
            for pk,row in update_rows.iteritems():
                values=[]
                try:
                    for field,value in row.iteritems():
                        if field[0]!='#':
                            ivalue=sql_formatstring(value,mparser.field[field])
                            values.append("%s = %s" % (field,ivalue))
                
                    sql="UPDATE %s SET %s WHERE %s = %s" % (table, ", ".join(values), pkey, sql_formatstring(pk,mparser.field[pkey]))
                    try:
                        ddb.query(sql)
                    except:
                        print "Error al ejecutar la SQL: " + sql
                except:
                    print "Cannot update: ",repr(row)
                    raise
            
            
            print  table, len(origin_rows),len(dest_rows),len(update_rows)
          
    import random
    try:
        for pkey in mparser.primary_key:
            tfield=mparser.field[pkey]
            if tfield.dtype=='serial':
                qry_serial=ddb.query("SELECT pg_get_serial_sequence('%s', '%s') as serial" % (table, tfield.name))
                dr_serial=qry_serial.dictresult()
                for dserial in dr_serial:
                    serial=dserial['serial']
                    if serial:
                        desired_serial = "%s_%s_seq" % (table, tfield.name)
                        if serial != "public." + desired_serial:
                            print "WARNING: Sequence does not match desired name: %s != %s " % (serial, desired_serial)
                            try:
                                # ALTER SEQUENCE - RENAME TO - solo esta disponible  a partir de psql 8.3
                                # ALTER TABLE - RENAME TO - es compatible y funciona desde 8.0 o antes
                                qry_serial=ddb.query("ALTER TABLE %s RENAME TO %s" % (desired_serial, desired_serial+str(random.randint(100,100000))))
                            except:
                                pass
                            try:
                                qry_serial=ddb.query("ALTER TABLE %s RENAME TO %s" % (serial, desired_serial))
                                serial = desired_serial
                                print "INFO: Sequence renamed succefully to %s " % serial
                            except:
                                pass
                                
                            
                        qry_maxserial=ddb.query("SELECT MAX(%s) as max FROM %s" % (tfield.name,table))
                        max_serial=1
                        dr_maxserial=qry_maxserial.dictresult()
                        for dmaxserial in dr_maxserial:
                            if dmaxserial['max']:
                                max_serial=dmaxserial['max']+1
    
                        ddb.query("ALTER SEQUENCE %s RESTART WITH %d;" % (serial, max_serial))
    except:
        print "PKeys: %s" % mparser.primary_key
        raise
                
    return (not fail)
        
# Comprobar Primary Keys en una tabla:
# SELECT * FROM information_schema.constraint_table_usage WHERE table_name='articulos'
# SELECT * FROM information_schema.key_column_usage
#

# Obtener el nombre del sequence para una columna y tabla dada:
# SELECT pg_get_serial_sequence('lineasalbaranescli', 'idlinea')

# Obtener los índices
# SELECT pc.relname as tabla , pc2.relname as indice,pi.indkey as vector_campos
#    FROM pg_class pc 
#    INNER JOIN pg_index pi ON pc.oid=pi.indrelid 
#    INNER JOIN pg_class pc2 ON pi.indexrelid=pc2.oid
#  WHERE NOT pi.indisprimary AND NOT pi.indisunique
#
# SELECT pg_get_indexdef(indexrelid) FROM pg_catalog.pg_index WHERE indrelid = 'clientes'::regclass;
# 

# REINICIAR LOS CONTADORES!!!

# ALTER SEQUENCE tallasset_id_seq
#   RESTART WITH 8;
        

def export_table(options,db,table, fields=['*'], old_pkey = None, new_pkey = None):
    filas=None
    try:
        sql = "SELECT " + ",".join(fields) + " FROM %s" % table
        qry = db.query(sql)
        filas = qry.dictresult() 
        if old_pkey and new_pkey:
            if old_pkey != new_pkey:
                for row in filas:
                    row[new_pkey] = row[old_pkey]
            
    except:
        print sql
        raise
    return filas

def auto_import_table(options,ddb,table,data,mparser_field,pkey):
    sz = len(data)
    #print "Autoimport",table, sz
    if sz == 0: return [];
    
    if sz > 100:
        half = 50
        if sz > 500: half *= 2
        if sz > 1000: half *= 2
        if sz > 5000: half *= 2
        if sz > 25000: half *= 2
        
        log = []
        lh = 0
        while (lh < sz):
            rh = lh + half
            if rh > sz: rh = sz
        
            if (options.debug):
                print "** Subdividiendo importacion de tabla %s (%d:%d) **" % (table,lh,rh)
            else:
                import sys
                sys.stdout.write('.')
                sys.stdout.flush()
                
            log += auto_import_table(options,ddb,table,data[lh:rh],mparser_field, pkey)
            lh = rh
        if half == 50:  print "|",
        elif half == 100:  print "|"
        elif half > 100:  print "$"
        
        del data
        
        return log
    
    
    try:
        import_table(options,ddb,table,data,mparser_field)
    except:
        pass

    lst_filas = []
    for fila in data:
        lst_filas.append("'" + pg.escape_string(str(fila[pkey])) + "'")
        
    newdata = data[:]
    sz2 = 0
    sql = ""
    try:
        sql = "SELECT %s as pkey FROM %s WHERE %s IN (%s)" % (pkey,table,pkey,", ".join(lst_filas))
        qry = ddb.query(sql)
        cnum = qry.dictresult() 
        for rkval in cnum:
            kval ="'" + pg.escape_string(str(rkval["pkey"])) + "'" 
            try:
                num = lst_filas.index(kval)
                if lst_filas[num] != kval:
                    print "** se esperaba " , kval , " y se encontro " , lst_filas[num] , " en el array de pkeys."
                else:
                    del newdata[num]
                    del lst_filas[num]
            except:
                print " ** Error desconocido ocurrio comparando los primary keys. ** "
                print " kval :: " , kval
                import traceback
                
                why = traceback.format_exc()
                print why
        sz2 =len(cnum)
    except:
        sz2 = 0
        why = ""
        import traceback
        why = traceback.format_exc()
        print "--- Error al contar filas: "
        print why
        print "SQL  :: " , sql[:100]
        print "Pkey :: " , pkey
        print "Table:: " , table
        print "Filas:: " , len(lst_filas)
        print "******"
        
        
    
    
    if sz2 == sz:      
        if len(newdata):
            print "*** INFO: habian %d filas en los datos restantes para insertar, pero no eran necesarios." % len(newdata)
        return []
    nsz = len(newdata)
    if nsz != (sz-sz2):
        print " *** Se esperaba que NSZ fuera (%d - %d) = %d y en realidad vale %d (datos probablemente corruptos) *** " % (sz,sz2,sz-sz2,nsz)
        
    if nsz > 1:
        half = nsz / 2
        log = []
        log += auto_import_table(options,ddb,table,newdata[:half],mparser_field, pkey)
        log += auto_import_table(options,ddb,table,newdata[half:],mparser_field, pkey)
    else:
        why = ""
        import traceback
        why = traceback.format_exc()
        for line in data:
            line["*why*"] = why
            
        log = data
    del newdata

    del data
    return log
        
    
    
def sql_formatstring(value,field,format='i'):
    # Format:
    #  i - fomrato inserción o update
    #  c - formato COPY
    
    # Value:
    #  Valor que queremos representar para la base de datos.
    
    # Field:
    #  Una clase de parseo MTD donde indique el tipo de campo que es.
    
    ivalue=""
    cvalue=""
    if (value is not None):#Si el valor NO es nulo
        ivalue="'" + pg.escape_string(str(value)) + "'"
        cvalue=copy_escapechars(value)
    else:
        # ¿Permite null este campo o es Serial?
        if field.null==True or field.dtype == 'serial': 
            ivalue="NULL"
            cvalue="\\N"
        else:
            # Si no, vamos a los valores por defecto.
            if field.dtype in ["integer","double precision","smallint","boolean","bool"]:
                ivalue="'0'"
                cvalue="0"
            elif field.dtype in ["character varying","text"]:
                ivalue="''"
                cvalue=""
            elif field.dtype in ["date","time","datetime"]:
                ivalue="'1990-01-01 23:50:50'"
                cvalue="1990-01-01 23:50:50"
            else:
                print "NO SE RECONOCE EL TIPO: %s " %  field.dtype
                ivalue="'0'"
                cvalue="0"
    
    
    if format=='i': return ivalue
    if format=='c': return cvalue
    
def import_table(options,db,table,data,nfields):  
    if not len(data):
        return 
    
    sqlarray=[]
    error=False
    
    total_fields=set(nfields.keys())
    selected_fields=set(data[0].keys())
    fields_toadd=total_fields-selected_fields
    
    if len(data)>10:
        db.query("SET client_min_messages = fatal;");            
    else:
        db.query("SET client_min_messages = notice;");            

    for fila in data:
        fields=[]
        values=[]
        copy_values=[]
        sqlvar={}
        for field in fields_toadd:
            fila[field]=None
        for key in fila:
            if nfields.has_key(key):
                _field=nfields[key]
                campo=fila[key]
                fields+=[key]
                values.append(sql_formatstring(campo,nfields[key],'i'))
                copy_values.append(sql_formatstring(campo,nfields[key],'c'))
                            
            
        sqlvar['tabla']=table
        sqlvar['fields']=", ".join(fields)
        sqlvar['values']=", ".join(values)
        sqlarray+=["\t".join(copy_values)]
        
        if len(sqlarray)>2048:
            sql_text="COPY %(tabla)s (%(fields)s) FROM stdin;" % sqlvar
            db.query(sql_text )
            for line in sqlarray:
                db.putline(line+"\n")
            db.putline("\\."+"\n")
            db.endcopy()
            sqlarray=[]
            
    del data
    
    sql_text="COPY %(tabla)s (%(fields)s) FROM stdin;" % sqlvar
    db.query(sql_text)
    for line in sqlarray:
        db.putline(line+"\n")
    db.putline("\\."+"\n")
    db.endcopy()
    sqlarray=[]
    db.query("SET client_min_messages = warning;");            
    
    

def comprobarRelaciones():
    global Tables
    print "Inicio de las comprobaciones de relaciones M1."
    for tablename in Tables:
        table = Tables[tablename]
        for child_table in table.child_tables:
            orig_fk="%s.%s" % (child_table['ntable'],child_table['nfield'])
            dest_pk="%s.%s" % (child_table['table'],child_table['field'])
            try:
              ptable = Tables[child_table['table']]
              fk = table.field[child_table['nfield']]
              pk = ptable.field[child_table['field']]
              error = False
              pkdtype= pk.dtype
              if pkdtype == "serial": pkdtype="integer"
              # if fk.null != pk.null: error = True
              if fk.dtype != pkdtype: error = True
              if fk.length < pk.length: error = True
              # if fk.pk == True: error = True
              # if pk.pk == False: error = True
                
              
              if error:
                print "ERROR: Fallo al comprobar la relación %s -> %s" % (orig_fk,dest_pk)
                # print "null:", fk.null, "|" , pk.null
                print "dtype:", fk.dtype, "|", pk.dtype
                print "length:", fk.length, "|", pk.length
                #print "pk:", fk.pk, "|", pk.pk
            except:
              pass
              #print "warning: Imposible comparar %s -> %s" % (orig_fk,dest_pk)
            
            
    
  

def procesarOLAP():
    global Tables
    print "Inicio del proceso de tablas para OLAP."
    primarykeys=[]
    used_primarykeys=[]
    relations=[]
    fk_relations=[]
    table_nr=[]
    for tablename in Tables:
        table = Tables[tablename]
        for pk in table.primary_key:
            primarykeys.append("%s.%s" % (tablename ,pk))
        
    for tablename in Tables:
        table = Tables[tablename]
        nrelations=0
        for child_table in table.child_tables:
            orig_fk="%s.%s" % (child_table['ntable'],child_table['nfield'])
            dest_pk="%s.%s" % (child_table['table'],child_table['field'])

            if  dest_pk in primarykeys:
                primarykeys.remove(dest_pk)
                used_primarykeys.append(dest_pk)
            
            if dest_pk in used_primarykeys:
                relations.append("%s@%s" % (orig_fk,dest_pk))
                nrelations+=1
            else:
                fk_relations.append("%s@%s" % (orig_fk,dest_pk))
        if nrelations == 0:
            for pk in table.primary_key:
                table_nr.append("%s.%s" % (tablename ,pk))

    primarykeys.sort()
    used_primarykeys.sort()
    relations.sort()
    fk_relations.sort()
    table_nr.sort()
    
    #for pk in primarykeys:
     #   print "Tabla sin heredadas %s" % (pk)
        
    for table in table_nr:
        if table in primarykeys:
            table_nr.remove(table)
        #else:
         #   print "Tabla Padre %s" % table        
    #for relation in fk_relations:
    #    print "Imposible validar relación %s" % (relation)

    print "%d Primary Key no usados." % len(primarykeys)
    
    print "%d Primary Key válidos." % len(used_primarykeys)
    print "%d Relaciones usadas." % len(relations)
    print "%d Relaciones no validadas." % len(fk_relations)
    
    seltable="facturascli"
    print "___________________________________"
    s,j=CalculateTable(Tables,seltable)
    print "SELECT " 
    print ", ".join(s)
    print "\n\nFROM %s t" % seltable
    print j
    print "\n\n"
    
    

def CalculateTable(Tables,tablename,asname="",tablehistory=[],maxdepth=1):
    if asname=="":
        asname="t"
    table=Tables[tablename]
    banned_tables = [
        "co_subcuentas",
        "impuestos",
        "co_asientos",
        "series",
        "divisas",
        "ejercicios",
        "pagosdevolcli",
        "settallas",
        "setcolores",
        "setstallas",
        "setscolores",
        "temporadas",
        "unidades",
        "setmedidas",
        "",
        "",
    ]
    tablehistory+=[tablename]
    joins = ""
    select = []
    axes = []
    for child_table in table.child_tables:
        axes.append(child_table['nfield'])

    for field in table.basic_fields:
        if not field in axes:
            if len(select)==0:
                add = "\n\n /** %s **/  " % ">".join(tablehistory)
            else:
                add = ""
            select.append(add + "%s.%s as %s_%s" % (asname,field,asname,field,) )
        
    n=0
    for child_table in table.child_tables:
        if not child_table['table'] in tablehistory and not child_table['table'] in banned_tables :
            if maxdepth>0:
                n+=1
                child_table['asname']=asname + "_" + str(n) #child_table['nfield']
                child_table['ntable']=asname 
                
                s,j = CalculateTable(Tables,child_table['table'],child_table['asname'],tablehistory[:],maxdepth-1)
                joins += "\n" * maxdepth + "  " * (4-maxdepth) + "/** %d **/" % maxdepth+ " LEFT JOIN %(table)s %(asname)s ON %(ntable)s.%(nfield)s = %(asname)s.%(field)s" % (child_table)
                joins += j
                                    
                select += s
        
        

    return (select,joins)
