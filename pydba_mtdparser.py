#!/usr/bin/python
# -*- coding: utf-8 -*-
#     encoding: UTF8

# Fichero de parser de MTD y carga de tablas para PyDBa
import pg             # depends - python-pygresql
import _mysql     # depends - python-mysqldb
import traceback
import os
import re
import sys, math
import datetime, random
import zlib , math
from base64 import b64decode, b64encode
from pydba_utils import *
def get_timehash():
    now = datetime.datetime.now()
    nseed = random.randint(0,256)
    timehash = "%02d%02d%02d%02d%02x" % (now.month,now.day,now.hour,now.minute, nseed)
    return timehash 
        
exec_hash = get_timehash();

last_sync_pos = 0

class MTDParser_data:
    name=""
    alias=""
    null=False
    dtype="integer"
    length=0
    pk=False
    has_relations_1m=False
    has_relations_m1=False
    default=None
    

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
        self.unique_fields=[]
        self.parent = None
        
    def check_field_attrs(self,field,table,debug=False):
        tfield=MTDParser_data()
        name=getattr(field,"name","noname")
        for childname in field._children: 
            child = getattr(field,childname)
            if type(child) is list:
                if childname != "relation":
                    setattr(field,childname,child[0])
                    print "ERROR: Tabla '%s' campo '%s' , etiqueta '%s' duplicada!" % (table, name, childname)
            else:
                if childname == "relation":
                    setattr(field,childname,[child])
                    #print childname, repr(child)
            if childname != "relation":
                misspellings = {
                    "false" : [ "false", "flase", "fasle", "fales" ] ,
                    "true" : [ "true", "ture", "treu"] ,
                }
                text = str(child)
                ltext = text.lower()
                replace = None
                for original, misslist in misspellings.iteritems():
                    if text == original: break
                    if ltext == original: replace = original; break
                    if ltext in misslist: replace = original; break
                
                if replace is not None:
                    setattr(field,childname,replace)
                    print "WARN: '%s'.'%s' <%s> se ha reemplazado el valor '%s' por '%s'" % (table, name, childname,text,replace)
                    
                    
                    
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
        field.ck = getattr(field,"ck",'false')
        #print "*CK",name, field.ck  
                
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
                        required = False
                        if str(field.null) == "false": 
                            required = True
                            if not hasattr(field,"default"):
                                required = False
                                if debug: print "DEBUG:",table,name, "es relacion obligatoria y no tiene default."
                            #print "** REQ in Field %s.%s." % (table,name)
                        if str(getattr(relation,"delc","false")) == "true": 
                            required = True
                        
    
                        self.child_tables.append({"ntable" : str(table), "nfield" : str(name), "table" : str(relation.table), "field" : str(relation.field) , "required" : required })
                    else:
                        print "ERROR: Relation card unknown '%s' in Field %s.%s." % (str(relation.card),table,name)
                        
        if hasattr(field,"freerelation"):
            for relation in field.freerelation:
                relation_is_ok=True
                if not hasattr(relation,"table"):
                    print "INFO: FREE Relation table ommited in Field %s.%s." % (table,name)
                    relation_is_ok=False
                if not hasattr(relation,"field"):
                    print "INFO: FREE Relation field ommited in Field %s.%s." % (table,name)
                    relation_is_ok=False
                if not hasattr(relation,"card"):
                    print "INFO: FREE Relation card ommited in Field %s.%s." % (table,name)
                    relation_is_ok=False
                if relation_is_ok:
                    if str(relation.card)=="1M":
                        tfield.has_relations_1m=True
                    elif str(relation.card)=="M1":
                        tfield.has_relations_m1=True
                        required = False
                        #if str(field.null) == "false": 
                        #    required = True
                            #print "** REQ in Field %s.%s." % (table,name)
                        
                        # print "Relation field %s.%s -> %s.%s" % (table, name, relation.table,relation.field)
                        rel = {"ntable" : str(table), "nfield" : str(name), "table" : str(relation.table), "field" : str(relation.field) , "required" : required}
                        self.child_tables.append(rel)
                        #print "relation", rel
                    else:
                        print "INFO: FREE Relation card unknown '%s' in Field %s.%s." % (str(relation.card),table,name)
                                        
        tfield.name=str(field.name).lower()
        if str(field.name) != tfield.name:
            print "WARN: Uso de mayusculas en campo %s.%s." % (table,name)
            
        tfield.alias=str(field.alias)
        
        if not self.typetr.has_key(str(field.type_)):
            print "ERROR: Unknown field type '%s' in Field %s.%s." % (str(field.type_),table,name)
        else:
            tfield.dtype=self.typetr[str(field.type_)]
        
        
        if hasattr(field,"default"):
            tfield.default=field.default
            #if field.default == "false": field.default=False
            #if field.default == "true": field.default=True
            #print tfield.name, "default:", tfield.default
        else:
            tfield.default=None
            
        
        if str(field.null)=='false':
            tfield.null=False
        elif str(field.null)=='true':
            tfield.null=True
        else:
            tfield.null=True
            print "ERROR: Unknown null '%s' in Field %s.%s." % (str(field.null),table,name)
            
        if str(field.pk)=='false':
            tfield.pk=False
            if str(getattr(field,"unique","false")) != "false":
                self.unique_fields.append(tfield.name)
        elif str(field.pk)=='true':
            tfield.pk=True
            self.primary_key+=[tfield.name]
            self.unique_fields.append(tfield.name)
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
        
    
    def parse_mtd(self,mtd,debug=False):
        self.field={}
        self.primary_key=[]        
        self.child_tables=[]        
        self.name = mtd.name
        self.parent = getattr(mtd,"parent",None)
        for n,field in list(reversed(list(enumerate(mtd.field)))):
            tfield=self.check_field_attrs(field,mtd.name,debug=False)
            if tfield.name in self.field:
                print "ERROR: El campo %s en la tabla %s ha aparecido más de 1 vez!!" % (tfield.name, self.name)
                del mtd.field[n]
                
            self.field[tfield.name]=tfield
            
        

# Crea una tabla según las especificaciones del MTD
def create_table(options,db,table,mtd,oldtable=None,addchecks = False, issue_create = True):
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
    ck = [] 
    fieldnames = []
    pkeyname = "%s_pkey" % table
    for field in mtd.field:
        if str(field.name).lower() in fieldnames:
            print "FATAL: El campo %s en la tabla %s ha aparecido más de 1 vez!!" % (str(field.name), table)
            continue 
            #raise NameError, "ERROR: El campo %s en la tabla %s ha aparecido más de 1 vez!!" % (str(field.name), table)
        else:
            fieldnames.append(str(field.name).lower())
        row={}
        unique_index = ""
        row['name']=str(field.name).lower()
        field_ck = getattr(field,"ck",'None')
        ispkey = False
        isunique = False
        index_options = []
        #print "CK %s.%s %s" % (table,row['name'],str(field.ck))
        if type(field_ck) is list: 
            field_ck = field_ck[0]
            print "WARN: La columna %s.%s tiene repetido el apartado ''ck''." % (table,str(field.name))
        if type(field_ck) is str:
            if re.match('false',field_ck, re.I): field_ck = False
            elif re.match('true',field_ck, re.I): field_ck = True
            else: 
                print("WARNING: %s.%s tiene un CK con valor desconocido %s " % (table,row['name'], field_ck))
                field_ck = False
        
        if type(field_ck) is str:
            print("WARNING: %s.%s tiene un CK con valor desconocido %s " % (table,row['name'], field_ck))
            field_ck = False
        if field_ck:
            #print "CK %s.%s %s" % (table,row['name'],str(field.ck))
            
            ck.append(str(field.name))
            
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
        
                        
        if row['type']=='character varying':
            index_adds="varchar_pattern_ops"
        elif row['type']=='text':
            index_adds="text_pattern_ops"
        else:                    
            index_adds=""
        this_field_requires_index = False
        
        if hasattr(field,"pk"):
            if type(field.pk) is list: 
                field.pk = field.pk[0]
                print "WARN: La columna %s.%s tiene repetido el apartado ''pk''." % (table,str(field.name))
                
            if str(field.pk)=='true':
                ispkey = True
                # Si tiene constraint, tiene internamente un indice asociado. 
                this_field_requires_index = True # se habilita por compatibilidad con abanQ
                #unique_index = " UNIQUE "
                
                for dbrow in db.query("SELECT relname FROM pg_class WHERE relname = '%s'" % pkeyname).getresult():
                    random.seed()
                    rn1 = random.randint(0,16**4)
                    rn2 = random.randint(0,16**4)             
                    pkeyname = "%s_pkey_%04x%04x" % (table,rn1,rn2)
                   
                constraints+=["CONSTRAINT %s PRIMARY KEY (%s)" % (pkeyname,row['name'])]

        if hasattr(field,"unique"):
            if str(field.unique)=='true':
                this_field_requires_index = True
                unique_index = " UNIQUE "
                isunique = True
            elif str(field.unique)=='false':
                pass
            else:
                print("WARNING: %s.%s unkown 'unique' value: %s" % (table,row['name'],str(field.unique)))
            
                
        if hasattr(field,"relation"):
            this_field_requires_index = True
            for relation in field.relation:
                if not hasattr(relation,"card"):
                    print("WARNING: %s.%s has one relation without "
                            "'card' tag" % (table,row['name']))
                #if hasattr(relation,"card"):
                #    if str(relation.card)=='M1':
                #        this_field_requires_index = True
                #else:
                #    print("WARNING: %s.%s has one relation without "
                #            "'card' tag" % (table,row['name']))
        if hasattr(field,"freerelation"):
            this_field_requires_index = True
            for relation in field.freerelation:
                if not hasattr(relation,"card"):
                    print("WARNING: %s.%s has one free-relation without "
                            "'card' tag" % (table,row['name']))
                #if hasattr(relation,"card"):
                #    if str(relation.card)=='M1':
                #        this_field_requires_index = True
                #else:
                #    print("WARNING: %s.%s has one relation without "
                #            "'card' tag" % (table,row['name']))
        if unique_index and addchecks==False:
            unique_index = "  "
            
        if hasattr(field,"index"):
            if str(field.index) in ["true","fastwrite"]:
                if str(field.index) == 'fastwrite': index_options.append('fastwrite')
                this_field_requires_index = True
            elif str(field.index)=="false":
                this_field_requires_index = False
            else:
                print("WARNING: %s.%s unknown 'index' value: %s" % (table,row['name'],str(field.index)))
        
            
        if this_field_requires_index:
            if row['type'].startswith("character varying") and 'fastwrite' not in index_options:
                indexes+=["CREATE INDEX %s_%sup_m1_idx ON %s (upper(%s::text) %s);" 
                        % (table,row['name'],table,row['name'], index_adds)]
            concurrent = ""
            options_with = ""
            if 'fastwrite' in index_options:
                options_with = "WITH (fillfactor = 60)"
                # concurrent = "CONCURRENTLY"
            indexes+=["CREATE %s INDEX %s %s_%s_m1_idx ON %s (%s) %s;" 
                    % (unique_index,concurrent,table,row['name'],table,row['name'], options_with)]
                    
        calculated = None
        if hasattr(field,"calculated"):
            if str(field.calculated) == "true": calculated = True
            elif str(field.calculated) == "false": calculated = False
            else:
                print("WARNING: %s.%s unkown 'calculated' value: '%s'" % (table,row['name'],str(field.calculated)))
                calculated = False
                
                        
        if not hasattr(field,"null"): canbenull = True
        else: 
            if str(field.null)=='false':   canbenull = False
            elif str(field.null)=='true':  canbenull = True
            else:
                print("WARNING: %s.%s unkown 'null' value: %s " % (table,row['name'],str(field.null)))
                canbenull = True
                
           
        if (unique_index) and calculated:
            print "-- FATAL ERROR -- !!"
            print("FATAL: %s.%s simultaneamente tiene propiedades UNIQUE (pk, unique) y CALCULATED." % (table,row['name']))
            print("-- Debe corregir esto e intentarlo de nuevo.")
            sys.exit(1)
            
        if (unique_index) and canbenull:
            print "WARN: Si la columna %s es de algún modo única (pk,unique), nunca puede admitir NULL." % str(field.name)
            canbenull = False
            
                
        if calculated and not canbenull:
            if options.verbose:
                print "INFO: Si la columna %s.%s es calculada, debe admitir NULL." % (table,str(field.name))
            canbenull = True
            
        if hasattr(field,"default"):
            if type(field.default) is list: 
                field.default = str(field.default[0])
                print "WARN: La columna %s.%s tiene repetido el apartado ''default''." % (table,str(field.name))
            else:
                field.default = str(field.default)
                
            if field.default == 'null' : field.default = 'NULL'
            elif field.default == 'true' : field.default = 'TRUE'
            elif field.default == 'false' : field.default = 'FALSE'
            else: field.default = "'%s'" % field.default
            
            
            row['options']+=" DEFAULT "+field.default+" "
        
        
        if canbenull:
            row['options']+=" NULL"
        else:
            row['options']+=" NOT NULL"
        
        
        txtfields+=["\"%(name)s\" %(type)s %(options)s" % row]
    
    if len(ck):
        if len(ck) > 6:
            print "ERROR: (%s) no se puede crear un indice CK tan grande:" % table, ck
        else:
            composedfieldname = ", ".join(ck)
            composedfieldname2 = "".join(ck)
            if addchecks:
                unique = " UNIQUE "
            else:
                unique = " "
            
            
            indexes+=["CREATE %s INDEX %s_%s_m1_idx ON %s (%s);" 
                    % (unique,table,composedfieldname2,table,composedfieldname)]
    
    if hasattr(mtd,"index"):
        n = 0
        for index in mtd.index:
            n += 1
            name = str(getattr(index,"name",""))
            unique = str(getattr(index,"unique","false"))
            concurrent = str(getattr(index,"concurrent","false"))
            method = str(getattr(index,"method",""))
            columns = str(getattr(index,"columns","NO COLUMNS FOUND!"))
            fillfactor = str(getattr(index,"fillfactor",""))
            where = str(getattr(index,"where",""))
            tablespace = str(getattr(index,"tablespace",""))
            
            ir = {}
            if not name: name = "userindex%02d" % n
            ir['name'] = "%s_%s_idx" % (table,name)
            ir['table'] = table
            if method:
                ir['method'] = " USING %s " % method
            else:
                ir['method'] = ""
            ir['columns'] = columns
            if unique[0].lower() == "t": ir['unique'] = " UNIQUE " 
            else: ir['unique'] = " "
            
            if concurrent[0].lower() == "t": ir['concurrent'] = " CONCURRENTLY " 
            else: ir['concurrent'] = " "
            withparam = {}
            if fillfactor:
                withparam['fillfactor'] = fillfactor
            
            if withparam:
                ir['with'] = " WITH ( %s) " % ", ".join([ "%s = %s" % (k,v) for k,v in withparam.iteritems() ])
            else:
                ir['with'] = ""
            
            if tablespace:
                ir['tablespace'] = " TABLESPACE %s " % tablespace
            else:
                ir['tablespace'] = ""
                
            if where:
                ir['where'] = " WHERE %s " % where
            else:
                ir['where'] = ""

            sql = """
            CREATE %(unique)s INDEX %(concurrent)s %(name)s ON %(table)s %(method)s
                ( %(columns)s ) %(with)s
            %(tablespace)s
            %(where)s ;
            """ % ir
            #print sql
            indexes.append(sql)
            
        
    
    
    if issue_create:
        for drop in drops:
            try:
                db.query(drop)
            except:
                pass
                #print "ERROR:", drop , " .. execution failed:"
                #traceback.print_exc(file=sys.stdout)
                #print "-------"
            
    txtfields+=constraints
    txtcreate="CREATE TABLE %s (%s) WITH (fillfactor = 90,  OIDS=FALSE) ;" % (table, ",\n".join(txtfields))
    
    if issue_create:
        try:
            db.query(txtcreate)
        except Exception, e:
            print e, txtcreate
            raise     
        sql = "ALTER INDEX %s SET (fillfactor = 80);" % (pkeyname)
        try:
            db.query(sql)
        except Exception, e:
            print e
            print txtcreate
            print sql
            
               
    return indexes



def create_indexes(db,indexes,table):
    status = True
    for index in indexes:
        try:
            split_index=index.split(" ")
            for indname in split_index:
                if indname[:len(table)] == table and len(indname) > len(table):
                    index_name=indname
                    break
            
            # Borrar primero el índice si existe
            qry_indexes = db.query("""
    SELECT pc2.relname as indice
    FROM pg_class pc2 
    WHERE pc2.relname = '%s'
            """ % index_name)
            dt_indexes=qry_indexes.dictresult() 
            for fila in dt_indexes:
                db.query("DROP INDEX %s;" % fila['indice'])
            try:
                db.query(index)
            except:
                print "Error al crear el índice!", index
                status = False
                traceback.print_exc(file=sys.stdout)
                
        except:
            status = False
            print index
            traceback.print_exc(file=sys.stdout)
    
    return status                
        
Tables={}        
        
        
def load_mtd(options,odb,ddb,table,mtd_parse):
    fail = False
    mtd=mtd_parse.root.tmd
    if table!=table.lower():
        print "ERROR: Table name '%s' has uppercaps" % table
        table=table.lower()
    mtdquery = getattr(mtd, "query", None)
    if mtdquery:
        if str(mtdquery)!=table:
            print "ERROR: Expected '%s' in MTD QUERY name but '%s' found ***" % (table,str(mtdquery))
        # Probablemente se puede ignorar las cargas de estas queries. No son tablas en realidad.    
        if str(mtd.name)==table:
            print "ERROR: MTD Query Filename '%s' HAS BEEN FOUND in the <name> attribute: '%s' ***" % (table,str(mtd.name))
        return False
    else:
        if str(mtd.name)!=table:
            print "ERROR: Expected '%s' in MTD name but '%s' found ***" % (table,str(mtd.name))
        
    if str(getattr(mtd,"create","true")) == "false":
        if options.verbose:
            print "Ignorando seguimiento de tabla %s." % table
        return False
        
    if options.debug:
        print "Analizando tabla %s . . ." % table
    
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
    mparser.parse_mtd(mtd,options.debug)
    Tables[table]=mparser
    if len(tablefields)==0:
        if options.debug:
            print "- tabla %s es nueva." % table
    
        sql = """SELECT * from information_schema.tables 
            WHERE table_schema = 'public' AND table_name ~ '^%s_[0-9]{12}[0-9a-f]{2}$'""" % table
        qry_columns2=odb.query(sql)
        aux_columns2=qry_columns2.dictresult()
        if len(aux_columns2):
            print "ERROR GRAVE: Ya existe un backup de la tabla %s pero la tabla no existe! Revise esto manualmente." % table
            return False
            
        if (options.debug):
            print "Creando tabla '%s' ..." % table
        try:
            idx = create_table(options,ddb,table,mtd, addchecks = options.addchecks)
            create_indexes(ddb,idx,table)
        except:
            print "Error no esperado!"
            print traceback.format_exc()
            return False

        if options.transactions:
            try:
                ltable = table
                sql = "LOCK %s NOWAIT;" % ltable
                if (options.verbose): print sql
                ddb.query(sql);
                if (options.verbose): print "done."
            except:
                print "Error al bloquear la tabla %s , ¡algun otro usuario está conectado!" % ltable
                ddb.query("ROLLBACK;");
            raise
            
        return True
        
    if options.debug:
        print "- tabla %s existe." % table

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
    
        name=str(field.name).lower()
        mfield = mparser.field[name]
        if mfield.pk: new_pkey = name
        new_fields.append(name)
                
        if hasattr(field,"default") and str(field.default):
            if str(field.default) == "null": default_value = "NULL"
            else:
                default_value = "'" + pg.escape_string(str(field.default)) + "'"
            if options.debug and options.verbose:
                print "Leido valor por defecto %s  para la columna %s tabla %s"  % (default_value,name,table)
        else:            
            default_value = "NULL"
        
        
        if default_value == "NULL" and not mfield.null:
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
            if options.debug:
                print "Asumiendo valor por defecto %s  para la columna %s tabla %s"  % (default_value,name,table)
        
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
                    
                old_fields.append(default_value)
                
        else:

            null=origin_fielddata[name]["is_nullable"]
            if null=="YES": null=True
            if null=="NO": null=False
            if not mfield.null:
                old_fields.append("COALESCE(%s,%s)" % (name,default_value))
            else:
                old_fields.append(name)
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
        #        prin create_table(options,db,table,mtd)t "%s : '%s'" % (atr,getattr(field,atr))
        
    
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
            
    if options.debug:
        print "- ",options.loadbaselec ,table 

    if options.loadbaselec and table == "baselec":
        if os.path.isfile(options.loadbaselec):
            print "Iniciando volcado de Baselec ****"
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
        else:
            print "ERROR en volcado de Baselec **** no existe el fichero:", options.loadbaselec 
            

    
    if Regenerar and options.safe:
        print "WARN: Se iba a regenerar %s , pero safe mode está activo." % table
        Regenerar = False
        
    #if options.getdiskcopy and len(mparser.basic_fields)>0 and len(mparser.primary_key)>0:
    #    Regenerar = True
    if Regenerar and options.rebuildalone:
        qry = ddb.query("select usename, client_addr  FROM pg_stat_activity WHERE datname ='%s';" % options.ddb);
        
        rows = qry.dictresult()
        if len(rows)>2:
            print "WARN: Más de un usuario está conectado a '%s', no se regeneran tablas." % options.ddb
            for row in rows:
                print row
            Regenerar = False
    
    if Regenerar:
        try:
            ltable = table
            if options.transactions:
                ddb.query("SAVEPOINT lock_%s;" % ltable);
                sql = "LOCK %s NOWAIT;" % ltable
                if (options.verbose): print sql
                ddb.query(sql);
                if (options.verbose): print "done."
                ddb.query("RELEASE SAVEPOINT lock_%s;" % ltable);
        except:
            print "Error al bloquear la tabla %s , ¡algun otro usuario está conectado!" % ltable
            if options.transactions:
                ddb.query("ROLLBACK TO SAVEPOINT lock_%s;" % ltable);
            Regenerar = False
            print "ERROR: ¡No se regenará la tabla %s!" % ltable
        
        
        
    if not Regenerar:
        reindex = options.reindex
        qry_indexes = ddb.query("""
            SELECT pc.relname as tabla , pc2.relname as indice,pi.indkey as vector_campos
            FROM pg_class pc 
            INNER JOIN pg_index pi ON pc.oid=pi.indrelid 
            INNER JOIN pg_class pc2 ON pi.indexrelid=pc2.oid
            WHERE NOT pi.indisprimary AND NOT pi.indisunique
            AND pc.relname = '%s'
                """ % table)
        dt_indexes=qry_indexes.dictresult() 
        indexes = create_table(options,ddb,table,mtd,oldtable=table,addchecks = options.addchecks, issue_create = False)
        if not reindex:
            i_names = []
            i_names2 = [ r['indice'] for r in dt_indexes ] 

            idx_name = {}
            for index in indexes:
                y = [ n for n in index.split(" ") if n.startswith(table+"_") and n.endswith("idx") ]
                x = y[0][:63]
                i_names.append(x)
                idx_name[x] = index
                
            i_add = set(i_names) - set(i_names2)
            i_del = set(i_names2) - set(i_names)
            if options.verbose:
                if i_del: print "Indexes of table", table,": to delete::", i_del
                if i_add: print "Indexes of table", table,": to append::", i_add
            if i_del or i_add: 
                print "Reindexing table %s . . . (%d added, %d removed)" % (table,len(i_add), len(i_del))

                for idx in i_del:
                    ddb.query("DROP INDEX \"%s\";" % idx)
                
                add_indexes = [ idx_name[name] for name in i_add ]
                    
                create_indexes(ddb,add_indexes, table)
            
        if reindex:
            print "Reindexing table %s . . . (full reindex)" % table
            for fila in dt_indexes:
                ddb.query("DROP INDEX %s;" % fila['indice'])
        
            create_indexes(ddb,indexes, table)
    if Regenerar:
        indexes = []
        qry_otable_count = odb.query("SELECT COUNT(*) as n from %s" % table)
        dict_otable_count = qry_otable_count.dictresult()
        existent_rows = int(dict_otable_count[0]["n"])
    else:
        existent_rows = -1
        
    if Regenerar and existent_rows < 1 :  # **** REGENERACION TABLA VACIA *** 
        try:
            ddb.query("DROP TABLE %s CASCADE;" % (table))
        except:
            print "No se pudo borrar la tabla antigua." , table
        try:
            indexes = create_table(options,ddb,table,mtd,oldtable=table,addchecks = options.addchecks)
        except:
            print "ERROR: Se encontraron errores graves al crear la tabla %s" % table
            why = traceback.format_exc()
            print "**** Motivo:" , why
        if not create_indexes(ddb,indexes, table): 
            print "No se pudieron crear todos los indices."
                
    elif Regenerar: # **** REGENERACION DE TABLA CON CONTENIDOS ****
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
              indexes = create_table(options,ddb,table,mtd,oldtable=newnametable,addchecks = options.addchecks)
            except:
              fail = True
              print "ERROR: Se encontraron errores graves al crear la tabla %s" % table
              why = traceback.format_exc()
              print "**** Motivo:" , why
            if options.transactions:  
                sql = "LOCK %s;" % table
                if (options.verbose): print sql
                ddb.query(sql);
                if (options.verbose): print "done."
              
            if not fail and options.loadbaselec and table == "baselec" and os.path.isfile(options.loadbaselec):
                print "Tabla Baselec encontrada."
            elif not fail and options.getdiskcopy and len(mparser.basic_fields)>0 and len(mparser.primary_key)>0 : 
                # Generar comandos copy si se especifico
                #print "Cargando desde .dat"
                # ANULADO!!, no hace nada. 
                primarykey = mparser.primary_key[0]
                fields = ', '.join(mparser.basic_fields)
                """
                try:
                    sql = "COPY %s (%s) FROM '/tmp/psqldiskcopy/%s.dat'" % (table, fields, table)
                    qry = ddb.query(sql)
                except:
                    print "Error al cargar datos."
                    print traceback.format_exc()
                    print "--------------"
                """
        
        
            elif not fail:
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
                    if options.transactions:
                        ddb.query("SAVEPOINT lock_sql;");
                    ddb.query(sql)
                    if options.transactions:
                        ddb.query("RELEASE SAVEPOINT lock_sql;");
                except:
                    fail1 = True
                    print sql
                    print "Error al intentar regenerar por SQL la tabla %s:" % table
                    print traceback.format_exc()
                    print "Se intentará hacer manualmente."
                    if options.transactions:
                        ddb.query("ROLLBACK TO SAVEPOINT lock_sql;");
            
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
                    else:
                        odb.query("VACUUM ANALYZE %s" % table)
                         
                if fail1: 
                    fail = True
                    fail1 = False
                
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
            
            if not create_indexes(ddb,indexes, table): 
              print "No se pudieron crear todos los indices."
              fail = True
              
            try:
              ddb.query("DROP TABLE %s CASCADE;" % (newnametable))
            except:
              print "No se pudo borrar la tabla de backup."
              fail = True
            if not options.transactions:
                try:
                  ddb.query("VACUUM ANALYZE %s;" % (table))
                except:
                  print "No se pudo analizar la nueva tabla."
                  fail = True

    if options.diskcopy and len(mparser.basic_fields)>0 and len(mparser.primary_key)>0:
        # Generar comandos copy si se especifico
        global last_sync_pos
        primarykey = mparser.primary_key[0]
        fields = ', '.join(mparser.basic_fields)
        filename = "%s-%s.pydbabackup" % (options.ddb,exec_hash)
        qry = ddb.query("SELECT COUNT(*) as count FROM %s" % (table))
        num = 0
        for row in qry.dictresult():
            try:
                num = int(row['count'])
            except:
                pass
                
        if num > 0:
            print "Volcando tabla %s (%d)\t>> %s" % (table, num, filename)
    
        """
        f1 = open("/tmp/psqldiskcopy/%s.restore.sql" % table, "w")
        f1.write("-- primary key: %s\n\n" % primarykey)
        f1.write("COPY %s (%s) FROM '/tmp/psqldiskcopy/%s.dat'" % (table, fields, table))
        f1.close()
    
        sql = "COPY (SELECT %s FROM %s ORDER BY %s) TO '/tmp/psqldiskcopy/%s.dat'" % (fields, table, primarykey, table)
        print "Copiando a disco %s . . . " % table
        qry = ddb.query(sql)
        """
        f1 = open(filename, "a")
        f1.write("\n");
        f1.write("--TABLE--\n")
        f1.write("-- rows: %04X\n" % num)
        f1.write("-- table: %s\n" % table)
        f1.write("-- fields: %s\n" % ",".join(mparser.basic_fields))
        f1.write("-- primarykey: %s\n" % primarykey)
        #f1.write("--*TRUNCATE %s;\n" % (table))
        #f1.write("--*COPY %s (%s) FROM STDIN;\n" % (table, fields))
        f1.write("--BEGIN-COPY--\n")
        try:
            sql = "COPY (SELECT %s FROM %s ORDER BY %s) TO STDOUT" % (fields, table, primarykey)
            qry = ddb.query(sql)
        except:
            sql = "COPY %s (%s) TO STDOUT" % (table, fields)
            qry = ddb.query(sql)
            
        buffers = []
        softlimit = 16033  # TIENE QUE SER PRIMO!! 
        #primelist = [127  , 353 , 607,877,1153] # ef. 6.13x , 3.9Mb @ 1024
        #primelist = [127  , 353 , 607,877,1153,2689, 4001, 6763] # 7.14x, 3.3Mb @ 1024
        # fichero de 2 meses: 4.7Mb 5.39x
        # fichero de 2 meses: 4.8Mb 5.21x
        
        primelist = [127 ,353 ,607,877, 947,1153,1619,2689,3467, 4001,6551, 6763, 7307]  # 7.20  ,3.4Mb @ 1024 (3.3 @ 512)
        # fichero de 2 meses: 4.6Mb 5.40x
        # fichero de 2 meses: 4.5Mb 5.04x
        # escoger uno de:    127  , 353 , 607 ,   877 , 1153 , 1453 ,  2689   , 4001   , 6763   
        
        
        primelist = [53,79, 127, 227 ,353, 457,587,607,751,877, 947,1153,1237,1483,1619,2689,3467, 4001,5813,6551, 6763, 7307,7919]  

        blocksize = 512
        # 10531, 16033 376931
        conflimit = num+1 # (2 * softlimit) / (len(primelist) )
        pages = math.ceil(float(num) / conflimit)
        if pages < 1: pages = 1
        limit = math.floor(float(num) / pages)
        if limit < 1: limit = 1
        bufsize = 0
        blocks = 0
        try:        
            n = 0
            while True:
                line = ddb.getline()
                
                splitted = line.split("\t")
                line_hash = zlib.adler32(splitted[0]) & 0xffffffff
                if len(buffers) == 0:
                    for f in splitted:
                        buffers.append([])
                n += 1
                if line != "\\.": 
                    bufsize +=1
                    for field,buffer1 in zip(splitted,buffers):
                        buffer1.append(field)
                        
                    
                if ((line_hash % softlimit in primelist and bufsize >= softlimit / (len(primelist) * 2) ) or bufsize >= limit or line == "\\.") and bufsize > 0:
                    f1.write("-- bindata >>\n")
                    #f1.write("-- rows: %d %d-%d\n" % (bufsize,n-bufsize+1,n))
                    #f1.write("-- lenghts: ")
                    bufs = []
                    totallen = 0
                    if len(buffers) != len(mparser.basic_fields):
                        print "ERROR: Las longitudes de los arrays no coinciden!!"
                        print "buffers %d, fields %d" % (len(buffers), len(mparser.basic_fields))
                        
                    for buffer1,fname in zip(buffers,mparser.basic_fields):
                        #buf = "\t".join(buffer1) 
                        buf = zlib.compress("\t".join(buffer1),9)
                        #f1.write("%X " % len(buf))
                        totallen += len(buf)
                        bufs.append(buf)
                    #f1.write("\n")
                    
                    nsz = blocksize
                    f1.flush()
                    pos = f1.tell() 
                    over = pos % nsz
                    if nsz-over < totallen/5.0 or nsz-over < (pos - last_sync_pos) / (blocks+1):
                        if pos - last_sync_pos > nsz-over * 100: 
                            if nsz-over > 3:
                                f1.write("-- " + "." * (nsz-over-4)+"\n")
                            else:
                                f1.write("\n" * (nsz-over))
                            f1.flush()
                            last_sync_pos = f1.tell() 
                    
                    blocks += 1
                    for buf,fname in zip(bufs,mparser.basic_fields):
                        nsz = blocksize
                        f1.flush()
                        pos = f1.tell() 
                        over = pos % nsz
                        if nsz-over < len(buf)/30.0 :
                            if pos - last_sync_pos > nsz-over * 100:
                                if nsz-over > 3:
                                    f1.write("-- " + "." * (nsz-over-4)+"\n")
                                else:
                                    f1.write("\n" * (nsz-over))
                                f1.flush()
                                last_sync_pos = f1.tell() 
                            
                        #f1.write("%d:%s;" % (len(buf),fname))
                        #f1.write("%s;" % (fname))
                        f1.write(b64encode(buf))
                        f1.write("\n")
                        
                    buffers = []
                    bufsize = 0
                    for f in mparser.basic_fields:
                        buffers.append([])
                        
                    
                #f1.write(line+"\n");
                
                if line == "\\.": break
            ddb.endcopy()
        except:
            print "Un error ocurrió durante la copia (%d/%d lineas fueron copiadas):" % (n,num)
            print traceback.format_exc()
            f1.write("\\.\n");
        f1.write("\n");
        
        
        
        
    # ************************************* BASELEC *****************************************
            
    if options.loadbaselec and table == "baselec" and os.path.isfile(options.loadbaselec):
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
        required_fields = [
              "referenciabaselec",
              "referenciafabricante",
              "modelofabricante",
              "pvpactual",
              ]
        validfields = 0
        for n,field in enumerate(csvfields):
            field = field.replace(" ","")
            field = field.lower()
            field = field.replace("+","mas")
            field = re.sub(r'[^a-z0-9]','',field)
            if field in mparser.field:
              csvfields[n]=field
              validfields += 1
            else:
              csvfields[n]="* " + field
            
        if validfields == 0:
            print "@ ERROR: No se encontraron campos validos en el CSV!"
            raise ValueError,"@ ERROR: No se encontraron campos validos en el CSV!"
            
        if validfields < len(required_fields):
            print "@ ERROR: No se encontraron suficientes campos validos en el CSV! (Se encontraron %d de %d campos requeridos)" % (len(validfields), len(required_fields))
            raise ValueError,"@ ERROR: No se encontraron suficientes campos validos en el CSV!"
        
        for reqfield in required_fields:
            if reqfield not in csvfields:
                print "@ ERROR: El campo %s no existe en el CSV. No se puede importar" % reqfield
                raise ValueError, "@ ERROR: El campo %s no existe en el CSV. No se puede importar" % reqfield
        
        
        
        data = []
        to1 = 0

        for csvline in csv:
            csvreg = csvline.split("\t")
            line = {}
            for n, val in enumerate(csvreg):
                fieldname = csvfields[n]
                if fieldname[0]=="*": continue
                try:
                    #val=val.decode("cp1252")
                    #val=val.encode("utf8")
                    pass
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
            if len(data)>200 or lineas - to1 < 2:
                from1 = to1 - len(data)
                pfrom1 = from1 * 100.0 / lineas
                pto1 = to1 * 100.0 / lineas
                
                
                #sys.stdout.write('.')
                sys.stdout.flush()
                # print "Copiando registros %.1f%% - %.1f%% . . ." % (pfrom1, pto1)
                # auto_import_table(options,ddb,table,data,mparser.field,mparser.primary_key[0])    
                previ = 0
                while len(data)>0:
                    pfrom1 = from1 * 100.0 / lineas
                    if import_table(options,ddb,table,data,mparser.field):
                        print "@ %.2f %% registros copiados %d - %d . . ." % (pfrom1, from1+1, to1+1)
                        data = []
                        break

                    for i in range(10):
                        if i < previ - 1 : continue
                        n = 3**(i+1)
                        frac = int(math.ceil(float(len(data)) / n))
                        if frac<20: frac = 5
                        previ = i
                        if import_table(options,ddb,table,data[:frac],mparser.field):
                            print "@ %.2f %% registros copiados %d - %d . . ." % (pfrom1, from1+1, from1+frac)
                            data = data[frac:]
                            from1 += frac
                            break
                            
                        if frac<20: 
                            data = data[frac:]
                            from1 += frac
                            break
                    
                    

        from1 = to1 - len(data)
        pfrom1 = from1 * 100.0 / lineas
        pto1 = to1 * 100.0 / lineas

        print "@ %.2f %% Copiando registros %d - %d . . ." % (pfrom1, from1+1, to1+1)
        sys.stdout.flush()
        #auto_import_table(options,ddb,table,data,mparser.field,mparser.primary_key[0])    
        import_table(options,ddb,table,data,mparser.field)
        data = []                              

        csv.close()
                        
          
          
    # SINCRONIZACION MAESTRO>ESCLAVO      
    if options.odb != options.ddb : # TODO: Aquí falta comparar también los puertos y IP's.
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
            row_hash=sha_hexdigest(repr(row))
            row['#hash']=row_hash
            origin_rows[row[pkey]]=row
        
        dest_data=export_table(options,ddb,table,origin_fields)
        for row in dest_data:
            row_hash=sha_hexdigest(repr(row))
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
                        prev_max = 1
                        dr_maxserial=qry_maxserial.dictresult()
                        for dmaxserial in dr_maxserial:
                            if dmaxserial['max']:
                                prev_max=dmaxserial['max']+1
                                max_serial=prev_max
                                
                        increment = 1
                        if options.seqsync:
                            servernumber, serversize = options.seqsync
                            increment = serversize
                            auxv1 = 0 
                            while max_serial % increment != servernumber and auxv1 < 20:
                                max_serial += 1
                                auxv1 += 1
                            if auxv1 > 15: 
                                print "PANIC: Error en calculo de max_serial!!!"
                        if options.verbose:
                            print "INFO: Actualizando %s de %d a %d (+%d)" % (serial, prev_max, max_serial, increment)
                            
                        ddb.query("ALTER SEQUENCE %s INCREMENT BY %d RESTART WITH %d;" % (serial, increment, max_serial))
    except:
        print "Fieldname, table:",tfield.name,table
        print "PKeys: %s" % mparser.primary_key
        raise
                
    return (not fail and Regenerar)
        
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
                
                why = traceback.format_exc()
                print why
        sz2 =len(cnum)
    except:
        sz2 = 0
        why = ""
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
    elif (field.null==False and str(field.default) is not None):#Si el valor defecto NO es nulo
        ivalue="'" + pg.escape_string(str(field.default)) + "'"
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
    
    #if len(data)>10:
    #    db.query("SET client_min_messages = fatal;");            
    #else:
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
        """
        if len(sqlarray)>2048:
            sql_text="COPY %(tabla)s (%(fields)s) FROM stdin;" % sqlvar
            db.query(sql_text )
            for line in sqlarray:
                db.putline(line+"\n")
            db.putline("\\."+"\n")
            db.endcopy()
            sqlarray=[]
        """    
    del data
    
    sql_text="COPY %(tabla)s (%(fields)s) FROM stdin;" % sqlvar
    try:
        db.query(sql_text)
        for line in sqlarray:
            db.putline(line+"\n")
        db.putline("\\."+"\n")
        db.endcopy()
    except:
        if options.verbose:
            print "!!!! Error grave importando !!!!"
            print traceback.format_exc()
            print "===================="
            print sql_text
            print "----------"
            for line in sqlarray:
                print line
            print "========================"
            db.query("SET client_min_messages = warning;");            
        else:
            print "Error de importacion"
        return False
    sqlarray=[]
    db.query("SET client_min_messages = warning;");            
    return True
    
    

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
            
def procesarOLAP(db):
    try:
        import yaml
        try:
            from yaml import CLoader as yamlLoader
            from yaml import CDumper as yamlDumper
        except ImportError:
            from yaml import Loader as yamlLoader
            from yaml import Dumper as yamlDumper
            
    except:
        print "*** No se encontró la librería 'yaml' (necesaria para generar salida de tablas)."
        print " Para instalar 'yaml':"
        print " ... Debian & Ubuntu: sudo aptitude install python-yaml"
        print
        print traceback.format_exc(1)
        return
        
    global Tables
    tables = {}
    for tablename,table in Tables.iteritems():
        if len(table.primary_key)<1: continue
        
        tables[tablename] = procesarTabla(tablename, table)
        

    for tablename,table in Tables.iteritems():
        table.parent_tables = table.child_tables
        #        print table.unique_fields
        for parent_table in table.parent_tables:
            ptablename = parent_table['table']
            if ptablename not in Tables: continue 
            ptable = Tables[ptablename]
            
            if parent_table['field'] in ptable.unique_fields: parent_table['unique']=True
            else:  parent_table['unique']=False
            procesarRelacionesTabla(tables,parent_table)
        """    
        for child_table in table.child_tables:
            ctablename = child_table['table']
            if ctablename not in Tables: continue 
            ctable = Tables[ctablename]
            
            if child_table['field'] in ctable.unique_fields: child_table['unique']=True
            else:  child_table['unique']=False

            procesarRelacionesTabla(tables,child_table)
           """ 
    export = {"tables" : tables}
    dump = yaml.dump(export, Dumper=yamlDumper)
    return dump

def procesarRelacionesTabla(tables, crelation):
    global Tables
    if crelation['ntable'] in tables:
        rparents = tables[crelation['ntable']]["parents"]
    else:
        print "WARN: Table %s unknown!" % (repr(crelation['ntable']))
        rparents = []
        
    if crelation['table'] in tables:
        rchilds = tables[crelation['table']]["childs"]
    else:
        print "WARN: Table %s unknown!" % (repr(crelation['table']))
        rchilds = []
        
    try:
        crelation['default'] = str(Tables[crelation['ntable']].field[crelation['nfield']].default)
    except:
        crelation['default'] = None
    
    
    parent_rel = {
        "local_field" : crelation['nfield'],
        "remote_field" : crelation['field'],
        "remote_table" : crelation['table'],
        "local_table" : crelation['ntable'],
        "required" : crelation['required'],
        "unique" : crelation['unique'],
        "default" : crelation['default'],
    }
    child_rel = {
        "local_field" : crelation['field'],
        "remote_field" : crelation['nfield'],
        "remote_table" : crelation['ntable'],
        "local_table" : crelation['table'],
        "required" : crelation['required'],
        "unique" : crelation['unique'],
        "default" : crelation['default'],
    }
    rparents.append(parent_rel)
    rchilds.append(child_rel)
    
    

def procesarTabla(tablename, table):
    global Tables
    primarykey = table.primary_key[0]
    fields = Tables[tablename].field.keys()
    tabla = { 
#        "name" : tablename,
        "primarykey" : primarykey,

        "fields" : fields,
        "parents" : [],
        "childs" : [],
    }
    return tabla
            
def _procesarOLAP(db):
    global Tables
    print "Inicio del proceso de tablas para OLAP."
    primarykeys=[]
    tables_column0=[]
    real_child_tables={}
    for tablename,table in Tables.iteritems():
        rchild_tables = {}
        real_child_tables[tablename] = rchild_tables
        print table.field.keys()
        for field in table.field.values():
            print field.default
            

    for tablename,table in Tables.iteritems():
        primarykey = None
        
        for pk in table.primary_key:
            primarykey = pk
            """
        if tablename == 'lineasfacturascli':
            table.child_tables.append({
                    'table': 'albaranescli',
                    'ntable':  'lineasfacturascli',
                    'field': 'idalbaran',
                    'nfield': 'idalbaran',
                    })"""
            
        for child_table in table.child_tables:
            ctablename = child_table['table']
            primarykey2 = None
            if ctablename not in Tables: continue 
            
            for pk in Tables[ctablename].primary_key:
                primarykey2 = pk
            if child_table['field']!=primarykey2: child_table['type']="weak"
            else:  child_table['type']="strong"
                #print "?", primarykey, primarykey2, child_table['field'], child_table['nfield']
            
            if ctablename in real_child_tables:
                if tablename not in real_child_tables[ctablename]:
                    real_child_tables[ctablename][tablename] = []
                real_child_tables[ctablename][tablename].append(child_table)
                
            if tablename in real_child_tables:
                if ctablename not in real_child_tables[tablename]:
                    real_child_tables[tablename][ctablename] = []

                reverse_child = {
                    'table': child_table['ntable'],
                    'ntable':  child_table['table'],
                    'field': child_table['nfield'],
                    'nfield': child_table['field'],
                    'type': 'reverse',
                    'required': child_table['required'],
                    }
                
                real_child_tables[tablename][ctablename].append(reverse_child)
    ltables = []    
    for tablename,table in Tables.iteritems():
        for pk in table.primary_key:
            primarykeys.append("%s.%s" % (tablename ,pk))
            
        if 'column0' in table.field:
            tables_column0.append(tablename)

        ltables.append(tablename)
            
    print "*** TABLAS ***"               
    computarTablas(db,list(sorted(ltables)),real_child_tables,tables_column0)
    print "=============="
    
def computarTablas(db,lstTablas,real_child_tables,tables_column0, seen_tables = [], depth = 1):
    doctables = []
    lchild = []
    for table in sorted(lstTablas):
        """ 
        estadistica = {0:0,1:0,2:0}
        if table in tables_column0:
            result = db.query("SELECT column0, COUNT(*) as cantidad FROM %s GROUP BY column0;" % table);
        else:
            result = db.query("SELECT 0 as column0, COUNT(*) as cantidad FROM %s;" % table);
        for row in result.dictresult():
            try:
                column0 = int(row['column0'])
            except TypeError:
                column0 = 0
            cantidad = row['cantidad']
            estadistica[column0] = cantidad
        """
        print table #, estadistica[0], estadistica[1], estadistica[2]
        complejidad = 0
        tipo = "general"
        for ctablename, lstrel in sorted(real_child_tables[table].iteritems()):
            rtype = "weak"
            reltypes = set([])
                
            for rel in lstrel: 
                reltypes|=set([rel['type']])
                if rel['required']: reltypes|=set(["required"])
            
            if "required" in reltypes: rtype = "required"
            elif "strong" in reltypes: rtype = "strong"
            elif "reverse" in reltypes: rtype = "reverse"
            
            
            if re.match("lineas",ctablename): 
                tipo = "documento"
                rtype = "documento"
                if ctablename not in doctables: doctables.append(ctablename)

            if "reverse" in reltypes:
                printit = False
                if table not in tables_column0:
                    if ctablename in tables_column0 :
                        print "  <<*",
                        printit = True
                        
                else:
                    if ctablename not in tables_column0 :
                        print "  << ",
                        printit = True
                        
                if printit:
                    if "required" in reltypes: print ":",ctablename,
                    else: print " ",ctablename,
                    for rel in lstrel:
                        field1 = rel['field']
                        field2 = rel['nfield']
                        print field1 , "->", field2, 
                    print ";"
                    
                elif "required" in reltypes: 
                    print "  <<?:",ctablename
                continue
                
            if rtype == "reverse" and ctablename not in tables_column0 and rtype!="documento":           continue
                
            if "required" in reltypes: print ":",
            else: print " ",
            
            if rtype == "reverse":
                print " <<",
            elif rtype == "weak":
                print " ..",
            elif ctablename == table:
                print "@--",
                complejidad+=5
            elif ctablename in tables_column0:
                print "*--",
                complejidad+=20
            #elif ctablename in lstTablas:
            #    print "?--",
            #    complejidad+=10
            #elif rtype == "documento":
            #    print "|--",
            #    complejidad+=10
            else:
                print " --",
                if "required" in reltypes: lchild.append(ctablename)
                complejidad+=1
                
            estadistica2 = {0:0,1:0,2:0}
            print ctablename,":",
            for rel in lstrel:
                field1 = rel['field']
                field2 = rel['nfield']
                print field1 , "->", field2, 
                """
                if table in tables_column0:
                    result = db.query("SELECT t1.column0, COUNT(*) as cantidad FROM %s t1 INNER JOIN %s t2 ON t1.%s = t2.%s GROUP BY t1.column0;" % (table,ctablename,field1,field2));
                else:
                    result = db.query("SELECT 0 as column0, COUNT(*) as cantidad FROM %s t1 INNER JOIN %s t2 ON t1.%s = t2.%s;" % (table,ctablename,field1,field2));
                for row in result.dictresult():
                    try:
                        column0 = int(row['column0'])
                    except TypeError:
                        column0 = 0
                    cantidad = row['cantidad']
                    estadistica2[column0] += cantidad
                """
            print ";" #, estadistica2[0], estadistica2[1], estadistica2[2]
        print "> %s (%d)" % (tipo, complejidad)
        if len(real_child_tables[table]): print
           
    """doctables = list(set(doctables) - set(seen_tables))
    if len(doctables):
        computarTablas(db,list(sorted(doctables)),real_child_tables,tables_column0, list(set(seen_tables) | set(lstTablas)))"""
    doctables = list(set(lchild) - set(seen_tables))        
    if len(doctables) and depth > 0: 
        print "==========>", ", ". join(doctables)
        print
        computarTablas(db,list(sorted(doctables)),real_child_tables,tables_column0, list(set(seen_tables) | set(lstTablas)), depth - 1)
    
            
    
  

def _procesarOLAP():
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
            
        if 'column0' in table.field:
            print tablename
        
        
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
    print relations
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
