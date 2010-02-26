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
        self.parent = None
        
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
                        if field.null == "false": required = True
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
                        if field.null == "false": required = True
                        
                        # print "Relation field %s.%s -> %s.%s" % (table, name, relation.table,relation.field)
                        rel = {"ntable" : str(table), "nfield" : str(name), "table" : str(relation.table), "field" : str(relation.field) , "required" : required}
                        self.child_tables.append(rel)
                        #print "relation", rel
                    else:
                        print "INFO: FREE Relation card unknown '%s' in Field %s.%s." % (str(relation.card),table,name)
                                        
        tfield.name=str(field.name)
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
        self.parent = getattr(mtd,"parent",None)
        for field in mtd.field:
            tfield=self.check_field_attrs(field,mtd.name)
            self.field[tfield.name]=tfield
            
        

# Crea una tabla según las especificaciones del MTD
def create_table(db,table,mtd,oldtable=None):
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
    for field in mtd.field:
        if str(field.name) in fieldnames:
            print "ERROR: El campo %s en la tabla %s ha aparecido más de 1 vez!!" % (str(field.name), table)
            raise NameError, "ERROR: El campo %s en la tabla %s ha aparecido más de 1 vez!!" % (str(field.name), table)
        else:
            fieldnames.append(str(field.name))
        row={}
        unique_index = ""
        row['name']=str(field.name)
        field_ck = getattr(field,"ck",'None')
        ispkey = False
        isunique = False
        
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
                this_field_requires_index = True
                unique_index = " UNIQUE "
                import random
                random.seed()
                rn1 = random.randint(0,16**4)
                rn2 = random.randint(0,16**4)                
                constraints+=["CONSTRAINT %s_pkey_%04x%04x PRIMARY KEY (%s)" % (table,rn1,rn2,row['name'])]

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
                            
        if this_field_requires_index:
            indexes+=["CREATE %s INDEX %s_%s_m1_idx ON %s (%s);" 
                    % (unique_index,table,row['name'],table,row['name'])]
            indexes+=["CREATE INDEX %s_%sup_m1_idx ON %s (upper(%s::text));" 
                        % (table,row['name'],table,row['name'])]
            if index_adds:                                    
                indexes+=["CREATE %s INDEX %s_%s_m1_idx ON %s (%s %s);" 
                        % (unique_index,table,row['name'],table,row['name'], index_adds)]
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
            indexes+=["CREATE UNIQUE INDEX %s_%s_m1_idx ON %s (%s);" 
                    % (table,composedfieldname2,table,composedfieldname)]
    
        
        
    
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
                import sys
                traceback.print_exc(file=sys.stdout)
                
        except:
            status = False
            print index
            import sys
            traceback.print_exc(file=sys.stdout)
    
    return status                
        
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
        try:
            idx = create_table(ddb,table,mtd)
            create_indexes(ddb,idx,table)
        except:
            print "Error no esperado!"
            print traceback.format_exc()
            return False
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
                if str(field.default):
                    default_value = "'" + pg.escape_string(str(field.default)) + "'"
                    if options.verbose:
                        print "Asumiendo valor por defecto %s  para la columna %s tabla %s"  % (default_value,name,table)
                else:            
                    default_value = "NULL"
                
                Regenerar=True
                
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
        indexes = []
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
              indexes = create_table(ddb,table,mtd,oldtable=newnametable)
            except:
              fail = True
              print "ERROR: Se encontraron errores graves al crear la tabla %s" % table
              why = traceback.format_exc()
              print "**** Motivo:" , why
            if not fail and options.loadbaselec and table == "baselec" and os.path.isfile(options.loadbaselec):
                print "Tabla Baselec encontrada."
            elif not fail and options.getdiskcopy and len(mparser.basic_fields)>0 and len(mparser.primary_key)>0:
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
              
            try:
              ddb.query("VACUUM ANALYZE %s;" % (table))
            except:
              print "No se pudo analizar la nueva tabla."
              fail = True
               
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
            
            
def procesarOLAP(db):
    global Tables
    print "Inicio del proceso de tablas para OLAP."
    primarykeys=[]
    tables_column0=[]
    real_child_tables={}
    for tablename,table in Tables.iteritems():
        rchild_tables = {}
        real_child_tables[tablename] = rchild_tables

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
                    'required': False,
                    }
                
                real_child_tables[tablename][ctablename].append(reverse_child)
        
    for tablename,table in Tables.iteritems():
        for pk in table.primary_key:
            primarykeys.append("%s.%s" % (tablename ,pk))
            
        if 'column0' in table.field:
            tables_column0.append(tablename)
            
    print "*** TABLAS ***"               
    computarTablas(db,list(sorted(tables_column0)),real_child_tables,tables_column0)
    print "=============="
    
def computarTablas(db,lstTablas,real_child_tables,tables_column0, seen_tables = []):
    doctables = []
    for table in sorted(lstTablas): 
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

        print table, estadistica[0], estadistica[1], estadistica[2]
        complejidad = 0
        tipo = "general"
        
        for ctablename, lstrel in sorted(real_child_tables[table].iteritems()):
            rtype = "weak"
            reltypes = set([])
                
            for rel in lstrel: reltypes|=set([rel['type']])
            
            if "required" in reltypes: rtype = "required"
            elif "strong" in reltypes: rtype = "strong"
            elif "reverse" in reltypes: rtype = "reverse"
            
            if "required" in reltypes: print ":",
            
            if re.match("lineas",ctablename): 
                tipo = "documento"
                rtype = "documento"
                if ctablename not in doctables: doctables.append(ctablename)

            if rtype == "reverse" and ctablename not in tables_column0 and rtype!="documento":           continue
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
            elif ctablename in lstTablas:
                print "?--",
                complejidad+=10
            elif rtype == "documento":
                print "|--",
                complejidad+=10
            else:
                print " --",
                complejidad+=1
                
            estadistica2 = {0:0,1:0,2:0}
            print ctablename,":",
            for rel in lstrel:
                field1 = rel['field']
                field2 = rel['nfield']
                print field1 , "->", field2, 
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
                
            print ";", estadistica2[0], estadistica2[1], estadistica2[2]
        print "> %s (%d)" % (tipo, complejidad)
        if len(real_child_tables[table]): print
           
    doctables = list(set(doctables) - set(seen_tables))
    if len(doctables):
        computarTablas(db,list(sorted(doctables)),real_child_tables,tables_column0, list(set(seen_tables) | set(lstTablas)))
    
            
    
  

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
