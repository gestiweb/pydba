# -*- coding: utf-8 -*-
import re, traceback
import pg       # depends - python-pygresql

pgobjects = {}
dependency_order = [] # En orden de creación
idxfullfilename = {}

def loadpgsqlfile(options, database, pgname, pgtype, pgtext, fullfilename):
    #print "--- Llamada a loadpgsqlfile" 
    #print " --- tipo: ", pgtype
    #print " --- name: ", pgname
    formatos_soportados = {}
    formatos_soportados["view"] = loadview
    formatos_soportados["function"] = loadfunction
    formatos_soportados["sql1"] = loadsql1
    
    if pgtype not in formatos_soportados:
        print "ERROR: Tipo de objeto PgSQL no soportado %s para %s" % (pgtype,pgname)
        return 
        
    code, text = extractcode(pgtext)
    
    if pgname not in idxfullfilename: idxfullfilename[pgname] = []
    idxfullfilename[pgname].append(fullfilename)
    return formatos_soportados[pgtype](options, database, pgname, code, text)
        

    



def loadview(options, database, pgname, code, sql):
    # print "Iniciando creacion de la vista %s . . ." % pgname
    if ";" in sql:
        print "ERROR: No se admiten puntos y coma en el fichero %s " % pgname
        return
        
    # 1.- Comprobar la cabecera del texto.
    try:
        linea1 = code[0].split(" ")
        if linea1[0]=="VIEW":
            if linea1[1]==pgname:
                # print "Cabecera valida."
                pass
            else:
                print "El nombre del fichero %s no coincide con el nombre de la vista %s" % (pgname, linea1[1])
                return
        else:
            print "Error leyendo la cabecera de la vista."
            return
        
    except:
        print "Error inesperado leyendo el fichero de la vista."
        return
     
    """    
    try:
        database.query("DROP VIEW %s;" % pgname)
    except:
        pass
    """
    #if options.transactions:
    #    database.query("SAVEPOINT tmp_view;")
    """
    try:
        database.query("CREATE OR REPLACE VIEW %s AS \n %s" % (pgname,sql))
    except:
        import sys, traceback
        #if options.transactions:
        #    database.query("ROLLBACK TO tmp_view;")
        print "ERROR: No se pudo crear la vista %s" % pgname
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60
    """
    obj = ObjPgSql()
    obj.setAttr("name",pgname)
    obj.setAttr("drop","DROP VIEW %s;" % pgname)
    obj.setAttr("create","CREATE OR REPLACE VIEW %s AS \n %s" % (pgname,sql))
    
    if obj.name in pgobjects:
        if options.verbose:
            print "Ya existe una vista con el nombre %s, se ignora." % obj.name
    else:
        pgobjects[obj.name] = obj
    #if options.transactions:
    #    database.query("RELEASE SAVEPOINT tmp_view;")
    return True
   
    
    





def loadfunction(options, database, pgname, code, sql):
    print "Iniciando creacion de la funcion %s . . ." % pgname
    print "Aviso: Aun no se soporta este tipo de objeto. No se carga."    
    return True
    
    
    
    
    
def extractcode(pgtext):
    code = []
    text = []
    
    for linea in pgtext.split("\n"):
        p_linea = linea.strip()
        if len(p_linea)>2 and p_linea[0:3]=="--:":
            code.append(p_linea[3:].strip())
        else:
            text.append(linea)

    return (code, "\n".join(text));
    
    
class ObjPgSql:
    # (required, default, vartype)
    check_dict = {
        "depends" : (False, [],     list),
        "name" :    (True,  None,   str),
        "drop" :    (True,  None,   str),
        "create" :  (True,  None,   str),
    }
    def __init__(self):
    
        for k, v in self.check_dict.iteritems():
            required, default, vartype = v
            if required == False:
                self.setAttr(k,default)
                
    def check(self):
        result = True
        for k, v in self.check_dict.iteritems():
            required, default, vartype = v
            if not hasattr(self,k):
                print "falta el atributo", k
                result = False
            elif type(getattr(self,k)) is not vartype :
                print "La variable", k, repr(getattr(self,k)), "no es del tipo", vartype
                result = False
                        
            
        return result
        
        
    def setAttr(self,name, value):
        assert(name in self.check_dict)
        try:
            if self.check_dict[name][2] is list and type(value) is not list:
                value = [ s.strip() for s in value.split(",")]
                value = filter(lambda x: len(x)>0, value)
            value = self.check_dict[name][2](value)
        except:
            print "Error al intentar convertir", type(value), repr(value), "en el tipo", self.check_dict[name][2]
            print traceback.format_exc()
            raise
        
        setattr(self,name,value)
        
    def __str__(self):
        txt = "{"
        for k, v in self.check_dict.iteritems():
            if not hasattr(self,k):
                txt += "%s: Undefined, " % k
            else:
                val = getattr(self,k)
                txt += "%s: %s, " % (k, repr(val))
        
        txt += "}"
        
        return txt
    



def processsql(pgtext):
    obj = ObjPgSql()
    text = ""
    savetovar = None
    for linea in pgtext.split("\n"):
        ro = re.match("^--\\* (\w+)([:;=/%&#!|@]+)(.*)$",linea)
        if ro:
            #print ro.group(0)
            if savetovar:
                obj.setAttr(savetovar, text)
                savetovar = None
            varname, operador, valor = ro.group(1,2,3)
            valor = valor.strip()
            varname = varname.lower()
            
            if operador == ":" :
                obj.setAttr(varname, valor)
                
            if operador == "::" :
                text = ""
                savetovar = varname
        else:
            text += linea + "\n"

    if savetovar:
        obj.setAttr(savetovar, text)
        savetovar = None
                
    if obj.check():
        #print str(obj)
        return obj
    else:
        return None
        

    
    
def loadsql1(options, database, pgname, code, sql):    
    global pgobjects
    
    if options.debug: print "Iniciando carga del fichero %s . . ." % pgname
    #print sql
    obj = processsql(sql)
    if obj is None:
        print "Se encontraron errores al cargar %s." % pgname
        return 
        
    obj.pgname = pgname
    if obj.name in pgobjects:
        print "ERROR: Ya existe un fichero cargado con el nombre %s (%s). (original era: %s) " % (obj.name,pgname,pgobjects[obj.name].name)
        return
    pgobjects[obj.name] = obj
    return True
            
    #    print "Aviso: Aun no se soporta este tipo de objeto. No se carga."    


def process_dependencies():
    global pgobjects,dependency_order
    pOrigen = []
    pDestino = []
    #print "Comprobando dependencias ..."
    
    for name, obj in pgobjects.iteritems():
        pOrigen.append((obj.name,obj.depends))
    
    nd = 0
    maxdepth = 25
    
    while len(pOrigen)>0 and nd < maxdepth:
        modificadas = 0
        for name, depends in pOrigen[:]:
            insertar = True
            for depname in depends:
                if depname not in pDestino:
                    insertar = False
            if insertar:
                pDestino.append(name)
                pOrigen.remove((name,depends))
                modificadas += 1
        
        if modificadas == 0:
            print "Imposible solucionar las siguientes dependencias: (relación circular)"
            print pOrigen
            break;
                
            
            
    
        
    if nd >= maxdepth :
        print "** ERROR: se ha superado la profundidad máxima de resolucion de dependencias"
        print nd, maxdepth
    
    if len(pOrigen)>0:
        for name, depends in pOrigen[:]:
            pDestino.append(name)
            pOrigen.remove((name,depends))
    
    dependency_order = pDestino

def filename(pgname):
    global idxfullfilename
    if pgname not in idxfullfilename:
        print "PyDBA internal error: %s not found on filename index !" % pgname
        print ", ".join(idxfullfilename.keys())
        return pgname
    return ":".join(idxfullfilename[pgname])            

def process_drop(options, db):
    global pgobjects,dependency_order
    if options.safe: # disable drop in safe-mode. 
        return False
    for name in reversed(dependency_order):
        #print "Borrando objeto", name
        obj = pgobjects[name]
        try:
            db.query(obj.drop)
        except pg.ProgrammingError, e:
            if e.args[0].startswith("ERROR:  no existe"):
                debug = False
                if options.verbose:
                    print "INFO: objeto '%s' aun no existe: " % name, e.args[0].strip()
            else:
                print "ERROR: Error borrando objeto '%s':" % name, filename(name)
                debug = True
            if debug:
                print e.args[0].strip()
                if len(e.args) > 1 and e.args[1]:print e.args[1]
        
        
        
def process_create(options,db):
    global pgobjects,dependency_order
    try:
        db.query("CREATE LANGUAGE plpgsql;")
    except:
        pass
    for name in dependency_order:
        #print "Creando objeto", name
        obj = pgobjects[name]
        try:
            db.query(obj.create)
        except:
            if not options.safe or options.verbose: # ignore this error in safe-mode
                print "Error creando objeto %s:" % name, filename(name)
                # print "sql:",obj.create
                print traceback.format_exc()
        
