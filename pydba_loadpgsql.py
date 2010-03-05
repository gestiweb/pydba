
def loadpgsqlfile(database, pgname, pgtype, pgtext):
    #print "--- Llamada a loadpgsqlfile" 
    #print " --- tipo: ", pgtype
    #print " --- name: ", pgname
    formatos_soportados = {}
    formatos_soportados["view"] = loadview
    formatos_soportados["function"] = loadfunction
    
    if pgtype not in formatos_soportados:
        print "ERROR: Tipo de objeto PgSQL no soportado %s para %s" % (pgtype,pgname)
        return
        
    code, text = extractcode(pgtext)
    
    formatos_soportados[pgtype](database, pgname, code, text)
        

    



def loadview(database, pgname, code, sql):
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
    database.query("SAVEPOINT tmp_view;")
    try:
        database.query("CREATE OR REPLACE VIEW %s AS \n %s" % (pgname,sql))
    except:
        import sys, traceback
        database.query("ROLLBACK TO tmp_view;")
        print "ERROR: No se pudo crear la vista %s" % pgname
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60
    finally:
        database.query("RELEASE SAVEPOINT tmp_view;")
        
   
    
    





def loadfunction(database, pgname, code, sql):
    print "Iniciando creacion de la funcion %s . . ." % pgname
    print "Aviso: Aun no se soporta este tipo de objeto. No se carga."    
    
    
    
    
    
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