#!/usr/bin/python
#   encoding: UTF8

# Fichero de utilidades para PyDBa
import sha
import pg       # depends - python-pygresql
import _mysql   # depends - python-mysqldb
import os       # permite la función os.join

# Calcular extension de un fichero
def f_ext(filename):
    name_s=filename.split(".")
    numsplits=len(name_s)
    return name_s[numsplits-1]

# Cargar un fichero en ASCII codificado como UTF8
def loadfile_inutf8(root, name):
    f1=open(os.path.join(root, name),"r")
    contents=f1.read()
    f1.close()
    contents=contents.decode('iso-8859-15')
    contents=contents.encode('utf8')
    return contents


# Crear una firma SHA1 preferentemente a partir de iso-8859-15
def SHA1(text):
    try:
        utext=text.decode("utf8")
        isotext=utext.encode("iso-8859-15")
    except:
        print("WARNING: Error al convertir texto utf8 a iso-8859-15."
                " Se utiliza el utf8 en su lugar.")
        isotext=text
        raise
    return sha.new(isotext).hexdigest();



def dbconnect(options):
    if (not options.dpasswd):
        options.dpasswd = raw_input("Password: ")
    try:
        if (options.ddriver=='mysql'):
            cn = _mysql.connect(
                        db=options.ddb, 
                        port=int(options.dport),
                        host=options.dhost, 
                        user=options.duser, 
                        passwd=options.dpasswd )
            cn.set_character_set("UTF8") # Hacemos la codificación a UTF8.
        # Si la conexión es a Postgres , de la siguiente manera            
        else:
            cn = pg.connect(
                        dbname=options.ddb, 
                        port=int(options.dport),
                        host=options.dhost, 
                        user=options.duser, 
                        passwd=options.dpasswd )  
            cn.query("SET client_encoding = 'UTF8';")                
    except:
        print("Error trying to connect to database '%s' in host %s:%s" 
                " using user '%s'" % (
                        options.ddb,
                        options.dhost,
                        options.dport,
                        options.duser,
                        ))
        return 0
            
    if options.debug: 
        print("* Succesfully connected to database '%s' in host %s:%s" 
                " using user '%s'" % (
                        options.ddb,
                        options.dhost,
                        options.dport,
                        options.duser,
                        ))
    return cn
    
    
