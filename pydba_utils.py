#!/usr/bin/python
#   encoding: UTF8

# Fichero de utilidades para PyDBa

import pg       # depends - python-pygresql
import _mysql   # depends - python-mysqldb
import os       # permite la función os.join
import getpass
typeSHALib = None
try:
    import hashlib
    typeSHALib = "hashlib"
except ImportError:
    import sha
    typeSHALib = "sha"

ddb_cn = None
odb_cn = None
# Calcular extension de un fichero
def f_ext(filename):
    name_s=filename.split(".")
    numsplits=len(name_s)
    return name_s[numsplits-1]

def copy_escapechars(text):
    if text is None: return "\\N"
    if text is True: return "t"
    if text is False: return "f"
        
    text=str(text)
    transorder = [
        "\\n",
        "\\",
        "\b","\f","\r","\t","\v","\n"
        ]
    transstr={
        "\\n": "\n",
        "\\": "\\\\",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
        "\v": "\\v"
        }

    for key in transorder:
        val = transstr[key]
        text=text.replace(key,val)
    
    return text
    
# Cargar un fichero en ASCII codificado como UTF8
def loadfile_inutf8(root, name):
    f1=open(os.path.join(root, name),"r")
    contents=f1.read()
    f1.close()
    contents=contents.decode('iso-8859-15')
    contents=contents.encode('utf8')
    return contents

flscriptparser_filelist = [] 

def flscriptparser(root=None,name=None,launch=False):
    global flscriptparser_filelist
    if root and name:
        filename=os.path.join(root, name)
        flscriptparser_filelist.append(filename)
    
    if launch and len(flscriptparser_filelist)>0:
        os.execvp("flscriptparser2",["flscriptparser2"]+flscriptparser_filelist)
        flscriptparser_filelist=[]

def sha_hexdigest(text):
    global typeSHALib
    if typeSHALib == "sha":
        return sha.new(text).hexdigest()
    elif typeSHALib == "hashlib":
        return hashlib.sha1(text).hexdigest();



# Crear una firma SHA1 preferentemente a partir de iso-8859-15
def SHA1(text):

    utext=text.decode("utf8")
    isotext=""
    for line in utext.split("\n"):
        line+="\n"
        try:
            isotext+=line.encode("iso-8859-15")
        except:
            try:
                isotext+=line.encode("windows-1250")
            except:
                try:
                    isotext+=line.encode("iso-8859-1")
                except:
                    print line
                    return None
    
    return sha_hexdigest(isotext)



def dbconnect(options):
    global ddb_cn, odb_cn
    if (not options.dpasswd):
        options.dpasswd = getpass.getpass("dPassword:")
        
    if (
        options.ddriver == options.odriver and
        options.dhost == options.ohost  and
        options.dport == options.oport and
        options.ddb == options.odb and
        options.duser == options.ouser 
        ):
        options.opasswd = options.dpasswd 
        options.samedatabase = True
        if odb_cn:
            ddb_cn = odb_cn
            return ddb_cn
    else:
        print "WARN: El servidor de destino no es el mismo que el de origen."
        print options.ddriver , options.odriver 
        print options.dhost ,options.ohost  
        print options.dport ,options.oport
        print options.duser ,options.ouser 
        options.samedatabase = False
            
        #options.dpasswd = raw_input("Password: ")
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
            cn.query("SET client_min_messages = warning;");            
    except:
        print("Error trying to connect to database '%s' in host %s:%s" 
                " using user '%s'" % (
                        options.ddb,
                        options.dhost,
                        options.dport,
                        options.duser,
                        ))
        raise
        return 0
            
    if options.debug: 
        print("* Succesfully connected to database '%s' in host %s:%s" 
                " using user '%s'" % (
                        options.ddb,
                        options.dhost,
                        options.dport,
                        options.duser,
                        ))
    ddb_cn = cn
    return cn
    
    

def odbconnect(options):
    global ddb_cn, odb_cn
    if (not options.opasswd):
        options.opasswd = getpass.getpass("oPassword:")
        
    if (
        options.ddriver == options.odriver and
        options.dhost == options.ohost  and
        options.dport == options.oport and
        options.ddb == options.odb and
        options.duser == options.ouser 
        ):
        options.dpasswd = options.opasswd 
        options.samedatabase = True
        if ddb_cn:
            odb_cn = ddb_cn
            return odb_cn
    else:
        print "WARN: El servidor de destino no es el mismo que el de origen."
        print options.ddriver , options.odriver 
        print options.dhost ,options.ohost  
        print options.dport ,options.oport
        print options.duser ,options.ouser 
        options.samedatabase = False

        
        #options.opasswd = raw_input("Password: ")
    try:
        if (options.odriver=='mysql'):
            cn = _mysql.connect(
                        db=options.odb, 
                        port=int(options.oport),
                        host=options.ohost, 
                        user=options.ouser, 
                        passwd=options.opasswd )
            cn.set_character_set("UTF8") # Hacemos la codificación a UTF8.
        # Si la conexión es a Postgres , de la siguiente manera            
        else:
            cn = pg.connect(
                        dbname=options.odb, 
                        port=int(options.oport),
                        host=options.ohost, 
                        user=options.ouser, 
                        passwd=options.opasswd )  
            cn.query("SET client_encoding = 'UTF8';")                
            cn.query("SET client_min_messages = warning;");            
    except:
        print("Error trying to connect to *origin* database '%s' in host %s:%s" 
                " using user '%s'" % (
                        options.odb,
                        options.ohost,
                        options.oport,
                        options.ouser,
                        ))
        return 0
            
    if options.debug: 
        print("* Succesfully connected to *origin* database '%s' in host %s:%s" 
                " using user '%s'" % (
                        options.odb,
                        options.ohost,
                        options.oport,
                        options.ouser,
                        ))
                        
    odb_cn = cn

    return cn
    
    
