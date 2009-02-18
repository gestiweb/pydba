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

def copy_escapechars(text):
    if text is None: return "\\N"
    if text is True: return "t"
    if text is False: return "f"
        
    text=str(text)
    transstr={
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
        "\v": "\\v",
        "\\": "\\\\"}

    for key,val in transstr.iteritems():
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
    
    if launch:
        os.execvp("flscriptparser",["flscriptparser"]+flscriptparser_filelist)
        flscriptparser_filelist=[]


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
    
    

def odbconnect(options):
    if (not options.opasswd):
        options.opasswd = raw_input("Password: ")
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
    return cn
    
    
