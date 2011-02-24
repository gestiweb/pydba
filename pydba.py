#!/usr/bin/python
# -*- coding: utf-8 -*-
#   encoding: UTF8

import traceback
from base64 import b64decode, b64encode
import zlib
import optparse
import os, sys 		# variables entorno

start_errors = 0
try:
    import pg             # depends - python-pygresql
except:
    start_errors += 1
    print "*** No se encontró la librería 'pg'."
    print " Para instalar 'pg':"
    print " ... Debian & Ubuntu: sudo aptitude install python-pygresql"
    print
    print traceback.format_exc(1)
    
try:
    import _mysql     # depends - python-mysqldb
except:
    start_errors += 1
    print "*** No se encontró la librería '_mysql'."
    print " Para instalar '_mysql':"
    print " ... Debian & Ubuntu: sudo aptitude install python-mysqldb"
    print
    print traceback.format_exc(1)

try:
    from pydba_loadmodule import load_module
    from pydba_mtdparser import procesarOLAP, comprobarRelaciones
    from pydba_repairdb import repair_db
    from pydba_createdb import create_db
    from pydba_execini import exec_ini
except:
    start_errors += 1
    print "** Error mientras se importaban los modulos de pydba::"
    print traceback.format_exc(1)
    
    
if start_errors > 0:
    print "$$$ han habido %d errores de inicio de PyDBA. Se aborta la carga." % start_errors
    sys.exit(1)
    
def main():

    parser = optparse.OptionParser()
    parser.set_defaults(
                        action="none", 
                        dhost="localhost", 
                        dport="5432", 
                        ddriver="pgsql", 
                        duser="postgres", 
                        dpasswd="",
                        loaddir=".",
                        quiet=False,
                        verbose=False,
                        debug=False,
                        full=False,
                        diskcopy=False,
                        getdiskcopy=False,
                        rebuildtables=False,
                        flscriptparser=False,
                        addchecks=False,
                        preparse = False,
                        rebuildalone  = False,
                        transactions = False,
                        files_loaded=[],
                        modules={}
                        )
    parser.add_option("--rebuildalone", help="Disallow rebuilds if other users are connected"
                        ,dest="rebuildalone", action="store_true")
                        
    parser.add_option("--diskcopy", help="Create a backup .pydbabackup"
                        ,dest="diskcopy", action="store_true")
                        
    parser.add_option("--getdiskcopy", help="Loads a .pydbabackup"
                        ,dest="getdiskcopy")

    parser.add_option("--addchecks", help="Creates database checks (constraints, unique indexes)"
                        ,dest="addchecks", action="store_true")
                        
    parser.add_option("--preparse", help="Checks and parses all MTD's"
                        ,dest="preparse", action="store_true")
                        
    parser.add_option("--transactional", help="Use transactions inside PyDBA"
                        ,dest="transactions", action="store_true")
                        
    parser.add_option("--debug", help="Tons of debug output"
                        ,dest="debug", action="store_true")
                        
    parser.add_option("--safe", help="Enable safe mode. Disables table rebuild and psql load."
                        ,dest="safe", action="store_true")
                        
    parser.add_option("-v", help="Be more verbose"
                        ,dest="verbose", action="store_true")
                        
    parser.add_option("-q", help="Quiet. Produce less output."
                        ,dest="quiet", action="store_true")
    
    parser.add_option("-P", help="Parse QS Files using flscriptarser"
                        ,dest="flscriptparser", action="store_true")
    
    parser.add_option("--full", help="Deeper and slower checks"
                        ,dest="full", action="store_true")
    
    parser.add_option("--rebuildtables", help="DROP and CREATE tables again"
                        ,dest="rebuildtables", action="store_true")
    
    g_action = optparse.OptionGroup(parser, "Actions","You MUST provide one of :")
    
    # ******************* ACTIONS
    g_action.add_option("-T","--TEST", action="store_const", const="test_pydba"
        ,dest="action", help="Do some checks on PyDBA")
    
    g_action.add_option("-l","--lmod", action="store_const", const="load_module"
        ,dest="action", help="load modules")
        
    g_action.add_option("-O","--olap", action="store_const", const="setup_olap"
        ,dest="action", help="setup olap tables")
        
    g_action.add_option("-r","--reload-mtd", action="store_const", const="reload_mtd"
        ,dest="action", help="Parses MTD files on DB and complete tables")
                
    g_action.add_option("-R","--repairdb", action="store_const", const="repair_db"
        ,dest="action", help="Execute tests to repair DB")
    
    g_action.add_option("--create","--createdb", action="store_const", const="create_db"
        ,dest="action", help="Create a new Database with basic fl* tables")
    
    g_action.add_option("-c","--check", action="store_const", const="check"
        ,dest="action", help="Check relations for the DB")

    g_action.add_option("-M","--mysql2pgsql", action="store_const", const="mysql_convert"
        ,dest="action", help="Convert MySQL Database to PostgreSQL")
                
    parser.add_option_group(g_action)  
    # ******************* CONFIG
    
    g_options = optparse.OptionGroup(parser, "Options",
                "Optional database and host selection. Some actions use them")
    g_options.add_option("--dhost",dest="dhost", help="Set the destination host")
    g_options.add_option("--dport",dest="dport", help="Set the destination port")
    g_options.add_option("--ddriver",dest="ddriver", help="Set the driver for dest DB (mysql; pgsql)")
    g_options.add_option("--ddb",dest="ddb", help="Set DDB as destination Database")
    g_options.add_option("--duser",dest="duser", help="Provide user for DB connection")
    g_options.add_option("--dpasswd",dest="dpasswd", help="Provide password for DB connection")
    
    g_options.add_option("--ohost",dest="ohost", help="Set the origin host")
    g_options.add_option("--oport",dest="oport", help="Set the origin port")
    g_options.add_option("--odriver",dest="odriver", help="Set the driver for origin DB (mysql; pgsql)")
    g_options.add_option("--odb",dest="odb", help="Set DDB as origin Database")
    g_options.add_option("--ouser",dest="ouser", help="Provide user for origin DB connection")
    g_options.add_option("--opasswd",dest="opasswd", help="Provide password for origin DB connection")
    
    g_options.add_option("--loaddir",dest="loaddir", help="Select Working Directory for Modules")

    g_options.add_option("--loadbaselec",dest="loadbaselec", help="Import CSV File to Baselec table ('-' for stdin)")
    
    g_options.add_option("--loadini", dest="loadini", help="load and execute INI file")
                
    parser.add_option_group(g_options)  
    
    
                
    #parser.add_option("-f", "--file", dest="filename",
    #                  help="write report to FILE", metavar="FILE")
    #parser.add_option("-q", "--quiet",
    #                  action="store_false", dest="verbose", default=True,
    #                  help="don't print status messages to stdout")
    
    for param in os.environ.keys():
        if param[:5]=="PYDBA":
          value=os.environ[param]
          if param=="PYDBA_DHOST":
            parser.set_defaults(dhost=value)
          elif param=="PYDBA_DUSER":
            parser.set_defaults(duser=value)
          elif param=="PYDBA_DPASSWD":
            parser.set_defaults(dpasswd=value)
          elif param=="PYDBA_DPORT":
            parser.set_defaults(dport=value)
          else:
            print "Unknown env var: %20s %s" % (param,value)    

    (options, args) = parser.parse_args()
    
    if options.loadini: 
        exec_ini(options, options.loadini)
     
    if options.ddriver and not options.odriver: options.odriver=options.ddriver
    if options.dhost and not options.ohost: options.ohost=options.dhost
    if options.duser and not options.ouser: options.ouser=options.duser
    if options.dpasswd and not options.opasswd: options.opasswd=options.dpasswd
    if options.ddb and not options.odb: options.odb=options.ddb
    if options.dport and not options.oport: options.oport=options.dport
    if options.diskcopy:
        options.full = True
        #options.rebuildtables = True
        
    if ( options.action=="setup_olap" or 
        options.action=="check" or
        options.getdiskcopy or
        options.diskcopy ) : 
        options.preparse = True
        
        
           
    if (options.action=="none"):
        print "You must provide at least one action"
    
    elif (options.action=="load_module"):
        db=load_module(options, preparse = options.preparse)
        repair_db(options,db)
    elif (options.action=="setup_olap"):
        db=load_module(options, preparse = options.preparse)
        olap = procesarOLAP(db)
        olapfilename = "relationdata.yaml"
        f1 = open(olapfilename,"w")
        f1.write(olap)
        f1.close()
        print "Fichero %s guardado." % olapfilename
        
    elif (options.action=="check"):
        db=load_module(options, preparse = options.preparse)
        comprobarRelaciones()
    elif (options.action=="repair_db"):
        db=repair_db(options)
    elif (options.action=="create_db"):
        db=create_db(options)
        db=load_module(options, db, preparse = options.preparse)
        repair_db(options,db)
    elif (options.action=="test_pydba"):
        from pydba_mtdparser import export_table,create_table,import_table
        from pydba_utils import dbconnect
        
        db=dbconnect(options)
        if not db: return
        data=export_table(options,db,"pedidoscli")
        db.query("DELETE FROM %s" % "pedidoscli_aux")
        import_table(options,db,"pedidoscli_aux",data,data[0])
        
    else:
        print "Unknown action: " + options.action;
    
    if options.getdiskcopy:
        print "Iniciando carga del fichero '%s'" % options.getdiskcopy
        if not options.full:
            print "WARN: No se ha pasado --full , por lo que no se han revisado las tablas antes de empezar."
        db.query("SET client_min_messages = error;");            
        f1 = open(options.getdiskcopy)
        mode = 0 # 0 - espera tabla; 1 - configurando; 2 - volcando datos
        while True:
            line = f1.readline()
            if not line: break
            if mode == 0:
                if line == "--TABLE--\n": mode = 1
                msg = ''
                rows = -1
                table = ""
                fields = []
                primarykey = ""
                buffers = []
                
            elif mode == 1:
                if line[:3] == '-- ':
                    splline = line[:-1].split(" ")               
                    
                    if splline[1] == 'primarykey:':
                        primarykey = splline[2]
                    
                    if splline[1] == 'fields:':
                        fields = splline[2].split(",")
                        
                    if splline[1] == 'table:':
                        table = splline[2]
                        msg = 'loading table: ' + table + " "
                        sys.stdout.write(msg)
                        sys.stdout.flush()
                    if splline[1] == 'rows:':
                        try:
                            rows = int(splline[2],16)
                        except:
                            rows = -1
                
                """
                if line[:3] == '--*':
                    try:
                        sys.stdout.write("\r%s Ejecutando: %s... " % (msg,line[3:15]))
                        sys.stdout.flush()
                        db.query(line[3:]);
                    except:
                        print "Error en la sql:"
                        print line[3:-1]
                        print traceback.format_exc()
                """
                if line == "--BEGIN-COPY--\n":
                    try:
                        sql = "BEGIN; LOCK \"%s\";" % table
                        sys.stdout.write("\r%s LOCKING" % (msg))
                        sys.stdout.flush()
                        db.query(sql);
                    except:
                        print "Error en la sql:"
                        print sql
                        print traceback.format_exc()
                        raise
                    
                    try:
                        sql = "TRUNCATE \"%s\";" % table
                        sys.stdout.write("\r%s TRUNCATE" % (msg))
                        sys.stdout.flush()
                        db.query(sql);
                    except:
                        print "Error en la sql:"
                        print sql
                        print traceback.format_exc()
                        raise
                    
                    mode = 2
                    nlineas=0
            elif mode == 2:
                if len(line)<2: continue
                if line == "--TABLE--\n": 
                    if nlineas > 0:
                        db.putline("\\.\n")
                            
                        if rows > 0:
                            sys.stdout.write("\r%s %d registros (%.2f%%).  SAVING      " % (msg,nlineas,float(nlineas*100.0)/rows))
                        else:
                            sys.stdout.write("\r%s %d registros.           SAVING      " % (msg,nlineas))
                        sys.stdout.flush()
                        try:
                            db.endcopy()
                            if rows > 0:
                                sys.stdout.write("\r%s %d registros (%.2f%%).  OK          " % (msg,nlineas,float(nlineas*100.0)/rows))
                            else:
                                sys.stdout.write("\r%s %d registros.           OK          " % (msg,nlineas))
                        except IOError:
                            sys.stdout.write("*** ERROR!\n")
                        sys.stdout.flush()
                    else:
                        sys.stdout.write("\r%s .. empty" % (msg))
                        sys.stdout.flush()
                        
                    sys.stdout.write("\r%s .. COMMIT                  " % (msg))
                    sys.stdout.flush()
                    try:
                        sql = "COMMIT;"
                        db.query(sql);
                    except:
                        print "Error en la sql:"
                        print sql
                        print traceback.format_exc()
                        raise
                        
                    sys.stdout.write("\r%s .. VACUUM                  " % (msg))
                    sys.stdout.flush()
                    try:
                        sql = "VACUUM FULL FREEZE \"%s\";" % table
                        db.query(sql);
                    except:
                        print "Error en la sql:"
                        print sql
                        print traceback.format_exc()
                        raise
                        

                    sys.stdout.write("\r%s .. ANALYZE                  " % (msg))
                    sys.stdout.flush()
                    try:
                        sql = "ANALYZE \"%s\";" % table
                        db.query(sql);
                    except:
                        print "Error en la sql:"
                        print sql
                        print traceback.format_exc()
                        raise
                        
                        
                    if rows > 0:
                        sys.stdout.write("\r%s %d registros (%.2f%%).              " % (msg,nlineas,float(nlineas*100.0)/rows))
                    else:
                        sys.stdout.write("\r%s %d registros.                       " % (msg,nlineas))
                    sys.stdout.write("\n")
                        
                    if len(buffers)> 0 and len(buffers) != len(fields):
                        print "\n ERROR: Se esperaban %d campos pero hay %d ?? " % (len(fields),len(buffers))
                    mode = 1
                    msg = ''
                    rows = -1
                    table = ""
                    fields = []
                    primarykey = ""
                        
                    buffers = []
                    
                if line == '-- bindata >>\n':
                    if len(buffers)> 0 and len(buffers) != len(fields):
                        print "\n ERROR: Se esperaban %d campos pero hay %d ?? " % (len(fields),len(buffers))
                    if rows > 0:
                        sys.stdout.write("\r%s %d registros (%.2f%%).  PRELOAD     " % (msg,nlineas,float(nlineas*100.0)/rows))
                    else:
                        sys.stdout.write("\r%s %d registros.           PRELOAD     " % (msg,nlineas))
                    sys.stdout.flush()
                    buffers = []
                    continue
                if line[:2] == '--': continue
                if len(buffers)<len(fields):
                    if rows > 0:
                        sys.stdout.write("\r%s %d registros (%.2f%%).  PROCESS    " % (msg,nlineas,float(nlineas*100.0)/rows))
                    else:
                        sys.stdout.write("\r%s %d registros.           PROCESS    " % (msg,nlineas))
                    sys.stdout.flush()
                    buf = zlib.decompress(b64decode(line[:-1]))
                    mfields = buf.split("\t")
                    buffers.append(mfields)
                    if len(buffers) == len(fields):
                        if nlineas == 0:
                            
                            try:
                                sql = "COPY \"%s\" (%s) FROM STDIN;" % (table, ", ".join(fields))
                                db.query(sql);
                            except:
                                print "Error en la sql:"
                                print sql
                                print traceback.format_exc()
                                raise

                        # ROTAR FILAS
                        lrows = []
                        for n in buffers[0]: lrows.append([])
                        for n,buffer1 in enumerate(buffers):
                            for row,val in zip(lrows,buffer1):
                                row.append(val)
                            
                        if rows > 0:
                            sys.stdout.write("\r%s %d registros (%.2f%%).  UPLOADING   " % (msg,nlineas,float(nlineas*100.0)/rows))
                        else:
                            sys.stdout.write("\r%s %d registros.           UPLOADING   " % (msg,nlineas))
                        sys.stdout.flush()
                        
                        for row in lrows:
                            db.putline("\t".join(row)+"\n")
                            nlineas+=1
                        del lrows
                        
                            
                    """
                    db.putline(line)
                    if line != "\\.\n":
                        nlineas+=1
                
                    if nlineas % 7 == 0 or line == "\\.\n":
                        if rows > 0:
                            sys.stdout.write("\r%s %d registros cargados (%.2f%%). " % (msg,nlineas,float(nlineas*100.0)/rows))
                        else:
                            sys.stdout.write("\r%s %d registros cargados. " % (msg,nlineas))
                        sys.stdout.flush()
                    if line == "\\.\n":
                        try:
                            db.endcopy()
                            sys.stdout.write("OK             \n")
                        except IOError:
                            sys.stdout.write("*** ERROR!\n")
                        
                        sys.stdout.flush()
                        mode = 0
                        """


            
            
        
    
    
if __name__ == "__main__":
    main()
