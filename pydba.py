#!/usr/bin/python
# -*- coding: utf-8 -*-
#   encoding: UTF8
import optparse

from pydba_loadmodule import load_module
from pydba_mtdparser import procesarOLAP, comprobarRelaciones
from pydba_repairdb import repair_db
from pydba_createdb import create_db
from pydba_execini import exec_ini

import os, sys, traceback 		# variables entorno

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
                        files_loaded=[],
                        modules={}
                        )
    parser.add_option("--diskcopy", help="Create a backup .pydbabackup"
                        ,dest="diskcopy", action="store_true")
                        
    parser.add_option("--getdiskcopy", help="Loads a .pydbabackup"
                        ,dest="getdiskcopy")

    parser.add_option("--addchecks", help="Creates database checks (constraints, unique indexes)"
                        ,dest="addchecks", action="store_true")
                        
    parser.add_option("--debug", help="Tons of debug output"
                        ,dest="debug", action="store_true")
                        
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
    
    g_action.add_option("-C","--createdb", action="store_const", const="create_db"
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
        
   
    if (options.action=="none"):
        print "You must provide at least one action"
    
    elif (options.action=="load_module"):
        db=load_module(options)
        repair_db(options,db)
    elif (options.action=="setup_olap"):
        db=load_module(options, preparse=True)
        procesarOLAP(db)
    elif (options.action=="check"):
        db=load_module(options, preparse=True)
        comprobarRelaciones()
    elif (options.action=="repair_db"):
        db=repair_db(options)
    elif (options.action=="create_db"):
        db=create_db(options)
        load_module(options,db)
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
                msg = ''
                if line == "--TABLE--\n": mode = 1
            elif mode == 1:
                if line[:3] == '-- ':
                    if line[3:9] == 'table:':
                        msg = 'loading ' + line[3:-1] + " "
                        sys.stdout.write(msg)
                        sys.stdout.flush()
                
                if line[:3] == '--*':
                    try:
                        sys.stdout.write("\r%s Ejecutando: %s... " % (msg,line[3:15]))
                        sys.stdout.flush()
                        db.query(line[3:]);
                    except:
                        print "Error en la sql:"
                        print line[3:-1]
                        print traceback.format_exc()
                if line == "--BEGIN-COPY--\n":
                    mode = 2
                    nlineas=0
            elif mode == 2:
                nlineas+=1
                db.putline(line)
                if nlineas % 13 == 0 or line == "\\.\n":
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

                
            
            
        
    
    
if __name__ == "__main__":
    main()
