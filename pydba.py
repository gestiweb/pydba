#!/usr/bin/python
#   encoding: UTF8
import optparse

from pydba_loadmodule import load_module
from pydba_repairdb import repair_db
from pydba_createdb import create_db

    

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
												rebuildtables=False,
                        )
    parser.add_option("--debug", help="Tons of debug output"
                        ,dest="debug", action="store_true")
                        
    parser.add_option("-v", help="Be more verbose"
                        ,dest="verbose", action="store_true")
                        
    parser.add_option("-q", help="Quiet. Produce less output."
                        ,dest="quiet", action="store_true")
    
    parser.add_option("--full", help="Deeper and slower checks"
                        ,dest="full", action="store_true")
    
    parser.add_option("--rebuildtables", help="DROP and CREATE tables again"
                        ,dest="rebuildtables", action="store_true")
    
    g_action = optparse.OptionGroup(parser, "Actions","You MUST provide one of :")
    
    # ******************* ACTIONS
    g_action.add_option("-l","--lmod", action="store_const", const="load_module"
        ,dest="action", help="load modules")
        
    g_action.add_option("-r","--reload-mtd", action="store_const", const="reload_mtd"
        ,dest="action", help="Parses MTD files on DB and complete tables")
                
    g_action.add_option("-R","--repairdb", action="store_const", const="repair_db"
        ,dest="action", help="Execute tests to repair DB")
    
    g_action.add_option("-C","--createdb", action="store_const", const="create_db"
        ,dest="action", help="Create a new Database with basic fl* tables")
    
    g_action.add_option("-M","--mysql2pgsql", action="store_const", const="mysql_convert"
        ,dest="action", help="Convert MySQL Database to PostgreSQL")
                
    g_action.add_option("-i","--ini", action="store_const", const="exec_ini"
        ,dest="action", help="load and execute INI file")
                
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
    g_options.add_option("--loaddir",dest="loaddir", help="Select Working Directory for Modules")
    
    parser.add_option_group(g_options)  
    
    
                
    #parser.add_option("-f", "--file", dest="filename",
    #                  help="write report to FILE", metavar="FILE")
    #parser.add_option("-q", "--quiet",
    #                  action="store_false", dest="verbose", default=True,
    #                  help="don't print status messages to stdout")
    
    (options, args) = parser.parse_args()
    
    if (options.action=="none"):
        print "You must provide at least one action"
    
    elif (options.action=="load_module"):
        db=load_module(options);
        repair_db(options,db)
    elif (options.action=="repair_db"):
        db=repair_db(options);
    elif (options.action=="create_db"):
        db=create_db(options);
        load_module(options,db)
        repair_db(options,db)
    else:
        print "Unknown action: " + options.action;
    
    
    
if __name__ == "__main__":
    main()