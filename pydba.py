#!/usr/bin/python
#   encoding: UTF8

#Importar módulo mysql para MySQL sobre Python 
import _mysql
# Importar módulo PyGreSQL para Postgres sobre Python
import pg
# Importar módulo para leer ficheros CFG y INI
import ConfigParser
# Importar el módulo para leer la línea de comandos
from optparse import OptionParser
from optparse import OptionGroup
import sys
import sha
import os
import re


# import subprocess

import xml.parsers.expat

    
def f_ext(filename):
  name_s=filename.split(".")
  numsplits=len(name_s)
  return name_s[numsplits-1]

def loadfile_inutf8(root, name):
  f1=open(os.path.join(root, name),"r")
  contents=f1.read()
  f1.close()
  #if (f_ext(name) in ['ui','ts']):
  #  contents=contents.decode('iso-8859-15')
  #else:
  #  contents=contents.decode('iso-8859-15')
  
  contents=contents.decode('iso-8859-15')
  contents=contents.encode('utf8')
  return contents
      
      


# Funcion de compatibilidad con QSA y AbanQ.
def QT_TRANSLATE_NOOP(From,String):
  return String

class XMLParser_data:
  _name=""
  _data=""
  _attrs={}
  def __str__(self):
    return self._data.encode('utf8')
    
  pass
  
class XMLParser:
  root=XMLParser_data()
  root._name="root"
  stack=[]
  xmlusing=root
  num_element=1
  
  
  def start_element(self,name, attrs):
    method_name=name.lower()
    if (not method_name.isalpha()):
      num_element+=1
      method_name="element%d" % num_element
    
    new=XMLParser_data()
    setattr(self.xmlusing,method_name,new)

    self.stack.append(self.xmlusing)
    self.xmlusing=new
    self.xmlusing._name=name
    self.xmlusing._attrs=attrs
    
  def end_element(self,name):
    if (name!=self.xmlusing._name):
      print "WARNING: <%s> ... </%s>" % (xmlusing._name,name)
    self.xmlusing=self.stack.pop()
  def char_data(self,data):
    
    m = re.search('QT_TRANSLATE_NOOP\("([^"]*)","([^"]*)"\)', data)
   
    if (m):
      data=QT_TRANSLATE_NOOP(m.group(1),m.group(2))
    
    self.xmlusing._data=data    
  
  def parseText(self,text):
  
    p = xml.parsers.expat.ParserCreate()
    
    p.StartElementHandler = self.start_element
    p.EndElementHandler = self.end_element
    p.CharacterDataHandler = self.char_data
    
    p.Parse(text, 1)

def SHA1(text):
  return sha.new(text).hexdigest();

def main():
  
  parser = OptionParser()
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
                      debug=False
                      )
  parser.add_option("--debug",dest="debug", action="store_true", help="Tons of debug output")
  parser.add_option("-v",dest="verbose", action="store_true", help="Be more verbose")
  parser.add_option("-q",dest="quiet", action="store_true", help="Be less verbose")
  
  g_action = OptionGroup(parser, "Actions","You MUST provide one of these:")
  
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
  
  g_options = OptionGroup(parser, "Options","Optional database and host selection. Some actions use them")
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
    load_module(options);
  elif (options.action=="repair_db"):
    repair_db(options);
  elif (options.action=="create_db"):
    create_db(options);
  else:
    print "Unknown action: " + options.action;
  
  # sys.exit(0)

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
    print("Error trying to connect to database '" + options.ddb + "' in host " + 
      options.dhost + ":" + options.dport + " using user '" + options.duser + "'")
    return 0
         
  if options.debug: 
    print("* Succesfully connected to database '" + options.ddb + "' in host " + 
    options.dhost + ":" + options.dport + " using user '" + options.duser + "'")
  return cn
  
  
def create_db(options):
  print "Creando Base de datos %s ..." % options.ddb
  ddb=options.ddb
  options.ddb="postgres"
  db=dbconnect(options)
  options.ddb=ddb
  try:
    db.query("CREATE DATABASE %s WITH TEMPLATE = template0 ENCODING = 'UTF8';" % options.ddb)
  except:
    print "Fallo al crear la base de datos %s. Se asume que ya está creada y se continúa." % ddb
  db.close
  db=dbconnect(options)
  db.query("""
CREATE TABLE flareas (
    descripcion character varying(100) NOT NULL,
    bloqueo boolean NOT NULL,
    idarea character varying(15) NOT NULL,
    CONSTRAINT flareas_pkey PRIMARY KEY (idarea)
);

CREATE TABLE flfiles (
    contenido text,
    bloqueo boolean NOT NULL,
    sha character varying(255),
    idmodulo character varying(15) NOT NULL,
    nombre character varying(255) NOT NULL,
    CONSTRAINT flfiles_pkey PRIMARY KEY (nombre)
);

CREATE TABLE flmetadata (
    tabla character varying(255) NOT NULL,
    bloqueo boolean,
    seq integer NOT NULL,
    "xml" text,
    CONSTRAINT flmetadata_pkey PRIMARY KEY (tabla)
);

CREATE TABLE flmodules (
    descripcion character varying(100) NOT NULL,
    bloqueo boolean NOT NULL,
    idmodulo character varying(15) NOT NULL,
    "version" character varying(3) NOT NULL,
    idarea character varying(15) NOT NULL,
    icono text,
    CONSTRAINT flmodules_pkey PRIMARY KEY (idmodulo)
);

CREATE TABLE flseqs (
    tabla character varying(255) NOT NULL,
    campo character varying(255) NOT NULL,
    seq integer NOT NULL,
    CONSTRAINT flseqs_pkey PRIMARY KEY (tabla)
);

CREATE SEQUENCE flserial_serie_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

SELECT pg_catalog.setval('flserial_serie_seq', 1, false);

CREATE TABLE flserial (
    serie integer DEFAULT nextval('flserial_serie_seq'::regclass) NOT NULL,
    sha character varying(255),
    CONSTRAINT flserial_pkey PRIMARY KEY (serie)
);

CREATE TABLE flsettings (
    valor text,
    flkey character varying(30) NOT NULL,
    CONSTRAINT flsettings_pkey PRIMARY KEY (flkey)
);

CREATE SEQUENCE flvar_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    NO MINVALUE
    CACHE 1;

SELECT pg_catalog.setval('flvar_id_seq', 1, false);

CREATE TABLE flvar (
    idsesion character varying(30) NOT NULL,
    valor text NOT NULL,
    idvar character varying(30) NOT NULL,
    id integer DEFAULT nextval('flvar_id_seq'::regclass) NOT NULL,
    CONSTRAINT flvar_pkey PRIMARY KEY (id)
);


INSERT INTO flareas (descripcion, bloqueo, idarea) VALUES('Sistema','f','sys');

INSERT INTO flmodules (descripcion, bloqueo, idmodulo, "version", idarea, icono) 
            VALUES('Administración','f','sys','0.0','sys','%s');

INSERT INTO  flsettings (valor, flkey) VALUES('2.3 Build 10872','sysmodver');


CREATE INDEX flareas_idarea_m1_idx ON flareas USING btree (idarea text_pattern_ops);
CREATE INDEX flareas_idareaup_m1_idx ON flareas USING btree (upper((idarea)::text) text_pattern_ops);
CREATE INDEX flfiles_nombre_m1_idx ON flfiles USING btree (nombre text_pattern_ops);
CREATE INDEX flfiles_nombreup_m1_idx ON flfiles USING btree (upper((nombre)::text) text_pattern_ops);
CREATE INDEX flmetadata_tabla_m1_idx ON flmetadata USING btree (tabla text_pattern_ops);
CREATE INDEX flmetadata_tablaup_m1_idx ON flmetadata USING btree (upper((tabla)::text) text_pattern_ops);
CREATE INDEX flmodules_idarea_m1_idx ON flmodules USING btree (idarea text_pattern_ops);
CREATE INDEX flmodules_idareaup_m1_idx ON flmodules USING btree (upper((idarea)::text) text_pattern_ops);
CREATE INDEX flmodules_idmodulo_m1_idx ON flmodules USING btree (idmodulo text_pattern_ops);
CREATE INDEX flmodules_idmoduloup_m1_idx ON flmodules USING btree (upper((idmodulo)::text) text_pattern_ops);
CREATE INDEX flsettings_flkey_m1_idx ON flsettings USING btree (flkey text_pattern_ops);
CREATE INDEX flsettings_flkeyup_m1_idx ON flsettings USING btree (upper((flkey)::text) text_pattern_ops);
CREATE INDEX flvar_id_m1_idx ON flvar USING btree (id);

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;
  
  """ % pg.escape_string("""/* XPM */\nstatic char * configure_xpm[] = {\n"32 32 267 2",\n"  \tc None",\n". \tc #B9B9C8",\n"+ \tc #BABAC8",\n"@ \tc #BABAC9",\n"# \tc #CCCCD7",\n"$ \tc #B5B5C4",\n"% \tc #D5D5DE",\n"& \tc #FFFFFF",\n"* \tc #F2F2F4",\n"= \tc #D1D1DB",\n"- \tc #B4B4C3",\n"; \tc #B0B0C0",\n"> \tc #E1E1E7",\n", \tc #F0F0F3",\n"' \tc #BFBFCC",\n") \tc #AFAFBF",\n"! \tc #AAAABC",\n"~ \tc #DFDFE5",\n"{ \tc #FAFAFB",\n"] \tc #A9A9BB",\n"^ \tc #A4A4B7",\n"/ \tc #DCDCE3",\n"( \tc #F7F7F9",\n"_ \tc #AAAABB",\n": \tc #9F9FB3",\n"< \tc #D9D9E1",\n"[ \tc #FEFEFE",\n"} \tc #D7D7DF",\n"| \tc #9E9EB2",\n"1 \tc #9999AF",\n"2 \tc #E7E7EC",\n"3 \tc #F1F1F4",\n"4 \tc #9494AA",\n"5 \tc #9B9BAF",\n"6 \tc #9393A9",\n"7 \tc #9A9AAF",\n"8 \tc #ECECF0",\n"9 \tc #FDFDFD",\n"0 \tc #F6F6F8",\n"a \tc #8F8FA6",\n"b \tc #E6E6EB",\n"c \tc #9595AB",\n"d \tc #8E8EA5",\n"e \tc #A1A1B4",\n"f \tc #F8F8FA",\n"g \tc #F4F4F6",\n"h \tc #BBBBC9",\n"i \tc #9D9DB1",\n"j \tc #F5F5F7",\n"k \tc #E0E0E7",\n"l \tc #8888A0",\n"m \tc #9C9CB0",\n"n \tc #E9E9ED",\n"o \tc #EFEFF2",\n"p \tc #8989A1",\n"q \tc #9191A8",\n"r \tc #F1F1F3",\n"s \tc #DBDBE2",\n"t \tc #8A8AA2",\n"u \tc #83839C",\n"v \tc #9696AB",\n"w \tc #E4E4EA",\n"x \tc #EAEAEE",\n"y \tc #AEAEBF",\n"z \tc #83839D",\n"A \tc #7E7E99",\n"B \tc #EBEBEF",\n"C \tc #85859E",\n"D \tc #7D7D98",\n"E \tc #9191A7",\n"F \tc #EBEBF0",\n"G \tc #DEDEE5",\n"H \tc #797995",\n"I \tc #C3C3CF",\n"J \tc #E8E8EC",\n"K \tc #CFCFD9",\n"L \tc #9898AD",\n"M \tc #DADAE1",\n"N \tc #D6D6DE",\n"O \tc #DFDFE6",\n"P \tc #E3E3E9",\n"Q \tc #E0E0E6",\n"R \tc #D8D8E0",\n"S \tc #72728F",\n"T \tc #6D6D8C",\n"U \tc #DDDDE4",\n"V \tc #CECED8",\n"W \tc #B2B2C1",\n"X \tc #B8B8C6",\n"Y \tc #9E9AA7",\n"Z \tc #CB8E38",\n"` \tc #DA942B",\n" .\tc #6E6E8C",\n"..\tc #C5C5D0",\n"+.\tc #D9D9E0",\n"@.\tc #DADAE2",\n"#.\tc #BEBECB",\n"$.\tc #C2C2CE",\n"%.\tc #C7C3C7",\n"&.\tc #D59E4D",\n"*.\tc #DDA42F",\n"=.\tc #EDCD37",\n"-.\tc #D4902B",\n";.\tc #636383",\n">.\tc #ACACBD",\n",.\tc #D6D6DF",\n"'.\tc #D2D2DB",\n").\tc #C6C6D2",\n"!.\tc #CACAD4",\n"~.\tc #CDC9CC",\n"{.\tc #CE974A",\n"].\tc #D99F2D",\n"^.\tc #EFC229",\n"/.\tc #E9A91D",\n"(.\tc #EECE36",\n"_.\tc #CF8C2A",\n":.\tc #5D5D7F",\n"<.\tc #86869F",\n"[.\tc #A6A6B8",\n"}.\tc #C5C5D1",\n"|.\tc #C1C1CE",\n"1.\tc #BDBDCA",\n"2.\tc #D0D0DA",\n"3.\tc #D4D0D2",\n"4.\tc #CC964C",\n"5.\tc #D49B2D",\n"6.\tc #F0C128",\n"7.\tc #E5840B",\n"8.\tc #F18809",\n"9.\tc #ECAC1E",\n"0.\tc #ECC833",\n"a.\tc #CA882A",\n"b.\tc #59597B",\n"c.\tc #58587A",\n"d.\tc #656584",\n"e.\tc #71718E",\n"f.\tc #787893",\n"g.\tc #6B6B89",\n"h.\tc #CECED9",\n"i.\tc #D9D5D6",\n"j.\tc #C9944D",\n"k.\tc #D0962C",\n"l.\tc #F0BB24",\n"m.\tc #E88D0F",\n"n.\tc #FD980D",\n"o.\tc #FF990D",\n"p.\tc #F4930E",\n"q.\tc #EEAF1E",\n"r.\tc #EAC231",\n"s.\tc #C5842A",\n"t.\tc #555578",\n"u.\tc #9D99A7",\n"v.\tc #C28D4A",\n"w.\tc #CC922B",\n"x.\tc #EDAC1E",\n"y.\tc #EC9612",\n"z.\tc #FDA213",\n"A.\tc #FFA314",\n"B.\tc #F69D13",\n"C.\tc #F1B21E",\n"D.\tc #E8BC2F",\n"E.\tc #C1802A",\n"F.\tc #A87438",\n"G.\tc #C88D2A",\n"H.\tc #EFAF1E",\n"I.\tc #EFA015",\n"J.\tc #FEAC1A",\n"K.\tc #FFAD1A",\n"L.\tc #FFAE1A",\n"M.\tc #F7A718",\n"N.\tc #F3B51E",\n"O.\tc #E6B62C",\n"P.\tc #BC7C29",\n"Q.\tc #B77829",\n"R.\tc #DFAB2A",\n"S.\tc #F7B81F",\n"T.\tc #FEB720",\n"U.\tc #FFB820",\n"V.\tc #F9B11D",\n"W.\tc #F6B81F",\n"X.\tc #E4B12A",\n"Y.\tc #B27529",\n"Z.\tc #E2AC28",\n"`.\tc #FFC526",\n" +\tc #FFC327",\n".+\tc #FFC126",\n"++\tc #FFBE24",\n"@+\tc #FFBD23",\n"#+\tc #FDBA21",\n"$+\tc #FEC124",\n"%+\tc #E2AA27",\n"&+\tc #AD7129",\n"*+\tc #E0A525",\n"=+\tc #FFC929",\n"-+\tc #FFCD2D",\n";+\tc #FFCA2B",\n">+\tc #FFB920",\n",+\tc #E0A425",\n"'+\tc #A86D28",\n")+\tc #DEA023",\n"!+\tc #FFCD2B",\n"~+\tc #FFD834",\n"{+\tc #FFC025",\n"]+\tc #FFB21D",\n"^+\tc #FFB01C",\n"/+\tc #FF9A0E",\n"(+\tc #FF930A",\n"_+\tc #FF9209",\n":+\tc #FFAF1B",\n"<+\tc #FFB61E",\n"[+\tc #C88B25",\n"}+\tc #A76C28",\n"|+\tc #A36928",\n"1+\tc #DC9921",\n"2+\tc #FFD02E",\n"3+\tc #FFE23A",\n"4+\tc #FFB01B",\n"5+\tc #FFA817",\n"6+\tc #FF9D10",\n"7+\tc #FF9E11",\n"8+\tc #FF9108",\n"9+\tc #EEA81E",\n"0+\tc #9E6528",\n"a+\tc #DA941E",\n"b+\tc #FFD430",\n"c+\tc #FFA716",\n"d+\tc #FF8401",\n"e+\tc #FF9109",\n"f+\tc #FFA213",\n"g+\tc #FF8904",\n"h+\tc #FFA816",\n"i+\tc #FFB018",\n"j+\tc #996128",\n"k+\tc #D88E1C",\n"l+\tc #FFA113",\n"m+\tc #FFCC2D",\n"n+\tc #FF940B",\n"o+\tc #FF8501",\n"p+\tc #FF950B",\n"q+\tc #FF960B",\n"r+\tc #FFA413",\n"s+\tc #F9A415",\n"t+\tc #945D28",\n"u+\tc #D68619",\n"v+\tc #FF9C0F",\n"w+\tc #FFAA17",\n"x+\tc #FFD733",\n"y+\tc #FFB61F",\n"z+\tc #FFAC19",\n"A+\tc #FFA00F",\n"B+\tc #D0831A",\n"C+\tc #8F5927",\n"D+\tc #D58117",\n"E+\tc #FF970C",\n"F+\tc #FF960C",\n"G+\tc #FF990C",\n"H+\tc #EA8E12",\n"I+\tc #965D25",\n"J+\tc #8A5527",\n"K+\tc #B56D1C",\n"L+\tc #F08B0D",\n"M+\tc #FF9309",\n"N+\tc #F88F0B",\n"O+\tc #C47418",\n"P+\tc #915925",\n"Q+\tc #8A5627",\n"R+\tc #855227",\n"            . + @ # # @ + .                                     ",\n"            $ % & & & & * = $ -                                 ",\n"              ; > & & & & & , ' )                               ",\n"                ! ~ & & & & & { + ]                             ",\n"                  ^ / & & & & & ( _                             ",\n"                    : < [ & & & [ } |                           ",\n"                      1 2 & & & & 3 :                           ",\n"4 5                 6 7 8 9 9 9 9 0 ;                           ",\n"a b c             d e > f f f f f g h a                         ",\n"i j k a         l m n g g g g g g o $ p                         ",\n"q r , s t     u v w , , , , , , , x y z                         ",\n"A B 8 8 % C D E ~ 8 8 8 8 8 8 F F G 6                           ",\n"H I 2 J J K L M 2 2 2 2 2 2 2 2 2 N H                           ",\n"  5 O P P P Q P P P P P P P P P R / i S                         ",\n"  T + U ~ ~ ~ ~ ~ ~ ~ ~ ~ G G V W X R Y Z `                     ",\n"     ...+.@.@.@.@.@.@.@.@.@.V + #.$.%.&.*.=.-.                  ",\n"    ;. .>.# ,.,.,.,.,.N N '.I ).!.~.{.].^./.(._.                ",\n"        :.<.[.. I }.|.- 1.2.V = 3.4.5.6.7.8.9.0.a.              ",\n"          b.c.d.e.f.g.;.c.e h.i.j.k.l.m.n.o.p.q.r.s.            ",\n"                  t.      t.u.v.w.x.y.z.A.A.A.B.C.D.E.          ",\n"                            F.G.H.I.J.K.K.K.K.L.M.N.O.P.        ",\n"                            Q.R.S.T.U.U.U.U.U.U.U.V.W.X.Q.      ",\n"                              Y.Z.`. + + + + +.+++@+#+$+%+Y.    ",\n"                                &+*+=+-+-+-+;+>+U.U.U.U.>+,+&+  ",\n"                                  '+)+!+~+~+{+]+^+/+(+_+:+<+[+}+",\n"                                    |+1+2+3+4+K.5+_+6+7+8+L.9+|+",\n"                                      0+a+b+c+c+++d+e+f+g+h+i+0+",\n"                                        j+k+c+l+m+n+o+p+q+r+s+j+",\n"                                          t+u+v+w+x+y+z+v+A+B+t+",\n"                                            C+D+E+F+F+E+G+H+I+  ",\n"                                              J+K+L+M+N+O+P+Q+  ",\n"                                                R+R+R+R+R+      "};\n"""))
  load_module(options,db)
  
  
    
#  *************************** LOAD MODULE *****
#  
  
def load_module(options,db=None):
  if (not db):
    if (not options.ddb):
      print "LoadModule requiere una base de datos y no proporcionó ninguna."
      return 0
    db=dbconnect(options)
    if (not db): 
      return 0
  
  modules=[]
  dirs2={}
  for root, dirs, files in os.walk(options.loaddir):
    FoundModule=False
    dirs2[root]=root
    for name in files:
      if (options.debug):
        print "Searching '%s' ..." % name
      if name[-4:]==".mod":
        FoundModule=True
        if options.verbose:
          print "Found module '%s' at '%s'" % (name, root)
        modules+=[root]
        
    delindex=[]        
    for index, directory in enumerate(dirs):
      Search=not FoundModule
      if directory[0]==".":
        Search=False
      if not Search:
        delindex+=[index]
    delindex.reverse()
    for delete in delindex:
      del dirs[delete]
  
  if options.debug:      
    dirs2=list(dirs2);
    dirs2.sort()
    lendir=len(options.loaddir)
    for directory in dirs2:
      print "Searched into '%s' directory." % directory[lendir:]
      
  if (len(modules)==0):
    print("LoadModule requiere uno o más módulos "
          "y no se encontró ninguno en la ruta '%s'." % options.loaddir )         
    return 0
  else:
    if (not options.quiet):
      print "%d modules found." % len(modules)
    
  options.modules_loaded={}
  for module in modules:
    load_module_loadone(options,module,db)
  
  repair_db(options,db)


  
def load_module_loadone(options,modpath,db):
  module=""
  tables=[]
  filetypes=["xml","ui","qry","kut","qs","mtd","ts"]
  unicode_filetypes=["ui","ts"]
  
  files=[]
  for root, dirs, walk_files in os.walk(modpath):
      for name in walk_files:
        if f_ext(name)=="mod":
          module=name[:-4]
          file_module=loadfile_inutf8(root, name)
          module_parse=XMLParser()
          module_parse.parseText(file_module)
          d_module={
            'name' :    str(module_parse.root.module.name),
            'alias' :   str(module_parse.root.module.alias),
            'area' :    str(module_parse.root.module.area),
            'areaname' :    str(module_parse.root.module.areaname),
            'version' :    str(module_parse.root.module.version),
            'icon' :    str(module_parse.root.module.icon),
            }
          d_module['icon_data']=loadfile_inutf8(root, d_module['icon'])
        if f_ext(name)=="mtd":
          table=name[:-4]
          # print "Table: " + table
          tables+=[table]
        
        if f_ext(name) in filetypes:
          
          contents=loadfile_inutf8(root,name)
          sha=SHA1(contents)
          contents=pg.escape_string(contents)
          
          
          file={'name' : name,'sha': sha, 'contents' : contents, 'root' : root}
          if (options.modules_loaded.has_key(name)):
            print "ERROR: %s file was already loaded." % name
            print "--> this file was found at %s" % root
            print "--> previous file found at %s" % options.modules_loaded[name]['root']
            print "* skipping file"
          else:
            options.modules_loaded[name]=file
            files+=[file]
  
  qry_areas=db.query("SELECT descripcion, bloqueo, idarea"
                        " FROM flareas WHERE idarea='%s'" % d_module['area'])
  tareas=qry_areas.dictresult()            
  if len(tareas)==0:
    print "Creando Area %s - %s " %(d_module['area'],d_module['areaname'])
    db.query("INSERT INTO flareas (descripcion, bloqueo, idarea)"
                        "VALUES('%s','t','%s')" % (d_module['areaname'],d_module['area']))
          
                          
  
  habilitar_carga=False
  qry_modulo=db.query("SELECT idmodulo, version, descripcion, bloqueo, idarea"
                        " FROM flmodules WHERE idmodulo='%s'" % module);
  tmodulo=qry_modulo.dictresult() 
  cargado=False
  for pmodulo in tmodulo:
    cargado=True
    if pmodulo['bloqueo']=='t':  # TRUE Es que NO está bloqueado. Está al revés.s
      habilitar_carga=True
  
  if not cargado:
    print "Se procede a crear el módulo nuevo %s" % module
    
    idmodulo    = pg.escape_string(d_module['name']) 
    idarea      = pg.escape_string(d_module['area'])
    version     = pg.escape_string(d_module['version'])
    bloqueo     = "t"
    descripcion = pg.escape_string(d_module['alias'])
    icono       = pg.escape_string(d_module['icon_data'])
    
    sql=("INSERT INTO flmodules (idmodulo, idarea, version, bloqueo, descripcion,icono) "  
          "VALUES('%s','%s','%s','%s','%s','%s')" % 
              (idmodulo, idarea, version, bloqueo, descripcion,icono))
    db.query(sql)   
    habilitar_carga=True   

  if not habilitar_carga:
    print "Error when trying to update the module '%s': non-loaded or locked module" % module
    return 0
  
  qry_modulos=db.query("SELECT * FROM flfiles WHERE idmodulo='%s'" % module);
  tuplas_modulos=qry_modulos.dictresult() 
  dmodulos={}
  for modulo in tuplas_modulos:
    dmodulos[modulo['nombre']]=modulo;
    
  loaded=[]
  
  for file in files:
    
    update=True    
    if (dmodulos.has_key(file['name'])):
      dm=dmodulos[file['name']]
      if (dm['sha']==file['sha']):
        update=False
    
    if (update):
      loaded+=[file['name']]
      if (options.verbose):
        print "* Loading file '%s' => '%s'..." % (file['name'],file['sha'])
      sql="DELETE FROM flfiles WHERE nombre='%s';\n" % file['name']
      db.query(sql)
    
      file['module']=module
      
      sql=("INSERT INTO flfiles (contenido, bloqueo, sha, idmodulo, nombre) "  
            "VALUES('%(contents)s', 't', '%(sha)s','%(module)s', '%(name)s')" % file)
      db.query(sql)
  
  
  
  
  if (not options.quiet and len(loaded)>0): 
    print "Module %s: loaded %d of %d files. (%s)" % (module, len(loaded), len(files),",".join(loaded))
      
  
#  *************************** REPAIR DATABASE *****
#  
  
def repair_db(options,db=None,mode=0):
  if (not db):
    if (not options.ddb):
      print "RepairDB requiere una base de datos y no proporcionó ninguna."
      return 0
    db=dbconnect(options)
    if (not db): 
      return 0
  
  if (options.verbose):
    print "Inicializando reparación de la base de datos '%s'..." % options.ddb
    print " * Calcular firmas SHA1 de files y metadata"
  
  qry_modulos=db.query("SELECT idmodulo, nombre, contenido, sha FROM flfiles ORDER BY idmodulo, nombre");
  modulos=qry_modulos.dictresult() # El resultado de la consulta anterior lo volcamos en una variable de lista
  sql=""
  resha1="";
  for modulo in modulos:
    sha1=SHA1(modulo['contenido'])
    resha1=SHA1(resha1+sha1)
    if (modulo['sha']!=sha1):
      print "Updating " + modulo['nombre'] + " => " + sha1 + " ..."
      sql+="UPDATE flfiles SET sha='%s' WHERE nombre='%s';\n" %  (sha1,modulo['nombre'])
    elif (options.debug):
      print modulo['nombre'] + " is ok."
    
    if (modulo['nombre'][-4:]==".mtd"):
      tabla=modulo['nombre'][:-4]
      qry_modulos=db.query("SELECT xml FROM flmetadata WHERE tabla='%s'" % tabla);
      tablas=qry_modulos.dictresult() 
      for txml in tablas:
        if txml['xml']!=sha1:
          print "Actualizada la Tabla: " + tabla
          sql+="UPDATE flmetadata SET xml='%s' WHERE tabla='%s';\n" %  (sha1,tabla)
      
    if (len(sql)>1024):
      db.query(sql)
      sql=""
  
  if (len(sql)>0):  
    db.query(sql)
    sql=""
  
  qry_serial=db.query("SELECT sha FROM flserial");
  serials=qry_serial.dictresult() # El resultado de la consulta anterior lo volcamos en una variable de lista
  for serial in serials:
    if (serial['sha']==resha1):
      resha1=False
        
          
  if (resha1):
    if len(serials)>0:
      db.query("UPDATE flserial SET sha='%s';" %  (resha1))
      print "Updated flserial => %s." %  (resha1)   
    else:    
      db.query("INSERT INTO flserial (serie,sha) VALUES(1,'%s')" %  (resha1))
      print "Created flserial => %s." %  (resha1)   
  





#def exec_ini():
  ## Abrimos el archivo y leemos el fichero de configuración "replicas.ini"
  #ini = ConfigParser.ConfigParser()
  #ini.readfp(open('replicas.ini'))
  
  ## Inicializamos lista de tablas
  #tablas=[]
  
  ## Añadimos a la variable secciones los diferentes modulos del archivo replicas.ini
  #secciones=ini.sections()
  
  ## Recorremos los módulos (secciones) del archivo
  #for seccion in secciones:
    #items_seccion=ini.items(seccion)
    #tsec=seccion.split(".") # Divide la cadena por el caracter que le indicas
    #tipoSeccion=tsec[0] # Primera parte de los nombres de cada sección del archivo
    #nombreSeccion=tsec[1] # Segunda parte de los nombres de cada sección del archivo
    ## Si la sección empieza por mod.
    #if tipoSeccion=="mod":
      ## Recorremos los campos de cada tabla
      ## Dividimos de cada modulo los campos en los que aparece un punto
      #for tabla in items_seccion:    
        #t_tabla=tabla[0].split(".")
        #if (len(t_tabla)>0):
          #tabla_n1=t_tabla[0]
        #if (len(t_tabla)>1):
          #tabla_n2=t_tabla[1]
        ## Cogemos los campos que no empiezan por "__" ni terminan con "__"
        #if (tabla[0:3]=='__'):
          #continue
          
        #if len(t_tabla)==1 and tabla[1]=="Yes":
          #tablas.append(tabla[0])
          
    ## Si la sección empieza por db.
    #if tipoSeccion=="db":
      #configdb={}
      ## Recorremos cada item de cada sección
      #for item in items_seccion:
        #configdb[item[0]]=item[1]
      
      #if nombreSeccion=="origen":
        #configdborigen = configdb
        
      #if nombreSeccion=="destino":
        #configdbdestino = configdb
        
  
  ## Nos conectamos a la base de datos origen
  ## Si la conexión es a MySQL pasaremos por este if.  
  #if (configdborigen['driver']=='mysql'):
    #conectbd = _mysql.connect(
              #db=configdborigen['dbname'], 
              #port=int(configdborigen['port']),
              #host=configdborigen['host'], 
              #user=configdborigen['user'], 
              #passwd=configdborigen['passwd'])
    #conectbd.set_character_set("UTF8") # Hacemos la codificación a UTF8.
  ## Si la conexión es a Postgres , de la siguiente manera            
  #else:
    #conectbd = pg.connect(
              #dbname=configdborigen['dbname'], 
              #port=int(configdborigen['port']),
              #host=configdborigen['host'], 
              #user=configdborigen['user'], 
              #passwd=configdborigen['passwd'])
  
  ## Nos conectamos a la base de datos destino
  #psql = pg.connect(
              #dbname=configdbdestino['dbname'], 
              #port=int(configdbdestino['port']),
              #host=configdbdestino['host'], 
              #user=configdbdestino['user'], 
              #passwd=configdbdestino['passwd']
              #)
              
  ## Hacemos una consulta para sacar las tablas que concuerdan con la restriccion LIKE 'fl%'
  ##qry_tablas = psql.query(
  ##  "select table_name from information_schema.tables"   
  ##  " where table_schema='public' and table_type='BASE TABLE' and table_name LIKE 'fl%%'")
  
  #qry_tablas = psql.query(
    #"select table_name from information_schema.tables"   
    #" where table_schema='public' and table_type='BASE TABLE'")
    
  ## Guardamos el resultado de la consulta anterior en la variable
  #tupla_tablas=qry_tablas.getresult()
  
  
  ## Añadimos valores de tupla_tablas a tablas
  #for tupla in tupla_tablas:
    #tablas.append(tupla[0])
  
    
  #for tabla in tablas:
    #print tabla
      ##tablas.append(tabla[0])
    #qry_deltables= psql.query("delete from %s" % tabla)
    #if (configdborigen['driver']=='mysql'):
      #conectbd.query("select * from %s" % tabla) # Hacemos una select del contenido de la tabla
      #r=conectbd.store_result()
      #filas=r.fetch_row(maxrows=0,how=1) # Nos saca el resultado de la fila
      #if (len(filas)==0):
        #continue
      #campos=filas[0].keys()
    #else:  
      #qry_seltables= conectbd.query("select * from %s" % tabla) # Hacemos una select del contenido de la tabla
      #filas=qry_seltables.getresult() # El resultado de la consulta anterior lo volcamos en una variable de lista
      #campos=qry_seltables.listfields() # Cargamos la lista de nombres de campos a la variable campos
    
    #sqlvars={}
    #sqlvars['tabla']=tabla
    #separador=", "
    #sqlvars['fields']=separador.join(campos)
    ## *** Inicio proceso de insert into en la tabla
    ## insert into table (field1,field2) VALUES (val1,val2),(val1,val2),(val1,val2)
    #f=0
    #bytes=0
    #porcentaje=0
    #for fila in filas:
      #n=0
      #valores=[]
      #for campo in fila:
        #if (configdborigen['driver']=='mysql'):
          #campo=fila[campo]
        
        #if (campo is not None):#Si el valor es nulo
          #valores.append("'" + pg.escape_string(str(campo)) + "'")
        #else:
          #valores.append("NULL")
        #n+=1
      #text="(" + separador.join(valores) + ")"
      #bytes+=len(text)
      #f+=1
      ## En postgres no funcionan los insert multilínea
      #sqlvars['rows']=text
      #sql_text="INSERT INTO %(tabla)s (%(fields)s) VALUES %(rows)s;" % sqlvars
      #qry_instables=psql.query(sql_text)
      #bytes=0
      #if (porcentaje+5<=f*100/len(filas)):
        #porcentaje=f*100/len(filas)
        #print tabla + "(" + str(porcentaje) + "%)"
    
if __name__ == "__main__":
  main()