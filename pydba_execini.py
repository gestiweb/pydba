#!/usr/bin/python
#     encoding: UTF8

# Fichero de ejecución de INI para PyDBa
import pg             # depends - python-pygresql
import _mysql     # depends - python-mysqldb

import ConfigParser         # lectura de INI/CFG
        

def exec_ini(options, inifile='replicas.ini'):
    # Abrimos el archivo y leemos el fichero de configuración "replicas.ini"
    ini = ConfigParser.ConfigParser()
    ini.readfp(open(inifile))
    
    # Inicializamos lista de tablas
    modules={}
       
    # Añadimos a la variable secciones los diferentes modulos del archivo replicas.ini
    secciones=ini.sections()
    
    # Recorremos los módulos (secciones) del archivo
    for seccion in secciones:
        items_seccion=ini.items(seccion)
        tsec=seccion.split(".") # Divide la cadena por el caracter que le indicas
        tipoSeccion=tsec[0] # Primera parte de los nombres de cada sección del archivo
        nombreSeccion=tsec[1] # Segunda parte de los nombres de cada sección del archivo
        # Si la sección empieza por mod.
        if tipoSeccion=="mod":
            # Recorremos los campos de cada tabla
            # Dividimos de cada modulo los campos en los que aparece un punto
            modules[nombreSeccion]={}
            for tabla in items_seccion:        
                t_tabla=tabla[0].split(".")
                if (len(t_tabla)>0):
                    tabla_n1=t_tabla[0]
                if (len(t_tabla)>1):
                    tabla_n2=t_tabla[1]
                # Cogemos los campos que no empiezan por "__" ni terminan con "__"
                if (tabla[:3]=='__'):
                    continue
                if (tabla[-3:]=='__'):
                    continue
                    
                if len(t_tabla)==1: # Si tiene el formato tabla = Yes/No
                    valor=False
                    if tabla[1]=="Yes":
                        valor=True
                    
                    modules[nombreSeccion][tabla[0]]=valor
                    
                    
        # Si la sección empieza por db.
        elif tipoSeccion=="pydba":
            # Recorremos cada item de cada sección
            if nombreSeccion=="options":
                for item in items_seccion:
                    setattr(options,item[0],item[1])

    options.modules=modules
    
