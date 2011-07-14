#!/usr/bin/python
#   encoding: UTF8

# Fichero de XMLParser 
import xml.parsers.expat as Expat
import re


class XMLParser_data:
    def __init__(self):
        self._name=""
        self._data=""
        self._attrs={}
        self._children=[]
        
    def __getitem__(self, key):
        return self
        
    def __len__(self):
        return 1
        
    def __iter__(self):
        return iter([self])
    
    def __str__(self):
        return self._data.encode('utf8')



# Funcion de compatibilidad con QSA y AbanQ.
def QT_TRANSLATE_NOOP(From,String):
    return String
  
class XMLParser:
    
    reserved_words=('and','assert','break','class','continue','def','del','elif','else',
                'except','exec','finally','for','from','global','if','import','in','is',
                'lambda','not','or','pass','print','raise','return','try','while',
                'Data','Float','Int','Numeric','Oxphys','array','close','float',
                'int','input','open','range','type','write','zeros')
    
    def start_element(self,name, attrs):
        method_name=name.lower()
        if not method_name.isalpha():
            method_name="_" +method_name
        if method_name in self.reserved_words:
            method_name+="_"
            
        new=XMLParser_data()
        if (not hasattr(self.xmlusing,method_name)):
            setattr(self.xmlusing,method_name,new)
            self.xmlusing._children.append(method_name)
        else:
            # -> Si ya existía un atributo con este nombre, hay que convertirlo a lista.
            prevattr=getattr(self.xmlusing,method_name)
            if (isinstance(prevattr, XMLParser_data)):
                # Si el atributo anterior era una clase de datos, lo pasamos a lista
                prevattr=[prevattr]
            
            # Agregamos new
            prevattr.append(new)
            setattr(self.xmlusing,method_name,prevattr)
            
        self.stack.append(self.xmlusing)
        self.xmlusing=new
        self.xmlusing._name=name
        self.xmlusing._attrs=attrs
        
        
    def end_element(self,name):
        if (name!=self.xmlusing._name):
            print "WARNING: <%s> ... </%s>" % (xmlusing._name,name)
        self.xmlusing=self.stack.pop()
        
        
    def char_data(self,data):
        data=data.replace("\n"," ")
        data=data.strip()
        atext=data.split(";")
        ret=[]
        for text in atext:
            m = re.search('QT_TRANSLATE_NOOP\("([^"]*)","([^"]*)"\)', text)
            if (m):
                ret+=[QT_TRANSLATE_NOOP(m.group(1),m.group(2))]
        if len(ret):
            data=";".join(ret)
        self.xmlusing._data =  self.xmlusing._data.strip()
        self.xmlusing._data+=" " + data        
        self.xmlusing._data =  self.xmlusing._data.strip()
    
    def __init__(self, name="noname"):
        self.name = name
    
    def parseText(self,text):
        self.root=XMLParser_data()
        self.root._name="root"
        self.stack=[]
        self.xmlusing=self.root
        self.num_element=1
    
        p = Expat.ParserCreate()
        
        p.StartElementHandler = self.start_element
        p.EndElementHandler = self.end_element
        p.CharacterDataHandler = self.char_data
        try:
            p.Parse(text, 1)
        except Expat.ExpatError, error:
            lines = text.split("\n")
            line = lines[error.lineno-1]
            print "ERROR: parsing xml %s (%d) %s [%s]" % (self.name,error.code, error.args[0], line[error.offset-16:error.offset] + "·" +  line[error.offset:error.offset+16]) 
            # print "Linea: %d  , Offset: %d" % (error.lineno, error.offset)
            
            self.root=None
