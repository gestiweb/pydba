#!/usr/bin/python
#   encoding: UTF8

# Fichero de XMLParser 
import xml.parsers.expat as Expat
import re


class XMLParser_data:
    _name=""
    _data=""
    _attrs={}
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
    
    
    def start_element(self,name, attrs):
        method_name=name.lower()
        if (not method_name.isalpha()):
            num_element+=1
            method_name="element%d" % num_element
        
        new=XMLParser_data()
        if (not hasattr(self.xmlusing,method_name)):
            setattr(self.xmlusing,method_name,new)
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
        m = re.search('QT_TRANSLATE_NOOP\("([^"]*)","([^"]*)"\)', data)
        if (m):
            data=QT_TRANSLATE_NOOP(m.group(1),m.group(2))
        
        self.xmlusing._data=data        
    
    
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
        
        p.Parse(text, 1)
