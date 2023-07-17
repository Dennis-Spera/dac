#!/usr/local/bin/python3
"""
dac.py 

To Do:
utilize lv


Usage:
    dac.py -d|download_file_name {download-file.tar.gz} -c|customer {customer-name-corp} 
           -s|start_time {YYYY-MM-DD:HR24:MN:SC} -e|end_time {YYYY-MM-DD:HR24:MN:SC} -case {01193472} -v [verbose]
          
Sample:
   dac.py -f mongodb-logfiles_atlas-14ltwo-shard-0_2023-05-17T1605Z.tar.gz -c acme -s 2023.11.22.12.00.00 -e 2023.11.22.14.00.00 -case 11122244 -v
   dac.py -f mongodb-logfiles_atlas-gp89aq-shard-0_2023-06-27T2103Z.tar.gz -c sundance -s 2023.06.27.12.15.59 -e 2023.06.27.12.16.46 -case 00000000 
   dac.py -h
   dac.py -setRoot full-path-root

Options:
    -help      help context
    -log       true|false default true
    -settings  true|false automate setting default settings -- default program will exit with error

PEP8:
   https://peps.python.org/pep-0008/#maximum-line-length  (exceptions when necessary)  
     
Test cases
  case 0001 - validate that log is 4.4+
       test 0001.1 - test trying logs from version 4.2
       test 0001.1 - test random data
"""

from commandlines import Command as cmd
from pathlib import Path as path
import re as regex
from datetime import datetime
import os, yaml
import subprocess as proc
import shutil as scpy
import time, math
import json as j

_undef_ = None
_version_ = '0.0.0.1.0'

class Dac:
     def __init__(self):
         self.yaml = '.dac.yaml'
         self._initSettings() 
         self.listOfFiles = list()
         self.dataDirs= list()

     def _printStruct(self):
         for file in self.listOfFiles:      
             print("\n")           
             for name, value in file.items():
                 print('%-8s : %-20s' %(name, value))

     def _modifySettings(self, setting, value):
        defSettings = path(self._settings)
        if defSettings.is_file():  
          
           with open(self._settings, 'r') as stream:
                data_loaded = yaml.safe_load(stream) 

        if setting == 'root':
           data_loaded['root'] = value
           with open(self._settings, 'w') as outfile:
              yaml.dump(data_loaded, outfile, default_flow_style=False)

        if setting == 'lv':
           data_loaded['lv'] = value
           with open(self._settings, 'w') as outfile:
              yaml.dump(data_loaded, outfile, default_flow_style=False)


     def _validateLog(self):
         '''
         case 0001 - validate that log is 4.4+
         '''
         _count = int()
         
         for struct in self.listOfFiles:
             if struct["type"] == "mongodb-log":
                   file = open(struct["fullPath"], 'r')
                   line = file.readline()
                   while line:
                        d = j.loads(line)

                        try:
                         dateT = d["t"]["$date"]
                         _count+=1

                        except:
                         pass  
                        '''
                          Short Circuit when a valid file is detected
                        '''
                        if _count > 100:
                           return(1)

                   file.close()

                   if _count < 50:
                      self._error('mongodb log files are the incorrect version', 0) 
               
     def _initializeDirectories(self):
          
          _settings = self._getSettings()
          self.superDir = _settings["root"] + '/' + self.customer + '/' + self.case 
          
          _uniqueHostNames = dict()         
          for object in self.listOfFiles: 
              if object["host"] is not None:
                 _uniqueHostNames[object["host"]] = 'replicaSetHostName'

          _uniqueHostNamesList = list(_uniqueHostNames.keys())

          for server in _uniqueHostNamesList:

            dataDir = _settings["root"] + '/' + self.customer + '/' + self.case +  '/' + self.start\
                      + '__' + self.end + '__' + server     

            self.dataDirs.append(dataDir)

            try:
             scpy.rmtree(dataDir)
            except OSError as error:
             pass

            try:
             os.mkdir(dataDir)
            except OSError as error:
             print(error)             

     def _identifyLogEntries(self):
          
          self._initializeDirectories()
          _settings = self._getSettings()

          for object in self.listOfFiles: 
            try:     
              file = open(object["fullPath"], 'r')
              line = file.readline()
                     
              while line:                
                line = file.readline()
                try:
                  d = j.loads(line)
                  dateT = d["t"]["$date"]
                except:
                  pass 

                match_object = regex.match( r'(\d{4})([-])(\d{2})([-])(\d{2})([T])(\d{2})([:])(\d{2})([:])(\d{2})(.*)', dateT)

                d_t_fmt = datetime.strptime(match_object.group(1)+match_object.group(3)\
                                           +match_object.group(5)\
                                           +match_object.group(7)+match_object.group(9)
                                           +match_object.group(11), '%Y%m%d%H%M%S')

                if d_t_fmt >= self.start_d_t_fmt and d_t_fmt <= self.end_d_t_fmt:
                   command = d["c"]

                   _file = open(_settings["root"] + '/' + self.customer + '/' + self.case +  '/' + self.start + '__' +\
                                 self.end + '__' + object["host"] + '/' + command, 'a')

                   _mfile = open(_settings["root"] + '/' + self.customer + '/' + self.case +  '/' + self.start + '__' +\
                                 self.end + '__' + object["host"] + '/' + 'mongodb.log', 'a')                   


                   try:
                     jstr = j.dumps(d, indent=4)
                   except:
                     print(jstr)   

                   _file.write("------------------------------------------------------------------------------------"+"\n")  
                   _file.write(line)  
                   _file.write("------------------------------------------------------------------------------------"+"\n")  
                   _file.write(jstr)                             
                   _file.write("\n\n\n")
                   _file.close()

                   _mfile.write(line)
                   _mfile.close()  

              file.close()    
            
            except:
               pass

     def _initSettings(self):
         self._settings = os.environ['HOME']+'/'+self.yaml
         isFile = path(self._settings)
         if not isFile.is_file():
           self._error('file: '+self._settings+' not found', 0) 

           data = dict(
              root  = 'not-found',
              lv    = 'not-found'
           )
           with open(self._settings, 'w') as outfile:
              yaml.dump(data, outfile, default_flow_style=False)

     def _error(self, text, err):
         print('_ERROR_: ', text)
         if err:
            exit(0)
     
     def _verbose(self, text):
         if self.verbose:
            print(text)  
              
     def calculate_time(func):    
        def inner1(*args, **kwargs):
          begin = time.time()
          func(*args, **kwargs)
          end = time.time()
          print("Timing : ", func.__name__, end - begin)
        return inner1        
     
     @staticmethod
     def _help():
         print("# standard context")
         print('dac.py -d|download_file_name {download-file.tar.gz}  -c|customer {customer-name-corp} -s|start_time '+ 
               '{YYYY-MM-DD:HR24:MN:SC} -e|end_time {YYYY-MM-DD:HR24:MN:SC} -case {01193472} -v [verbose]')      
         print("# specialized context")
         print("dac.py -setRoot full-path-root")           
         exit(0)

     def _extraction(self):
         _env = self._getSettings()

         try:
          os.makedirs(_env['root'] + '/' + self.customer + '/' + self.case, \
                       mode=0o750, exist_ok=True) 
         except:
          self._error("[exiting] Error making directory " + _env['root'] + '/' \
                       + self.customer + '/' + self.case, 1)  
         try:
          scpy.copy(self.file, _env['root'] + '/' + self.customer + '/' + self.case)
         except scpy.SameFileError:
          self._error("[exiting] source and destination are the same: " + self.file, 1 )
      
         except PermissionError:
          self._error("[exiting] Permission denied while copying file: " + self.file, 1 )
         except:
          self._error("[exiting] Error occurred while copying file: " + self.file, 1)

         try:
          os.chdir(_env['root'] + '/' + self.customer + '/' + self.case +'/' ) 
          proc.run(["tar", "-zxvf",  _env['root'] + '/' + self.customer + '/' + self.case +'/' + self.file], stdout=proc.DEVNULL, stderr=proc.STDOUT) 
          os.remove(_env['root'] + '/' + self.customer + '/' + self.case +'/' + self.file)          
         except:
          self._error("[exiting] Error extracting from archive: " + self.file, 1)

         for r, d, f in os.walk(_env['root'] + '/' + self.customer + '/' + self.case +'/' ):
          for file in f:
             fullFilePath = os.path.join(r, file)
      
             if regex.match( r'(.*)(mongodb)([.])(log)$', fullFilePath):

                dirNameFile = os.path.dirname(fullFilePath)  
                os.rename(fullFilePath, dirNameFile+'/'+'mongodb.log.2099-12-31T23-59-59')

     def _extractPort(self, path):
         if regex.match( r'(.*)([/])(\d{5})([/])(.*)', path):
            match_object = regex.match( r'(.*)([/])(\d{5})([/])(.*)', path)
            return(match_object.group(3))
         
     def _extractHost(self, path):
         if regex.match( r'(.*)([/])(atlas)(.*)(mongodb.net)(/)(.*)', path):
            match_object = regex.match( r'(.*)([/])(atlas)(.*)(mongodb.net)(/)(.*)', path)
            return(match_object.group(3)+match_object.group(4)+match_object.group(5))         
         
     def _extractType(self, path):
         if regex.match( r'(.*)([/])(diagnostic.data)([/])(.*)', path):
            match_object = regex.match( r'(.*)([/])(diagnostic.data)([/])(.*)', path)
            return("diagnostic-data") 
         if regex.match( r'(.*)([/])(mongodb)([/])(.*)', path):
            match_object = regex.match( r'(.*)([/])(mongodb)([/])(.*)', path)
            return("mongodb-log")            
         
     def _extractDateTime(self, path):
         
         if regex.match( r'(.*)([/])(diagnostic.data)([/])(.*)', path) and not regex.match( r'(.*)(metrics.interim)', path):
           
            baseNameFile = os.path.basename(path)
            match_object = regex.match( r'(.*)([.])(\d{4})([-])(\d{2})([-])(\d{2})([T])(\d{2})([-])(\d{2})([-])(\d{2})(.*)', baseNameFile)

            try:
             start_d_t_fmt = datetime.strptime(match_object.group(3)+match_object.group(5)\
                                              +match_object.group(7)\
                                              +match_object.group(9)+match_object.group(11)
                                              +match_object.group(13), '%Y%m%d%H%M%S')
            
             return(start_d_t_fmt)           
            except:
             self._error('error converting start date-time to a datatime format',1)    
    
         if regex.match( r'(.*)([/])(mongodb.log.\d{4})(.*)', path):
            match_object = regex.match( r'(.*)([/])(mongodb)([/])(.*)', path)
            baseNameFile = os.path.basename(path)
            match_object = regex.match( r'(.*)([.])(\d{4})([-])(\d{2})([-])(\d{2})([T])(\d{2})([-])(\d{2})([-])(\d{2})', baseNameFile)

            try:
             start_d_t_fmt = datetime.strptime(match_object.group(3)+match_object.group(5)\
                                              +match_object.group(7)\
                                              +match_object.group(9)+match_object.group(11)
                                              +match_object.group(13), '%Y%m%d%H%M%S')
            
             return(start_d_t_fmt)           
            except:
             self._error('error converting start date-time to a datatime format',1)  

         if regex.match( r'(.*)([/])(mongodb.log)', path):
             return(datetime.strptime("20991231235959",'%Y%m%d%H%M%S'))           

     def _compileMetadata(self):
        defSettings = path(self._settings)
        if defSettings.is_file():  
          
           with open(self._settings, 'r') as stream:
                data_loaded = yaml.safe_load(stream)

        archiveLoc = data_loaded['root'] + '/' + self.customer + '/' + self.case +'/'

        for r, d, f in os.walk(archiveLoc):
         for file in f:
             fullFilePath = os.path.join(r, file)
      
             if regex.match( r'(.*)(mongodb)([.])(log)(.*)', fullFilePath) or\
                regex.match( r'(.*)(diagnostic.data)(.*)', fullFilePath):          

                  struct = {
                     "fullPath" : _undef_, 
                     "port"     : _undef_,
                     "type"     : _undef_,
                     "dateTime" : _undef_,
                     "host"     : _undef_
                  }
            
                  struct["fullPath"] = fullFilePath
                  struct["port"] = self._extractPort(fullFilePath) 
                  struct["type"] = self._extractType(fullFilePath)
                  struct["host"] = self._extractHost(fullFilePath)
                  struct["dateTime"] = self._extractDateTime(fullFilePath)

                  self.listOfFiles.append(struct)
           
     def _getSettings(self):

        defSettings = path(self._settings)
        if defSettings.is_file():  
          
           with open(self._settings, 'r') as stream:
                data_loaded = yaml.safe_load(stream)      

           if (data_loaded['root']) == 'not-found':
              print ("default root directory has not been set","\n")
              print (" dac.py -setRoot full-path-root","\n")
              self._error('[exiting] default root not set', 1)        

        return(data_loaded)                        
      
     def _setParameters(self):
   
        c = cmd()
        try:  
            '''
            define input parameters
            '''   
            self.file = str()
            self.helpSyntax = str()
            self.customer = str()
            self.start = str()
            self.end = str()
            self.verbose = False
            self.case  = int()
            self.help = str()
            self.setRoot = str()
            self.lv = str()
            self.setlv = str()
            '''
            overload input variables
            ignore when parameter is not defined
            '''
            try:   self.file = c.get_definition('file')  
            except:pass 
            try:   self.file = c.get_definition('f') 
            except:pass          
            try:   self.help = c.contains_switches('h')
            except:pass
            try:   self.customer = c.get_definition('customer')
            except:pass
            try:   self.customer = c.get_definition('c')
            except:pass
            try:   self.start = c.get_definition('s')
            except:pass
            try:   self.start = c.get_definition('start')
            except:pass
            try:   self.end = c.get_definition('e')
            except:pass
            try:   self.end = c.get_definition('end')
            except:pass
            try:   self.case = c.get_definition('case')
            except:pass            
            try:   self.dbversion = c.get_definition('d')
            except:pass            
            try:   self.dbversion = c.get_definition('dbversion')
            except:pass 

            #----------------------------------------------------

            try:   self.setRoot = c.get_definition('setRoot')
            except:pass 

            try:   self.setlv = c.get_definition('setlv')
            except:pass             

            try:
                if c.contains_switches('v'):
                   self.verbose = True
            except:pass

            try:
                if c.contains_switches('l'):
                   self.lv = True
            except:pass            

        except:
            self._error("unhandled commandline exception processing",1)
        
        '''
        check that input data falls within scope
        '''
        if self.help:
           self._help()

        if self.setRoot:
           self._modifySettings('root', self.setRoot)   
           exit(0)   

        if self.setlv:
           self._modifySettings('lv', self.setlv)   
           exit(0)              

        if not self.file:
           self._error('file not defined',1) 

        isFile = path(self.file)
        if not isFile.is_file():
           self._error('file: '+self.file+' not found',1)

        if not self.customer:
           self._error('customer not defined',1) 
        else:
           self.customer = self.customer.replace(" ", "_" )

        if not self.start:
           self._error('start date-time not defined',1) 
        else:
           # YYYY.MM.DD.HR.MN.SC
           if regex.match( r'(20)(\d{2})([.])(\d{2})([.])(\d{2})([.])'+ \
                          '(\d{2})([.])(\d{2})([.])(\d{2})', self.start):
              match_object = regex.match( r'(20)(\d{2})([.])(\d{2})([.])'+\
                          '(\d{2})([.])(\d{2})([.])(\d{2})([.])(\d{2})', \
                           self.start)
              
              try:
                 self.start_d_t_fmt = datetime.strptime(match_object.group(1)+match_object.group(2)\
                                                       +match_object.group(4)\
                                                       +match_object.group(6)+match_object.group(8)
                                                       +match_object.group(10)\
                                                       +match_object.group(12), '%Y%m%d%H%M%S')
              except:
                 self._error('error converting start date-time to a datatime format',1)  
           else:
              self._error('invalid start date-time : YYYY.MM.DD.HR.MN.SC',1)  


        if not self.end:
           self._error('end date-time not defined',1) 
        else:
           # YYYY.MM.DD.HR.MN.SC
           if regex.match( r'(20)(\d{2})([.])(\d{2})([.])(\d{2})([.])'+\
                            '(\d{2})([.])(\d{2})([.])(\d{2})', self.end):
              match_object = regex.match( r'(20)(\d{2})([.])(\d{2})([.])'+\
                            '(\d{2})([.])(\d{2})([.])(\d{2})([.])(\d{2})', \
                            self.end)
              
              try:
                 self.end_d_t_fmt = datetime.strptime( match_object.group(1)+match_object.group(2)\
                                                      +match_object.group(4)\
                                                      +match_object.group(6)+match_object.group(8)
                                                      +match_object.group(10)\
                                                      +match_object.group(12), '%Y%m%d%H%M%S')
              except:
                 self._error('error converting end date-time to a datatime format',1)  
           else:
              self._error('invalid end date-time : YYYY.MM.DD.HR.MN.SC',1)  

        if not self.case:
           self._error('case number not defined',1)  
        else:
           if regex.match( r'(\d{8})', self.case):
              match_object = regex.match( r'(\d{8})', self.case)
              self.case = match_object.group(1) 
           else:
              self._error('inproperly formed case number',1)                 


     def _spawnReplicaSetGui(self):
        defSettings = path(self._settings)
        if defSettings.is_file():  
          
           with open(self._settings, 'r') as stream:
                data_loaded = yaml.safe_load(stream)      

           if (data_loaded['lv']) == 'not-found':
              print ("default lv location has not been set","\n")
              print (" dac.py -setlv full-path-lv","\n")
              self._error('[exiting] lv location not set', 1) 

               
        for _dir in self.dataDirs: 
            defSettings = path(self._settings)
            if defSettings.is_file():  
          
               with open(self._settings, 'r') as stream:
                    data_loaded = yaml.safe_load(stream)      

               proc.run([data_loaded['lv'], _dir+'/'+'mongodb.log'])

     @calculate_time
     def _execution(self):
        
         self._setParameters()
         self._getSettings()
         self._extraction()
         self._compileMetadata()
         self._validateLog()
         self._identifyLogEntries()         

'''

M A I N

'''

obj = Dac()
obj._execution()

if obj.verbose:
  obj._printStruct()

proc.run(["open", obj.superDir])

if obj.lv:
   obj._spawnReplicaSetGui()