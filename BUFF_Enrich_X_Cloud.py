import json
from datetime import datetime
import time
import os
import sys
import glob
from elasticsearch import Elasticsearch, helpers
import configparser
import pandas as pd


class BuffEnrich:

    def __init__(self, param_jason):
       print ("init")
       # self.es = Elasticsearch()
       with open(param_jason) as f:
          self.param_data = json.load(f)

       self.path = self.param_data["path"]
       self.pathlength = len(self.path)
       self.pathProcessed= self.param_data["pathProcessed"]
       self.index = self.param_data["index"]
       self.esServer = self.param_data["ESServer"]
       self.esUser = self.param_data["ESUser"]
       self.esPwd = self.param_data["ESPwd"]
       # es = Elasticsearch(['http://localhost:9200'], http_auth=('user', 'pass'))

       config = configparser.ConfigParser()
       config.read('example.ini')

       try :
         self.es = Elasticsearch(
            # ['ct22t-bum.kb.us-central1.gcp.cloud.es.io'],
            [config['ELASTIC']['host']],
            cloud_id=config['ELASTIC']['cloud_id'],
            http_compress=True,
            scheme="https",
            port=9243,
            http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
            )
       except e:
           print (e)
           exit(1)

       es_cloud = self.es
       print (es_cloud.info())
       print ("After connection to ES")

       self.InProg = self.path + 'Buff_Enrich_In_Progress'
       if os.path.exists(self.InProg) == True :
          print ("Buff Process in Progress - Exiting" )
          # os.system('rm -f ' + self.path + 'Buff_Enrich_In_Progress')
          exit(0)
       else:
          os.system('touch ' + self.InProg)

       self.myListCollectors = []
       self.csvFiles=[]
       self.DataFiles=[]


    # ---- getting Collectors MetaData
    def MetaData(self):
        print ("MetaData")
        A_COLLECTORS_tmp = p1.es.search(index="a_collectors", body={"query": {"match_all" : {}}, "size" : 1000})

        for hit in A_COLLECTORS_tmp['hits']['hits']:
            self.myListCollectors.append(hit["_source"])


    def DataFile_List(self):
        print ("DataFile_List")
        self.csvFiles=glob.glob(self.path + "*BUFF_USAGE*.csv")
        if len(self.csvFiles) == 0:
           print ("No File to Process")
           os.system('rm -f ' + self.path + 'Buff_Enrich_In_Progress')
           sys.exit()
        DataFile=[]
        for file in self.csvFiles:
          if "BUFF_USAGE" in file:
           COLL = file.split('_')[1]
           print (COLL)
           DataFile.append(COLL)
           DataFile.append(file)
           self.DataFiles.append(DataFile)
           DataFile=[]
           print (self.DataFiles)

    # ---- Lookups into the Metadata  -----
    def lookup_A_COLLECTOR(self,COLL):
      print ("lookup_A_COLLECTOR")
      if COLL != None:
         # print ("Youpi ...",SRC_PRG)
         for i in range(0,len(self.myListCollectors)):
             # if self.myListCollectors[i]['Collectors'] in COLL:
             if COLL in self.myListCollectors[i]['Collectors']:
              return(self.myListCollectors[i])

    def processOneFile(self,datafile):

      print ("processOneFile " , datafile)

      # mydateparser = lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M")
      mydateparser = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
      df = pd.read_csv(datafile[1], parse_dates=['Timestamp'], date_parser=mydateparser)
      df.columns.values[0] = "UTC"
      df_json=df.to_json(orient='records', date_format = 'iso')
      parsed=json.loads(df_json)
      print (" --- COLL IN DATAFILE  --- " , datafile[0])
      coll_metadata = p1.lookup_A_COLLECTOR(datafile[0])
      #print('Parsed ' ,parsed)
      print('coll_medata ' ,coll_metadata)
      for i in range(len(parsed)):
          # print (parsed[i])
          parsed[i]["Metadata Colector"]=coll_metadata
      # print('Parsed after' ,parsed)
      print('Parsed after', len(parsed))
      # print(self.index)
      # input()
      try:
         response = helpers.bulk(self.es,parsed, index=self.index)
         print ("ES response : ", response )
      except Exception as e:
         print ("ES Error :", e)

      return(len(parsed))


# --- Main  ---
if __name__ == '__main__':
    print("Start BUFF Enrichment")

    p1 = BuffEnrich("param_data.json")

    p1.MetaData()

    p1.DataFile_List()
    # --- Loop for each DAM data file
    doc_count_total = 0
    for datafile in p1.DataFiles:
        print('Processing : ',datafile)
        doc_count=p1.processOneFile(datafile)
        # move the processed file to Processed directory
        shortname=datafile[1][p1.pathlength:]
        # print ("Datafile",datafile," Short name= ",shortname)
        os.rename(datafile[1],p1.pathProcessed + shortname)
    doc_count_total = doc_count_total + doc_count
    print ('Nbr of Docs Processed' , doc_count_total)
    os.system('rm -f ' + p1.path + 'Buff_Enrich_In_Progress')
    print("End BUFF Enrichment")
