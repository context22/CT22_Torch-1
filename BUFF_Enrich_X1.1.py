#=======================================================
#   Dec 16 2024
#  Change :
# - get the Metadata from Common instead of a Index in ES
#=======================================================
import json
from datetime import datetime
import os
import sys
import glob
from elasticsearch import Elasticsearch, helpers
import pandas as pd
from datetime import date
from icecream import ic
import metadata


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
       self.sqlite = self.param_data["sqlite"]
       # es = Elasticsearch(['http://localhost:9200'], http_auth=('user', 'pass'))
       self.es = Elasticsearch([self.esServer], http_auth=(self.esUser, self.esPwd))
       print ("After connection to ES")
       self.na_colls = {}

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
    def metadata(self):
        p1 = metadata.MetaData(self.sqlite)
        # ---- Get Collectors
        self.myListColls = p1.get_Colls()
        # print(type(self.myListIPs), " -- " ,self.myListIPs)
        columns_colls = self.myListColls.columns.tolist()
        for column in columns_colls :
            self.na_colls [column] = 'N/A'
        # breakpoint()

        return()

    def DataFile_List(self):
        print ("DataFile_List")
        # self.csvFiles=glob.glob(self.path + "*BUFF_USAGE*.csv")
        self.csvFiles=glob.glob(self.path + "*BUFF_USAGE*.gz")
        if len(self.csvFiles) == 0:
           print ("No File to Process")
           os.system('rm -f ' + self.path + 'Buff_Enrich_In_Progress')
           sys.exit()
        DataFile=[]
        for file in self.csvFiles:
          if "BUFF_USAGE" in file:
           # breakpoint()
           COLLt = file.split('/')[-1]
           print (COLLt)
           COLL = COLLt.split('_')[1]
           print (COLL)
           DataFile.append(COLL)
           DataFile.append(file)
           self.DataFiles.append(DataFile)
           DataFile=[]
           print (self.DataFiles)


    # ---- Lookups into the Metadata  -----
    def lookup_A_COLLECTOR(self,COLL):
      print ("lookup_A_COLLECTOR")
      coll_metadata = self.myListColls[self.myListColls['Collectors'] == COLL]
      if coll_metadata.empty == True:
          return (self.na_colls)
      else:
          coll_metadata = coll_metadata.to_dict(orient='records')[0]
          return (coll_metadata)

      # breakpoint()

    def processOneFile(self,datafile):

      print ("processOneFile " , datafile)

      # mydateparser = lambda x: datetime.strptime(x, "%m/%d/%Y %H:%M")
      mydateparser = lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
      df = pd.read_csv(datafile[1], parse_dates=['Timestamp'], date_parser=mydateparser, compression='gzip')
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
      print (datafile[1])
      file = datafile[1]
      fileSplit_1 = file.split('/')
      print(fileSplit_1)
      fileName = fileSplit_1[len(fileSplit_1) - 1]
      print(fileName)
      fileSplit_2 = fileName.split('_')
      print(fileSplit_2)
      COLL = fileSplit_2[1]
      print(COLL)
      fileDate = fileSplit_2[5][:8]
      breakpoint()
      # today=date.today()
      # year = today.year
      # month = today.month
      # day = today.day
      try:
         # response = helpers.bulk(self.es,parsed, index=self.index+"-"+str(year)+"."+str(month)+"."+str(day))
         response = helpers.bulk(self.es,parsed, index=self.index+"-"+str(fileDate[:4])+"."+str(fileDate[4:6])+"."+str(fileDate[6:8]))
         print ("ES response : ", response )
      except Exception as e:
         print ("ES Error :", e)

      return(len(parsed))

# --- Main  ---
if __name__ == '__main__':
    print("Start BUFF Enrichment")

    p1 = BuffEnrich("param_data.json")

    p1.metadata()

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
