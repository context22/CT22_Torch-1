import pandas as pd
from pandas import read_csv
import json
from elasticsearch import Elasticsearch, helpers
# import sys
import os
# import pdb
import glob
from datetime import date
import metadata


class BasicIngestion :

    def __init__(self, param_jason):
       print ("init")
       # self.es = Elasticsearch()
       with open(param_jason) as f:
          self.param_data = json.load(f)

       self.path = self.param_data["path"]
       self.index = self.param_data["indexToWriteIn"]
       self.pathlength = len(self.path)
       self.sqlite = self.param_data["sqlite"]
       self.pathProcessed= self.param_data["pathProcessed"]
       self.esServer = self.param_data["ESServer"]
       self.esUser = self.param_data["ESUser"]
       self.esPwd = self.param_data["ESPwd"]
       # es = Elasticsearch(['http://localhost:9200'], http_auth=('user', 'pass'))
       self.es = Elasticsearch([self.esServer], http_auth=(self.esUser, self.esPwd))
       print ("After connection to ES")

       # --- Set up the Kanban - Semaphore for "Process in progress"
       self.InProg = self.path + 'Enrich_Buff_In_Progress'
       if os.path.exists(self.InProg) == True :
           print ('Process in Progress - Exiting')
           exit(0)
       else:
           os.system('touch ' + self.InProg)

    # --- Main  ---
    def main_process(self):
        # printdatafile("Start Basic Ingestion","  Arg 1 : filename , Arg 2 : Index name")
        print("Start Enrich BUFF Ingestion")

        # ---- Get list of files
        DataFiles = self.DataFile_List()
        print('Nbr of Files to Process : ', len(DataFiles))
        if len(DataFiles) == 0:
            print("NO file to process ")
            os.system('rm -f ' + self.InProg)
            exit(0)

        self.metadata()

        # ---- Process ALL  file
        self.process_all_files(DataFiles)

        # df = p1.read_file_tbi()

        # p1.ingest(df)

        print("End of Enrich BUFF Ingestion")

    # ---- getting Client, Servers and DBUsers MetaData
    def metadata(self):
        print("In MetaData")
        p1 = metadata.MetaData(self.sqlite)

        # print ("In MetaData")
        # ---- Get Colls

        self.myListColls = p1.get_Colls()
        print(self.myListColls)
        # breakpoint()
        columns_colls = self.myListColls.columns.tolist()
        data = {}
        default_value = None
        num_rows = 1
        # for column in columns_nodes:
        for column in columns_colls:
            data['metadata.'+column] = [default_value] * num_rows
        # return pd.DataFrame(data)
        self.na_colls = pd.DataFrame(data)
        # [column] = None
        # breakpoint()

    # --- Getting the DAM Files to be Processed  ----
    def DataFile_List(self):
       DataFile=[]
       DataFiles=[]
       # csvFiles=glob.glob(self.path + "*" + self.collector + "*FSQL*.csv")
       csvFiles=glob.glob(self.path + "*BUF*.csv.gz")
       # breakpoint()
       for file in csvFiles:
           if "BUFF" in file:
                COLLt = file.split('/')[-1]
                # print(COLLt)
                COLL = COLLt.split('_')[1]
                DataFile.append(COLL)
                DataFile.append(file)
                DataFiles.append(DataFile)
                DataFile=[]
       return (DataFiles)

    # --- Process All Files
    def process_all_files(self,DataFiles):
      # --- Loop for each DAM data file
      for datafile in DataFiles:
        print('Will be Processing : ',datafile)
        os.system('printf "' + datafile[1] + '\n" >> ' + self.InProg )
        COLL =  datafile[0]
        self.coll_metadata = self.myListColls[self.myListColls['Collectors'] == COLL]
        # breakpoint()
        if self.coll_metadata.empty == True:
            self.coll_metadata = self.na_colls

        # -- Process One File
        df=self.read_file_tbi(datafile)

        # -- Upload into ES (ETL)
        print("Nbr of Docs to be Inserted", len(df))
        # print (" list SQLCounters ALL FILES " , self.p1.listSqlCounters)
        if len(df) > 0 :
           # --- insert enrich full sql
           self.ingest(df, datafile)

        os.system('rm -f ' + self.InProg)

        self.rename_file(datafile)

      return()


    def read_file_tbi(self,datafile) :

       df = read_csv(datafile[1])
       COLL = datafile[0]
       # Parse dates column
       df['Collector'] = COLL
       df['Timestamp'] = pd.to_datetime(df['Timestamp'])
       # Add the Coll Metadata
       columns = self.coll_metadata.columns
       for column in columns:
           print(column)
           # df[column]=self.coll_metadata[column].item()
           df['metadata.' + column] = self.coll_metadata[column].item()
       # coll_metadata =
       # df['Session Start'] = pd.to_datetime(df['Session Start'])
       columns = list(df.columns)
       columns[0] = "UTC Offset"
       df.columns = columns
       # print ( "DF : ", df.info())
       print (df)
       # exit(0)

       return df


    def ingest(self, df, datafile):
      print ("In ingest")
      df_json=df.to_json(orient='records', date_format = 'iso')
      # df_json=df.to_json()
      parsed=json.loads(df_json)
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

      try:
         # response = helpers.bulk(self.es,parsed, index=sys.argv[2])
         response = helpers.bulk(self.es,parsed, index=self.index+"-enrich-"+str(fileDate[:4])+"."+str(fileDate[4:6])+"."+str(fileDate[6:8]))
         # print ("ES response : ", response )
      except Exception as e:
         print ("ES Error :", e)

      return(len(parsed))

    # --- Move FullSQL csv file to Processed Folder
    def rename_file(self,datafile):

      shortname=datafile[1][self.pathlength:]
      print ("Rename as processed" , shortname)
      os.rename(datafile[1],self.pathProcessed + shortname)


