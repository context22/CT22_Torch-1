import pandas as pd
import json
import pdb
from datetime import datetime
import time
import os
import sys
import glob
from elasticsearch import Elasticsearch, helpers
from datetime import date


class BuffEnrich:

    def __init__(self, param_jason):
        print("init")
        # self.es = Elasticsearch()
        with open(param_jason) as f:
            self.param_data = json.load(f)

        self.path = self.param_data["path"]
        self.pathlength = len(self.path)
        self.pathProcessed = self.param_data["pathProcessed"]
        self.index = self.param_data["index"]
        self.esServer = self.param_data["ESServer"]
        self.esUser = self.param_data["ESUser"]
        self.esPwd = self.param_data["ESPwd"]
        # es = Elasticsearch(['http://localhost:9200'], http_auth=('user', 'pass'))
        self.es = Elasticsearch([self.esServer], http_auth=(self.esUser, self.esPwd))
        print("After connection to ES")

        self.InProg = self.path + 'Buff_Enrich_In_Progress'
        if os.path.exists(self.InProg) == True:
            print("Buff Process in Progress - Exiting")
            # os.system('rm -f ' + self.path + 'Buff_Enrich_In_Progress')
            exit(0)
        else:
            os.system('touch ' + self.InProg)

        self.myListCollectors = []
        self.csvFiles = []
        self.DataFiles = []

    def main_process(self):
        print("BUM_In_Progress Test data Gen")

        self.DataFile_List()
        # --- Loop for each DAM data file
        breakpoint()
        doc_count_total = 0
        for datafile in self.DataFiles:
            print('Processing : ', datafile)
            #   Process ONE file  -------------
            df = self.processOneFile(datafile)
            # move the processed file to Processed directory
            print("datafile for current file ", datafile[4])
            print(self.pathProcessed + datafile[4])
            # os.rename(datafile[1],self.pathProcessed + datafile[4])
            newPath = self.pathProcessed + datafile[4]
            # df.to_csv('data.csv', index=False)
            df.to_csv(newPath, index=False)
            # os.rename(datafile[1],self.pathProcessed + shortname)
            # doc_count_total = doc_count_total + doc_count
            # print ('Nbr of Docs Processed' , doc_count_total)
            os.system('rm -f ' + self.path + 'BUM_In_Progress')
        print("End STAP Convertion")
        return (0)

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


