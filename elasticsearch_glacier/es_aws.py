#!/usr/bin/env python
'''
pip install elasticsearch
pip install boto3
'''
import sys
import os.path
import boto3
from elasticsearch import Elasticsearch
from elasticsearch import ElasticsearchException

class awsglacier:
      def __init__(self,region_name="eu-west-1",aws_access_key_id="xxx",aws_secret_access_key="xxx",service_name="glacier",vaultname="xxx"):
         self.region=region_name
         self.key=aws_access_key_id
         self.secret=aws_secret_access_key
         self.service=service_name
         self.vault=vaultname
      def getjobid(self,downloadfile):
         client=boto3.client(self.service,region_name=self.region,aws_access_key_id=self.key,aws_secret_access_key=self.secret)
         response =client.initiate_job(vaultName=self.vaultname)
         '''
         get  archiveid using function describe_job
         '''

      def uploadfile(self,uploadfile):
         client=boto3.client(self.service,region_name=self.region,aws_access_key_id=self.key,aws_secret_access_key=self.secret)
         if os.path.isfile(uploadfile)==False:
            return False
         response=client.upload_archive(vaultName=self.vault,body=uploadfile)
 
class indices:
      def __init__(self,host,region_name,aws_access_key_id,aws_secret_access_key):
         self.host=host
         self.region=region_name
         self.access=aws_access_key_id
         self.secret=aws_secret_access_key
      def get(self,indexs,logdir="/root",datadir="/opt"):
         es=Elasticsearch(self.host)
         try:
             count=es.count(index=indexs)
             count=int(count["count"])
             logfile=logdir+"/"+indexs
             if os.path.isfile(logfile):
                file=open(logfile,"r")
                value=int(file.readline())
                file.close()
                if count<=value:
                   return False
             file=open(logfile,"w")
             file.write(str(count))
             file.close()
             num=count/10
             j=0
             datafile=datadir+"/"+indexs
             while j<=num:
                rs=es.search(index=indexs,from_=j*10,size=10)
                file=open(datafile,"a")
                for doc in rs["hits"]["hits"]:
                    file.write(str(doc["_source"])+"\n")
                file.close()
                j=j+1
             if os.path.isfile(datafile):
                glacier=awsglacier(self.region,self.access,self.secret)
                glacier.uploadfile(datafile)
                os.remove(datafile)
         except ElasticsearchException:
             print "elasticsearch exceptiont"
      def getindices(self):
         es=Elasticsearch(self.host)
         try:
            id=es.cat.indices(h="index")
            str=id.replace("\n","")
            i=0
            while True:
               index=str.split(" ")[i]
               i=i+1
               if index=="":
                  print "that value is null"
               else:
                   self.get(index)
         except  ElasticsearchException:
             print  "ElasticsearchException"
         except IndexError:
             print "out of index"
es=indices(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4])
es.getindices()
