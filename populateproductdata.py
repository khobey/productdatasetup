#delete if not yet
#create elasticsearch
#read file and parse
#push data

from elasticsearch import Elasticsearch
import json
import sys

client = Elasticsearch([{'host':'localhost', 'port':'9200'}])
indexname = 'bakeryproducts'

def checkESStatus():
    try:
        elastic_info =Elasticsearch.info(client);
        print ("Cluster info:", json.dumps(elastic_info))
    except Exception as err:
        print ("Elasticsearch client error: ", err)
        sys.exit()

def deleteIfIndexExists():
    if(client.indices.exists(index=indexname)):
        client.indices.delete(indexname)

def getMapping():
    try:
        return open("productdatamapping","r").read()
    except Exception as err:
        print("Unable to get mapping: ", err) 
    
def createIndex(mapping):
    response = client.indices.create( indexname, body=mapping)
    print ('create index response:', response)

def  populateIndex():
    try:
        data = open("productdata","r").read()
        response = client.bulk(data)
    except Exception as err:
        print("Elasticsearch insert data error: ", err)
    print ('bulk insert response:', response)

checkESStatus()
deleteIfIndexExists()
indexmapping = getMapping()

if(indexmapping is None):
    print("Exiting script because essential steps did not push through.")
    sys.exit()
    
createIndex(indexmapping)
populateIndex()
