# -*-coding:utf-8 -*-
from elasticsearch import Elasticsearch
import time

es = Elasticsearch("219.224.134.220:9200")
index_name = "flow_timestamp"


###用户情绪特征表  user_emotion
index_info = {
      "settings": {
          "number_of_shards": 3,  
          "number_of_replicas":1, 
          "analysis":{ 
              "analyzer":{
                  "my_analyzer":{
                      "type":"pattern",
                      "patern":"&"
                  }
              }
          }
      },
      "mappings":{
          "text":{
              "properties":{
                  "timestamp":{#记录时间
                          "type" : "long"
                  },
                  "date":{
                      "format": "dateOptionalTime",
                      "type":"date"
                  }
                    
              }
          }
      }
  }


exist_indice = es.indices.exists(index = index_name)

print(exist_indice)
if not exist_indice:
    print(es.indices.create(index = index_name, body=index_info, ignore = 400))

