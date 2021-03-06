# -*-coding:utf-8 -*-
from elasticsearch import Elasticsearch

es = Elasticsearch("219.224.134.220:9200")
index_name = "user_preference"


## 用户偏好特征  user_preference
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
              "timestamp":{ #记录时间
                      "type" : "long",
                      "index" : "not_analyzed"
              },
              "uid":{                             
                  "type":"string",
                  "index":"not_analyzed"
                    
              },
                
              "domain":{    #领域分布                      
                  "type":"object",
                  "index":"not_analyzed"

                    
              },                              
              "main_domain":{ #最可能领域
                  "type":"string",
                  "analyzer":"ik_smart"
                    
              },
              "topic":{ #话题分布
                  "type":"object",
                  "analyzer":"ik_smart"
                    
              }
              ,
              "key_words":{ #关键词
                  "type":"object",
                  "analyzer":"ik_smart"
                    
              },
              "sensitive_words":{#敏感词
                  "type":"object",
                  "analyzer":"ik_smart"
                    
              }
              ,
              "micro_words":{ #微话题
                  "type":"object",
                  "analyzer":"ik_smart"
                    
              }
                
          }
      }
  }
}



exist_indice = es.indices.exists(index = index_name)

print(exist_indice)
if not exist_indice:
    print(es.indices.create(index = index_name, body=index_info, ignore = 400))