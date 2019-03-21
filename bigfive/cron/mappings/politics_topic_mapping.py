# -*-coding:utf-8 -*-
from elasticsearch import Elasticsearch

es = Elasticsearch("219.224.134.220:9200")
index_name = "politics_topic"
index_info={
        "settings": {
          "index": {
            "number_of_shards":3,
            "number_of_replicas":1,
            "analysis": {
              "analyzer": {
                "my_analyzer": {
                  "type": "pattern",
                  "pattern": "&"
                    }
                  }
                }
              }
            },
        "mappings": {
          "text": {
            "properties": {
              "politics_id": {
                "index": "not_analyzed",
                "type": "string"
              },
              "user_type": {
                "index": "not_analyzed",
                "type": "string"
              },
              "sentiment": {
                "index": "not_analyzed",
                "type": "string"
              },
              "key_words_topic_1": {
                "index": "not_analyzed",
                "type": "object"
              },
              "key_words_topic_2": {
                "index": "not_analyzed",
                "type": "object"
              },
              "key_words_topic_3": {
                "index": "not_analyzed",
                "type": "object"
              },
              "key_words_topic_4": {
                "index": "not_analyzed",
                "type": "object"
              },
              "key_words_topic_5": {
                "index": "not_analyzed",
                "type": "object"
              },
              "weibo_topic_1": {
                "index": "not_analyzed",
                "type": "string"
              },
              "weibo_topic_2": {
                "index": "not_analyzed",
                "type": "string"
              },
              "weibo_topic_3": {
                "index": "not_analyzed",
                "type": "string"
              },
              "weibo_topic_4": {
                "index": "not_analyzed",
                "type": "string"
              },
              "weibo_topic_5": {
                "index": "not_analyzed",
                "type": "string"
              }
            }
          }
        }
      }

# es.indices.delete(index = index_name)
exist_indice = es.indices.exists(index = index_name)

print(exist_indice)
if not exist_indice:
    print(es.indices.create(index = index_name, body=index_info, ignore = 400))


# key= group_id+timestamp