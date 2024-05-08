curl -XPUT http://0.0.0.0:9200/kafka-group -H 'Content-Type: application/json' -H 'Authorization: Basic ZWxhc3RpYzplbGFzdGlj' -d'
{
  "settings":{
    "number_of_shards":3,
    "number_of_replicas":0
  },
    "mappings" : {
      "properties" : {
        "consumer_group" : {
          "type" : "keyword"
        },
            "earliest" : {
              "type" : "long"
            },
            "latest" : {
              "type" : "long"
            },
            "offset" : {
              "type" : "long"
            },
            "partition" : {
              "type" : "integer"
            },
        "insert_time" : {
          "type" : "date",
          "format" : "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||yyyy-MM-dd\u0027T\u0027HH:mm:ss.SSSZ"
        },
        "topic" : {
          "type" : "keyword"
        }
      }
    }
}
'