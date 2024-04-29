```json
PUT /kafka-group
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
        "consumer_offset" : {
          "properties" : {
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
            }
          }
        },
        "insert_time" : {
          "type" : "date",
          "format" : "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis||yyyy-MM-dd'T'HH:mm:ss.SSSZ"
        },
        "topic" : {
          "type" : "keyword"
        }
      }
    }
}
```