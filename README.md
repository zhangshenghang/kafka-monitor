
启动运行方法：
```
docker run -p 5601:5601 -p 9200:9200 -e KAFKA_BROKER=172.16.24.199:9092 -v  /root/hosts:/home/hosts -it -d registry.cn-hangzhou.aliyuncs.com/jast-tools/kafka-monitor:1.4
```


docker run -p 5601:5601 -p 9200:9200 -e KAFKA_BROKER=172.16.24.199:9092 -v  /root/hosts:/home/hosts -it -d kafka-monitor:basic