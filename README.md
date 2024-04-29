
启动运行方法：
```
docker run -p 5601:5601 -e KAFKA_BROKER=172.16.24.199:9092 -v  /root/hosts:/home/hosts -it -d kafka-monitor 
```