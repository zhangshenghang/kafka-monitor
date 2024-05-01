
## 启动运行方法
```
docker run -p 5601:5601 -p 9200:9200 -e KAFKA_BROKER=172.16.24.199:9092 -v  /root/hosts:/home/hosts -it -d registry.cn-hangzhou.aliyuncs.com/jast-tools/kafka-monitor:1.4
```
- /root/hosts : 挂载hosts文件里面需要包含Kafka集群的Hosts，/home/hosts 不要修改 
- KAFKA_BROKER : 需要监控的kafka集群，如: 192.168.1.1:9092
- 其他值为固定值

## 使用方法
访问 http://localhost:5601 ，账号密码: elastic/elastic