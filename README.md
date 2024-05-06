项目代码地址：[https://github.com/zhangshenghang/kafka-monitor](https://github.com/zhangshenghang/kafka-monitor)

当前最新版本：1.6

## 启动运行方法
```bash
docker run -p ${KIBANA_PORT}:5601 -p ${ES_PORT}:9200 -e KAFKA_BROKER=${KAFKA_BROKER} -v ${HOST_PATH}:/home/hosts -it -d registry.cn-hangzhou.aliyuncs.com/jast-tools/kafka-monitor:${VERSION}
```
> 样例：
> `docker run -p 5601:5601 -p 9200:9200 -e KAFKA_BROKER=192.168.1.1:9092 -v /etc/hosts:/home/hosts -it -d registry.cn-hangzhou.aliyuncs.com/jast-tools/kafka-monitor:1.6`
> 
- **参数解释：**
    - `${KIBANA_PORT}` : 用于访问 Kibana 控制台的端口号。
    - `${ES_PORT}` : 用于访问 Elasticsearch REST API 的端口号（这个也可以关闭）。
    - `${KAFKA_BROKER}` : 要监控的 Kafka 集群的 Broker 地址。
    - `${HOST_PATH}` : 主机上包含 Kafka 集群主机信息的 hosts 文件路径。
    - `${VERSION}` : 需要启动的镜像版本号。使用最新版本即可。

## 使用方法
1. 在浏览器中访问 [http://localhost:${KIBANA_PORT}](http://localhost:${KIBANA_PORT})。
2. 使用以下默认账号密码登录：
    - **用户名：** `elastic`
    - **密码：** `elastic`
3. 登录后，您将进入 Kibana 控制台，可以在其中查看 Kafka 集群的监控信息。

## 注意事项
- 在启动容器之前，请确保 Docker 已正确安装并正在运行。
- 请确保本地端口 `${KIBANA_PORT}` 和 `${ES_PORT}` 没有被其他应用程序占用，否则可能会导致端口冲突。
- 确保您的主机可以与指定的 Kafka Broker 通信，并且具有访问权限。
- 如果需要修改其他配置或参数，可以通过修改 Docker 命令中的环境变量来实现，或者在启动容器后通过 Kibana 控制台进行设置。