# 集成centos基础版本
FROM registry.cn-hangzhou.aliyuncs.com/jast-tools/kafka-monitor:basic

ARG VERSION
# 作者信息
MAINTAINER jast
# 设置工作目录，即进入容器后默认目录
ENV MYPATH /home/es

COPY target/kafka-monitor-application-${VERSION}.jar /home/es/kafka-monitor/kafka-monitor-application.jar
COPY target/kafka-monitor-application-${VERSION}.jar /home/es/kafka-monitor

WORKDIR $MYPATH
USER es

EXPOSE 9200
EXPOSE 5601

CMD sh /home/es/start.sh