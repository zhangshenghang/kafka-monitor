# 集成centos基础版本
FROM registry.cn-hangzhou.aliyuncs.com/jast-zsh/kafka-monitor:0.1
# 作者信息
MAINTAINER jast
# 设置工作目录，即进入容器后默认目录
ENV MYPATH /home/es
#
WORKDIR $MYPATH
USER es

EXPOSE 9200
EXPOSE 5601

CMD sh /home/es/start.sh