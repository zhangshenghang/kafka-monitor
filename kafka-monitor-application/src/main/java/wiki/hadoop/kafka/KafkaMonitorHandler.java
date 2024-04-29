package wiki.hadoop.kafka;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.TopicPartition;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import wiki.hadoop.kafka.bean.OffsetInfo;
import wiki.hadoop.kafka.monitor.KafkaMonitor;
import wiki.hadoop.kafka.monitor.KafkaMonitorClient;
import wiki.hadoop.kafka.util.ElasticSearchBulkWrite;
import wiki.hadoop.kafka.util.UnitConvert;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Jast
 * @description 监控Kafka信息
 * @date 2023-04-06 14:30
 */
@Slf4j
public class KafkaMonitorHandler {

    // Elasticsearch 连接信息常量
    private static String ELASTICSEARCH_HOST = "";
    private static int ELASTICSEARCH_PORT = 9200;
    private static String ELASTICSEARCH_USERNAME = "";
    private static String ELASTICSEARCH_PASSWORD = "";

    // Kafka 集群连接信息常量
    public static String KAFKA_BROKER_LIST = "";

    // Elasticsearch 索引名称常量
    private static final String KAFKA_TOPIC_INDEX = "kafka-topic";
    private static final String KAFKA_GROUP_INDEX = "kafka-group";

    /**
     * 创建 Elasticsearch 客户端
     *
     * @param host Elasticsearch 主机名
     * @param port Elasticsearch 端口号
     * @param username Elasticsearch 用户名
     * @param password Elasticsearch 密码
     * @return Elasticsearch 客户端
     */
    private static RestHighLevelClient createElasticsearchClient(
            String host, int port, String username, String password) {
        // 创建凭据提供者对象，并设置凭据信息
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if(StrUtil.isNotBlank(username) && StrUtil.isNotBlank(password)) {
            credentialsProvider.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        }
        // 创建 Elasticsearch 客户端对象
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port))
                        .setHttpClientConfigCallback(
                                httpClientBuilder ->
                                        httpClientBuilder.setDefaultCredentialsProvider(
                                                credentialsProvider)));
    }

    /**
     * 构建 Kafka topic 磁盘信息的 JSON 对象。
     *
     * @param diskUsage 磁盘使用量，单位为字节。
     * @param currentTime 当前时间戳。
     * @return Kafka topic 磁盘信息的 JSON 对象。
     */
    private static JSONObject buildKafkaTopicDiskInfo(long diskUsage, String currentTime) {
        JSONObject jsonObject = new JSONObject();
        // 将磁盘使用量转换为可读的大小表示
        jsonObject.put("use_disk_view", UnitConvert.bytesToSize(diskUsage));
        // 以字节为单位的磁盘使用量
        jsonObject.put("use_disk_bytes", diskUsage);
        // 插入时间
        jsonObject.put("insert_time", currentTime);
        return jsonObject;
    }

    /**
     * 构建 Kafka topic 描述信息的 JSON 对象。
     *
     * @param topicInfo Kafka topic 的描述信息。
     * @param currentTime 当前时间戳。
     * @return Kafka topic 描述信息的 JSON 对象。
     */
    private static JSONObject buildKafkaTopicDescriptionInfo(
            JSONObject topicInfo, String currentTime) {
        // 获取主题的分区信息
        JSONArray partitions = topicInfo.getJSONArray("partitions");
        JSONObject esjsonObject = new JSONObject();
        // 存储主题的分区信息
        esjsonObject.put("partitions", partitions);
        // 存储主题的分区数量
        esjsonObject.put("partitionNum", topicInfo.getLong("partitionNum"));
        // 插入时间
        esjsonObject.put("insert_time", currentTime);
        return esjsonObject;
    }

    /**
     * 构建 Kafka topic 偏移量信息的 JSON 对象。
     *
     * @param topic Kafka topic 的名称。
     * @param topicOffsetInfo 包含 Kafka topic 偏移量信息的 Map 对象。 Key 为 TopicPartition 对象，Value 为
     *     OffsetInfo 对象。
     * @param currentTime 当前时间戳，表示构建 JSON 对象的时间。
     * @return Kafka topic 偏移量信息的 JSON 对象。 包含以下字段： - "offset"：包含每个分区的最早和最新偏移量的 JSON 数组。 每个 JSON
     *     对象包含以下字段： - "topic_partition"：分区编号。 - "earliest"：最早的偏移量。 - "latest"：最新的偏移量。 -
     *     "topic"：Kafka topic 的名称。 - "insert_time"：插入时间。
     */
    private static JSONObject buildKafkaTopicOffsetInfo(
            String topic, Map<TopicPartition, OffsetInfo> topicOffsetInfo, String currentTime) {
        JSONArray offsetArray = new JSONArray();
        topicOffsetInfo.forEach(
                (x, y) -> {
                    JSONObject offset = new JSONObject();
                    offset.put("topic_partition", x.partition());
                    offset.put("earliest", y.getEarliestOffset());
                    offset.put("latest", y.getLatestOffset());
                    offsetArray.add(offset);
                });
        JSONObject esjsonObject = new JSONObject();
        esjsonObject.put("offset", offsetArray);
        esjsonObject.put("topic", topic);
        esjsonObject.put("insert_time", currentTime);
        return esjsonObject;
    }

    /**
     * 初始化全局变量
     *
     * @param args 参数数组，包含以下参数： args[0] Elasticsearch主机名 args[1] Elasticsearch端口号 args[2]
     *     Elasticsearch用户名 args[3] Elasticsearch密码 args[4] Kafka broker列表
     */
    public static void initialize(String[] args) {
        // Elasticsearch主机名
        ELASTICSEARCH_HOST = args[0];
        // Elasticsearch端口号
        ELASTICSEARCH_PORT = Integer.parseInt(args[1]);
        // Elasticsearch用户名
        ELASTICSEARCH_USERNAME = args[2];
        // Elasticsearch密码
        ELASTICSEARCH_PASSWORD = args[3];
        // Kafka broker列表
        KAFKA_BROKER_LIST = args[4];
        // 打印 Elasticsearch 连接信息常量
        log.info("Elasticsearch 主机名：" + ELASTICSEARCH_HOST);
        log.info("Elasticsearch 端口号：" + ELASTICSEARCH_PORT);
        log.info("Elasticsearch 用户名：" + ELASTICSEARCH_USERNAME);
        log.info("Elasticsearch 密码：" + ELASTICSEARCH_PASSWORD);

        // 打印 Kafka 集群连接信息常量
        log.info("Kafka broker 列表：" + KAFKA_BROKER_LIST);

        // 打印 Elasticsearch 索引名称常量
        log.info("Kafka topic 索引名称：" + KAFKA_TOPIC_INDEX);
        log.info("Kafka group 索引名称：" + KAFKA_GROUP_INDEX);
    }

    @SneakyThrows
    public static void start(String[] args)
            {

        initialize(args);

        RestHighLevelClient elasticsearchClient =
                createElasticsearchClient(
                        ELASTICSEARCH_HOST,
                        ELASTICSEARCH_PORT,
                        ELASTICSEARCH_USERNAME,
                        ELASTICSEARCH_PASSWORD);
        log.info("ElasticSearch连接创建完成");

        // 创建 BulkRequest 实例
        BulkRequest bulkRequest = new BulkRequest();

        // 获取当前时间
        long currentTimeMillis = System.currentTimeMillis();
        String currentTime = DateUtil.format(DateUtil.date(currentTimeMillis),"yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        // 创建 KafkaMonitorClient 对象，用于获取 Kafka 监控信息
        KafkaMonitor kafkaAdminClient = new KafkaMonitorClient(KAFKA_BROKER_LIST);

        // 获取 Kafka 集群中的所有 Topic 列表
        Set<String> topicList = kafkaAdminClient.topicList();

        Map<String,Map<Integer, OffsetInfo>> topicMap = new HashMap<>();

        // 遍历每个 Topic，获取其最大和最小 Offset，并将其写入 Elasticsearch
        for (String topic : topicList) {
            // 获取该 Topic 的最大和最小 Offset
            Map<TopicPartition, OffsetInfo> topicOffsetInfo =
                    kafkaAdminClient.getTopicOffsetInfo(topic);
            if(topicOffsetInfo==null){
                log.error("当前Topic获取失败:"+topic);
                continue;
            }
            HashMap<Integer, OffsetInfo> objectObjectHashMap = new HashMap<>();
            topicOffsetInfo.forEach(
                    (x, y) -> {
                        JSONObject offset = new JSONObject();
                        offset.put("topic_partition", x.partition());
                        offset.put("earliest", y.getEarliestOffset());
                        offset.put("latest", y.getLatestOffset());
                        OffsetInfo offsetInfo = new OffsetInfo(y.getEarliestOffset(), y.getLatestOffset());
                        objectObjectHashMap.put(x.partition(),offsetInfo);
                    });
            topicMap.put(topic,objectObjectHashMap);

            // 构建一个包含当前 Topic 最大和最小 Offset 的 JSON 对象
            JSONObject kafkaTopicOffsetInfo =
                    buildKafkaTopicOffsetInfo(topic, topicOffsetInfo, currentTime);
            // 将当前 Topic 的最大和最小 Offset 写入 Elasticsearch
            ElasticSearchBulkWrite.upsertDocument(
                    KAFKA_TOPIC_INDEX,
                    topic + "_" + currentTimeMillis,
                    kafkaTopicOffsetInfo,
                    bulkRequest);
        }
        // 获取Topic大小
        Map<String, Long> topicDisk = kafkaAdminClient.getTopicDisk();
        log.info("开始获取每个Topic占用磁盘空间");
        topicDisk.entrySet().stream()
                .forEach(
                        entry -> {
                            String topic = entry.getKey();
                            long diskUsage = entry.getValue();
                            JSONObject kafkaTopicDiskInfo =
                                    buildKafkaTopicDiskInfo(diskUsage, currentTime);

                            ElasticSearchBulkWrite.upsertDocument(
                                    KAFKA_TOPIC_INDEX,
                                    topic + "_" + currentTimeMillis,
                                    kafkaTopicDiskInfo,
                                    bulkRequest);
                        });
        log.info("Topic占用磁盘空间获取完成.");

        // 使用 KafkaAdminClient 获取 Kafka topic 描述信息，并将其写入 Elasticsearch
        // 获取 Kafka topic 描述信息
        String topicDescription = kafkaAdminClient.describeTopics();
        // 将 JSON 字符串转换为 JSONObject 对象
        JSONObject jsonObject = JSONObject.parseObject(topicDescription);
        // 遍历 Kafka topic 描述信息的 Map 对象
        for (Map.Entry<String, Object> topicInfo : jsonObject.entrySet()) {
            // 获取 topic 名称
            String topic = topicInfo.getKey();
            // 获取 topic 的描述信息
            JSONObject topicInfoObject = (JSONObject) topicInfo.getValue();
            // 将 topic 的描述信息转换为 Elasticsearch 文档
            JSONObject kafkaTopicDescriptionInfo =
                    buildKafkaTopicDescriptionInfo(topicInfoObject, currentTime);
            // 将 Elasticsearch 文档写入 Elasticsearch 中
            ElasticSearchBulkWrite.upsertDocument(
                    KAFKA_TOPIC_INDEX,
                    topic + "_" + currentTimeMillis,
                    kafkaTopicDescriptionInfo,
                    bulkRequest);
        }
                log.info("Topic基础信息获取完成.");


                // 获取消费者组详情
        // 获取 Kafka consumer group 列表
        Collection<ConsumerGroupListing> consumerGroupListings = kafkaAdminClient.listGroups();

        // 遍历 Kafka consumer group 列表
        for (ConsumerGroupListing consumerGroupListing : consumerGroupListings) {
            // 获取 consumer group id
            String groupId = consumerGroupListing.groupId();

            // 获取 consumer group 的消费位移信息
            Map<String, JSONObject> consumerGroupPosition =
                    kafkaAdminClient.getConsumerGroupPosition(groupId);

            // 遍历消费位移信息的 Map 对象
            for (Map.Entry<String, Object> stringObjectEntry :
                    consumerGroupPosition.get(groupId).entrySet()) {
                JSONArray jsonArray = new JSONArray();
                JSONObject esJsonObject = new JSONObject();

                // 获取 topic 名称
                String topic = stringObjectEntry.getKey();
                // 获取 partition 的消费位移信息
                JSONObject value = (JSONObject) stringObjectEntry.getValue();

                // 遍历每个 partition 的消费位移信息，并将其添加到 jsonArray 中
                for (String partition : value.keySet()) {
                    JSONObject jsonObject2 = new JSONObject();
                    jsonObject2.put("partition", partition);
                    jsonObject2.put("offset", value.getLong(partition));
                    jsonObject2.put("latest", topicMap.get(topic) ==null?-1:topicMap.get(topic).get(Integer.parseInt(partition)).getLatestOffset());
                    jsonObject2.put("earliest", topicMap.get(topic)==null?-1:topicMap.get(topic).get(Integer.parseInt(partition)).getEarliestOffset());
                    jsonArray.add(jsonObject2);
                }

                // 将消费位移信息写入 Elasticsearch
                esJsonObject.put("topic", topic);
                esJsonObject.put("consumer_group", groupId);
                esJsonObject.put("consumer_offset", jsonArray);
                esJsonObject.put("insert_time", currentTime);

                // 将 Elasticsearch 文档写入 Elasticsearch 中
                ElasticSearchBulkWrite.upsertDocument(
                        KAFKA_GROUP_INDEX,
                        topic + "_" + groupId + "_" + currentTimeMillis,
                        esJsonObject,
                        bulkRequest);
            }
        }
        log.info("消费者组信息获取完成.");

        try {
            // 执行批量请求
            elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            // 关闭 Elasticsearch 客户端连接
            elasticsearchClient.close();
            // 关闭Kafka连接
            kafkaAdminClient.close();
        }catch (Exception e ){
            log.error("写入异常",e);
        }
        log.info("本轮执行完成");
    }
}
