package wiki.hadoop.kafka.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @Description: Kafka Admin Client
 *
 * @author: Jast @Date: 2022/5/16
 */
@Slf4j
public class KafkaAdminClient2 {

    public AdminClient adminClient;

    /** @Description: 获取KafkaAdminClient @Date: 2022/5/16 */
    public AdminClient getAdminClient() {
        return adminClient;
    }

    /** @Description: 创建Kafka Admin Client @Date: 2022/5/16 @Param brokerList:Kafka节点 */
    public KafkaAdminClient2(String brokerList) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, 1000); // 设置重试等待时间为1秒
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 9000); // 设置请求超时时间为9秒

        adminClient = KafkaAdminClient.create(properties);
    }

    /** @Description: 创建Kafka Admin Client @Date: 2022/5/16 @Param brokerList:Kafka 配置参数 */
    public KafkaAdminClient2(Properties props) {
        adminClient = KafkaAdminClient.create(props);
    }

    /** @Description: 创建Kafka Admin Client @Date: 2022/5/16 @Param brokerList:Kafka 配置参数 */
    public KafkaAdminClient2(Map<String, Object> conf) {
        adminClient = KafkaAdminClient.create(conf);
    }

    /**
     * @Description: 删除指定offset之前的数据 @Date: 2022/5/16 @Param topic: Kafka主题 @Param partition:
     * 分区 @Param offset: 删除偏移量之前的文件
     */
    public DeleteRecordsResult deleteRecordsByOffset(String topic, int partition, int offset) {
        Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
        // 删除的topic信息
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        // 删除 16321699 偏移量之前的offset
        RecordsToDelete recordsToDelete1 = RecordsToDelete.beforeOffset(offset);
        recordsToDelete.put(topicPartition, recordsToDelete1);
        // 执行删除
        DeleteRecordsResult result = adminClient.deleteRecords(recordsToDelete);
        // 输出返回结果信息
        Map<TopicPartition, KafkaFuture<DeletedRecords>> lowWatermarks = result.lowWatermarks();
        try {
            for (Map.Entry<TopicPartition, KafkaFuture<DeletedRecords>> entry :
                    lowWatermarks.entrySet()) {
                DeletedRecords deletedRecords = entry.getValue().get();
                log.info(
                        "Delete records by offset ,topic:"
                                + entry.getKey().topic()
                                + ",partition:"
                                + entry.getKey().partition()
                                + ",offset:"
                                + deletedRecords.lowWatermark());
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("ERROR",e);
        }
        return result;
    }

    /**
     * @Description: 删除指定offset之前的数据 @Date: 2022/5/16 @Param topic:Kafka主题 @Param
     * partitionAndOffset:需要删除的分区,偏移量之前的数据
     */
    public DeleteRecordsResult deleteRecordsByOffset(
            String topic, Map<Integer, Long> partitionAndOffset) {
        Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
        partitionAndOffset.forEach(
                (partition, offset) -> {
                    // 删除的topic信息
                    TopicPartition topicPartition = new TopicPartition(topic, partition);
                    // 删除 offset 偏移量之前的offset
                    RecordsToDelete recordsToDelete1 = RecordsToDelete.beforeOffset(offset);
                    recordsToDelete.put(topicPartition, recordsToDelete1);
                });
        // 执行删除
        DeleteRecordsResult result = adminClient.deleteRecords(recordsToDelete);
        // 输出返回结果信息
        Map<TopicPartition, KafkaFuture<DeletedRecords>> lowWatermarks = result.lowWatermarks();
        try {
            for (Map.Entry<TopicPartition, KafkaFuture<DeletedRecords>> entry :
                    lowWatermarks.entrySet()) {
                DeletedRecords deletedRecords = entry.getValue().get();
                log.info(
                        "Delete records by offset ,topic:"
                                + entry.getKey().topic()
                                + ",partition:"
                                + entry.getKey().partition()
                                + ",offset:"
                                + deletedRecords.lowWatermark());
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("ERROR",e);
        }
        return result;
    }

    /**
     * @Description: 创建Kafka主题 @Date: 2022/5/16 @Param topicName: 主题名称 @Param numPartitions:
     * 分区数 @Param replicationFactor: 副本数
     */
    public void createTopic(String topicName, int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
        createTopicsResult.all().get();
    }

    /**
     * @Description: 创建Kafka主题,指定topic数据删除时间,
     * 注意：Kafka是定时检查是否需要删除，不是实时，参考参数：log.retention.check.interval.ms @Date: 2022/5/16 @Param
     * topicName:topic名称 @Param numPartitions:partition数量 @Param replicationFactor:副本数量 @Param
     * retentionMs:数据删除清理时间，单位毫秒
     */
    public void createTopic(
            String topicName, int numPartitions, short replicationFactor, long retentionMs)
            throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        HashMap<String, String> configs = new HashMap<>();
        // 设置数据删除时间
        configs.put("retention.ms", String.valueOf(retentionMs));
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
        createTopicsResult.all().get();
        log.info("Create topic " + topicName + " success");
    }

    /**
     * @Description: 创建Kafka主题,指定配置参数 @Date: 2022/5/16 @Param topicName: @Param
     * numPartitions: @Param replicationFactor: @Param configs:
     */
    public void createTopic(
            String topicName,
            int numPartitions,
            short replicationFactor,
            Map<String, String> configs)
            throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        newTopic.configs(configs);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
        createTopicsResult.all().get();
        log.info("Create topic " + topicName + " success");
    }

    /**
     * @Description: 查看Topic详细信息，包括分区数，副本数，分区Leader所在节点 @Date: 2022/5/16 @Param topicName:
     * 需要查看的TopicName
     */
    public Map<String, TopicDescription> describeTopic(String topicName)
            throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult =
                adminClient.describeTopics(Collections.singleton(topicName));
        Map<String, TopicDescription> map = describeTopicsResult.all().get();
        for (Map.Entry<String, TopicDescription> entry : map.entrySet()) {
            List<TopicPartitionInfo> listp =
                    entry.getValue().partitions(); // 拿到topic的partitions相关信息
            for (TopicPartitionInfo info : listp) {
                log.info(
                        "TopicName:"
                                + entry.getValue().name()
                                + "\tPartition Total Num:"
                                + entry.getValue().partitions().size()
                                + "\tPartition Leader:"
                                + info.partition()
                                + "\tBroker:"
                                + info.leader().host()
                                + ":"
                                + info.leader().port()
                                + "\tReplicas count:"
                                + info.replicas().size()
                                + "\treplicas:"
                                + info.replicas());
            }
        }
        return map;
    }

    /** @Description: 查看Topic列表，包含内部主题 __consumer_offset @Date: 2022/5/16 */
    public Set<String> topicListContainInternal() throws ExecutionException, InterruptedException {
        // 是否查看Internal选项(内部主题)
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names = listTopicsResult.names().get();
        log.info(names.toString());
        return names;
    }

    /** @Description: 查看Topic列表，仅返回名称，不包括内部主题 __consumer_offset @Date: 2022/5/16 */
    public Set<String> topicList() throws ExecutionException, InterruptedException {
        // 是否查看Internal选项(内部主题)
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names = listTopicsResult.names().get();
        log.info(names.toString());
        return names;
    }

    /** @Description: 删除topic列表 @Date: 2022/5/16 @Param topicList: */
    public void deleteTopics(List<String> topicList)
            throws ExecutionException, InterruptedException {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicList);
        deleteTopicsResult.all().get();
    }

    /** @Description: 删除topic @Date: 2022/5/16 @Param topicName: */
    public void deleteTopic(String topicName) throws ExecutionException, InterruptedException {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicName));
        deleteTopicsResult.all().get();
    }

    /** @Description: 输出Topic详细配置信息 @Date: 2022/5/16 */
    public Map<ConfigResource, Config> describeConfigTopic(String topic)
            throws ExecutionException, InterruptedException {

        List<String> topicNames = new ArrayList<>();
        topicNames.add(topic);
        List<ConfigResource> configResources = new ArrayList<>();
        topicNames.forEach(
                topicName ->
                        configResources.add(
                                // 指定要获取的源
                                new ConfigResource(ConfigResource.Type.TOPIC, topicName)));

        // 获取topic的配置信息
        DescribeConfigsResult result = adminClient.describeConfigs(configResources);

        // 解析topic的配置信息
        Map<ConfigResource, Config> resourceConfigMap = result.all().get();
        log.info("=================================================================");
        resourceConfigMap.forEach(
                (configResource, config) -> {
                    log.info(configResource.toString());
                    for (ConfigEntry entry : config.entries()) {
                        log.info(entry.toString());
                    }
                });
        log.info("=================================================================");
        return resourceConfigMap;
    }

    /**
     * @Description: 修改Topic partition 数量，只能高于topic当前partition数量，低于当前数量会报错 @Date: 2022/5/16 @Param
     * topicName: @Param partitionNum:
     */
    public void updateTopicPartition(String topicName, int partitionNum)
            throws ExecutionException, InterruptedException {
        // 构建修改分区的topic请求参数
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        newPartitions.put(topicName, NewPartitions.increaseTo(partitionNum));

        // 执行修改
        CreatePartitionsResult result = adminClient.createPartitions(newPartitions);

        // get方法是一个阻塞方法，一定要等到createPartitions完成之后才进行下一步操作
        result.all().get();
    }
    /** @Description: 关闭KafkaAdminClient连接 @Date: 2022/5/16 */
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
