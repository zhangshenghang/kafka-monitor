package wiki.hadoop.kafka.monitor;

import cn.hutool.json.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import wiki.hadoop.kafka.bean.OffsetInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author Jast
 * @description Kafka监控
 * @date 2023-04-07 15:33
 */
@Slf4j
public class KafkaMonitorClient extends wiki.hadoop.kafka.admin.KafkaAdminClient2
        implements KafkaMonitor {

    public KafkaMonitorClient(String brokerList) {
        super(brokerList);
    }

    public KafkaMonitorClient(Properties props) {
        super(props);
    }

    public KafkaMonitorClient(Map<String, Object> conf) {
        super(conf);
    }

    /**
     * 描述 Kafka 中所有主题及其分区信息的方法。
     *
     * @return 包含所有主题及其分区信息的 JSON 字符串。
     * @throws ExecutionException 执行 Kafka Admin 客户端操作时出现错误。
     * @throws InterruptedException 线程中断时抛出此异常。
     */
    @Override
    public String describeTopics() throws ExecutionException, InterruptedException {
        // 创建 ListTopicsOptions 对象，设置 listInternal 为 true，用于获取内部主题。
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);

        // 调用 AdminClient 的 listTopics 方法，获取主题列表。
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names = listTopicsResult.names().get();

        // 调用 AdminClient 的 describeTopics 方法，获取主题及其分区信息。
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(names);
        Map<String, TopicDescription> map = describeTopicsResult.all().get();

        // 将主题及其分区信息转换为 JSON 字符串。
        JSONObject result = new JSONObject();
        for (Map.Entry<String, TopicDescription> entry : map.entrySet()) {
            // 获取主题的分区信息。
            List<TopicPartitionInfo> partitions = entry.getValue().partitions();
            JSONArray partitionArray = new JSONArray();
            for (TopicPartitionInfo info : partitions) {
                // 将分区信息转换为 JSON 对象。
                JSONObject partitionObject = new JSONObject();
                partitionObject.put("partition", info.partition());
                partitionObject.put("leader", info.leader().host() + ":" + info.leader().port());
                partitionObject.put("replicas_count", info.replicas().size());
                partitionObject.put(
                        "replicas",
                        info.replicas().stream().map(Node::toString).collect(Collectors.toList()));
                partitionArray.add(partitionObject);
            }
            // 将主题及其分区信息转换为 JSON 对象。
            JSONObject topicObject = new JSONObject();
            topicObject.put("name", entry.getValue().name());
            topicObject.put("partitionNum", partitionArray.size());
            topicObject.put("partitions", partitionArray);
            result.put(entry.getKey(), topicObject);
        }

        // 返回包含所有主题及其分区信息的 JSON 字符串。
        return result.toString();
    }
    /**
     * 获取消费者组的消费状态。
     *
     * @param consumerGroupId 消费者组 ID。
     * @return 包含消费者组消费状态的 JSON 对象。
     */
    @Override
    public Map<String, JSONObject> getConsumerGroupPosition(String consumerGroupId) {
        // 获取消费者组的消费状态。
        Map<TopicPartition, OffsetAndMetadata> offsets = null;
        try {
            // 通过 AdminClient 获取消费者组的消费状态信息。
            offsets =
                    adminClient
                            .listConsumerGroupOffsets(consumerGroupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
        } catch (InterruptedException e) {
            log.error("ERROR",e);
        } catch (ExecutionException e) {
            log.error("ERROR",e);
        }

        // 处理消费状态信息，组装成 JSON 对象。
        HashMap<String, Map<Integer, Long>> consumerInfos = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            // 获取分区信息和消费状态信息。
            TopicPartition partition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            // 将分区的消费状态信息放入 Map 中。
            Map<Integer, Long> integerLongMap = new HashMap<>();
            if (consumerInfos.containsKey(partition.topic())) {
                integerLongMap = consumerInfos.get(partition.topic());
            }
            integerLongMap.put(partition.partition(), offsetAndMetadata.offset());
            consumerInfos.put(partition.topic(), integerLongMap);
        }

        // 将消费状态信息转换为 JSON 对象，并返回包含消费者组消费状态的 JSON 对象。
        Map<String, JSONObject> result = new HashMap<>();
        result.put(consumerGroupId, JSONObject.parseObject(JSONObject.toJSONString(consumerInfos)));
        return result;
    }

    /**
     * @date 2023/4/6 下午2:52
     * @description 查看所有消费者组
     * @author Jast
     */
    @Override
    public Collection<ConsumerGroupListing> listGroups() {
        // 获取消费者组列表
        ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupDescriptions;
        try {
            consumerGroupDescriptions = listConsumerGroupsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to list consumer groups", e);
        }
        // 打印消费者组列表
        // System.out.println("All consumer groups in the cluster:");
        // consumerGroupDescriptions.forEach(
        //        consumerGroupDescription ->
        // System.out.println(consumerGroupDescription.groupId()));
        return consumerGroupDescriptions;
    }

    /**
     * 获取 Kafka 集群中所有 Topic 的名称列表。不包括内部主题 __consumer_offset
     *
     * @return 包含所有 Topic 名称的 Set 集合。
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Override
    public Set<String> topicList() throws ExecutionException, InterruptedException {
        // 是否查看Internal选项(内部主题)
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);
        // 获取所有 Topic 的名称列表。
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names = listTopicsResult.names().get();
        // 打印 Topic 名称列表，并返回该列表。
        log.info(names.toString());
        return names;
    }

    /**
     * 获取每个Topic存储容量
     *
     * @date 2023/4/7 下午3:10
     * @description
     * @author Jast
     */
    @Override
    public Map<String, Long> getTopicDisk() throws ExecutionException, InterruptedException {
        // 构造DescribeClusterOptions，不需要额外的配置，这里直接使用默认选项
        DescribeClusterOptions describeClusterOptions = new DescribeClusterOptions();

        // 构造DescribeClusterResult，调用describeCluster方法获取Kafka集群信息
        DescribeClusterResult describeClusterResult =
                adminClient.describeCluster(describeClusterOptions);

        // 获取集群中的所有broker
        Collection<Node> nodes = describeClusterResult.nodes().get();
        // 类型从Node转换为集群id
        Collection<Integer> noedesInt = nodes.stream().map(Node::id).collect(Collectors.toList());

        Map<String, Long> topicSize = new HashMap<>();
        DescribeLogDirsResult logDirsResult = adminClient.describeLogDirs(noedesInt);
        Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions1 =
                logDirsResult.descriptions();
        descriptions1.forEach((brokerId, desc) -> forEachDescriptions(topicSize, desc));

        return topicSize.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()));
    }

    /**
     * 遍历获取到的主题分区信息，并更新主题的存储类型和大小。
     *
     * @param topicSize 包含主题大小的 Map 对象。
     * @param desc 主题分区信息。
     */
    private void forEachDescriptions(
            Map<String, Long> topicSize, KafkaFuture<Map<String, LogDirDescription>> desc) {
        try {
            // 获取主题分区信息。
            Map<String, LogDirDescription> stringLogDirDescriptionMap = desc.get();
            stringLogDirDescriptionMap.forEach((path,description)->{
            LogDirDescription logDirDescription = description;
            // 遍历主题分区信息，并更新主题的存储类型和大小。
            Map<TopicPartition, ReplicaInfo> topicPartitionReplicaInfoMap =
                    logDirDescription.replicaInfos();
            topicPartitionReplicaInfoMap.forEach(
                    (topicPartition, replicaInfo) ->
                            updateType(topicSize, topicPartition, replicaInfo));
            });
        } catch (InterruptedException e) {
            log.error("ERROR",e);
        } catch (ExecutionException e) {
            log.error("ERROR",e);
        }
    }

    /**
     * @date 2023/4/7 下午3:25
     * @description 修改bytes为自适配单位
     * @author Jast
     */
    private void updateType(
            Map<String, Long> topicSize, TopicPartition topicPartition, ReplicaInfo replicaInfo) {
        String topic = topicPartition.topic();
        long size = replicaInfo.size();
        updateMap(topicSize, topic, size);
    }

    /**
     * @date 2023/4/7 下午3:25
     * @description 统计工具类
     * @author Jast
     */
    public void updateMap(Map<String, Long> map, String key, Long value) {
        if (map.containsKey(key)) {
            map.put(key, map.get(key) + value);
        } else {
            map.put(key, value);
        }
    }

    @Override
    public Map<TopicPartition, OffsetInfo> getTopicOffsetInfo(String topicName)
             {
                 try {
                     // 获取指定 Topic 的分区信息
                     Map<String, TopicDescription> topicDescriptions =
                             adminClient.describeTopics(Collections.singletonList(topicName)).all().get();
                     List<TopicPartition> partitions =
                             topicDescriptions.get(topicName).partitions().stream()
                                     .map(
                                             topicPartitionInfo ->
                                                     new TopicPartition(
                                                             topicName, topicPartitionInfo.partition()))
                                     .collect(Collectors.toList());

                     // 获取指定 Topic 的最新和最早 Offset 信息
                     ListOffsetsResult listOffsetsResult =
                             adminClient.listOffsets(
                                     partitions.stream()
                                             .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())));
                     Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = listOffsetsResult.all().get(3L, TimeUnit.SECONDS);

                     // 统计最新 Offset 信息
                     Map<TopicPartition, Long> latestOffsetMap = new HashMap<>();
                     for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry :
                             latestOffsets.entrySet()) {
                         latestOffsetMap.put(entry.getKey(), entry.getValue().offset());
                     }

                     listOffsetsResult =
                             adminClient.listOffsets(
                                     partitions.stream()
                                             .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest())));
                     Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
                             listOffsetsResult.all().get();

                     // 统计最早 Offset 信息
                     Map<TopicPartition, Long> earliestOffsetMap = new HashMap<>();
                     for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry :
                             earliestOffsets.entrySet()) {
                         earliestOffsetMap.put(entry.getKey(), entry.getValue().offset());
                     }

                     // 合并最早和最新offset
                     Map<TopicPartition, OffsetInfo> offsetMap = new HashMap<>();
                     for (Map.Entry<TopicPartition, Long> entry : earliestOffsetMap.entrySet()) {
                         TopicPartition tp = entry.getKey();
                         offsetMap.put(tp, new OffsetInfo(entry.getValue(), latestOffsetMap.get(tp)));
                     }

                     return offsetMap;
                 }catch (TimeoutException e ){
                     log.warn("当前Topic:{}存在问题,获取信息超时",topicName);
                 }catch (Exception e ){
                     log.warn("Topic info get failed.",e);
                 }
                 return null;
             }

    public static void main(String[] args) {
         String topicName = "src-tsp-dx-huawei";
        KafkaMonitorClient kafkaAdminClient2 = new KafkaMonitorClient("172.16.24.220:9092");
        AdminClient adminClient = kafkaAdminClient2.adminClient;
        try {
                // 获取指定 Topic 的分区信息
                Map<String, TopicDescription> topicDescriptions =
                        adminClient.describeTopics(Collections.singletonList(topicName)).all().get();
                List<TopicPartition> partitions =
                        topicDescriptions.get(topicName).partitions().stream()
                                .map(
                                        topicPartitionInfo ->
                                                new TopicPartition(
                                                        topicName, topicPartitionInfo.partition()))
                                .collect(Collectors.toList());

                // 获取指定 Topic 的最新和最早 Offset 信息
                ListOffsetsResult listOffsetsResult =
                        adminClient.listOffsets(
                                partitions.stream()
                                        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())));
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = listOffsetsResult.all().get(3L, TimeUnit.SECONDS);

                // 统计最新 Offset 信息
                Map<TopicPartition, Long> latestOffsetMap = new HashMap<>();
                for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry :
                        latestOffsets.entrySet()) {
                    latestOffsetMap.put(entry.getKey(), entry.getValue().offset());
                }

                listOffsetsResult =
                        adminClient.listOffsets(
                                partitions.stream()
                                        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest())));
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
                        listOffsetsResult.all().get();

                // 统计最早 Offset 信息
                Map<TopicPartition, Long> earliestOffsetMap = new HashMap<>();
                for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry :
                        earliestOffsets.entrySet()) {
                    earliestOffsetMap.put(entry.getKey(), entry.getValue().offset());
                }

                // 合并最早和最新offset
                Map<TopicPartition, OffsetInfo> offsetMap = new HashMap<>();
                for (Map.Entry<TopicPartition, Long> entry : earliestOffsetMap.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    offsetMap.put(tp, new OffsetInfo(entry.getValue(), latestOffsetMap.get(tp)));
                }

            }catch (Exception e ){
            log.error("ERROR",e);
            }
    }
}
