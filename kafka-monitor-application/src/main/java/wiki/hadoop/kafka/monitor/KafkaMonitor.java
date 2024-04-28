package wiki.hadoop.kafka.monitor;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.common.TopicPartition;
import wiki.hadoop.kafka.bean.OffsetInfo;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * @author Jast
 * @description Kafka监控接口
 * @date 2023-04-07 15:42
 */
public interface KafkaMonitor {

    /**
     * 获取所有 Topic 的磁盘使用情况
     *
     * @return 返回一个 Map，其中 key 为 Topic 名称，value 为磁盘使用情况
     * @throws ExecutionException 如果执行过程中出现异常，则抛出 ExecutionException 异常
     * @throws InterruptedException 如果执行过程中线程被中断，则抛出 InterruptedException 异常
     */
    Map<String, Long> getTopicDisk() throws ExecutionException, InterruptedException;

    /**
     * 获取所有 Topic 的列表
     *
     * @return 返回一个 Set，其中包含所有 Topic 的名称
     * @throws ExecutionException 如果执行过程中出现异常，则抛出 ExecutionException 异常
     * @throws InterruptedException 如果执行过程中线程被中断，则抛出 InterruptedException 异常
     */
    Set<String> topicList() throws ExecutionException, InterruptedException;

    /**
     * 获取所有 Topic 的描述信息
     *
     * @return 返回一个 Map，其中 key 为 Topic 名称，value 为 Topic 的描述信息
     * @throws ExecutionException 如果执行过程中出现异常，则抛出 ExecutionException 异常
     * @throws InterruptedException 如果执行过程中线程被中断，则抛出 InterruptedException 异常
     */
    String describeTopics() throws ExecutionException, InterruptedException;

    /**
     * 获取所有 Consumer Group 的列表
     *
     * @return 返回一个 Collection，其中包含所有 Consumer Group 的列表
     */
    Collection<ConsumerGroupListing> listGroups();

    /**
     * 获取指定 Consumer Group 的消费位置信息
     *
     * @param consumerGroupId 指定 Consumer Group 的 ID
     */
    Map<String, JSONObject> getConsumerGroupPosition(String consumerGroupId);

    /**
     * 获取指定 Topic 的 Offset 信息
     *
     * @param topicName 指定 Topic 的名称
     * @throws ExecutionException 如果执行过程中出现异常，则抛出 ExecutionException 异常
     * @throws InterruptedException 如果执行过程中线程被中断，则抛出 InterruptedException 异常
     */
    Map<TopicPartition, OffsetInfo> getTopicOffsetInfo(String topicName)
            throws ExecutionException, InterruptedException, TimeoutException;

    void close();
}
