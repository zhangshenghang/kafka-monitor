package wiki.hadoop.kafka.bean;

/**
 * 该类用于表示消费者分区的偏移量信息
 *
 * @author Jast
 * @description
 * @date 2023-04-10 13:30
 */
public class ConsumerOffsetInfo {

    // 分区号
    private Integer partition;

    // 偏移量
    private Long offset;

    /**
     * 构造方法
     *
     * @param partition 分区号
     * @param offset 偏移量
     */
    public ConsumerOffsetInfo(Integer partition, Long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * 获取分区号
     *
     * @return 分区号
     */
    public Integer getPartition() {
        return partition;
    }

    /**
     * 设置分区号
     *
     * @param partition 分区号
     */
    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    /**
     * 获取偏移量
     *
     * @return 偏移量
     */
    public Long getOffset() {
        return offset;
    }

    /**
     * 设置偏移量
     *
     * @param offset 偏移量
     */
    public void setOffset(Long offset) {
        this.offset = offset;
    }
}
