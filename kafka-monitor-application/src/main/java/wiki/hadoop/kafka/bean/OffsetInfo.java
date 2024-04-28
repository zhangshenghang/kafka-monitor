package wiki.hadoop.kafka.bean;

/**
 * @author Jast
 * @description
 * @date 2023-04-10 13:30
 */
/**
 * OffsetInfo类用于存储最早和最近的偏移量信息
 *
 * @author Jast
 */
public class OffsetInfo {
    /** 最早偏移量 */
    private long earliestOffset;
    /** 最新偏移量 */
    private long latestOffset;

    /**
     * 构造函数，用于初始化最早和最近偏移量
     *
     * @param earliestOffset 最早偏移量
     * @param latestOffset 最新偏移量
     */
    public OffsetInfo(long earliestOffset, long latestOffset) {
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
    }

    /**
     * 获取最早偏移量
     *
     * @return 最早偏移量
     */
    public long getEarliestOffset() {
        return earliestOffset;
    }

    /**
     * 获取最新偏移量
     *
     * @return 最新偏移量
     */
    public long getLatestOffset() {
        return latestOffset;
    }

    /**
     * 将类的信息转换为字符串形式，方便输出和调试
     *
     * @return 包含最早和最新偏移量值的字符串
     */
    @Override
    public String toString() {
        return "OffsetInfo{"
                + "earliestOffset="
                + earliestOffset
                + ", latestOffset="
                + latestOffset
                + '}';
    }
}
