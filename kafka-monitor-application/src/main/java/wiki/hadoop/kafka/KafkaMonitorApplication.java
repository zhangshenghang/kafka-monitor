package wiki.hadoop.kafka;

import lombok.extern.slf4j.Slf4j;
import wiki.hadoop.kafka.util.ElasticSearchClean;
import wiki.hadoop.kafka.util.ParameterTool;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Jast
 * @description
 * @date 2023-04-10 16:39
 */
@Slf4j
public class KafkaMonitorApplication implements Runnable {
    private final String[] processedArgs;

    public KafkaMonitorApplication(String[] args) {
        this.processedArgs = processArgs(args);
    }
    private String[] processArgs(String[] args) {
        ParameterTool.parse(args);
        return new String[]{
                ParameterTool.get("elasticsearch_host"),
                ParameterTool.get("elasticsearch_port"),
                ParameterTool.get("elasticsearch_username"),
                ParameterTool.get("elasticsearch_password"),
                ParameterTool.get("kafka_broker_list")
        };
    }

    @Override
    public void run() {
        log.info("开始统计 ");
        try {
            KafkaMonitorHandler.start(processedArgs);
        }catch (Exception e ){
            log.error("统计过程中发生异常", e);
        }
        log.info("统计完成");
    }

    /**
     * --elasticsearch_host 172.16.24.192 --elasticsearch_port 9200 --elasticsearch_username elastic
     * --elasticsearch_password Avris2222 --kafka_broker_list 172.16.24.194:9092
     *
     * @param args
     */
    public static void main(String[] args) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleAtFixedRate(new KafkaMonitorApplication(args), 0, 1, TimeUnit.MINUTES);
        executor.scheduleAtFixedRate(new ElasticSearchClean(ParameterTool.get("elasticsearch_host"),
                Integer.parseInt(ParameterTool.get("elasticsearch_port")),
                "kafka-topic",
                ParameterTool.get("elasticsearch_username"),
                ParameterTool.get("elasticsearch_password"),7), 0, 1, TimeUnit.DAYS);
        executor.scheduleAtFixedRate(new ElasticSearchClean(ParameterTool.get("elasticsearch_host"),
                Integer.parseInt(ParameterTool.get("elasticsearch_port")),
                "kafka-group",
                ParameterTool.get("elasticsearch_username"),
                ParameterTool.get("elasticsearch_password"),7), 0, 1, TimeUnit.DAYS);

        // 注册JVM关闭钩子，用于优雅地关闭线程池
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            try {
                executor.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                log.error("线程池关闭时发生中断异常", e);
            }
        }));
    }
}
