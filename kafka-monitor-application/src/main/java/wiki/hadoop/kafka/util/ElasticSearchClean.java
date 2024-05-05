package wiki.hadoop.kafka.util;


import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.Method;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Slf4j
public class ElasticSearchClean implements Runnable{


    private String ip;
    private int port;
    private String topic;
    private String username;
    private String password;
    private int daysAgo;

    public ElasticSearchClean(String ip, int port, String topic, String username, String password, int daysAgo) {
        this.ip = ip;
        this.port = port;
        this.topic = topic;
        this.username = username;
        this.password = password;
        this.daysAgo = daysAgo;
    }

    private static void extracted(String ip, int port, String topic, String username, String password, int daysAgo) {
        try {
            String topicUrl = "http://" + ip + ":" + port + "/"+ topic +"/_delete_by_query?wait_for_completion=false";
            String response = sendDeleteRequest(topicUrl, username, password, daysAgo);
            log.info("删除历史数据请求API结果:{}",response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送删除请求到Kafka topic
     * @param topicUrl     Kafka topic的URL
     * @param username     Kafka的认证用户名
     * @param password     Kafka的认证密码
     * @param daysAgo      要删除的天数前的数据
     * @return 服务器响应内容
     * @throws Exception 网络或服务器错误
     */
    public static String sendDeleteRequest(String topicUrl, String username, String password, int daysAgo) throws Exception {
        // 计算指定天数前的日期字符串
        String dateBefore = getDateBefore(daysAgo);
        // 构建删除查询的JSON body
        String requestBody = getDeleteByQueryBody(dateBefore);
        log.info("删除历史数据请求API参数:{}",requestBody);
        // 创建HttpRequest对象
        HttpRequest request = HttpRequest.post(topicUrl)
                .header("Content-Type", "application/json")
                .basicAuth(username, password);

        // 设置请求体
        request.body(requestBody);

        // 发送请求
        HttpResponse response = request.execute();

        // 检查响应状态码
        if (response.getStatus() != 200) {
            log.error("Error response from server: " + response.getStatus());
        }

        // 返回响应内容
        return response.body();
    }

    /**
     * 获取指定天数前的日期字符串
     * @param daysAgo 天数
     * @return 日期字符串
     */
    private static String getDateBefore(int daysAgo) {
        // 创建日期对象，设置为指定天数前的日期
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -daysAgo);
        Date dateBefore = calendar.getTime();

        // 格式化日期为字符串
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dateBefore);
    }

    /**
     * 获取删除查询的JSON body
     * @param dateBefore 指定日期前的日期字符串
     * @return JSON字符串
     */
    private static String getDeleteByQueryBody(String dateBefore) {
        // 构建查询条件和排序
        String query = "{\"range\": {\"insert_time\": {\"lt\": \"" + dateBefore + "\"}}},\"sort\": [{\"insert_time\": {\"order\": \"desc\"}}]}";
        return "{\"query\": " + query + "}";
    }

    @Override
    public void run() {
        extracted(ip, port, topic, username, password, daysAgo);
    }
}

