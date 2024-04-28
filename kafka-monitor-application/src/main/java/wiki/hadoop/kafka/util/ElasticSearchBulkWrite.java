package wiki.hadoop.kafka.util;

/**
 * @author Jast
 * @description
 * @date 2023-04-10 13:20
 */
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;

/**
 * ElasticSearchBulkWrite 类提供了一些方法，可以将指定的 JSON 对象作为文档内容，通过 Upsert 的方式更新或插入到 Elasticsearch 中。
 *
 * @author Jast
 */
public class ElasticSearchBulkWrite {

    /**
     * 将指定的 JSON 对象作为文档内容，通过 Upsert 的方式更新或插入到 Elasticsearch 中
     *
     * @param indexName 索引名称
     * @param documentId 文档 ID
     * @param jsonObject JSON 对象，包含待更新或插入的文档内容
     * @param bulkRequest 批量请求对象，用于执行批量写入操作
     */
    public static void upsertDocument(
            String indexName, String documentId, JSONObject jsonObject, BulkRequest bulkRequest) {
        UpdateRequest updateRequest =
                new UpdateRequest(indexName, documentId).doc(jsonObject).upsert(jsonObject);
        bulkRequest.add(updateRequest);
    }
}
