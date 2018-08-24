package mygroupid.io;

import mygroupid.CommonPOJO;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

public class ESSinkFunctionHelper implements ElasticsearchSinkFunction<CommonPOJO> {
    public IndexRequest createIndexRequest(CommonPOJO element) {
        Map<String, String> json = new HashMap<>();
        json.put("data", element.input);

        return Requests.indexRequest()
                .index("document")
                .type("string")
                .source(json);
    }
    @Override
    public void process(CommonPOJO element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }
}
