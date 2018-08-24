package mygroupid.io;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * bulk.flush.max.actions instructs the sink to emit after every element, otherwise they would be buffered
 */
public class ESSink extends ElasticsearchSink {
    public ESSink(){
        super(
                new HashMap<String, String>(){{
                    put("cluster.name", "elasticsearch");
                    put("bulk.flush.max.actions", "1");
                }},
                new ArrayList<TransportAddress>(){{
                    add(new InetSocketTransportAddress("elasticsearch", 9300));
                }},
                new ESSinkFunctionHelper()
        );
    }
}
