package mygroupid;

import mygroupid.io.ESSink;
import mygroupid.io.RedisDBSink;
import mygroupid.operators.DeliveryDelayFlatmap;
import mygroupid.operators.ThresholdFlatmap;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;


public class StreamingJob {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//                .socketTextStream("producer", 9000, '\n')
        String[] elements = new String[100000];
        for(int i=0; i < 100000; i++){
            elements[i] = "2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34";
        }
        env
                .fromElements(elements)
                .flatMap(new DeliveryDelayFlatmap())
                .returns(new TypeHint<CommonPOJO>(){})
                .flatMap(new ThresholdFlatmap())
                .map(t -> new Tuple2<String, String>(t.input, t.isLessThanThreshold?"yes":"no"))
                .addSink(new RedisDBSink())
                ;
        env.execute("Send high delays to Elasticsearch");
    }
}
