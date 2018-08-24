package mygroupid;

import mygroupid.io.ESSink;
import mygroupid.io.RedisDBSink;
import mygroupid.operators.DeliveryDelayFlatmap;
import mygroupid.operators.ThresholdFlatmap;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;

import java.util.Random;

import static java.lang.String.format;


public class StreamingJob {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//                .socketTextStream("producer", 9000, '\n')
        String[] elements = new String[100000];
        Random random = new Random();
        for(int i=0; i < 100000; i++){
            int a = random.ints(0,(59+1)).findFirst().getAsInt();
            int b = random.ints(0,(59+1)).findFirst().getAsInt();
            elements[i] = format("2018-08-06 19:16:%02d Europe/Zurich,2018-08-07 19:16:%02d",
                    a,
                    b);
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
