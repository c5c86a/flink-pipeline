package mygroupid;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class StreamingJob {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> text;
        // TODO: kafka IO
        text = env.socketTextStream("kafka-producer", 9000, '\n');

        //  TODO: .timeWindow(Time.seconds(5))
        DataStream<CommonPOJO> dataStream = text
                .flatMap(new DeliveryDelayOperator());
        env.execute("Compare timestamps");
    }
}
