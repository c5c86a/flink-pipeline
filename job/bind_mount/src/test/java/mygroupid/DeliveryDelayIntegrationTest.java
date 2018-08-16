package mygroupid;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.util.List;
import java.util.ArrayList;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.flink.api.java.tuple.Tuple2;

public class DeliveryDelayIntegrationTest extends AbstractTestBase {

    @Test
    public void testparsing() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        CollectSink.values.clear();

			  // TODO: text = env.readTextFile(params.get("input"));
        // create a stream of custom elements and apply transformations
        env.fromElements("2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34")
                .flatMap(new DeliveryDelay())
                .addSink(new CollectSink());

        // execute
        env.execute();

        // verify your results
        Tuple2<String, Integer> expected = new Tuple2<String, Integer>("2018-08-06 19:16:32 Europe/Zurich", 86402000);
        assertEquals(Lists.newArrayList(expected), CollectSink.values);
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Tuple2<String, Integer>> {
        // must be static
        public static final List<Tuple2<String, Integer>> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Tuple2<String, Integer> value) throws Exception {
            values.add(value);
        }
    }
}
