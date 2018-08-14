package mygroupid;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.util.List;
import java.util.ArrayList;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class MultiplyByTwoIntegrationTest extends AbstractTestBase {

    @Test
    public void testMultiply() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        CollectSink.values.clear();

			  // TODO: text = env.readTextFile(params.get("input"));
        // create a stream of custom elements and apply transformations
        env.fromElements(1L, 21L, 22L)
                .map(new MultiplyByTwo())
                .addSink(new CollectSink());

        // execute
        env.execute();

        // verify your results
        assertEquals(Lists.newArrayList(2L, 42L, 44L), CollectSink.values);
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Long> {

        // must be static
        public static final List<Long> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Long value) throws Exception {
            values.add(value);
        }
    }
}
