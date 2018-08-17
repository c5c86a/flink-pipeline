package mygroupid;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.util.List;
import java.util.ArrayList;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeliveryDelayOperatorIntegrationTest extends AbstractTestBase {
    // create a testing sink
    private static class CollectSink implements SinkFunction<Tuple2<String, Integer>> {
        // must be static
        static final List<Tuple2<String, Integer>> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Tuple2<String, Integer> value, SinkFunction.Context context) {
            values.add(value);
        }
    }

    @Test
    public void test_subtract() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements("2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34")
                .flatMap(new DeliveryDelayOperator())
                .returns(new TypeHint<Tuple2<String, Integer>>(){})
                .addSink(new CollectSink())
        ;
        // execute
        env.execute();

        // verify your results
        Tuple2<String, Integer> expected_element = new Tuple2<>("2018-08-06 19:16:32 Europe/Zurich", 86402000);
        List<Tuple2<String, Integer>> expected = new ArrayList<>();
        expected.add(expected_element);
        assertEquals(CollectSink.values, expected);
    }
    @Test
    public void test_benchmark() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text;

        String[] elements = new String[100000];
        // TODO: text = env.readTextFile();
        for(int i=0; i < 100000; i++){
            elements[i] = "2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34";
        }
        text = env.fromElements(elements);

        DataStream<Tuple2<String, Integer>> dataStream = text
                .flatMap(new DeliveryDelayOperator())
                ;
        dataStream.writeAsCsv("bind_mount/output.csv", FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult compare_timestamps = env.execute("Compare timestamps");

        Logger logger = LoggerFactory.getLogger(StreamingJob.class);
        logger.info("Job with JobID " + compare_timestamps.getJobID() + " has finished.");
        logger.info("Job Runtime: " + compare_timestamps.getNetRuntime() + " ms");

        assertTrue(compare_timestamps.getNetRuntime() < 6000);
    }
}
