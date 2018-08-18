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

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;


import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeliveryDelayOperatorIntegrationTest extends AbstractTestBase {
    // create a testing sink
    private static class DeliveryDelaysSink implements SinkFunction<Tuple2<String, Integer>> {
        // must be static
        static final List<Tuple2<String, Integer>> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Tuple2<String, Integer> value, SinkFunction.Context context) {
            values.add(value);
        }
    }
    private static class ThresholdSink implements SinkFunction<Tuple2<Tuple2<String, Integer>, Boolean>> {
        // must be static
        static final List<Tuple2<Tuple2<String, Integer>, Boolean>> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Tuple2<Tuple2<String, Integer>, Boolean> value, SinkFunction.Context context) {
            values.add(value);
        }
    }

    @Test
    public void test_subtract() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        DeliveryDelaysSink.values.clear();
        ThresholdSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements("2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34")
                .flatMap(new DeliveryDelayOperator())
                .returns(new TypeHint<Tuple2<String, Integer>>(){})
                .addSink(new DeliveryDelaysSink())
        ;
        // execute
        env.execute();

        // verify your results
        assertThat(DeliveryDelaysSink.values.get(0).f1).isEqualTo(86402000);
    }
    @Test
    public void test_threshold() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        ThresholdSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements("2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34")
                .flatMap(new DeliveryDelayOperator())
                .returns(new TypeHint<Tuple2<String, Integer>>(){})
                .flatMap(new ThresholdOperator())
                .addSink(new ThresholdSink())
        ;
        // execute
        env.execute();

        // verify your results
        assertThat(ThresholdSink.values.get(0).f1).isTrue();
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

        DataStream<Tuple2<Tuple2<String, Integer>, Boolean>> dataStream = text
                .flatMap(new DeliveryDelayOperator())
                .returns(new TypeHint<Tuple2<String, Integer>>(){})
                .flatMap(new ThresholdOperator())
                ;
        dataStream.writeAsCsv("output.csv", FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult compare_timestamps = env.execute("Compare timestamps");

        Logger logger = LoggerFactory.getLogger(StreamingJob.class);
        logger.info("Job with JobID " + compare_timestamps.getJobID() + " has finished.");
        logger.info("Job Runtime: " + compare_timestamps.getNetRuntime() + " ms");

        assertThat(compare_timestamps.getNetRuntime()).isLessThan(10000);
    }
}
