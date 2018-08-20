package mygroupid;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;


import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.flinkspector.core.quantify.MatchTuples;
import io.flinkspector.core.quantify.OutputMatcher;
import io.flinkspector.datastream.DataStreamTestBase;
import io.flinkspector.datastream.input.time.InWindow;
import org.apache.flink.streaming.api.windowing.time.Time;

import static org.hamcrest.Matchers.*;

public class DeliveryDelayOperatorIntegrationTest extends DataStreamTestBase {
    // create a testing sink
    private static class DeliveryDelaysSink implements SinkFunction<CommonPOJO> {
        // must be static
        static final List<CommonPOJO> values = new ArrayList<>();

        @Override
        public synchronized void invoke(CommonPOJO value, SinkFunction.Context context) {
            values.add(value);
        }
    }
    private static class ThresholdSink implements SinkFunction<CommonPOJO> {
        // must be static
        static final List<CommonPOJO> values = new ArrayList<>();

        @Override
        public synchronized void invoke(CommonPOJO value, SinkFunction.Context context) {
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
                .returns(new TypeHint<CommonPOJO>(){})
                .addSink(new DeliveryDelaysSink())
        ;
        // execute
        env.execute();

        // verify your results
        assertThat(DeliveryDelaysSink.values.get(0).deliveryDelay).isEqualTo(86402000);
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
                .returns(new TypeHint<CommonPOJO>(){})
                .flatMap(new ThresholdOperator())
                .addSink(new ThresholdSink())
        ;
        // execute
        env.execute();

        // verify your results
        assertThat(ThresholdSink.values.get(0).isLessThanThreshold).isTrue();
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

        DataStream<CommonPOJO> dataStream = text
                .flatMap(new DeliveryDelayOperator())
                .returns(new TypeHint<CommonPOJO>(){})
                .flatMap(new ThresholdOperator())
                ;
        dataStream.writeAsText("output.txt", FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult compare_timestamps = env.execute("Compare timestamps");

        Logger logger = LoggerFactory.getLogger(StreamingJob.class);
        logger.info("Job with JobID " + compare_timestamps.getJobID() + " has finished.");
        logger.info("Job Runtime: " + compare_timestamps.getNetRuntime() + " ms");

        assertThat(compare_timestamps.getNetRuntime()).isLessThan(10000);
    }
//    @Test
//    public void test_avg_window() {
//        setParallelism(2);
//
//        /* The before and after keywords define the time span !between! the previous record and the current record.
//         */
//        DataStream<Tuple2<String, Long>> inputStream = createTimedTestStreamWith(Tuple2.of("fritz", 1L))
//                        .emit(Tuple2.of("2", 2L))
//                        //it's possible to generate unsorted input
//                        .emit(Tuple2.of("3", 3L))
//                        //emit the tuple multiple times, with the time span between:
//                        .emit(Tuple2.of("4", 4L), InWindow.to(20, seconds), times(2))
//                        .close();
//        /*
//         * Creates an OutputMatcher using MatchTuples.
//         * MatchTuples builds an OutputMatcher working on Tuples.
//         * You assign String identifiers to your Tuple,
//         * and add hamcrest matchers testing the values.
//         */
//        OutputMatcher<Tuple1<Double>> matcher =
//                //name the values in your tuple with keys:
//                new MatchTuples<Tuple1<Double>>("value", "name")
//                        //add an assertion using a value and hamcrest matchers
//                        .assertThat("value", greaterThan(2))
//                        .assertThat("name", either(is("fritz")).or(is("peter")))
//                        //express how many matchers must return true for your test to pass:
//                        .anyOfThem()
//                        //define how many records need to fulfill the
//                        .onEachRecord();
//        DataStream<Tuple1<Double>> stream = inputStream
//                .keyBy(0)
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .aggregate(new AverageAggregateFunction());
//        assertStream(stream, matcher);
//    }
}
