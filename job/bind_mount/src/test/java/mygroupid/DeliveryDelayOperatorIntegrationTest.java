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

import io.flinkspector.core.quantify.MatchTuples;
import io.flinkspector.core.quantify.OutputMatcher;
import io.flinkspector.datastream.DataStreamTestBase;
import io.flinkspector.datastream.input.time.InWindow;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import static org.hamcrest.Matchers.*;

public class DeliveryDelayOperatorIntegrationTest extends DataStreamTestBase {
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

    /**
     * ThresholdWindowOperator: x < (sum/total)
     * https://ci.apache.org/projects/flink/flink-docs-release-1.6/quickstart/setup_quickstart.html#read-the-code
     * https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/stream/operators/
     * https://ci.apache.org/projects/flink/flink-docs-release-1.6/api/java/org/apache/flink/api/common/functions/AggregateFunction.html
     * https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/udfs.html#aggregation-functions
     * @throws Exception from StreamExecutionEnvironment
     */
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
    public static DataStream<Tuple2<Integer, String>> window(DataStream<Tuple2<Integer, String>> stream) {
        return stream.timeWindowAll(Time.of(20, seconds)).sum(0);
    }
    @Test
    public void testWindowing() {
        setParallelism(2);

        /*
         * Define the input DataStream:
         * Get a EventTimeSourceBuilder with, .createTimedTestStreamWith(record).
         * Add data records to it and retrieve a DataStreamSource
         * by calling .close().
         *
         * Note: The before and after keywords define the time span !between! the previous
         * record and the current record.
         */
        DataStream<Tuple2<Integer, String>> testStream =
                createTimedTestStreamWith(Tuple2.of(1, "fritz"))
                        .emit(Tuple2.of(2, "fritz"))
                        //it's possible to generate unsorted input
                        .emit(Tuple2.of(2, "fritz"))
                        //emit the tuple multiple times, with the time span between:
                        .emit(Tuple2.of(1, "peter"), InWindow.to(20, seconds), times(2))
                        .close();

        /*
         * Creates an OutputMatcher using MatchTuples.
         * MatchTuples builds an OutputMatcher working on Tuples.
         * You assign String identifiers to your Tuple,
         * and add hamcrest matchers testing the values.
         */
        OutputMatcher<Tuple2<Integer, String>> matcher =
                //name the values in your tuple with keys:
                new MatchTuples<Tuple2<Integer, String>>("value", "name")
                        //add an assertion using a value and hamcrest matchers
                        .assertThat("value", greaterThan(2))
                        .assertThat("name", either(is("fritz")).or(is("peter")))
                        //express how many matchers must return true for your test to pass:
                        .anyOfThem()
                        //define how many records need to fulfill the
                        .onEachRecord();

        /*
         * Use assertStream to map DataStream to an OutputMatcher.
         * You're also able to combine OutputMatchers with any
         * OutputMatcher. E.g:
         * assertStream(swap(stream), and(matcher, outputWithSize(greaterThan(4))
         * would additionally assert that the number of produced records is exactly 3.
         */
        assertStream(window(testStream), matcher);
    }
}
