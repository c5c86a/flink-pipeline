package mygroupid;

import io.flinkspector.datastream.DataStreamTestBase;
import mygroupid.operators.CommonPOJOMap;
import mygroupid.operators.ThresholdFlatmap;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class ThresholdOperatorTest extends DataStreamTestBase {
    private void runJob2SetCommonSink(String data) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CommonSink.values.clear();

        env.fromElements(data)
                .map(new CommonPOJOMap())
                .returns(new TypeHint<CommonPOJO>(){})
                .flatMap(new ThresholdFlatmap())
                .addSink(new CommonSink())
        ;
        env.execute("Compare timestamps");
    }
    @Test
    public void test_positive_delay_within_short_threshold() throws Exception {
        runJob2SetCommonSink("1, 5");
        // verify your results
        assertThat(CommonSink.values.size()).isEqualTo(1);
        assertThat(CommonSink.values.get(0).isLessThanThreshold)
                .as(CommonSink.values.get(0).toString())
                .isTrue();
    }
    @Test
    public void test_positive_delay_within_only_the_long_threshold() throws Exception {
        runJob2SetCommonSink("2, 15");
        // verify your results
        assertThat(CommonSink.values.size()).isEqualTo(1);
        assertThat(CommonSink.values.get(0).isLessThanThreshold)
                .as(CommonSink.values.get(0).toString())
                .isTrue();
    }
    @Test
    public void test_negative_delay_more_than_short_threshold() throws Exception {
        runJob2SetCommonSink("1, 15");
        // verify your results
        assertThat(CommonSink.values.size()).isEqualTo(1);
        assertThat(CommonSink.values.get(0).isLessThanThreshold)
                .as(CommonSink.values.get(0).toString())
                .isFalse();
    }
    @Test
    public void test_negative_delay__more_than_long_threshold() throws Exception {
        runJob2SetCommonSink("2, 105");
        // verify your results
        assertThat(CommonSink.values.size()).isEqualTo(1);
        assertThat(CommonSink.values.get(0).isLessThanThreshold)
                .as(CommonSink.values.get(0).toString())
                .isFalse();
    }
    @Test
    public void test_benchmark() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text;

        String[] elements = new String[100000];
        for(int i=0; i < 100000; i++){
            elements[i] = "1, " + i;
        }
        text = env.fromElements(elements);

        DataStream<CommonPOJO> dataStream = text
                .map(new CommonPOJOMap())
                .returns(new TypeHint<CommonPOJO>(){})
                .flatMap(new ThresholdFlatmap())
                ;
        dataStream.writeAsText("output.txt", FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult compare_timestamps = env.execute("Compare timestamps");

        Logger logger = LoggerFactory.getLogger(StreamingJob.class);
        logger.info("Job with JobID " + compare_timestamps.getJobID() + " has finished.");
        logger.info("Job Runtime: " + compare_timestamps.getNetRuntime() + " ms");

        assertThat(compare_timestamps.getNetRuntime()).isLessThan(10000);
    }
}