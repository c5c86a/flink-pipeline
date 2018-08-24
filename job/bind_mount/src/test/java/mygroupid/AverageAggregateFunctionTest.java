package mygroupid;

import io.flinkspector.core.collection.ExpectedRecords;
import io.flinkspector.datastream.DataStreamTestBase;
import io.flinkspector.datastream.input.EventTimeInput;
import io.flinkspector.datastream.input.EventTimeInputBuilder;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class AverageAggregateFunctionTest extends DataStreamTestBase {
    /**
     * This is a failing test: No operators defined in streaming topology. Cannot execute.
     * @throws Exception
     *
    @Test
    public void test_avg_window() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // given
        CommonPOJO a = new CommonPOJO();
        a.input = "2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34";
        a.deliveryDelay = 10000000;
        CommonPOJO b = new CommonPOJO();
        b.input = "2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34";
        b.deliveryDelay = 10000001;
        CommonPOJO c = new CommonPOJO();
        c.input = "2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34";
        c.deliveryDelay = 10000002;
        CommonPOJO d = new CommonPOJO();
        d.input = "2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34";
        d.deliveryDelay = 110000003;
        List<CommonPOJO> in = new ArrayList<CommonPOJO>();
        in.add(a);
        in.add(b);
        in.add(c);
        in.add(d);
        // when
        DataStreamSink<CommonPOJO> result = env.fromCollection(in)
                .keyBy("input")
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AverageAggregate())
                .flatMap(new ThresholdFlatmap())
                .addSink(new CommonSink())
        ;
        env.execute();
        JobExecutionResult job = env.execute("Compare timestamps");
        Logger logger = LoggerFactory.getLogger(StreamingJob.class);
        while(job.getNetRuntime() < 10000) {
            logger.info("Job Runtime: " + job.getNetRuntime() + " ms");
            Thread.sleep(1000);
        }
        // then
        c.isLessThanMovingAvg = true;
        d.isLessThanMovingAvg = false; // stays false
        List<CommonPOJO> integerList = new ArrayList<>();
        integerList.add(c);
        integerList.add(d);

        ExpectedRecords<CommonPOJO> expectedPOJOs = new ExpectedRecords<>();
        expectedPOJOs.expectAll(integerList);
        ExpectedRecords<CommonPOJO> expected = ExpectedRecords
                .create(c)
                .expect(d)
                ;
//        expected.refine().only();
//        assertStream(result, expected);
        assertThat(CommonSink.values.get(0).isLessThanThreshold).isTrue();
    }
    */
}