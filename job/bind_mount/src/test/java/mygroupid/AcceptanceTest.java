package mygroupid;

import io.flinkspector.datastream.DataStreamTestBase;
import mygroupid.categories.*;
import mygroupid.operators.CommonPOJOMap;
import mygroupid.operators.ThresholdFlatmap;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.contentOf;

public class AcceptanceTest extends DataStreamTestBase {
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
    @Category({Functionality.class, Positive.class})
    public void test_positive_delay_within_short_threshold() throws Exception {
        runJob2SetCommonSink("1, 5");
        // verify your results
        assertThat(CommonSink.values.size()).isEqualTo(1);
        assertThat(CommonSink.values.get(0).isLessThanThreshold)
                .as(CommonSink.values.get(0).toString())
                .isTrue();
    }
    @Test
    @Category({Functionality.class, Positive.class})
    public void test_positive_delay_within_only_the_long_threshold() throws Exception {
        runJob2SetCommonSink("2, 15");
        // verify your results
        assertThat(CommonSink.values.size()).isEqualTo(1);
        assertThat(CommonSink.values.get(0).isLessThanThreshold)
                .as(CommonSink.values.get(0).toString())
                .isTrue();
    }
    @Test
    @Category({Functionality.class, Negative.class})
    public void test_negative_delay_more_than_short_threshold() throws Exception {
        runJob2SetCommonSink("1, 15");
        // verify your results
        assertThat(CommonSink.values.size()).isEqualTo(1);
        assertThat(CommonSink.values.get(0).isLessThanThreshold)
                .as(CommonSink.values.get(0).toString())
                .isFalse();
    }
    @Test
    @Category({Functionality.class, Negative.class})
    public void test_negative_delay__more_than_long_threshold() throws Exception {
        runJob2SetCommonSink("2, 105");
        // verify your results
        assertThat(CommonSink.values.size()).isEqualTo(1);
        assertThat(CommonSink.values.get(0).isLessThanThreshold)
                .as(CommonSink.values.get(0).toString())
                .isFalse();
    }
    @Test
    @Category({Environment.class, Positive.class})
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
    @Test
    @Category({Data.class, Positive.class})
    public void test_input_data() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String cwd = System.getProperty("user.dir");
        env.readCsvFile(cwd + "/src/main/resources/from-elasticsearch.csv")
                .types(String.class, Integer.class)
                .map(t -> "1, " + t.f1)
                .map(new CommonPOJOMap())
                .returns(new TypeHint<CommonPOJO>(){})
                .flatMap(new ThresholdFlatmap())
                .writeAsText(
                        cwd + "/dataset-output-folder",
                        FileSystem.WriteMode.OVERWRITE)
        ;
        String myDirectoryPath = cwd + "/dataset-output-folder/";
        env.execute("Compare timestamps");
        Thread.sleep(4000);

        File dir = new File(myDirectoryPath);
        File[] directoryListing = dir.listFiles();
        assertThat(directoryListing)
                .as("the job has created an output directory")
                .isNotNull();
        dir.deleteOnExit();

        List<String> output = new ArrayList<>();
        for (File child : directoryListing) {
            try (Scanner scanner = new Scanner(child)) {
                while (scanner.hasNext()) {
                    output.add(scanner.nextLine());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        assertThat(output)
                .contains("(1,6,true,false)")
                .contains("(1,86404,false,false)")
                .contains("(1,999999,false,false)")
        ;
    }
}