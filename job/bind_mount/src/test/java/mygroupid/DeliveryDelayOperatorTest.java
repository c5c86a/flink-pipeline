package mygroupid;

import io.flinkspector.datastream.DataStreamTestBase;
import mygroupid.operators.DeliveryDelayFlatmap;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DeliveryDelayOperatorTest extends DataStreamTestBase {
    @Test
    public void test_subtract() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // values are collected in a static variable
        CommonSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements("2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34")
                .flatMap(new DeliveryDelayFlatmap())
                .returns(new TypeHint<CommonPOJO>(){})
                .addSink(new CommonSink())
        ;
        // execute
        env.execute();

        // verify your results
        assertThat(CommonSink.values.get(0).deliveryDelay).isEqualTo(86402000);
    }
}