package mygroupid;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class ThresholdOperator implements FlatMapFunction<CommonPOJO, CommonPOJO> {
    @Override
    public void flatMap(CommonPOJO in, Collector<CommonPOJO> out) {
        in.isLessThanThreshold = in.deliveryDelay < 90000000;
        out.collect(in);
    }
}
