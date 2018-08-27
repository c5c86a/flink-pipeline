package mygroupid.operators;

import mygroupid.CommonPOJO;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;


public class ThresholdFlatmap implements FlatMapFunction<CommonPOJO, CommonPOJO> {
    @Override
    public void flatMap(CommonPOJO in, Collector<CommonPOJO> out) {
        int a = in.deliveryDelay;
        int b = new Integer(in.schema.get("deliveryDelayThreshold"));
        in.isLessThanThreshold = a < b;
        out.collect(in);
    }
}
