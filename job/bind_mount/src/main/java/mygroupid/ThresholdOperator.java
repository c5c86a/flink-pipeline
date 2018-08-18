package mygroupid;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class ThresholdOperator implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<Tuple2<String, Integer>, Boolean>> {
    @Override
    public void flatMap(Tuple2<String, Integer> in, Collector<Tuple2<Tuple2<String, Integer>, Boolean>> out) {
        Boolean isWithin = in.f1 < 90000000;
        out.collect(new Tuple2<>(in, isWithin));
    }
}
