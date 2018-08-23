package mygroupid;

import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageAggregateFunction implements AggregateFunction<
        CommonPOJO,
        AverageAccumulator,
        CommonPOJO
        > {

    @Override
    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator(0L, 0L);
    }
    @Override
    public AverageAccumulator add(CommonPOJO in, AverageAccumulator accumulator) {
        return accumulator.add(in);
    }
    @Override
    public CommonPOJO getResult(AverageAccumulator accumulator){
        return accumulator.getResult();
    }
    @Override
    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
        return a.merge(b);
    }
}
