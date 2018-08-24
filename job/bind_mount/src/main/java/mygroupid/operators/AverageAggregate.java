package mygroupid.operators;

import mygroupid.CommonPOJO;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AverageAggregate implements AggregateFunction<
        CommonPOJO,
        AverageAccumulatorHelper,
        CommonPOJO
        > {

    @Override
    public AverageAccumulatorHelper createAccumulator() {
        return new AverageAccumulatorHelper(0L, 0L);
    }
    @Override
    public AverageAccumulatorHelper add(CommonPOJO in, AverageAccumulatorHelper accumulator) {
        return accumulator.add(in);
    }
    @Override
    public CommonPOJO getResult(AverageAccumulatorHelper accumulator){
        return accumulator.getResult();
    }
    @Override
    public AverageAccumulatorHelper merge(AverageAccumulatorHelper a, AverageAccumulatorHelper b) {
        return a.merge(b);
    }
}
