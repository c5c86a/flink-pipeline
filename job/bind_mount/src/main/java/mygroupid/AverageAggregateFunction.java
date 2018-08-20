package mygroupid;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Computes the average of the second field of the elements in the window
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#aggregatefunction
 */
public class AverageAggregateFunction implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Tuple1<Double>> {
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return new Tuple2<>(0L, 0L);
    }
    @Override
    public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
        return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);
    }
    @Override
    public Tuple1<Double> getResult(Tuple2<Long, Long> accumulator){
        return new Tuple1<Double>(new Double(((double) accumulator.f0) / accumulator.f1));
    }
    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}