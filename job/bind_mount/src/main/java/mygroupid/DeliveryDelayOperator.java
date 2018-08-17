package mygroupid;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


/**
 * Collects datetime and msec. To use it in a job:
 * <pre>{@code
 * DataStreamSource<String> x = ...
 * x.flatMap(new DeliveryDelayOperator())
 * .returns(new TypeHint<Tuple2<String, Integer>>(){})
 * }</pre>
 */
public class DeliveryDelayOperator implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) {
        String[] timestamps = sentence.split(",");
        DateTimeFormatter column_0_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss VV");
        DateTimeFormatter column_1_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long start = ZonedDateTime.parse(timestamps[0], column_0_format).toInstant().toEpochMilli();
        long end   = LocalDateTime.parse(timestamps[1], column_1_format).atZone(ZoneId.of("Europe/Zurich")).toInstant().toEpochMilli();
        int msec = (int) (end - start);

        out.collect(new Tuple2<>(timestamps[0], msec));
    }
}
