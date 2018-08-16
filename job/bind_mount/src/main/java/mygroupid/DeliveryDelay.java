package mygroupid;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.utils.ParameterTool;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZonedDateTime;
import java.time.Duration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import java.time.ZoneId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * collects datetime and msec as Tuple2<String, Integer>
 */
public class DeliveryDelay implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] timestamps = sentence.split(",");
        DateTimeFormatter column_0_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss VV");
        DateTimeFormatter column_1_format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        long start = ZonedDateTime.parse(timestamps[0], column_0_format).toInstant().toEpochMilli();
        long end   = LocalDateTime.parse(timestamps[1], column_1_format).atZone(ZoneId.of("Europe/Zurich")).toInstant().toEpochMilli();
        /* to debug: Logger logger = LoggerFactory.getLogger(StreamingJob.class);
        logger.info("start = %d" + String.valueOf(start));
        logger.info("end = %d" + String.valueOf(end));
        */
        int msec = (int) (end - start);
      
        out.collect(new Tuple2<String, Integer>(timestamps[0], msec));
    }
}
