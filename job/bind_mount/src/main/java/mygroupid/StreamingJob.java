package mygroupid;

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


public class StreamingJob {
  public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
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
          int seconds = (int) (end - start);
        
          out.collect(new Tuple2<String, Integer>(timestamps[0], seconds));
      }
  }
	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		DataStream<String> text;
    if (params.has("input")) {
      text = env.socketTextStream("producer", 9000, '\n');
		} else {
      String[] elements = new String[100000];
      for(int i=0; i < 100000; i++){
        elements[i] = "2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34";
      }
			text = env.fromElements(elements);
    }
	  /* http://flink.apache.org/docs/latest/apis/streaming/index.html */
    //  .timeWindow(Time.seconds(5))
    DataStream<Tuple2<String, Integer>> dataStream = text
      .flatMap(new Splitter());
    dataStream.writeAsCsv("bind_mount/output.csv", WriteMode.OVERWRITE);
    env.execute("Compare timestamps");
	}
}
