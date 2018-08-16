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
			// TODO: text = env.readTextFile(params.get("input"));
      for(int i=0; i < 100000; i++){
        elements[i] = "2018-08-06 19:16:32 Europe/Zurich,2018-08-07 19:16:34";
      }
			text = env.fromElements(elements);
    }
	  /* http://flink.apache.org/docs/latest/apis/streaming/index.html */
    //  .timeWindow(Time.seconds(5))
    DataStream<Tuple2<String, Integer>> dataStream = text
      .flatMap(new DeliveryDelay());
    dataStream.writeAsCsv("bind_mount/output.csv", WriteMode.OVERWRITE);
    env.execute("Compare timestamps");
	}
}
