package mygroupid;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.utils.ParameterTool;

public class StreamingJob {
  public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
      @Override
      public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
          for (String word: sentence.split(" ")) {
              out.collect(new Tuple2<String, Integer>(word, 1));
          }
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
			// TODO: for loop
			text = env.fromElements(new String[] {
        "To be, or not to be,--that is the question:--",
        "Whether 'tis nobler in the mind to suffer"
      });
    }
	  /* http://flink.apache.org/docs/latest/apis/streaming/index.html */
    DataStream<Tuple2<String, Integer>> dataStream = text
      .flatMap(new Splitter())
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1);
    dataStream.print();
    env.execute("Window WordCount");
	}
}
