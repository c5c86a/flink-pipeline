package mygroupid.operators;

import mygroupid.CommonPOJO;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


public class CommonPOJOMap implements MapFunction<String, CommonPOJO> {
    @Override
    public CommonPOJO map(String in) throws Exception {
        String[] words = in.split(",");
        Integer deliveryDelay = new Integer(words[1].trim());
        CommonPOJO pojo = new CommonPOJO(words[0], deliveryDelay);
        return pojo;
    }
}
