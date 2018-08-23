package mygroupid;

import mygroupid.CommonPOJO;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class CommonSink implements SinkFunction<CommonPOJO> {
    // must be static
    public static final List<CommonPOJO> values = new ArrayList<>();

    @Override
    public synchronized void invoke(CommonPOJO value, Context context) {
        values.add(value);
    }
}
