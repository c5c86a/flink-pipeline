package mygroupid;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Used only for testing:
 * Values are collected in a static variable
 * <pre>{@code
 * CommonSink.values.clear();
 * }</pre>
 *
 * then
 *
 * <pre>{@code
 * env.execute()
 * }</pre>
 *
 * then use assertJ
 */
public class CommonSink implements SinkFunction<CommonPOJO> {
    // must be static
    public static final List<CommonPOJO> values = new ArrayList<>();

    @Override
    public synchronized void invoke(CommonPOJO value, Context context) {
        values.add(value);
    }
}
