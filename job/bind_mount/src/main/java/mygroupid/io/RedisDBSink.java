package mygroupid.io;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisDBSink extends RedisSink<Tuple2<String, String>> {
    public RedisDBSink() {
        super(
                new FlinkJedisPoolConfig.Builder().setHost("redis").build(),
                new RedisMapperHelper()
        );
    }
}
