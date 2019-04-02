package me.yuanbin.kafka.task;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import me.yuanbin.common.config.AppConfigFactory;
import me.yuanbin.common.config.ConfigUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractTask {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTask.class);

    private static final String SOURCE_TOPICS = "source.topics";
    private static final String SINK_TOPIC = "sink.topic";
    protected static final String WHITELIST = "whitelist";

    protected static final Config appConfig = AppConfigFactory.load();
    protected final Config taskConfig = appConfig.getConfig("task." + this.getClass().getSimpleName());
    protected final List<String> sourceTopics = ConfigUtil.getStringList(taskConfig, SOURCE_TOPICS);
    protected final String sinkTopic = taskConfig.getString(SINK_TOPIC);
    // JSON untyped Serde
    private final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
    protected final Consumed<JsonNode, JsonNode> jsonConsumed = Consumed.with(jsonSerde, jsonSerde);
    protected final Produced<JsonNode, JsonNode> jsonProduced = Produced.with(jsonSerde, jsonSerde);
    protected final Consumed<String, JsonNode> stringJsonConsumed = Consumed.with(Serdes.String(), jsonSerde);
    protected final Produced<String, JsonNode> stringJsonProduced = Produced.with(Serdes.String(), jsonSerde);
    protected final Consumed<JsonNode, byte[]> consumed = Consumed.with(jsonSerde, Serdes.ByteArray());
    protected final Consumed<byte[], byte[]> bytesConsumed = Consumed.with(Serdes.ByteArray(), Serdes.ByteArray());
    protected final Produced<JsonNode, byte[]> produced = Produced.with(jsonSerde, Serdes.ByteArray());
    protected final Produced<byte[], byte[]> bytesProduced = Produced.with(Serdes.ByteArray(), Serdes.ByteArray());

}
