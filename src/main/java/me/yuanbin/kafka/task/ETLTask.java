package me.yuanbin.kafka.task;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public interface ETLTask<K, V, KR, VR> {
    /**
     * ETL core business logic
     * @param source input KStream source
     * @return another expected KStream
     */
    KStream<KR, VR> transform(KStream<K, V> source);

    /**
     * stream task builder
     * @param builder streams instance created by StreamTasksFactory
     */
    void build(StreamsBuilder builder);
}
