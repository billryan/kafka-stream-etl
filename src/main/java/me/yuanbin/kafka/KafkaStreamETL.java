package me.yuanbin.kafka;

import me.yuanbin.kafka.task.impl.SqlBatchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamETL {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamETL.class);

    public static void main(final String[] args) throws Exception {

        StreamsTaskFactory.load(new SqlBatchTask());

        StreamsTaskFactory.start();
    }
}
