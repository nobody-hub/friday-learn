package com.nobodyhub.learn.kafka.definitive.guide.chapter4;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.slf4j.Logger;

import java.util.Properties;

/**
 * @author Ryan
 */
public abstract class ConsumerTestBase {
    protected Properties properties;
    protected Consumer<String, String> consumer;

    @Before
    public void setup() {
        initializerProps();
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Lists.newArrayList("CustomerCountry"));
    }

    protected void initializerProps() {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerTest" + System.currentTimeMillis());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60 * 1000);
    }

    protected void printLog(Logger logger, ConsumerRecord<String, String> record) {
        logger.info("topics = {}, partition = {}, offset = {}, customer = {}, country = {}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }

}
