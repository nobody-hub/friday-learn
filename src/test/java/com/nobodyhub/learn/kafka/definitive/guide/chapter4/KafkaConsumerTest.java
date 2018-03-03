package com.nobodyhub.learn.kafka.definitive.guide.chapter4;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @author Ryan
 */
public class KafkaConsumerTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTest.class);
    protected Properties properties;
    protected Consumer<String, String> consumer;

    @Before
    public void setup() {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerTest4");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Lists.newArrayList("CustomerCountry"));
    }

    @Test
    public void test() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topics = {}, partition = {}, offset = {}, customer = {}, country = {}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());
                }
                try {
                    consumer.commitAsync(new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            if (exception != null) {
                                logger.error(exception.getMessage());
                            } else {
                                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                                    TopicPartition tp = entry.getKey();
                                    OffsetAndMetadata meta = entry.getValue();
                                    logger.debug("Callback Debug Info: topic = {}, partition = {}, offset = {}, metadata = ",
                                            tp.topic(), tp.partition(), meta.offset(), meta.metadata());
                                }
                            }
                        }
                    });
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch(Exception e) {
            logger.error(e.getMessage());
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }


}
