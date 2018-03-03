package com.nobodyhub.learn.kafka.definitive.guide.chapter4;

import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Ryan
 */
public class KafkaConsumerOffsetTest extends ConsumerTestBase {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerOffsetTest.class);

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = Maps.newHashMap();

    @Test
    public void test() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    printLog(logger, record);
                    consumer.seek(new TopicPartition(record.topic(), record.partition()), 0);
                    break;
                }
            }
        } catch (Exception e) {
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
