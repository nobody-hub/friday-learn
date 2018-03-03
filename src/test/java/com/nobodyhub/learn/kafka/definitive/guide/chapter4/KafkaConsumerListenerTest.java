package com.nobodyhub.learn.kafka.definitive.guide.chapter4;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


/**
 * @author Ryan
 */
public class KafkaConsumerListenerTest extends ConsumerTestBase {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerListenerTest.class);

    @Before
    @Override
    public void setup() {
        super.setup();
        consumer.subscribe(Lists.newArrayList("CustomerCountry"), new TestRebalanceListener());
    }

    private class TestRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            for (TopicPartition tp : partitions) {
                logger.debug("PartitionRevoked for partition: {}:{}", tp.topic(), tp.partition());
                consumer.commitSync();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition tp : partitions) {
                logger.debug("PartitionsAssigned for partition: {}:{}", tp.topic(), tp.partition());
            }
        }
    }

    @Test
    public void test() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                printLog(logger, record);
            }
        }
    }
}
