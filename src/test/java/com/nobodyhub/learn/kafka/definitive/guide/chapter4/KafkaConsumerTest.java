package com.nobodyhub.learn.kafka.definitive.guide.chapter4;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Ryan
 */
public class KafkaConsumerTest extends ConsumerTestBase {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerTest.class);

    @Test
    public void test() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    printLog(logger, record);
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
