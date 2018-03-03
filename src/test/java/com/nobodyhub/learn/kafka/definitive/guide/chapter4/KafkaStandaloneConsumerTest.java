package com.nobodyhub.learn.kafka.definitive.guide.chapter4;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Ryan
 */
public class KafkaStandaloneConsumerTest extends ConsumerTestBase {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStandaloneConsumerTest.class);

    @Before
    @Override
    public void setup() {
        initializerProps();
        consumer = new KafkaConsumer<>(properties);
    }

    @Test
    public void test() {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("CustomerCountry");
        List<TopicPartition> partitionsToAssign = Lists.newArrayList();
        if (partitionInfos != null) {
            for (PartitionInfo info : partitionInfos) {
                logger.info("topic:{}, partition:{}, leader:{}:{}:{}:{}, replicas:{}, inSyncReplicas:{}, offlineReplicas:{}",
                        info.topic(),
                        info.partition(),
                        info.leader().id(), info.leader().host(), info.leader().port(), info.leader().rack(),
                        info.replicas(),
                        info.inSyncReplicas(),
                        info.offlineReplicas()
                );
                partitionsToAssign.add(new TopicPartition(info.topic(), info.partition()));
            }
            consumer.assign(partitionsToAssign);
        }
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                printLog(logger, record);
            }
            consumer.commitSync();
        }
    }
}
