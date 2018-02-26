package com.nobodyhub.learn.kafka.definitive.guide.chapter3;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/**
 * @author Ryan
 */
public class CustomSerializerTest extends ProducerTestBase {
    private ProducerRecord<String, Customer> record;

    @Override
    @Before
    public void setup() {
        super.setup();
        Customer customer = new Customer(1019, "Javis");
        this.record = new ProducerRecord<>(TOPIC,
                "customerMgmt.customSerialize",
                customer);
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        producer.send(record).get();
    }

    @Override
    protected void initProducerProperties() {
        super.initProducerProperties();
        this.producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerSerializer.class);
    }
}
