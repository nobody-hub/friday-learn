package com.nobodyhub.learn.kafka.definitive.guide.chapter3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;

import java.util.Properties;

/**
 * @author Ryan
 */
public class ProducerTestBase {

    protected Properties producerProps;
    protected Producer producer;
    protected final String TOPIC = "CustomerCountry";

    @Before
    public void setup() {
        initProducerProperties();
        this.producer = new KafkaProducer<String, String>(this.producerProps);
    }

    protected void initProducerProperties() {
        this.producerProps = new Properties();
        this.producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }
}
