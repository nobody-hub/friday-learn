package com.nobodyhub.learn.kafka.definitive.guide.chapter3;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Ryan
 */
public class KafkaProducerTest {
    private Properties producerProps;
    private Producer producer;
    private ProducerRecord<String, String> record;

    @Before
    public void setup() {
        initProducerProperties();
        this.producer = new KafkaProducer<String, String>(this.producerProps);
        this.record = new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
    }

    @Test
    public void testFireAndForget() {
        Future<RecordMetadata> meta = producer.send(record);
//        producer.flush();
    }

    @Test
    public void testSynchronously() {
        try {
            RecordMetadata meta = (RecordMetadata) producer.send(record).get();
//            producer.flush();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private class ProducerCallBack implements Callback {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                exception.printStackTrace();
            }
        }
    }

    @Test
    public void testAsynchronouslys() {
        producer.send(record, new ProducerCallBack());
//        producer.flush();
    }


    private void initProducerProperties() {
        this.producerProps = new Properties();
        this.producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }
}
