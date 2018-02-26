package com.nobodyhub.learn.kafka.definitive.guide.chapter3;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Ryan
 */
public class KafkaProducerTest extends ProducerTestBase {
    private ProducerRecord<String, String> record;

    @Override
    @Before
    public void setup() {
        super.setup();
        this.record = new ProducerRecord<>(TOPIC, "Precision Products", "France");
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
}
