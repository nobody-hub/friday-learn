package com.nobodyhub.learn.kafka.definitive.guide.chapter3;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author Ryan
 */
public class AvroSerializerTest extends ProducerTestBase {


    @Before
    @Override
    public void setup() {
        super.setup();
        this.producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        this.producer = new KafkaProducer<String, GenericRecord>(producerProps);
    }

    @Test
    public void test1() throws IOException, ExecutionException, InterruptedException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getSchema());
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        for (int nCustomer = 0; nCustomer < 3; nCustomer++) {
            String name = "exampleCustomer" + nCustomer;
            String email = "example" + nCustomer + "@example.com";
            GenericRecord customer = new GenericData.Record(schema);
            customer.put("id", nCustomer);
            customer.put("name", name);
            customer.put("email", email);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            writer.write(customer, encoder);
            encoder.flush();
            ProducerRecord<String, byte[]> data = new ProducerRecord<>(TOPIC, name, out.toByteArray());
            producer.send(data).get();
        }
    }

    private String getSchema() {
        return "{\n" +
                "  \"namespace\": \"customerManager.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Customer\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"type\": \"int\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"email\",\n" +
                "      \"type\": [\n" +
                "        \"null\",\n" +
                "        \"string\"\n" +
                "      ],\n" +
                "      \"default\": \"null\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
    }
}