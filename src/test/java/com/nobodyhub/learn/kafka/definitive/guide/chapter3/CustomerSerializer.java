package com.nobodyhub.learn.kafka.definitive.guide.chapter3;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author Ryan
 */
public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public byte[] serialize(String topic, Customer data) {
        byte[] serializedName;
        int stringSize;
        if (data == null) {
            return null;
        }
        try {
            if (data.getCustomerName() != null) {
                serializedName = data.getCustomerName().getBytes("UTF-8");
                stringSize = serializedName.length;
            } else {
                serializedName = new byte[0];
                stringSize = 0;
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.getCustomerId());
            buffer.putInt(stringSize);
            buffer.put(serializedName);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new SerializationException("Error when serializing Customer to byte[]" + e);
        }
    }

    @Override
    public void close() {
        // nothing to close
    }
}
