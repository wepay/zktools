package com.wepay.zktools.zookeeper.serializer;

import com.wepay.zktools.zookeeper.Serializer;

import java.nio.charset.StandardCharsets;

/**
 * A simple string serializer
 */
public class StringSerializer implements Serializer<String> {

    @Override
    public byte[] serialize(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
