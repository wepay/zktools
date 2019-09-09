package com.wepay.zktools.zookeeper.serializer;

import com.wepay.zktools.zookeeper.Serializer;

/**
 * A simple integer serializer
 */
public class IntegerSerializer implements Serializer<Integer> {

    @Override
    public byte[] serialize(Integer value) {
        byte[] bytes = new byte[4];
        int val = value.intValue();

        bytes[0] = (byte) (val >> 24);
        bytes[1] = (byte) (val >> 16);
        bytes[2] = (byte) (val >> 8);
        bytes[3] = (byte) val;

        return bytes;
    }

    @Override
    public Integer deserialize(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

}
