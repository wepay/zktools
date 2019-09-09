package com.wepay.zktools.zookeeper.serializer;

import com.wepay.zktools.zookeeper.Serializer;

/**
 * ByteArraySerializer is no-op serializer. It just passes a byte array through.
 */
public class ByteArraySerializer implements Serializer<byte[]> {

    @Override
    public byte[] serialize(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] deserialize(byte[] bytes) {
        return bytes;
    }

}
