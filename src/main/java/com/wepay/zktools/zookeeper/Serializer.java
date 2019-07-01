package com.wepay.zktools.zookeeper;

/**
 * A serializer interface
 */
public interface Serializer<T> {

    byte[] serialize(T value);

    T deserialize(byte[] bytes);

}
