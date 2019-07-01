package com.wepay.zktools.zookeeper.serializer;

import com.wepay.zktools.util.Logging;
import com.wepay.zktools.zookeeper.Serializer;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A helper class for defining a serializer using DataInput/DataOutput
 */
public abstract class SerializerHelper<T> implements Serializer<T> {

    private static final Logger logger = Logging.getLogger(SerializerHelper.class);

    public abstract void serialize(T value, DataOutput out) throws IOException;

    public abstract T deserialize(DataInput in) throws IOException;

    @Override
    public byte[] serialize(T value) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        try {
            serialize(value, out);

            out.flush();
            return baos.toByteArray();

        } catch (IOException ex) {
            logger.error("failed to serialize value", ex);
            throw new RuntimeException("serialization failure", ex);
        }
    }

    @Override
    public T deserialize(byte[] bytes) {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));

        try {
            return deserialize(in);

        } catch (IOException ex) {
            logger.error("failed to deserialize value", ex);
            throw new RuntimeException("deserialization failure", ex);
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                // Ignore
            }
        }
    }

}
