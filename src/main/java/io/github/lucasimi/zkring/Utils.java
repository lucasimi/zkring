package io.github.lucasimi.zkring;

import java.io.*;

import org.apache.commons.codec.digest.MurmurHash3;
public class Utils {

    private Utils() {}

    public static <V extends Serializable> int hash(V obj) {
        byte[] arr = serialize(obj);
        return MurmurHash3.hash32x86(arr);
    }

    public static <V extends Serializable> V deserialize(byte[] arr, Class<V> clazz) {
        ByteArrayInputStream bis = new ByteArrayInputStream(arr);
        try {
            ObjectInput in = new ObjectInputStream(bis);
            return clazz.cast(in.readObject());
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <V extends Serializable> byte[] serialize(V obj) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            os.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}