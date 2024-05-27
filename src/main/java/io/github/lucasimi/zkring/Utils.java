package io.github.lucasimi.zkring;

import java.io.*;
import java.util.*;

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

    public static <V extends Serializable> Comparator<V> hashComparator() {
        return (v1, v2) -> Integer.compare(hash(v1), hash(v2));
    }

    public static <S> List<S> merge(List<S> first, List<S> second) {
        List<S> smaller = first;
        List<S> larger = second;
        if (first.size() > second.size()) {
            smaller = second;
            larger = first;
        }
        larger.addAll(smaller);
        return larger;
    }
    public static <S> List<S> mergeImmutable(List<S> first, List<S> second) {
        List<S> toReturn = new ArrayList<>(first.size() + second.size());
        toReturn.addAll(first);
        toReturn.addAll(second);
        return toReturn;
    }

    public static <S> List<S> singletonList(S obj) {
        List<S> l = new LinkedList<>();
        l.add(obj);
        return l;
    }

}