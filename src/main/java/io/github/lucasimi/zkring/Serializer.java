package io.github.lucasimi.zkring;

public interface Serializer<S> {

    public byte[] serialize(S data);
    
}
