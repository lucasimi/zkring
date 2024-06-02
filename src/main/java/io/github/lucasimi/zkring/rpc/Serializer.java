package io.github.lucasimi.zkring.rpc;

public interface Serializer<S> {

    public byte[] serialize(S data);
    
}
