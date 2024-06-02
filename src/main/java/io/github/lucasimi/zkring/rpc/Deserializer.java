package io.github.lucasimi.zkring.rpc;

public interface Deserializer<S> {

    public S deserialize(byte[] data);
    
}
