package io.github.lucasimi.zkring;

public interface Deserializer<S> {

    public S deserialize(byte[] data);
    
}
