package io.github.lucasimi.zkring.rpc;

import java.util.function.Function;

public interface Service<S, T> extends Deserializer<S>, Serializer<T>, Function<S, T> {
    
} 
