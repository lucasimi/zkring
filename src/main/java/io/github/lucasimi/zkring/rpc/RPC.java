package io.github.lucasimi.zkring.rpc;

import java.util.Optional;

public interface RPC {

    public <S, T> Optional<T> get(String ringId, S request, SerDes<S, T> serdes);
    
} 
