package io.github.lucasimi.zkring.discovery;

import java.util.Optional;

import io.github.lucasimi.zkring.ring.Ring;

public interface Discovery {

    void subscribe(String ringId);
    
    void unsubscribe(String ringId);

    Optional<Ring> getRing(String ringId);

}
