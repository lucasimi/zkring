package io.github.lucasimi.zkring.discovery;

import java.util.Optional;

import io.github.lucasimi.zkring.consistency.ConsistentCollection;

public interface RingDiscovery {

    void subscribe(String ringId);
    
    void unsubscribe(String ringId);

    Optional<ConsistentCollection> getRing(String ringId);

}
