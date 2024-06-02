package io.github.lucasimi.zkring;

import java.util.Optional;

public interface RingDiscovery {

    void subscribe(String ringId);
    
    void unsubscribe(String ringId);

    Optional<ConsistentCollection> getRing(String ringId);

}
