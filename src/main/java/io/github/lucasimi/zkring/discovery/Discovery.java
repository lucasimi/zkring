package io.github.lucasimi.zkring.discovery;

import java.util.Optional;
import java.util.function.Supplier;

import io.github.lucasimi.zkring.ring.Ring;

public interface Discovery {

    void subscribe(String ringId, Supplier<Ring> ringSupplier);

    void unsubscribe(String ringId);

    Optional<Ring> getRing(String ringId);

}
