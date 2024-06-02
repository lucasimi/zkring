package io.github.lucasimi.zkring;

import java.util.Collection;
import java.util.List;

public interface ConsistentCollection {
    
    void add(Node node);

    void addAll(Collection<Node> nodes);

    void remove(Node node);

    void removeAll(Collection<Node> nodes);

    void clear();

    <T> Node get(T key);

    <T> List<Node> get(T key, int rep);

    int size();
    
}
