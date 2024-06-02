package io.github.lucasimi.zkring.consistency;

import java.util.Collection;
import java.util.List;

import io.github.lucasimi.zkring.Node;

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
