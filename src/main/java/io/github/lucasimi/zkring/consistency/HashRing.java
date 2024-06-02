package io.github.lucasimi.zkring.consistency;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.github.lucasimi.zkring.Node;
import io.github.lucasimi.zkring.VNode;

public class HashRing implements ConsistentCollection {

    private final int partitions;

    private final int replication;

    private final Hash hash;

    private final SortedMap<Integer, VNode> vnodes = new TreeMap<>();

    public HashRing(int partitions, int replication, Hash hash) {
        this.partitions = partitions;
        this.replication = replication;
        this.hash = hash;
    }

    @Override
    public void add(Node peer) {
        this.addAll(Collections.singletonList(peer));
    }

    @Override
    public void clear() {
        this.vnodes.clear();
    }

    @Override
    public void remove(Node peer) {
        this.removeAll(Collections.singletonList(peer));
    }

    @Override
    public void addAll(Collection<Node> peers) {
        Map<Integer, VNode> toAdd = getVNodes(peers);
        this.vnodes.putAll(toAdd);
    }

    @Override
    public void removeAll(Collection<Node> peers) {
        Map<Integer, VNode> toRemove = getVNodes(peers);
        toRemove.keySet().forEach(vnodes::remove);
    }

    @Override
    public <S> Node get(S obj) {
        return get(obj, 1).getFirst();
    }

    @Override
    public <S> List<Node> get(S obj, int num) {
        List<Node> peers = new ArrayList<>(num);
        Optional<VNode> vnodeOp = next(obj);
        vnodeOp.map(VNode::node).ifPresent(peers::add);
        for (int i = 0; i < num - 1; i++) {
            vnodeOp = vnodeOp.flatMap(this::next);
            vnodeOp.map(VNode::node).ifPresent(peers::add);
        }
        return peers;
    }

    @Override
    public int size() {
        return this.vnodes.size();
    }

    private <S> int hash(S obj) {
        return hash.hash(obj) % partitions;
    }

    private Map<Integer, VNode> getVNodes(Collection<Node> peers) {
        return peers.stream()
                .flatMap(p -> IntStream.range(0, replication)
                        .mapToObj(i -> new VNode(p, i)))
                .collect(Collectors.toMap(
                        this::hash,
                        Function.identity(),
                        (x, y) -> hash.hash(x) > hash.hash(y) ? x : y));
    }

    private <S> Optional<VNode> next(S obj) {
        int objRingId = hash(obj);
        Map.Entry<Integer, VNode> firstEntry = nextEntryOrFirst(vnodes, objRingId + 1);
        return Optional.ofNullable(firstEntry)
                .map(Map.Entry::getValue);
    }

    private static <K, V> Map.Entry<K, V> nextEntryOrFirst(SortedMap<K, V> map, K from) {
        SortedMap<K, V> tailMap = map.tailMap(from);
        SortedMap<K, V> subMap = tailMap.isEmpty() ? map : tailMap;
        Iterator<Map.Entry<K, V>> iterator = subMap.entrySet().iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

}