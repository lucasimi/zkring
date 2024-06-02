package io.github.lucasimi.zkring;

import org.junit.jupiter.api.Test;

import io.github.lucasimi.zkring.collection.Hash;
import io.github.lucasimi.zkring.collection.HashRing;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HashRingTest {

    @Test
    public void testConsistentHashing() {
        Hash hashing = mock(Hash.class);

        Node node0 = new Node(UUID.randomUUID(), "testaddr0", 0);
        Node node2 = new Node(UUID.randomUUID(), "testaddr2", 2);
        Node node4 = new Node(UUID.randomUUID(), "testaddr4", 4);
        Node node6 = new Node(UUID.randomUUID(), "testaddr6", 6);

        when(hashing.hash(eq(new VNode(node0, 0)))).thenReturn(0);
        when(hashing.hash(eq(new VNode(node2, 0)))).thenReturn(2);
        when(hashing.hash(eq(new VNode(node4, 0)))).thenReturn(4);
        when(hashing.hash(eq(new VNode(node6, 0)))).thenReturn(6);

        when(hashing.hash(0)).thenReturn(0);
        when(hashing.hash(2)).thenReturn(2);
        when(hashing.hash(4)).thenReturn(4);
        when(hashing.hash(6)).thenReturn(6);
        when(hashing.hash(7)).thenReturn(7);

        HashRing hashRing = new HashRing(9, 1, hashing);
        hashRing.addAll(List.of(node0, node2, node4, node6));

        assertEquals(List.of(node2, node4, node6, node0), hashRing.get(0, 4));
        assertEquals(List.of(node2, node4, node6, node0), hashRing.get(1, 4));
        assertEquals(List.of(node4, node6, node0, node2), hashRing.get(2, 4));
        assertEquals(List.of(node0, node2, node4, node6), hashRing.get(7, 4));
    }

    @Test
    public void testSubscribeAndRebalancing() {
        Hash hashing = mock(Hash.class);

        Node node0 = new Node(UUID.randomUUID(), "testaddr0", 0);
        Node node2 = new Node(UUID.randomUUID(), "testaddr2", 2);
        Node node4 = new Node(UUID.randomUUID(), "testaddr4", 4);
        Node node6 = new Node(UUID.randomUUID(), "testaddr6", 6);

        when(hashing.hash(eq(new VNode(node0, 0)))).thenReturn(0);
        when(hashing.hash(eq(new VNode(node2, 0)))).thenReturn(2);
        when(hashing.hash(eq(new VNode(node4, 0)))).thenReturn(4);
        when(hashing.hash(eq(new VNode(node6, 0)))).thenReturn(6);

        when(hashing.hash(0)).thenReturn(0);
        when(hashing.hash(2)).thenReturn(2);
        when(hashing.hash(4)).thenReturn(4);
        when(hashing.hash(6)).thenReturn(6);
        when(hashing.hash(7)).thenReturn(7);

        HashRing hashRing = new HashRing(8, 1, hashing);
        hashRing.addAll(List.of(node0, node2, node4, node6));

        Node node0Get = hashRing.get(0);
        assertEquals(node2, node0Get);
        Node node2Get = hashRing.get(2);
        assertEquals(node4, node2Get);
        Node node4Get = hashRing.get(4);
        assertEquals(node6, node4Get);
        Node node6Get = hashRing.get(6);
        assertEquals(node0, node6Get);

        Node node7 = new Node(UUID.randomUUID(), "testaddr7", 7);
        when(hashing.hash(eq(new VNode(node7, 0)))).thenReturn(7);
        when(hashing.hash(7)).thenReturn(7);
        hashRing.addAll(List.of(node0, node2, node4, node6, node7));

        Node node0GetBis = hashRing.get(0);
        Node node2GetBis = hashRing.get(2);
        Node node4GetBis = hashRing.get(4);
        Node node6GetBis = hashRing.get(6);
        Node node7Get = hashRing.get(7);

        assertEquals(node2, node0GetBis);
        assertEquals(node4, node2GetBis);
        assertEquals(node6, node4GetBis);
        assertEquals(node7, node6GetBis);
        assertEquals(node0, node7Get);
    }

    @Test
    public void testReplicatedNodes() {
        Hash hashing = mock(Hash.class);

        Node node0 = new Node(UUID.randomUUID(), "testaddr0", 0);
        Node node1 = new Node(UUID.randomUUID(), "testaddr1", 1);
        Node node2 = new Node(UUID.randomUUID(), "testaddr2", 2);
        Node node3 = new Node(UUID.randomUUID(), "testaddr3", 3);

        for (int i = 0; i < 4; i++) {
            when(hashing.hash(eq(new VNode(node0, i)))).thenReturn(0 + 5 * i);
            when(hashing.hash(eq(new VNode(node1, i)))).thenReturn(1 + 5 * i);
            when(hashing.hash(eq(new VNode(node2, i)))).thenReturn(2 + 5 * i);
            when(hashing.hash(eq(new VNode(node3, i)))).thenReturn(3 + 5 * i);
        }

        when(hashing.hash(0)).thenReturn(0);
        when(hashing.hash(1)).thenReturn(1);
        when(hashing.hash(2)).thenReturn(2);
        when(hashing.hash(3)).thenReturn(3);

        HashRing hashRing = new HashRing(4, 4, hashing);
        hashRing.addAll(List.of(node0, node1, node2, node3));

        Node node0Get = hashRing.get(0);
        Node node1Get = hashRing.get(1);
        Node node2Get = hashRing.get(2);
        Node node3Get = hashRing.get(3);

        assertEquals(node2, node0Get);
        assertEquals(node3, node1Get);
        assertEquals(node0, node2Get);
        assertEquals(node1, node3Get);
    }

}